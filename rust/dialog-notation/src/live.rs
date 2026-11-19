//! A selection or a view kept current on a branch: the driver that lays
//! the rules the notation's queries read, subscribes to each query, polls
//! them, and hands back the answers or the view's patches.
//!
//! Nothing here evaluates a query. Each query a [`Selection`] or a
//! [`Plan`] holds is a Dialog-DB subscription, the rows of its parts by
//! [`Branch::subscribe`] and every read Dialog-DB folds by
//! [`Branch::subscribe_fold`]. Their rules are laid on the branch's
//! session overlay, where a subscription finds them and from which
//! nothing is committed. What is put together from the rows, a call
//! joined to the query it calls and a collection the lowering does not
//! fold, is put together by [`Selection::answer`] and the [`Maintainer`],
//! as when the rows are asked once.
//!
//! ```no_run
//! # use dialog_notation::live::{Live, Reader};
//! # use dialog_notation::{Plan, Selection};
//! # async fn example<E: Reader>(
//! #     branch: dialog_repository::Branch,
//! #     selection: Selection,
//! #     plan: Plan,
//! #     env: &E,
//! # ) -> Result<(), dialog_notation::live::LiveError> {
//! let live = Live::new(branch);
//! // Asked once.
//! let answers = live.select(&selection, env).await?;
//! // Kept current: each poll gives the answers again when a row moved.
//! let mut standing = live.standing(selection)?;
//! let first = standing.poll(env).await?;
//! // A view: each poll gives its patches.
//! let mut watched = live.watch(plan, None)?;
//! let patches = watched.poll(env).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;

use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSync, Holds};
use dialog_corpus::Value;
use dialog_corpus::tree::CorpusNode;
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::Resolve;
use dialog_query::error::EvaluationError;
use dialog_query::query::Output as _;
use dialog_query::{ConceptConclusion, ConceptQuery, FoldConclusion};
use dialog_repository::{Branch, Delta, FoldSubscription, RemoteSite, Subscription};
use dialog_search_tree::nodes;

use crate::select::Selection;
use crate::syntax::Syntax;
use crate::view::plan::{FoldedCount, Plan};
use crate::view::tree::{Change, Counts, Maintainer, Patch, Rendered, Row};
use crate::{Error, Query};

/// What a subscription of a branch reads through: the env every poll is
/// given, as [`Subscription::poll`] takes it.
pub trait Reader:
    Provider<BlobRead>
    + Provider<Get>
    + Provider<Put>
    + Provider<Resolve>
    + Provider<Identify>
    + Provider<ArchiveLoad>
    + Provider<nodes::Write<CorpusNode>>
    + Provider<nodes::Read<CorpusNode>>
    + Provider<nodes::Preload<CorpusNode>>
    + Provider<BlobLoad>
    + Provider<dialog_corpus::Preload>
    + Provider<dialog_corpus::Speculation>
    + Provider<Fork<RemoteSite, Resolve>>
    + Holds
    + ConditionalSync
    + 'static
{
}

impl<T> Reader for T where
    T: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<ArchiveLoad>
        + Provider<nodes::Write<CorpusNode>>
        + Provider<nodes::Read<CorpusNode>>
        + Provider<nodes::Preload<CorpusNode>>
        + Provider<BlobLoad>
        + Provider<dialog_corpus::Preload>
        + Provider<dialog_corpus::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + Holds
        + ConditionalSync
        + 'static
{
}

/// Why a selection or a view could not be kept.
#[derive(Debug, thiserror::Error)]
pub enum LiveError {
    /// The notation refused what it was given.
    #[error(transparent)]
    Notation(#[from] Error),
    /// Dialog-DB failed to evaluate a query.
    #[error(transparent)]
    Evaluation(#[from] EvaluationError),
    /// A rule could not be laid on the branch's overlay.
    #[error("a rule could not be laid on the branch: {0}")]
    Rule(String),
}

/// A branch the notation's selections and views are kept on.
#[derive(Clone)]
pub struct Live {
    branch: Branch,
}

impl Live {
    /// Keep selections and views on `branch`.
    pub fn new(branch: Branch) -> Live {
        Live { branch }
    }

    /// The branch.
    pub fn branch(&self) -> &Branch {
        &self.branch
    }

    fn lay<'r>(
        &self,
        rules: impl IntoIterator<Item = &'r dialog_query::DeductiveRule>,
    ) -> Result<(), LiveError> {
        for rule in rules {
            self.branch
                .overlay()
                .assert(rule.clone())
                .map_err(|error| LiveError::Rule(error.to_string()))?;
        }
        Ok(())
    }

    /// The answers of `selection` as the branch stands, asked once: each
    /// query with its rule laid over the branch for it alone, so nothing
    /// is left on the overlay.
    pub async fn select<E: Reader>(
        &self,
        selection: &Selection,
        env: &E,
    ) -> Result<Vec<Syntax>, LiveError> {
        let mut results = Vec::new();
        for (query, read) in selection.queries.iter().zip(selection.read()) {
            if !read {
                results.push(Vec::new());
                continue;
            }
            results.push(self.asked(query, env).await?);
        }
        let mut folded = Vec::new();
        for fold in &selection.folds {
            let mut layer = self.branch.query();
            for rule in &fold.rules {
                layer = layer.with(rule.clone());
            }
            let rows = layer
                .select(fold.fold.clone())
                .perform(env)
                .try_vec()
                .await?;
            folded.push(rows.iter().map(|row| fold.values(row)).collect());
        }
        Ok(selection.answer(&results, &folded)?)
    }

    /// The rows of `query` as the branch stands, asked once.
    async fn asked<E: Reader>(
        &self,
        query: &Query,
        env: &E,
    ) -> Result<Vec<BTreeMap<String, Value>>, LiveError> {
        let rows = self.conclusions(query, env).await?;
        Ok(rows
            .iter()
            .map(|row| query.values(row))
            .collect::<Result<Vec<_>, _>>()?)
    }

    async fn conclusions<E: Reader>(
        &self,
        query: &Query,
        env: &E,
    ) -> Result<Vec<ConceptConclusion>, LiveError> {
        let mut layer = self.branch.query();
        if let Some(rule) = &query.rule {
            layer = layer.with(rule.clone());
        }
        Ok(layer
            .select(query.select.clone())
            .perform(env)
            .try_vec()
            .await?)
    }

    /// The view `plan` rendered once for `entity`, every layer asked as
    /// the branch stands.
    pub async fn render<E: Reader>(
        &self,
        plan: &Plan,
        entity: Option<&Value>,
        env: &E,
    ) -> Result<Rendered, LiveError> {
        let mut rows = Vec::new();
        for layer in 0..plan.layers.len() {
            if plan.layers[layer].conditions.is_empty() {
                rows.push(Vec::new());
                continue;
            }
            let query = plan.query(layer, entity)?;
            let found = self.conclusions(&query, env).await?;
            rows.push(
                found
                    .iter()
                    .map(|row| plan.row(layer, &query, row, entity))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(crate::view::tree::render(plan, entity, &rows))
    }

    /// `selection` kept current: its rules laid, a subscription of each
    /// query whose rows [`Selection::answer`] reads, and a standing fold
    /// of each of [`Selection::folds`].
    pub fn standing(&self, selection: Selection) -> Result<Standing, LiveError> {
        let read = selection.read();
        let mut feeds = Vec::new();
        for (query, read) in selection.queries.iter().zip(read) {
            feeds.push(if read {
                self.lay(&query.rule)?;
                Some(self.branch.subscribe(query.select.clone()))
            } else {
                None
            });
        }
        let mut folds = Vec::new();
        for folded in &selection.folds {
            self.lay(&folded.rules)?;
            folds.push(self.branch.subscribe_fold(folded.fold.clone()));
        }
        Ok(Standing {
            rows: vec![Vec::new(); feeds.len()],
            folded: vec![Vec::new(); folds.len()],
            selection,
            feeds,
            folds,
            polled: false,
        })
    }

    /// The view `plan` kept current for `entity`, the entity it is
    /// rendered for where it answers of one: its layers' rules laid and a
    /// subscription of each layer's query, but for a count Dialog-DB
    /// keeps, [`Plan::folds`], whose layer is a standing fold instead.
    /// The [`Maintainer`] is given the fold's counts and no row of that
    /// layer.
    pub fn watch(&self, plan: Plan, entity: Option<Value>) -> Result<Watched, LiveError> {
        let mut maintainer = Maintainer::new(plan.clone(), entity.clone());
        let mut folds: BTreeMap<usize, FoldedCount> = BTreeMap::new();
        for folded in plan.folds() {
            let layer = plan.elements[plan.counts[folded.count].place].layer;
            folds.insert(layer, folded);
        }
        let mut layers = Vec::new();
        for layer in 0..plan.layers.len() {
            if plan.layers[layer].conditions.is_empty() {
                layers.push(None);
                continue;
            }
            if let Some(folded) = folds.remove(&layer) {
                self.lay(&folded.folded.rules)?;
                let feed = self.branch.subscribe_fold(folded.folded.fold.clone());
                maintainer.given(plan.counts[folded.count].place);
                layers.push(Some(Feed::Counted(Box::new((folded, feed)))));
                continue;
            }
            let query = plan.query(layer, entity.as_ref())?;
            self.lay(&query.rule)?;
            let feed = self.branch.subscribe(query.select.clone());
            layers.push(Some(Feed::Rows(Box::new((query, feed)))));
        }
        Ok(Watched {
            delivered: vec![0; layers.len()],
            maintainer,
            plan,
            entity,
            layers,
            polled: false,
        })
    }
}

/// Apply what a subscription reported to the rows kept of it.
fn applied<C: PartialEq>(rows: &mut Vec<C>, delta: Delta<C>) {
    for gone in delta.retracted {
        if let Some(at) = rows.iter().position(|row| *row == gone) {
            rows.swap_remove(at);
        }
    }
    rows.extend(delta.asserted);
}

/// A selection kept current on a branch. Made by [`Live::standing`].
pub struct Standing {
    selection: Selection,
    /// A subscription of each query the answers read, by its index in
    /// [`Selection::queries`].
    feeds: Vec<Option<Subscription<ConceptQuery>>>,
    /// The rows each one has reported.
    rows: Vec<Vec<ConceptConclusion>>,
    /// A standing fold of each of [`Selection::folds`].
    folds: Vec<FoldSubscription<ConceptQuery, ConceptQuery>>,
    /// The rows each one has reported.
    folded: Vec<Vec<FoldConclusion>>,
    polled: bool,
}

impl Standing {
    /// The selection kept.
    pub fn selection(&self) -> &Selection {
        &self.selection
    }

    /// The subscription of each query the answers read, by its index in
    /// [`Selection::queries`]; `None` for one only a fold reads.
    pub fn feeds(&self) -> &[Option<Subscription<ConceptQuery>>] {
        &self.feeds
    }

    /// The standing fold of each of [`Selection::folds`].
    pub fn folds(&self) -> &[FoldSubscription<ConceptQuery, ConceptQuery>] {
        &self.folds
    }

    /// Full evaluations of every query kept so far, the inner queries of
    /// each fold among them.
    pub fn recomputes(&self) -> usize {
        self.feeds
            .iter()
            .flatten()
            .map(Subscription::recomputes)
            .sum::<usize>()
            + self
                .folds
                .iter()
                .map(FoldSubscription::recomputes)
                .sum::<usize>()
    }

    /// Poll every subscription. `Ok(None)` when none reported a change
    /// since the last poll; otherwise the answers, put together from the
    /// rows every subscription has reported. The first poll gives them.
    pub async fn poll<E: Reader>(&mut self, env: &E) -> Result<Option<Vec<Syntax>>, LiveError> {
        let mut moved = !self.polled;
        for (feed, rows) in self.feeds.iter_mut().zip(&mut self.rows) {
            if let Some(feed) = feed
                && let Some(delta) = feed.poll(env).await?
            {
                moved = true;
                applied(rows, delta);
            }
        }
        for (fold, rows) in self.folds.iter_mut().zip(&mut self.folded) {
            if let Some(delta) = fold.poll(env).await? {
                moved = true;
                applied(rows, delta);
            }
        }
        self.polled = true;
        if !moved {
            return Ok(None);
        }
        let results = self
            .selection
            .queries
            .iter()
            .zip(&self.rows)
            .map(|(query, rows)| {
                rows.iter()
                    .map(|row| query.values(row))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let folded = self
            .selection
            .folds
            .iter()
            .zip(&self.folded)
            .map(|(fold, rows)| rows.iter().map(|row| fold.values(row)).collect())
            .collect::<Vec<Vec<BTreeMap<String, Value>>>>();
        Ok(Some(self.selection.answer(&results, &folded)?))
    }
}

/// What keeps a layer of a view.
enum Feed {
    /// The layer's query and its subscription.
    Rows(Box<(Query, Subscription<ConceptQuery>)>),
    /// A count Dialog-DB keeps, and its standing fold.
    Counted(Box<(FoldedCount, FoldSubscription<ConceptQuery, ConceptQuery>)>),
}

/// A view kept current on a branch. Made by [`Live::watch`].
pub struct Watched {
    plan: Plan,
    entity: Option<Value>,
    /// What keeps each layer; `None` for a layer with no condition.
    layers: Vec<Option<Feed>>,
    maintainer: Maintainer,
    /// The rows each layer delivered at the last poll, come and gone.
    delivered: Vec<usize>,
    polled: bool,
}

impl Watched {
    /// The plan watched.
    pub fn plan(&self) -> &Plan {
        &self.plan
    }

    /// The tree as the patches so far leave it.
    pub fn tree(&self) -> Rendered {
        self.maintainer.tree()
    }

    /// For each layer with a query, how many times its subscription
    /// evaluated it whole: a fold's, both of its queries.
    pub fn recomputes(&self) -> Vec<usize> {
        self.layers
            .iter()
            .flatten()
            .map(|feed| match feed {
                Feed::Rows(rows) => rows.1.recomputes(),
                Feed::Counted(counted) => counted.1.recomputes(),
            })
            .collect()
    }

    /// The maintainer of the tree, which holds the rows each layer gave
    /// it.
    pub fn maintainer(&self) -> &Maintainer {
        &self.maintainer
    }

    /// The layers whose count Dialog-DB keeps by a fold, by their index
    /// in [`Plan::layers`].
    pub fn folded(&self) -> Vec<usize> {
        self.layers
            .iter()
            .enumerate()
            .filter(|(_, feed)| matches!(feed, Some(Feed::Counted(_))))
            .map(|(layer, _)| layer)
            .collect()
    }

    /// How many rows each layer delivered at the last poll, come and
    /// gone, by its index in [`Plan::layers`].
    pub fn delivered(&self) -> &[usize] {
        &self.delivered
    }

    /// Poll every layer and give the patches what came and went makes.
    /// The first poll gives first what stands with no row, each root
    /// inserted.
    pub async fn poll<E: Reader>(&mut self, env: &E) -> Result<Vec<Patch>, LiveError> {
        let mut changes = Vec::new();
        let mut counts = Vec::new();
        self.delivered.iter_mut().for_each(|rows| *rows = 0);
        for (layer, at) in self.layers.iter_mut().enumerate() {
            let (query, feed) = match at {
                None => continue,
                Some(Feed::Counted(counted)) => {
                    let (folded, feed) = &mut **counted;
                    let Some(delta) = feed.poll(env).await? else {
                        continue;
                    };
                    self.delivered[layer] = delta.asserted.len() + delta.retracted.len();
                    let place = self.plan.counts[folded.count].place;
                    counts.push(Counts {
                        place,
                        arrived: delta
                            .asserted
                            .iter()
                            .map(|row| tallied(folded, row))
                            .collect::<Result<_, _>>()?,
                        left: delta
                            .retracted
                            .iter()
                            .map(|row| tallied(folded, row))
                            .collect::<Result<_, _>>()?,
                    });
                    continue;
                }
                Some(Feed::Rows(rows)) => {
                    let (query, feed) = &mut **rows;
                    (query, feed)
                }
            };
            let Some(delta) = feed.poll(env).await? else {
                continue;
            };
            self.delivered[layer] = delta.asserted.len() + delta.retracted.len();
            let row =
                |row: &ConceptConclusion| self.plan.row(layer, query, row, self.entity.as_ref());
            changes.push(Change {
                layer,
                arrived: delta.asserted.iter().map(row).collect::<Result<_, _>>()?,
                left: delta.retracted.iter().map(row).collect::<Result<_, _>>()?,
            });
        }
        let mut patches = Vec::new();
        if !self.polled {
            for (index, instance) in self.maintainer.tree().roots.into_iter().enumerate() {
                patches.push(Patch::Insert {
                    parent: Vec::new(),
                    slot: 0,
                    index,
                    instance,
                });
            }
            self.polled = true;
        }
        patches.extend(self.maintainer.apply_counted(&changes, &counts));
        Ok(patches)
    }
}

/// A folded row of a count as the host instance it names and the count
/// there.
fn tallied(folded: &FoldedCount, row: &FoldConclusion) -> Result<(Row, u64), LiveError> {
    let values = folded.folded.values(row);
    let host: Row = folded
        .shared
        .iter()
        .filter_map(|name| Some((name.clone(), values.get(name)?.clone())))
        .collect();
    let number = match values.get(&folded.field) {
        Some(Value::UnsignedInt(number)) => u64::try_from(*number).map_err(|_| {
            LiveError::Notation(Error::Refused(format!("a count of {number} is too many")))
        })?,
        Some(other) => {
            return Err(LiveError::Notation(Error::Refused(format!(
                "a fold counted {other:?}, which is no count"
            ))));
        }
        None => 0,
    };
    Ok((host, number))
}
