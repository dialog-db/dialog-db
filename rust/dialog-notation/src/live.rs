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
//! fold, is put together by [`Selection::answer`] and the maintainer of a view's tree,
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSync, Holds};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Entity, Selector, Value};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::Resolve;
use dialog_query::error::EvaluationError;
use dialog_query::query::Output as _;
use dialog_query::{ConceptConclusion, ConceptQuery, FoldConclusion};
use dialog_repository::{Branch, Delta, Ephemeral, FoldSubscription, RemoteSite, Subscription};
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
///
/// What a selection kept standing or a view watched needs of a rule is
/// laid on the branch's session overlay, and taken off when the last
/// selection or view that holds it is dropped, whichever `Live` made it.
/// Who holds a rule is written on the overlay itself, so every `Live` on
/// the branch sees one count. A rule that was on the overlay before the
/// notation laid it is used and never taken off.
#[derive(Clone)]
pub struct Live {
    branch: Branch,
}

/// The relation a selection or a view holding a rule on the overlay is
/// written in: the rule's entity, and the holder's own name.
const HOLDER: &str = "dialog.notation.live/holder";

/// The relation that says the notation laid a rule on the overlay, and
/// may take it off: the rule's entity, and `true`.
const LAID: &str = "dialog.notation.live/laid";

/// The relation that says which site a rule the notation laid derives a
/// report relation for: the rule's entity, and the relation and the site.
const DERIVES: &str = "dialog.notation.live/derives";

/// The names of the holders in this process, each once. The overlay is
/// never synced, so a holder is named in this process alone.
static HOLDERS: AtomicU64 = AtomicU64::new(0);

/// What laying a rule and letting it go read and then write, kept one at
/// a time in this process.
static LAYING: Mutex<()> = Mutex::new(());

/// A fact of the notation's own on the overlay.
struct Mark {
    the: &'static str,
    of: Entity,
    is: Value,
}

impl Mark {
    fn relation(&self) -> Option<dialog_corpus::Relation> {
        self.the.parse().ok()
    }
}

impl dialog_corpus::Statement for Mark {
    fn assert(self, update: &mut impl dialog_corpus::Update) {
        if let Some(the) = self.relation() {
            update.associate(the, self.of, self.is, dialog_corpus::Cardinality::Many);
        }
    }

    fn retract(self, update: &mut impl dialog_corpus::Update) {
        if let Some(the) = self.relation() {
            update.dissociate(the, self.of, self.is);
        }
    }
}

/// The facts of the overlay about `entity`, a rule's: whether the rule
/// itself is there, whether the notation laid it, and how many hold it.
fn standing(overlay: &Ephemeral, entity: &Entity) -> (bool, bool, usize) {
    let facts = overlay.scan(&Selector::new().of(entity.clone()));
    let rule = facts
        .iter()
        .any(|fact| ![HOLDER, LAID, DERIVES].contains(&fact.the.as_str()));
    let laid = facts.iter().any(|fact| fact.the.as_str() == LAID);
    let holders = facts
        .iter()
        .filter(|fact| fact.the.as_str() == HOLDER)
        .count();
    (rule, laid, holders)
}

/// The rules a selection kept standing or a view watched holds on the
/// overlay: its holds are let go when it is dropped, and a rule the
/// notation laid and no one holds is taken off.
struct Lease {
    overlay: Ephemeral,
    /// The holder's name, written beside each rule it holds.
    holder: Value,
    /// The rules held, by their entities, each once.
    held: Vec<Entity>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let _laying = LAYING.lock().unwrap_or_else(PoisonError::into_inner);
        let mut gone: Vec<Entity> = Vec::new();
        for entity in &self.held {
            // A holder is a fact the overlay holds, so retracting it
            // removes it and hides nothing. Drop has no one to tell of a
            // failure: the rule would stay laid, as it was before a count
            // was kept.
            let _ = self.overlay.retract(Mark {
                the: HOLDER,
                of: entity.clone(),
                is: self.holder.clone(),
            });
            if let (_, true, 0) = standing(&self.overlay, entity) {
                gone.push(entity.clone());
            }
        }
        if !gone.is_empty() {
            // Dropped outright, so nothing beneath the overlay is hidden.
            self.overlay
                .retain_entities(|entity| !gone.contains(entity));
        }
    }
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

    /// Lay `rules` on the overlay, and hold them in `lease`: a rule that
    /// is not there is laid and marked as the notation's, and each is
    /// held by a fact beside it naming the lease.
    ///
    /// A rule that derives a report relation another site's rule on the
    /// overlay derives too, laid by another document, is refused: the two
    /// would answer as one.
    fn lay<'r>(
        &self,
        lease: &mut Lease,
        rules: impl IntoIterator<Item = &'r dialog_query::DeductiveRule>,
        derives: Option<&(String, String)>,
    ) -> Result<(), LiveError> {
        let overlay = self.branch.overlay();
        let _laying = LAYING.lock().unwrap_or_else(PoisonError::into_inner);
        let failed = |error: dialog_corpus::DialogCorpusError| LiveError::Rule(error.to_string());
        let claim = derives.map(|(relation, site)| format!("{relation} {site}"));
        if let (Some((relation, site)), Ok(the)) = (derives, DERIVES.parse()) {
            let other = overlay
                .scan(&Selector::new().the(the))
                .into_iter()
                .find_map(|fact| match fact.is {
                    Value::String(said) => said
                        .strip_prefix(&format!("{relation} "))
                        .filter(|other| *other != site)
                        .map(str::to_string),
                    _ => None,
                });
            if let Some(other) = other {
                return Err(LiveError::Notation(Error::Clash(
                    relation.clone(),
                    other,
                    site.clone(),
                )));
            }
        }
        for rule in rules {
            let entity = rule.this();
            if lease.held.contains(&entity) {
                continue;
            }
            let (there, _, _) = standing(overlay, &entity);
            if !there {
                overlay.assert(rule.clone()).map_err(failed)?;
                overlay
                    .assert(Mark {
                        the: LAID,
                        of: entity.clone(),
                        is: Value::Boolean(true),
                    })
                    .map_err(failed)?;
                if let Some(claim) = &claim {
                    overlay
                        .assert(Mark {
                            the: DERIVES,
                            of: entity.clone(),
                            is: Value::String(claim.clone()),
                        })
                        .map_err(failed)?;
                }
            }
            overlay
                .assert(Mark {
                    the: HOLDER,
                    of: entity.clone(),
                    is: lease.holder.clone(),
                })
                .map_err(failed)?;
            lease.held.push(entity);
        }
        Ok(())
    }

    /// A lease of nothing yet, on this branch's overlay.
    fn lease(&self) -> Lease {
        let holder = HOLDERS.fetch_add(1, Ordering::Relaxed);
        Lease {
            overlay: self.branch.overlay().clone(),
            holder: Value::String(format!("holder-{holder}")),
            held: Vec::new(),
        }
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
        let mut lease = self.lease();
        let mut feeds = Vec::new();
        for (query, read) in selection.queries.iter().zip(read) {
            feeds.push(if read {
                self.lay(&mut lease, &query.rule, query.derives.as_ref())?;
                Some(self.branch.subscribe(query.select.clone()))
            } else {
                None
            });
        }
        let mut folds = Vec::new();
        for folded in &selection.folds {
            self.lay(&mut lease, &folded.rules, None)?;
            folds.push(self.branch.subscribe_fold(folded.fold.clone()));
        }
        Ok(Standing {
            rows: vec![Vec::new(); feeds.len()],
            folded: vec![Vec::new(); folds.len()],
            selection,
            feeds,
            folds,
            moved: true,
            #[cfg(feature = "inspect")]
            failing: false,
            _lease: lease,
        })
    }

    /// The view `plan` kept current for `entity`, the entity it is
    /// rendered for where it answers of one: its layers' rules laid and a
    /// subscription of each layer's query, but for a count Dialog-DB
    /// keeps, [`Plan::folds`], whose layer is a standing fold instead.
    /// The tree's maintainer is given the fold's counts and no row of
    /// that layer.
    pub fn watch(&self, plan: Plan, entity: Option<Value>) -> Result<Watched, LiveError> {
        let mut maintainer = Maintainer::new(plan.clone(), entity.clone());
        let mut lease = self.lease();
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
                self.lay(&mut lease, &folded.folded.rules, None)?;
                let feed = self.branch.subscribe_fold(folded.folded.fold.clone());
                maintainer.given(plan.counts[folded.count].place);
                layers.push(Some(Feed::Counted(Box::new((folded, feed)))));
                continue;
            }
            let query = plan.query(layer, entity.as_ref())?;
            self.lay(&mut lease, &query.rule, query.derives.as_ref())?;
            let feed = self.branch.subscribe(query.select.clone());
            layers.push(Some(Feed::Rows(Box::new((query, feed)))));
        }
        Ok(Watched {
            delivered: vec![0; layers.len()],
            taken: (0..layers.len()).map(|_| Taken::default()).collect(),
            maintainer,
            plan,
            entity,
            layers,
            polled: false,
            _lease: lease,
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

/// `later`, a delta taken after `into`, added to it: a row one brought
/// and the other took away is in neither.
fn merged<C: PartialEq>(into: &mut Delta<C>, later: Delta<C>) {
    for gone in later.retracted {
        match into.asserted.iter().position(|row| *row == gone) {
            Some(at) => {
                into.asserted.swap_remove(at);
            }
            None => into.retracted.push(gone),
        }
    }
    for come in later.asserted {
        match into.retracted.iter().position(|row| *row == come) {
            Some(at) => {
                into.retracted.swap_remove(at);
            }
            None => into.asserted.push(come),
        }
    }
}

/// `delta` kept in `slot` until it is applied, after what is kept there.
fn kept<C: PartialEq>(slot: &mut Option<Delta<C>>, delta: Delta<C>) {
    match slot {
        Some(into) => merged(into, delta),
        None => *slot = Some(delta),
    }
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
    /// Whether rows moved since the answers were last given: kept across
    /// a poll that failed, so the next one gives them.
    moved: bool,
    /// Whether the next poll's answers fail to be put together, for a
    /// test that a failure there loses nothing.
    #[cfg(feature = "inspect")]
    failing: bool,
    /// The rules it holds on the overlay.
    _lease: Lease,
}

impl Standing {
    /// The selection kept.
    pub fn selection(&self) -> &Selection {
        &self.selection
    }

    /// Make the next poll fail where its answers are put together, after
    /// the rows moved: what a test needs to show that nothing is lost.
    #[cfg(feature = "inspect")]
    pub fn fail_next_answer(&mut self) {
        self.failing = true;
    }

    /// The subscription of each query the answers read, by its index in
    /// [`Selection::queries`]; `None` for one only a fold reads.
    #[cfg(feature = "inspect")]
    pub fn feeds(&self) -> &[Option<Subscription<ConceptQuery>>] {
        &self.feeds
    }

    /// The standing fold of each of [`Selection::folds`].
    #[cfg(feature = "inspect")]
    pub fn folds(&self) -> &[FoldSubscription<ConceptQuery, ConceptQuery>] {
        &self.folds
    }

    /// Full evaluations of every query kept so far, the inner queries of
    /// each fold among them.
    #[cfg(feature = "inspect")]
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
    /// A poll that fails gives nothing, and the next that does not gives
    /// the answers, whatever moved since they were last given.
    pub async fn poll<E: Reader>(&mut self, env: &E) -> Result<Option<Vec<Syntax>>, LiveError> {
        // A subscription's rows are kept as it reports them, and whether
        // any moved is kept until the answers are given: a poll that
        // fails part way leaves both for the next.
        for (feed, rows) in self.feeds.iter_mut().zip(&mut self.rows) {
            if let Some(feed) = feed
                && let Some(delta) = feed.poll(env).await?
            {
                self.moved = true;
                applied(rows, delta);
            }
        }
        for (fold, rows) in self.folds.iter_mut().zip(&mut self.folded) {
            if let Some(delta) = fold.poll(env).await? {
                self.moved = true;
                applied(rows, delta);
            }
        }
        if !self.moved {
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
        #[cfg(feature = "inspect")]
        if std::mem::take(&mut self.failing) {
            return Err(LiveError::Notation(Error::Refused(
                "the answers were made to fail".into(),
            )));
        }
        let answers = self.selection.answer(&results, &folded)?;
        self.moved = false;
        Ok(Some(answers))
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
    /// What each layer's subscription reported and the tree has not
    /// been given yet: a poll that fails part way keeps what it took
    /// here, and the next gives it.
    taken: Vec<Taken>,
    polled: bool,
    /// The rules it holds on the overlay.
    _lease: Lease,
}

/// What a layer's subscription reported, by its kind.
#[derive(Default)]
struct Taken {
    rows: Option<Delta<ConceptConclusion>>,
    counted: Option<Delta<FoldConclusion>>,
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
    #[cfg(feature = "inspect")]
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
    #[cfg(feature = "inspect")]
    pub fn maintainer(&self) -> &Maintainer {
        &self.maintainer
    }

    /// The layers whose count Dialog-DB keeps by a fold, by their index
    /// in [`Plan::layers`].
    #[cfg(feature = "inspect")]
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
    #[cfg(feature = "inspect")]
    pub fn delivered(&self) -> &[usize] {
        &self.delivered
    }

    /// Poll every layer and give the patches what came and went makes.
    /// The first poll gives first what stands with no row, each root
    /// inserted.
    ///
    /// A poll that fails gives nothing and loses nothing: what the layers
    /// reported before it failed is given by the next poll that does not.
    /// A row the plan cannot read fails every poll after it, and the view
    /// is watched again.
    pub async fn poll<E: Reader>(&mut self, env: &E) -> Result<Vec<Patch>, LiveError> {
        // Take what every layer reports first, keeping it: a layer that
        // fails leaves what the others took for the next poll.
        for (at, taken) in self.layers.iter_mut().zip(&mut self.taken) {
            match at {
                None => {}
                Some(Feed::Counted(counted)) => {
                    if let Some(delta) = counted.1.poll(env).await? {
                        kept(&mut taken.counted, delta);
                    }
                }
                Some(Feed::Rows(rows)) => {
                    if let Some(delta) = rows.1.poll(env).await? {
                        kept(&mut taken.rows, delta);
                    }
                }
            }
        }
        // Then read it all as rows, and only once every row is read is
        // anything let go of.
        let mut changes = Vec::new();
        let mut counts = Vec::new();
        let mut delivered = vec![0; self.layers.len()];
        for (layer, (at, taken)) in self.layers.iter().zip(&self.taken).enumerate() {
            match (at, &taken.rows, &taken.counted) {
                (Some(Feed::Counted(counted)), _, Some(delta)) => {
                    let folded = &counted.0;
                    delivered[layer] = delta.asserted.len() + delta.retracted.len();
                    counts.push(Counts {
                        place: self.plan.counts[folded.count].place,
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
                }
                (Some(Feed::Rows(rows)), Some(delta), _) => {
                    let query = &rows.0;
                    delivered[layer] = delta.asserted.len() + delta.retracted.len();
                    let row = |row: &ConceptConclusion| {
                        self.plan.row(layer, query, row, self.entity.as_ref())
                    };
                    changes.push(Change {
                        layer,
                        arrived: delta.asserted.iter().map(row).collect::<Result<_, _>>()?,
                        left: delta.retracted.iter().map(row).collect::<Result<_, _>>()?,
                    });
                }
                _ => {}
            }
        }
        self.taken
            .iter_mut()
            .for_each(|taken| *taken = Taken::default());
        self.delivered = delivered;
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
