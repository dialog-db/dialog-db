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
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Entity, Value};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::Resolve;
use dialog_query::concept::descriptor::ConceptStatement;
use dialog_query::error::EvaluationError;
use dialog_query::query::Output as _;
use dialog_query::{Any, ConceptConclusion, ConceptQuery, DeductiveRule, FoldConclusion, Term};
use dialog_repository::{Branch, Delta, FoldSubscription, RemoteSite, Subscription};
use dialog_search_tree::nodes;

use crate::analyze::Analyzer;
use crate::check::{Checked, check};
use crate::definition::{self, Binding, Definition, Kind as DefinitionKind, Want};
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

/// What a branch keeps of the notation's definitions.
#[derive(Clone, Debug, PartialEq)]
pub struct Kept {
    /// Every definition, as kept.
    pub definitions: Vec<Definition>,
    /// Every namespace's bindings.
    pub bindings: Vec<Binding>,
}

/// What a document writes to a branch for its definitions and names.
#[derive(Debug, Default)]
pub struct Written {
    /// Asserted: each definition, its source map and its two bindings
    /// ([`definition::statements`]), and each petname bound.
    pub asserted: Vec<ConceptStatement>,
    /// Retracted: the binding of each name taken away. What it was bound
    /// to stays.
    pub retracted: Vec<ConceptStatement>,
    /// The rule `Name` is derived by, which a branch keeps once however
    /// often it is written; none where the document defines nothing.
    pub rule: Option<DeductiveRule>,
}

/// What `checked` writes to a branch beside its steps.
pub fn written(checked: &Checked) -> Result<Written, Error> {
    let mut written = Written::default();
    for made in &checked.defined {
        written.asserted.extend(definition::statements(made)?);
    }
    for binding in &checked.named {
        written
            .asserted
            .push(definition::binding_statement(binding)?);
    }
    for binding in &checked.unnamed {
        written
            .retracted
            .push(definition::unbinding_statement(binding)?);
    }
    if !checked.defined.is_empty() {
        written.rule = Some(name_rule()?);
    }
    Ok(written)
}

/// The rule `Name` is derived by ([`definition::NAME_RULE`]), compiled.
pub fn name_rule() -> Result<DeductiveRule, Error> {
    let trees = crate::parse(definition::NAME_RULE)?;
    let analysis = Analyzer::new()?.analyze(&trees);
    if let Some(error) = analysis
        .refusals()
        .find_map(|refused| refused.refusal.clone())
    {
        return Err(error);
    }
    for tree in &analysis.analyzed {
        for step in crate::compile(tree)? {
            if let crate::Step::Deduce(rule) = step {
                return Ok(*rule);
            }
        }
    }
    Err(Error::Refused(
        "the rule of Name compiles to no rule".into(),
    ))
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
    /// Analysis asked for more after this many rounds of reading.
    #[error("analysis still asked for definitions after {0} rounds of reading them")]
    Rounds(usize),
}

/// The most rounds of reading [`Live::analyze`] runs.
pub const ROUNDS: usize = 16;

/// How a document was analyzed from a branch: how many rounds it took,
/// and how many definitions and bindings were read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rounds {
    /// Analyses run.
    pub rounds: usize,
    /// Definitions read.
    pub definitions: usize,
    /// Bindings read.
    pub bindings: usize,
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

    /// The authority the branch is opened under: the DID a peer is built
    /// to write as (`PeerBuilder::profile`, the home by default, a
    /// persona's for a device of that persona), which every revision it
    /// commits names as `dialog.revision/authority` and every query of
    /// the branch is given on `db:session` as `dialog.session/profile`.
    /// It is the local namespace a caller that acts as that authority
    /// gives [`Analyzer::with_namespace`].
    pub async fn authority<E: Reader>(&self, env: &E) -> Result<Entity, LiveError> {
        let rows = self
            .read(
                &[("profile", "dialog.session/profile", "entity:")],
                &[],
                env,
            )
            .await?;
        rows.into_iter()
            .find_map(|(this, values)| match values.get("profile") {
                Some(Value::Entity(profile)) if this.as_str() == "db:session" => {
                    Some(profile.clone())
                }
                _ => None,
            })
            .ok_or_else(|| {
                LiveError::Notation(Error::Read("the branch is read under no authority".into()))
            })
    }

    /// What the branch keeps of the notation's definitions: every
    /// definition and every namespace's bindings.
    ///
    /// Which namespace is mine is not among them: it is the authority the
    /// branch is opened under ([`authority`](Self::authority)), which the
    /// caller gives the analyzer.
    pub async fn kept<E: Reader>(&self, env: &E) -> Result<Kept, LiveError> {
        let mut definitions = Vec::new();
        for (this, values) in self
            .read(
                &[
                    ("is", definition::attribute::IS, "record:"),
                    ("kind", definition::attribute::KIND, "text:"),
                ],
                &[],
                env,
            )
            .await?
        {
            let (Some(form), Some(Value::String(kind))) = (values.get("is"), values.get("kind"))
            else {
                continue;
            };
            let kind: DefinitionKind = kind.parse()?;
            definitions.push(Definition {
                entity: this,
                kind,
                form: Syntax::try_from(form)?,
            });
        }
        let mut bindings = Vec::new();
        for (_, values) in self
            .read(
                &[
                    (
                        "namespace",
                        definition::attribute::BINDING_NAMESPACE,
                        "entity:",
                    ),
                    ("key", definition::attribute::BINDING_KEY, "text:"),
                    (
                        "referent",
                        definition::attribute::BINDING_REFERENT,
                        "entity:",
                    ),
                ],
                &[],
                env,
            )
            .await?
        {
            if let (
                Some(Value::Entity(namespace)),
                Some(Value::String(key)),
                Some(Value::Entity(referent)),
            ) = (
                values.get("namespace"),
                values.get("key"),
                values.get("referent"),
            ) {
                bindings.push(Binding {
                    namespace: namespace.clone(),
                    key: key.clone(),
                    referent: referent.clone(),
                });
            }
        }
        Ok(Kept {
            definitions,
            bindings,
        })
    }

    /// Analyze `text` with the definitions it reaches read from the
    /// branch: in rounds, each analyzing it with what the branch gave so
    /// far and reading what that asked for and was not given (a name's
    /// binding in mine or where a petname points, a name's signatures,
    /// a definition), until nothing more is asked or [`ROUNDS`] have
    /// run. Every read is an ordinary query of the branch, so it reads
    /// every layer it has, and replicates what it reads where the
    /// branch fetches on demand. What `analyzer` holds of bindings is
    /// read again for each document; a definition, its own content, is
    /// kept once read.
    pub async fn analyze<E: Reader>(
        &self,
        analyzer: &mut Analyzer,
        text: &str,
        env: &E,
    ) -> Result<(Checked, Rounds), LiveError> {
        analyzer.forget();
        if let Ok(trees) = crate::parse(text) {
            analyzer.ask_for(&trees);
        }
        let mut rounds = Rounds::default();
        while rounds.rounds < ROUNDS {
            rounds.rounds += 1;
            let mut trial = analyzer.clone();
            let checked = check(&mut trial, text);
            let wants = trial.wanted();
            if wants.is_empty() {
                *analyzer = trial;
                return Ok((checked, rounds));
            }
            let (definitions, bindings, answered) = self.answer(&wants, &mut rounds, env).await?;
            // Found nothing, the trial already read what is there: as if
            // none of it were bound, which it is not.
            let nothing = definitions.is_empty() && bindings.is_empty();
            analyzer.answer(answered.clone(), definitions, bindings)?;
            if nothing {
                trial.answer(answered, Vec::new(), Vec::new())?;
                *analyzer = trial;
                return Ok((checked, rounds));
            }
        }
        Err(LiveError::Rounds(ROUNDS))
    }

    /// Read what `wants` ask for. A binding to a name's namespace brings
    /// that namespace's signatures, and a signature its definition, so a
    /// name takes one round.
    async fn answer<E: Reader>(
        &self,
        wants: &[Want],
        rounds: &mut Rounds,
        env: &E,
    ) -> Result<(Vec<Definition>, Vec<Binding>, Vec<Want>), LiveError> {
        let mut definitions = Vec::new();
        let mut bindings = Vec::new();
        let mut answered = Vec::new();
        let mut pending: Vec<Want> = wants.to_vec();
        while let Some(want) = pending.pop() {
            if answered.contains(&want) {
                continue;
            }
            match &want {
                Want::Definition(entity) => {
                    if let Some(found) = self.definition(entity, env).await? {
                        rounds.definitions += 1;
                        definitions.push(found);
                    }
                }
                Want::Key(namespace, key) => {
                    for binding in self.bindings(namespace, Some(key), env).await? {
                        rounds.bindings += 1;
                        if definition::is_name_space(&binding.referent) {
                            pending.push(Want::Keys(binding.referent.clone()));
                        }
                        bindings.push(binding);
                    }
                }
                Want::Keys(namespace) => {
                    for binding in self.bindings(namespace, None, env).await? {
                        rounds.bindings += 1;
                        if definition::is_name_space(namespace)
                            && definition::is_definition(&binding.referent)
                        {
                            pending.push(Want::Definition(binding.referent.clone()));
                        }
                        bindings.push(binding);
                    }
                }
            }
            answered.push(want);
        }
        Ok((definitions, bindings, answered))
    }

    /// The bindings of `namespace`, of `key` alone where one is given.
    async fn bindings<E: Reader>(
        &self,
        namespace: &Entity,
        key: Option<&str>,
        env: &E,
    ) -> Result<Vec<Binding>, LiveError> {
        let mut given = vec![("namespace", Value::Entity(namespace.clone()))];
        if let Some(key) = key {
            given.push(("key", Value::String(key.to_string())));
        }
        let mut bindings = Vec::new();
        for (_, values) in self
            .read(
                &[
                    (
                        "namespace",
                        definition::attribute::BINDING_NAMESPACE,
                        "entity:",
                    ),
                    ("key", definition::attribute::BINDING_KEY, "text:"),
                    (
                        "referent",
                        definition::attribute::BINDING_REFERENT,
                        "entity:",
                    ),
                ],
                &given,
                env,
            )
            .await?
        {
            if let (Some(Value::String(key)), Some(Value::Entity(referent))) =
                (values.get("key"), values.get("referent"))
            {
                bindings.push(Binding {
                    namespace: namespace.clone(),
                    key: key.clone(),
                    referent: referent.clone(),
                });
            }
        }
        Ok(bindings)
    }

    /// The definition kept under `entity`, if the branch keeps it.
    async fn definition<E: Reader>(
        &self,
        entity: &Entity,
        env: &E,
    ) -> Result<Option<Definition>, LiveError> {
        let rows = self
            .read(
                &[
                    ("is", definition::attribute::IS, "record:"),
                    ("kind", definition::attribute::KIND, "text:"),
                ],
                &[("this", Value::Entity(entity.clone()))],
                env,
            )
            .await?;
        let Some((_, values)) = rows.into_iter().next() else {
            return Ok(None);
        };
        let (Some(form), Some(Value::String(kind))) = (values.get("is"), values.get("kind")) else {
            return Ok(None);
        };
        Ok(Some(Definition {
            entity: entity.clone(),
            kind: kind.parse()?,
            form: Syntax::try_from(form)?,
        }))
    }

    /// The names a prefix may be the start of, read from the branch: in
    /// mine and in each namespace a petname of mine points at, each name
    /// starting with `prefix`, under each of its signatures.
    pub async fn suggest<E: Reader>(
        &self,
        mine: &Entity,
        prefix: &str,
        env: &E,
    ) -> Result<Vec<definition::Suggestion>, LiveError> {
        let mut spaces = definition::Namespaces::new(mine.clone());
        let mut from = vec![mine.clone()];
        for binding in self.bindings(mine, None, env).await? {
            if !definition::is_name_space(&binding.referent)
                && !definition::is_definition(&binding.referent)
            {
                from.push(binding.referent.clone());
            }
            spaces.bind(binding);
        }
        for namespace in from.iter().skip(1) {
            for binding in self.bindings(namespace, None, env).await? {
                spaces.bind(binding);
            }
        }
        let named: Vec<Entity> = spaces
            .bindings()
            .filter(|binding| binding.key.starts_with(prefix) && from.contains(&binding.namespace))
            .map(|binding| binding.referent)
            .filter(definition::is_name_space)
            .collect();
        for namespace in named {
            for binding in self.bindings(&namespace, None, env).await? {
                spaces.bind(binding);
            }
        }
        Ok(spaces.suggest(prefix))
    }

    /// Every entity with each of `fields`, a field an attribute of a
    /// type, and what each is; those of `given` fixed.
    async fn read<E: Reader>(
        &self,
        fields: &[(&str, &str, &str)],
        given: &[(&str, Value)],
        env: &E,
    ) -> Result<Vec<(Entity, BTreeMap<String, Value>)>, LiveError> {
        let constant = |name: &str| {
            given
                .iter()
                .find(|(field, _)| *field == name)
                .map(|(_, value)| match value {
                    Value::Entity(entity) => serde_json::json!({ "entity:": entity.as_str() }),
                    Value::String(text) => serde_json::json!(text),
                    _ => serde_json::Value::Null,
                })
        };
        let mut with = serde_json::Map::new();
        let mut terms = serde_json::Map::new();
        terms.insert(
            "this".into(),
            constant("this").unwrap_or_else(|| serde_json::json!({ "?": { "name": "this" } })),
        );
        for (field, attribute, kind) in fields {
            with.insert(
                field.to_string(),
                serde_json::json!({ "the": attribute, "as": kind }),
            );
            terms.insert(
                field.to_string(),
                constant(field).unwrap_or_else(|| serde_json::json!({ "?": { "name": field } })),
            );
        }
        let query: ConceptQuery = serde_json::from_value(serde_json::json!({
            "assert": { "with": with },
            "where": terms,
        }))
        .map_err(|error| Error::Read(format!("a query of what is kept: {error}")))?;
        let found: Vec<ConceptConclusion> = self
            .branch
            .query()
            .select(query)
            .perform(env)
            .try_vec()
            .await?;
        let mut rows = Vec::new();
        for conclusion in &found {
            let value = |name: &str| -> Result<Value, LiveError> {
                if let Some((_, value)) = given.iter().find(|(field, _)| *field == name) {
                    return Ok(value.clone());
                }
                Ok(conclusion
                    .source()
                    .lookup(&Term::<Any>::var(name))?
                    .content()?)
            };
            let Value::Entity(this) = value("this")? else {
                continue;
            };
            let mut values = BTreeMap::new();
            for (field, _, _) in fields {
                values.insert(field.to_string(), value(field)?);
            }
            rows.push((this, values));
        }
        Ok(rows)
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
