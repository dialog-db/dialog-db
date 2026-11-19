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
use std::str::FromStr;

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
use dialog_query::{ConceptConclusion, ConceptQuery, DeductiveRule, FoldConclusion};
use dialog_repository::{Branch, Delta, FoldSubscription, RemoteSite, Subscription};
use dialog_search_tree::nodes;

use crate::analyze::Analyzer;
use crate::check::{Checked, check};
use crate::definition::{self, Binding, Definition, Want};
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

/// An analyzer of the core forms alone, made once: what the driver's own
/// reads are analyzed by.
fn core_forms() -> Result<Analyzer, Error> {
    static CORE: std::sync::OnceLock<Result<Analyzer, Error>> = std::sync::OnceLock::new();
    CORE.get_or_init(Analyzer::new).clone()
}

/// The read of every binding of the space.
const EVERY_BINDING: &str = "(select (syntax.binding ?b key: (:text ?key) name: (:text ?name) \
     repository: (:last (:entity ?repository)) \
     signature: (:text ?signature) definition: (:last (:entity ?definition))))";

/// The scan of the bindings whose key begins with `prefix`: one attribute,
/// `syntax.binding/key`, its value refined to begin so, which the engine
/// reads as one range of the value index. The rest of each binding it
/// finds is read by its entity.
///
/// It is built here and not written in the notation, whose reads compile
/// to concept premises: a concept premise is read whole and then
/// filtered, and only a scan of one attribute takes a range.
pub fn binding_range(prefix: &str) -> Result<dialog_query::AttributeQuery, Error> {
    use dialog_query::{AttributeQuery, Cardinality, Entity as Of, Term};
    let kind = dialog_query::type_system::Type::from(dialog_query::corpus::Type::String)
        .with_prefix(prefix)
        .ok_or_else(|| Error::Refused(format!("no key begins with {prefix:?}")))?;
    Ok(AttributeQuery::new(
        Term::from(dialog_query::the!("syntax.binding/key")),
        Term::<Of>::var("this"),
        Term::<dialog_query::Any>::var("key").with_kind(kind),
        Term::blank(),
        Some(Cardinality::One),
    ))
}

/// A text as the notation writes it.
fn quoted(text: &str) -> String {
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

/// The definition a row of `syntax.definition` says, its entity under
/// `this`.
fn kept_definition(values: &BTreeMap<String, Value>) -> Result<Option<Definition>, Error> {
    let (Some(form), Some(Value::String(kind))) = (values.get("is"), values.get("kind")) else {
        return Ok(None);
    };
    let entity = match values.get("this") {
        Some(Value::Entity(entity)) => entity.clone(),
        _ => return Ok(None),
    };
    Ok(Some(Definition {
        entity,
        kind: kind.parse()?,
        form: Syntax::try_from(form)?,
    }))
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
    /// Repositories the caller knows by a name of its own, beside the
    /// branch's remotes.
    knowing: Vec<(String, Entity)>,
}

impl Live {
    /// Keep selections and views on `branch`.
    pub fn new(branch: Branch) -> Live {
        Live {
            branch,
            knowing: Vec::new(),
        }
    }

    /// The same, knowing `repository` as `who`: `@who/name` is its `name`.
    /// For a repository the branch reads some other way than as a remote
    /// it pulls from.
    pub fn knowing(mut self, who: impl Into<String>, repository: Entity) -> Live {
        self.knowing.push((who.into(), repository));
        self
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

    /// The repository this branch belongs to, which a bare name is of: its
    /// subject.
    pub fn repository(&self) -> Result<Entity, LiveError> {
        Ok(Entity::from_str(self.branch.of().as_ref())
            .map_err(|error| Error::Read(format!("a repository that is no entity: {error}")))?)
    }

    /// The other repositories this branch knows, each by the name it
    /// knows it by: each remote it pulls from, by its peer's local name,
    /// with the repository's subject.
    pub fn known(&self) -> Vec<(String, Entity)> {
        let mut known = self.knowing.clone();
        known.extend(
            self.branch
                .pulls()
                .iter()
                .filter_map(|upstream| match upstream {
                    dialog_repository::Upstream::Remote { remote, .. } => {
                        Some((remote.name(), Entity::from_str(remote.did().as_ref()).ok()?))
                    }
                    _ => None,
                }),
        );
        known
    }

    /// What the branch keeps of the notation's definitions: every
    /// definition and every binding of the space.
    pub async fn kept<E: Reader>(&self, env: &E) -> Result<Kept, LiveError> {
        let mut definitions = Vec::new();
        for values in self
            .selected(
                "(select (syntax.definition ?d is: ?form kind: (:text ?kind)))",
                env,
            )
            .await?
        {
            if let Some(found) = kept_definition(&values)? {
                definitions.push(found);
            }
        }
        Ok(Kept {
            definitions,
            bindings: self.bindings("", env).await?,
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
        // A bare name is the repository's this branch belongs to; another
        // repository is known by the name its remote goes by.
        analyzer.set_repository(self.repository()?);
        for (who, repository) in self.known() {
            analyzer.know(who, repository);
        }
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
            // A refused trial runs again with what was not found known
            // as not found, so its refusal says what an error reads.
            let nothing = definitions.is_empty()
                && bindings.is_empty()
                && checked.refusals().next().is_none();
            analyzer.answer(answered.clone(), definitions, bindings)?;
            if nothing {
                trial.answer(answered, Vec::new(), Vec::new())?;
                *analyzer = trial;
                return Ok((checked, rounds));
            }
        }
        Err(LiveError::Rounds(ROUNDS))
    }

    /// Read what `wants` ask for. A name's bindings bring the definitions
    /// they are bound to, so a name takes one round.
    async fn answer<E: Reader>(
        &self,
        wants: &[Want],
        rounds: &mut Rounds,
        env: &E,
    ) -> Result<(Vec<Definition>, Vec<Binding>, Vec<Want>), LiveError> {
        let mut definitions: Vec<Definition> = Vec::new();
        let mut bindings = Vec::new();
        let mut answered = Vec::new();
        let mut pending: Vec<Want> = wants.to_vec();
        while let Some(want) = pending.pop() {
            if answered.contains(&want) {
                continue;
            }
            match &want {
                Want::Definition(entity) => {
                    if !definitions.iter().any(|known| &known.entity == entity)
                        && let Some(found) = self.definition(entity, env).await?
                    {
                        rounds.definitions += 1;
                        definitions.push(found);
                    }
                }
                Want::Hash(prefix) => {
                    for found in self.hashed(prefix, env).await? {
                        rounds.definitions += 1;
                        definitions.push(found);
                    }
                }
                Want::Name(name) => {
                    for binding in self.bindings(&definition::range(name), env).await? {
                        rounds.bindings += 1;
                        pending.push(Want::Definition(binding.definition.clone()));
                        bindings.push(binding);
                    }
                }
                Want::Prefix(prefix) => {
                    for binding in self.bindings(prefix, env).await? {
                        rounds.bindings += 1;
                        bindings.push(binding);
                    }
                }
                Want::All => {
                    for binding in self.bindings("", env).await? {
                        rounds.bindings += 1;
                        bindings.push(binding);
                    }
                }
            }
            answered.push(want);
        }
        Ok((definitions, bindings, answered))
    }

    /// The bindings whose key begins with `prefix`: one range of the
    /// value index over `syntax.binding/key`, every binding for the empty
    /// prefix.
    pub async fn bindings<E: Reader>(
        &self,
        prefix: &str,
        env: &E,
    ) -> Result<Vec<Binding>, LiveError> {
        let rows: Vec<BTreeMap<String, Value>> = if prefix.is_empty() {
            self.selected(EVERY_BINDING, env).await?
        } else {
            let found = self
                .branch
                .query()
                .select(binding_range(prefix)?)
                .perform(env)
                .try_vec()
                .await?;
            let mut rows = Vec::new();
            // The range holds what begins so; anything else is no key of
            // it, and is left out whatever the read gave.
            for claim in found
                .iter()
                .filter(|claim| matches!(claim.is(), Value::String(key) if key.starts_with(prefix)))
            {
                let text = format!(
                    "(select (syntax.binding {} name: (:text ?name) \
                       repository: (:last (:entity ?repository)) signature: (:text ?signature) \
                       definition: (:last (:entity ?definition))))",
                    claim.of()
                );
                rows.extend(self.selected(&text, env).await?);
            }
            rows
        };
        let mut bindings = Vec::new();
        for values in rows {
            if let (
                Some(Value::String(name)),
                Some(Value::Entity(repository)),
                Some(Value::String(signature)),
                Some(Value::Entity(definition)),
            ) = (
                values.get("name"),
                values.get("repository"),
                values.get("signature"),
                values.get("definition"),
            ) {
                bindings.push(Binding {
                    name: name.clone(),
                    repository: repository.clone(),
                    signature: signature.clone(),
                    definition: definition.clone(),
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
        let text = format!("(select (syntax.definition {entity} is: ?form kind: (:text ?kind)))");
        for values in self.selected(&text, env).await? {
            if let Some(found) = kept_definition(&values)? {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    /// The definitions whose hash begins with `prefix`: one range of the
    /// entity index.
    async fn hashed<E: Reader>(&self, prefix: &str, env: &E) -> Result<Vec<Definition>, LiveError> {
        let read = "(syntax.definition ?d is: ?form kind: (:text ?kind))";
        let text = format!(
            "(select {read} {read} (:starts-with ?d prefix: {}))",
            quoted(&definition::definitions_starting(prefix))
        );
        let mut found = Vec::new();
        for values in self.selected(&text, env).await? {
            found.extend(kept_definition(&values)?);
        }
        Ok(found)
    }

    /// The names a prefix may be the start of, read from the branch:
    /// every name of the space starting with `prefix`, under each of its
    /// signatures. One range.
    pub async fn suggest<E: Reader>(
        &self,
        prefix: &str,
        env: &E,
    ) -> Result<Vec<definition::Suggestion>, LiveError> {
        let mut names = definition::Names::new(self.repository()?);
        for (who, repository) in self.known() {
            names.know(who, repository);
        }
        for binding in self.bindings(prefix, env).await? {
            names.bind(binding);
        }
        Ok(names.suggest(prefix))
    }

    /// The rows `text`, a `(select …)` of the core forms, answers on the
    /// branch, each field of its head by label.
    async fn selected<E: Reader>(
        &self,
        text: &str,
        env: &E,
    ) -> Result<Vec<BTreeMap<String, Value>>, LiveError> {
        let analysis = core_forms()?.analyze(&crate::parse(text)?);
        if let Some(error) = analysis
            .refusals()
            .find_map(|refused| refused.refusal.clone())
        {
            return Err(error.into());
        }
        let mut rows = Vec::new();
        for tree in &analysis.analyzed {
            for step in crate::compile(tree)? {
                if let crate::Step::Query(query) = step {
                    for conclusion in self.conclusions(&query, env).await? {
                        let mut values = query.values(&conclusion)?;
                        // The entity the row is of, which no field names.
                        if let Some(Syntax::Constant(Value::Entity(entity))) = query
                            .row(&conclusion)?
                            .operand()
                            .and_then(|operand| operand.items.first())
                        {
                            values.insert("this".into(), Value::Entity(entity.clone()));
                        }
                        rows.push(values);
                    }
                }
            }
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
