use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_search_tree::nodes;
use std::cell::Cell;
use std::collections::HashSet;
use std::iter::once;

use dialog_capability::{Capability, Fork, Provider, Resume, Subject};
use dialog_common::{Blake3Hash, Buffer, ConditionalSend, ConditionalSync, Held, Holds};
use dialog_corpus::history::Edition;
use dialog_corpus::selector::Constrained;
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{
    Changes, DialogCorpusError, Entity, Estimate, Fact, FactStream, FactView, LayerIndex, Layered,
    Likelihood, Preload, PreloadRequest, Select, SelectIn, Selector, SortKey, Speculation,
    Statement, sort_key,
};
use dialog_corpus::{Relation as CorpusRelation, Standing, Value};
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_effects::authority::{Identify, Operator, OperatorExt as _};
use dialog_effects::memory::Resolve;
use dialog_query::concept::descriptor::{ConceptDescriptor, ConceptFieldDescriptor};
use dialog_query::concept::query::fixpoint::Continuation;
use dialog_query::concept::query::{ConceptRules, Exact, Installed, PlanCache};
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output};
use dialog_query::recall::{BodyMemo, Memo};
use dialog_query::rule::statement::on_entity;
use dialog_query::session::{ProgramAnalysis, Quarantine};
use dialog_query::source::SelectRules;
use dialog_query::{Claim, DeductiveRule, Negation, Premise, Proposition};
use dialog_search_tree::DialogSearchTreeError;
use dialog_search_tree::Manifest;
use futures_util::future::try_join_all;
use futures_util::{StreamExt, TryStreamExt as _, stream};
use std::future::Future;
use std::sync::OnceLock;
use std::sync::{Arc, LazyLock, Mutex};
use tokio::sync::OnceCell;

use crate::CommitError;
use crate::REGISTRY;
use crate::layer::{Hidden, MergeKeys, filter_hidden, merge_grouped, merge_grouped_in};
use crate::placement::{memory_layer, session_layer};
use crate::repository::archive::TreeEnv;
use crate::repository::branch::select::line_manifest;
use crate::repository::ephemeral::Window;
use crate::repository::fetch::Driven;
use crate::repository::source::{Live, Source, View};
use crate::repository::{CellSettlement, ReadObservation};
#[cfg(test)]
use crate::rules::is_rule_attribute;
use crate::rules::{
    RuleKey, RuleRead, Selecting, assemble, builtin, builtin_derives, builtin_deriving,
    conclusion_attr, derives_attr, derives_keys, derives_selector, head_onto, hydrate,
    hydrate_inductive, invalid_attr, quarantined_attr, rule_entities, source_attr, source_bytes,
    source_selector,
};
use crate::schema::{
    Branch as BranchConcept, DidExt as _, Replica, Session, SessionBranch, session,
};
use crate::{Branch, Ephemeral, RemoteSite, Snapshot, Staged};

/// A composable query over one or more lines (branches, snapshots)
/// plus an in-memory overlay.
///
/// `branch.query()` (or `snapshot.query()`) returns a `QueryLayer`
/// rooted at that line. From there:
///
/// - [`with`](Self::with) folds any [`Statement`] (a concept
///   instance, an attribute expression, a [`Changes`] batch) into the
///   overlay — its asserts/replaces surface alongside stored facts,
///   its retracts tombstone matching stored facts.
/// - [`join`](Self::join) merges in another branch, snapshot, or
///   `QueryLayer`.
/// - [`select`](Self::select) stages a query; `.perform(&env)` runs it.
///
/// All lines in the layer are peers — there is no distinguished
/// "primary". A query reads the union of every line's facts plus the
/// overlay.
///
/// # Auto-injected schema metadata
///
/// At `.perform(env)` the layer resolves the operator's identity via
/// [`Identify`] and folds in [`metadata`](Self::metadata): one
/// [`Replica`](crate::schema::Replica) + [`Branch`](crate::schema::Branch)
/// (+ [`BranchRevision`](crate::schema::BranchRevision) when committed)
/// per branch, a [`Replica`](crate::schema::Replica) per snapshot,
/// plus a single [`Session`]. Callers don't pass the profile or
/// operator DID, and nothing is written to any line's tree.
///
/// ```no_run
/// # use dialog_repository::{Branch, Snapshot};
/// # use dialog_query::query::Application;
/// # fn example<Q: Application>(branch: &Branch, snapshot: &Snapshot, query: Q, facts: dialog_corpus::Changes) {
/// let layer = branch
///     .query()
///     .join(snapshot)                 // another line
///     .with(facts);                   // user-asserted overlay facts
/// let staged = layer.select(query);   // `.perform(&env)` injects metadata
/// # let _ = staged;
/// # }
/// ```
///
/// A layer holds its lines live and pins them when a query is performed:
/// each `.select(..).perform(..)` reads the revision every line stood at
/// when it began, and the next reads the next (see
/// [`View`](crate::repository::source::View)).
#[derive(Default, Clone)]
pub struct QueryLayer<'a> {
    lines: Vec<Live<'a>>,
    /// The layers of a session read beside the lines, held out of line
    /// and `None` for a read of branches alone, so a plain read's query,
    /// and the stream that holds it, is no larger than it was before
    /// layers.
    layers: Option<Box<StackLayers<'a>>>,
    changes: Changes,
}

/// The layers of a session a [`QueryLayer`] reads beside its lines.
#[derive(Default, Clone)]
struct StackLayers<'a> {
    /// Memory layers read beside the lines, live, each by the name a
    /// row's `in` binds to.
    memories: Vec<(Entity, &'a Ephemeral)>,
    /// Lines read after `lines`, each a layer of a stack read by the
    /// name a row's `in` binds to: its durable layers, each pinned at the
    /// tree its primary points at.
    named: Vec<(Entity, Source)>,
}

/// The lines a query env reads, owned: every tree line, pinned, plus
/// every memory layer, live. What a [`QueryLayer`] resolves to when a
/// query is performed, and what a [`Stack`](crate::Stack) hands its
/// induction.
#[derive(Default, Clone)]
pub(crate) struct Composite {
    pub(crate) sources: Vec<Source>,
    /// The memory layers, each by the name a row's `in` binds to.
    pub(crate) memories: Vec<(Entity, Ephemeral)>,
    /// The name a row of each of `sources` binds its `in` to, by index:
    /// a durable layer's. A line with none named here is named as
    /// [`QueryEnv::line_layer`] says.
    pub(crate) names: Vec<Option<Entity>>,
    /// The channels of the stack the composite is read through, by name,
    /// the loopback among them: a rule head landing in one is stimulus
    /// of the next round, as one landing in [`control:`](crate::CONTROL)
    /// is, and is never written into a layer.
    pub(crate) channels: Vec<Entity>,
}

impl Composite {
    /// A composite of one tree line.
    pub(crate) fn of(source: Source) -> Self {
        Self {
            sources: vec![source],
            ..Self::default()
        }
    }
}

impl<'a> QueryLayer<'a> {
    /// An empty layer — no branches, no overlay.
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold a [`Statement`] into this layer's overlay changes.
    ///
    /// `Changes` itself implements `Statement`, so `.with(changes)`
    /// merges an existing batch in. Any concept instance, attribute
    /// expression, or other `Statement` works too. Chainable.
    ///
    /// A deductive rule is a `Statement` too, so `.with(rule)` folds it
    /// into the overlay — the query resolves it as a transient rule
    /// without persisting it.
    pub fn with<S: Statement>(mut self, statement: S) -> Self {
        statement.assert(&mut self.changes);
        self
    }

    /// Merge another layer in: union the lines, fold the other
    /// layer's changes via its `Statement` impl. Accepts anything
    /// convertible into a `QueryLayer` — a `&Branch`, a `&Snapshot`, or
    /// a `Changes`.
    pub fn join(mut self, other: impl Into<QueryLayer<'a>>) -> Self {
        let other = other.into();
        self.lines.extend(other.lines);
        if let Some(theirs) = other.layers {
            let ours = self.layers.get_or_insert_with(Box::default);
            ours.memories.extend(theirs.memories);
            ours.named.extend(theirs.named);
        }
        other.changes.assert(&mut self.changes);
        self
    }

    /// Read `memory` beside the lines, live, as the layer named `layer`:
    /// a layer of a session.
    pub(crate) fn with_memory(mut self, layer: Entity, memory: &'a Ephemeral) -> Self {
        self.layers
            .get_or_insert_with(Box::default)
            .memories
            .push((layer, memory));
        self
    }

    /// Read `source` after the lines, pinned, as the durable layer named
    /// `layer`: a layer of a stack, at the tree its primary points at.
    pub(crate) fn with_pinned(mut self, layer: Entity, source: Source) -> Self {
        self.layers
            .get_or_insert_with(Box::default)
            .named
            .push((layer, source));
        self
    }

    /// The lines pinned as `views` (see [`pin`](Self::pin)), with the
    /// memory layers, owned: what one query reads.
    pub(crate) fn composite(&self, views: Vec<View<'a>>) -> Composite {
        // Collected from the views alone, then extended: collecting the
        // pinned views can reuse their buffer, which a chained iterator does
        // not, so a read with no durable layer allocates nothing for it.
        let mut sources: Vec<Source> = views.into_iter().map(View::into_source).collect();
        let Some(layers) = self.layers.as_deref() else {
            return Composite {
                sources,
                ..Composite::default()
            };
        };
        // A line with no name reads as no entry at all, so a read with no
        // durable layer allocates nothing for the names.
        let names = if layers.named.is_empty() {
            Vec::new()
        } else {
            self.lines
                .iter()
                .map(|_| None)
                .chain(layers.named.iter().map(|(layer, _)| Some(layer.clone())))
                .collect()
        };
        sources.extend(layers.named.iter().map(|(_, source)| source.clone()));
        Composite {
            sources,
            memories: layers
                .memories
                .iter()
                .map(|(layer, memory)| (layer.clone(), (*memory).clone()))
                .collect(),
            names,
            channels: Vec::new(),
        }
    }

    /// The branches this layer reads from, in join order.
    pub fn branches(&self) -> Vec<&'a Branch> {
        self.lines.iter().filter_map(|line| line.branch()).collect()
    }

    /// The snapshots this layer reads from, in join order.
    pub fn snapshots(&self) -> Vec<&'a Snapshot> {
        self.lines
            .iter()
            .filter_map(|line| line.snapshot())
            .collect()
    }

    /// The caller-supplied overlay changes (no auto-injected metadata).
    pub fn changes(&self) -> &Changes {
        &self.changes
    }

    /// The schema-metadata [`Changes`] for this layer: every branch's
    /// [`BranchMetadata`](super::metadata::BranchMetadata), every
    /// snapshot's [`Replica`](crate::schema::Replica), plus a single
    /// [`Session`] (with one cardinality-many `dialog.session/branch`
    /// per branch in scope — a snapshot is not a branch and gets none).
    ///
    /// `operator` (from [`Identify`]) supplies the profile + operator
    /// DIDs the schema entities are derived from. The lines are pinned
    /// here, so the revision facts name the heads as they stand now.
    pub fn metadata(&self, operator: &Capability<Operator>) -> Changes {
        let (mut changes, heads) = derive_metadata(&self.pin(), operator);
        changes.merge(heads);
        changes
    }

    /// Pin every line of this layer at the head it stands at now: what
    /// one query of the layer reads throughout. A durable layer is pinned
    /// already, and is not among them.
    pub(crate) fn pin(&self) -> Vec<View<'a>> {
        self.lines.iter().map(|line| line.pin()).collect()
    }

    /// The full per-query overlay: this layer's own
    /// [`changes`](Self::changes) with [`metadata`](Self::metadata)
    /// folded in. This is exactly what `.select(..).perform(..)`
    /// queries against alongside the branch streams.
    ///
    /// Held as a line, like a branch's session overlay, so it is read
    /// by range and named by its revision: every cache keyed by the
    /// layers a query reads keys by it too, and a query whose overlay
    /// holds the same facts as an earlier one's is served what the
    /// earlier one assembled. A layer that adds no changes of its own
    /// queries the metadata alone, which is then the branch's shared
    /// line rather than a new one built for every query.
    ///
    /// The lines are pinned here; a query that has pinned them already
    /// takes the overlay for its views with
    /// [`overlay_at`](Self::overlay_at), so the revision facts name the
    /// heads the query reads.
    pub fn overlay(&self, operator: &Capability<Operator>) -> Ephemeral {
        self.overlay_at(&self.pin(), operator)
    }

    /// [`overlay`](Self::overlay) for the lines pinned as `views`.
    pub(crate) fn overlay_at(
        &self,
        views: &[View<'a>],
        operator: &Capability<Operator>,
    ) -> Ephemeral {
        let metadata = shared_metadata(views, operator);
        if self.changes.is_empty() {
            return metadata;
        }
        let mut overlay = self.changes.clone();
        metadata.export().assert(&mut overlay);
        Ephemeral::holding(overlay)
    }

    /// Stage a query application. Call `.perform(&operator)` to execute.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        SelectQuery {
            layer: self.clone(),
            query,
        }
    }
}

/// [`QueryLayer::metadata`] held as a line, shared with every other
/// query over the same branch, profile, operator and revision.
pub(crate) fn shared_metadata(views: &[View<'_>], operator: &Capability<Operator>) -> Ephemeral {
    // Every query folds this in, and for a layer over one branch it
    // depends only on the profile, the operator and the revision, so the
    // branch keeps it: deriving it hashes and base58-renders entities
    // and re-parses both DIDs each time.
    if let [view] = views
        && let Some(branch) = view.branch()
    {
        return branch.layer_metadata(
            operator,
            view.revision(),
            || derive_metadata(views, operator),
            || derive_heads(views, operator),
        );
    }
    let (mut changes, heads) = derive_metadata(views, operator);
    changes.merge(heads);
    Ephemeral::holding(changes)
}

/// The revision facts of [`derive_metadata`] alone: what a new head
/// adds to the facts that stay put.
fn derive_heads(views: &[View<'_>], operator: &Capability<Operator>) -> Changes {
    let mut heads = Changes::new();
    for view in views {
        if let Some(metadata) = view.branch_metadata(operator)
            && let Some(revision) = metadata.revision
        {
            revision.assert(&mut heads);
        }
    }
    heads
}

/// Derive what [`QueryLayer::metadata`] folds in for the lines pinned
/// as `views`, as the facts that stay put while the lines' heads move
/// and the revision facts that move with them.
fn derive_metadata(views: &[View<'_>], operator: &Capability<Operator>) -> (Changes, Changes) {
    let mut changes = Changes::new();
    let mut heads = Changes::new();

    let mut branch_entities = Vec::with_capacity(views.len());
    for view in views {
        if let Some(entity) = view.metadata(operator, &mut changes, &mut heads) {
            branch_entities.push(entity);
        }
    }

    // The registry describes itself here rather than in its own
    // tree: a branch registry that had to record itself would have
    // to exist before it could be created. Synthesizing the fact
    // means a listing sees `meta` like any other branch while
    // nothing about it is ever stored.
    //
    // One per repository in scope, since each has its own registry.
    let mut described = HashSet::new();
    for view in views {
        let subject = view.subject();
        if described.insert(subject.did().clone()) {
            let replica = Replica::new(operator.profile().clone(), subject.did().clone());
            BranchConcept::new(&replica, REGISTRY).assert(&mut changes);
        }
    }

    let session_entity = Session::entity();
    Session {
        this: session_entity.clone(),
        profile: session::Profile(operator.profile().this()),
        operator: session::Operator(operator.did().this()),
    }
    .assert(&mut changes);
    // One `SessionBranch` per branch — `dialog.session/branch` is
    // cardinality-many, so the entries accumulate on `db:session`.
    for branch_entity in branch_entities {
        SessionBranch {
            this: session_entity.clone(),
            branch: session::Branch(branch_entity),
        }
        .assert(&mut changes);
    }

    (changes, heads)
}

// A line's session overlay ([`Branch::overlay`], [`Snapshot::overlay`])
// is not folded here: [`QueryEnv`] reads it live at every evaluation,
// so every read path — `select`, `query`, transaction queries,
// subscription evaluations — sees session facts with no per-path
// wiring and no snapshot to go stale.
impl<'a> From<Live<'a>> for QueryLayer<'a> {
    fn from(line: Live<'a>) -> Self {
        Self {
            lines: vec![line],
            ..Self::default()
        }
    }
}

impl<'a> From<&'a Ephemeral> for QueryLayer<'a> {
    /// A memory layer read alone, as the session's own layer.
    fn from(memory: &'a Ephemeral) -> Self {
        Self {
            layers: Some(Box::new(StackLayers {
                memories: vec![(session_layer(), memory)],
                named: Vec::new(),
            })),
            ..Self::default()
        }
    }
}

impl<'a> From<&'a Branch> for QueryLayer<'a> {
    fn from(branch: &'a Branch) -> Self {
        Self::from(Live::from(branch))
    }
}

impl<'a> From<&'a Snapshot> for QueryLayer<'a> {
    fn from(snapshot: &'a Snapshot) -> Self {
        Self::from(Live::from(snapshot))
    }
}

impl From<Changes> for QueryLayer<'_> {
    fn from(changes: Changes) -> Self {
        Self {
            changes,
            ..Self::default()
        }
    }
}

/// A query command ready to be performed against an environment.
pub struct SelectQuery<'a, Q> {
    layer: QueryLayer<'a>,
    query: Q,
}

impl<'a, Q> SelectQuery<'a, Q> {
    pub(crate) fn new(line: impl Into<Live<'a>>, query: Q) -> Self {
        Self {
            layer: QueryLayer::from(line.into()),
            query,
        }
    }
}

impl<'a, Q: Application> SelectQuery<'a, Q> {
    /// Execute the query, returning a stream of results.
    ///
    /// Resolves the operator's identity via [`Identify`], builds the
    /// query overlay (caller changes + auto-injected schema metadata)
    /// via [`QueryLayer::overlay`], lifts any retracts in it into
    /// tombstones, and unions every line's stream (tombstone-filtered)
    /// with the overlay.
    pub fn perform<Env>(self, env: &'a Env) -> impl Output<Q::Conclusion> + 'a
    where
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<ArchiveLoad>
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<Preload>
            + Provider<Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        let SelectQuery { layer, query } = self;
        async_stream::try_stream! {
            let operator = Identify
                .perform(env)
                .await
                .map_err(|e| DialogCorpusError::Storage(format!("identify: {e}")))?;

            // The query's lines, pinned once here: every select the
            // query performs and the metadata it folds in name the
            // revisions the lines stood at now.
            let views = layer.pin();
            let overlay = layer.overlay_at(&views, &operator);
            let composite = layer.composite(views);
            let sources = composite.sources.clone();
            let query_env = QueryEnv::over(composite, overlay, env);
            let results = Box::pin(query.perform(&query_env));
            // The query's own stream drives the env's preload queue:
            // evaluator hints (its own and any concurrent evaluation's)
            // execute while this stream is polled, borrowing this same
            // env and ending with the stream. See
            // `crate::repository::fetch`.
            let queue = Provider::<Speculation>::execute(env, ()).await;
            let driven = Driven::new(results, sources, env, queue);
            for await result in driven {
                yield result?;
            }
        }
    }
}

/// The runtime environment that bridges the layer's lines and
/// per-query overlay changes into the query engine's Provider bounds.
///
/// Built fresh on each `.perform(env)`; the environment reference
/// is never captured on the layer itself.
/// The capabilities every read runs on, as one trait object: the
/// query environment holds `&dyn Capabilities`, so the evaluator is
/// instantiated once for every environment that reads through it
/// rather than once per concrete peer type. Each instantiation names
/// the peer type in every nested future it builds, and a commit that
/// settles successions reaches the evaluator from every crate that
/// commits; erased, those names and copies collapse.
pub(crate) trait Capabilities:
    TreeEnv
    + Provider<nodes::Write<CorpusNode>>
    + ConditionalSync
    + Provider<BlobRead>
    + Provider<Get>
    + Provider<Put>
    + Provider<Resolve>
    + Provider<ArchiveLoad>
    + Provider<nodes::Read<CorpusNode>>
    + Provider<nodes::Preload<CorpusNode>>
    + ConditionalSync
    + Provider<nodes::Write<CorpusNode>>
    + ConditionalSync
    + Provider<BlobLoad>
    + Provider<Preload>
    + Provider<Fork<RemoteSite, Resolve>>
    + Holds
    + ConditionalSync
{
}

impl<T> Capabilities for T where
    T: TreeEnv
        + Provider<nodes::Write<CorpusNode>>
        + ConditionalSync
        + Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<ArchiveLoad>
        + Provider<nodes::Read<CorpusNode>>
        + Provider<nodes::Preload<CorpusNode>>
        + ConditionalSync
        + Provider<nodes::Write<CorpusNode>>
        + ConditionalSync
        + Provider<BlobLoad>
        + Provider<Preload>
        + Provider<Fork<RemoteSite, Resolve>>
        + Holds
        + ConditionalSync
{
}

/// A read's capabilities, erased: the type every query environment
/// holds its environment as.
pub(crate) type Erased = dyn Capabilities;

pub(crate) struct QueryEnv<'a> {
    /// Owned (cheaply cloned: shared caches) so the env's only
    /// lifetime is the underlying `env` reference. A poll/evaluation
    /// can then type its `QueryEnv` with the *named* env lifetime
    /// instead of a generator-local borrow — which is what keeps the
    /// enclosing future `Send`-general on native (two independent
    /// erased lifetimes in `QueryEnv<'0>: Provider<Select<'1>>` hit
    /// rustc's #100013 limitation; a named lifetime does not).
    sources: Vec<Source>,
    /// What the env reads beside its lines, held out of line and `None`
    /// for a read of branches alone, so a plain read's env, and every
    /// future and stream that holds one, is no larger than it was before
    /// layers.
    layered: Option<Arc<SessionLayers>>,
    /// The query's own overlay: caller-asserted facts and the
    /// auto-injected metadata, held as a line (see
    /// [`QueryLayer::overlay`]) and read under the lines' format.
    overlay: Ephemeral,
    /// A transaction's writes and dispatched transients, each held so a
    /// selector is a range read (see [`Staged`]) and shared, never
    /// copied, per query.
    layers: Vec<Staged>,
    /// The lines' format and the tombstones keyed under it, resolved on
    /// the first read (it needs the lines' tree roots) and shared by clones.
    format: Arc<OnceCell<Format>>,
    /// When present, every selector this environment executes —
    /// fact scans and rule-discovery reads alike — records its
    /// demanded range here. Subscriptions use the recorded cover to
    /// gate re-evaluation.
    demand: Option<crate::Demand>,
    /// Every rule-discovery read this environment made, in order: what
    /// a rule set assembled here is recorded with, so a subscription
    /// reusing the set still records the reads as demand.
    reads: Arc<Mutex<Vec<RuleRead>>>,
    /// A polling subscription's retained fixpoint for one concept:
    /// attached to that concept's resolved rules so a recursive
    /// evaluation continues (or rebuilds into) the retained answer
    /// table instead of computing a throwaway one.
    fixpoint: Option<(Entity, Continuation)>,
    /// Whether any line can fetch what it lacks (see
    /// [`Routes::fetches`](crate::Routes::fetches)): preload hints are
    /// refused when none can. Asked of the env's routes on the first hint,
    /// and shared by clones.
    fetches: Arc<OnceLock<bool>>,
    /// The per-query memo rule heads share their source body's rows
    /// through.
    memo: Memo,
    env: &'a Erased,
}

/// What evaluation keeps between queries, the outputs of cached formulas
/// above all, is held by the environment the query runs in.
impl Holds for QueryEnv<'_> {
    fn held(&self, key: &str) -> Option<Held> {
        self.env.held(key)
    }

    fn hold(&self, key: String, handle: Held) {
        self.env.hold(key, handle)
    }

    fn held_or(&self, key: &str, make: &dyn Fn() -> Held) -> Held {
        self.env.held_or(key, make)
    }
}

/// What a query env reads beside its lines: the memory layers, the names
/// of the lines a stack names, and the stack's channels. Shared by the
/// env's clones.
struct SessionLayers {
    memories: Vec<(Entity, Ephemeral)>,
    names: Vec<Option<Entity>>,
    channels: Vec<Entity>,
}

/// The format [`Manifest`]s of the trees a [`QueryEnv`] reads, and the
/// tombstone sets keyed under them.
///
/// A line's rows are keyed under its tree's manifest. Overlay facts and
/// retracts are keyed under the lines' manifest too, so they order and
/// match exactly as the trees' own rows do. Lines written under different
/// manifests are still read together rather than refused: each line's rows
/// are filtered with tombstones keyed under its own manifest, and the merge
/// derives every row's key from its fields under the first line's
/// ([`MergeKeys::Fields`]), a slower path that keeps retracts and dedup
/// exact across formats.
struct Format {
    /// The manifest overlay rows and merge keys are taken under: the lines'
    /// shared manifest, or the first line's when they differ.
    manifest: Manifest,
    /// How the merge keys rows: off stored bytes when every line shares
    /// `manifest`, from fields when they do not.
    keys: MergeKeys,
    /// Per line, in line order: its manifest and the tombstone sets its
    /// streams are filtered with.
    lines: Vec<LineFormat>,
    /// Every fact the per-query changes and the staged layers retract,
    /// keyed under `manifest`: what a memory layer's stream is filtered
    /// with.
    staged: Hidden,
}

/// One line's manifest and the tombstone sets its streams are filtered
/// with before the merge, keyed under that manifest.
struct LineFormat {
    /// The manifest of the line's tree.
    manifest: Manifest,
    /// Every fact the per-query changes and the staged layers retract.
    /// The line's session overlay stream is filtered against these so a
    /// staged retract suppresses a session fact.
    staged: Hidden,
    /// `staged` and the line's session tombstones. The line's tree
    /// stream is filtered against these, so a read sees the tree as the
    /// commit will leave it.
    tombstones: Hidden,
}

impl<'a> QueryEnv<'a> {
    /// A runtime env over `sources` alone (see [`layered`](Self::layered)):
    /// what the tests build directly.
    #[cfg(test)]
    pub(crate) fn new(sources: Vec<Source>, overlay: Ephemeral, env: &'a Erased) -> Self {
        Self::layered(sources, None, overlay, env)
    }

    /// Build a runtime env from already-resolved parts: the lines to
    /// read, what is read beside them (`None` for a read of branches
    /// alone), the per-query overlay (caller changes + injected
    /// metadata), and the underlying capability env, in place. The
    /// tombstones are lifted on first read, once the lines' format is
    /// known: the per-query retracts, keyed under each line's manifest.
    ///
    /// `Branch::query`, `Snapshot::query`, and the transaction-query
    /// paths all construct through here so there is exactly one query
    /// env — a transaction query is just a single-line `QueryEnv`.
    /// Deductive-rule resolution is built in (a durable layer per line,
    /// its session overlay, and the per-query overlay as a transient
    /// layer), so the paths can never diverge on it.
    ///
    /// Every line arrives pinned to the revision it stood at when its
    /// [`Source`] was made, which is the only form a line can arrive in:
    /// one query performs many selects, and each must read the tree the
    /// query began with, not whatever a commit through another handle of
    /// the branch has moved the head to since (dialog-db/dialog-db#757).
    /// A line's session overlay is not pinned: it is read live, as every
    /// read of the branch reads it.
    fn layered(
        sources: Vec<Source>,
        layered: Option<Arc<SessionLayers>>,
        overlay: Ephemeral,
        env: &'a Erased,
    ) -> Self {
        Self {
            sources,
            layered,
            overlay,
            layers: Vec::new(),
            format: Arc::new(OnceCell::new()),
            demand: None,
            reads: Arc::new(Mutex::new(Vec::new())),
            fixpoint: None,
            fetches: Arc::default(),
            memo: Memo::default(),
            env,
        }
    }

    /// A runtime env over `composite`: its lines and its memory layers
    /// (see [`layered`](Self::layered)).
    pub(crate) fn over(composite: Composite, overlay: Ephemeral, env: &'a Erased) -> Self {
        let Composite {
            sources,
            memories,
            names,
            channels,
        } = composite;
        let layered =
            (!(memories.is_empty() && names.is_empty() && channels.is_empty())).then(|| {
                Arc::new(SessionLayers {
                    memories,
                    names,
                    channels,
                })
            });
        Self::layered(sources, layered, overlay, env)
    }

    /// The memory layers read beside the lines, live, each by its name.
    /// Their rows stand where a line's session overlay's do, and their
    /// tombstones hide facts in every line.
    fn memories(&self) -> &[(Entity, Ephemeral)] {
        self.layered
            .as_deref()
            .map_or(&[], |layered| &layered.memories)
    }

    /// The name a row of each line binds its `in` to, by index, where the
    /// composite named one: a durable layer's (see [`Composite::names`]).
    fn names(&self) -> &[Option<Entity>] {
        self.layered
            .as_deref()
            .map_or(&[], |layered| &layered.names)
    }

    /// The channels of the stack read through (see [`Composite::channels`]).
    fn channels(&self) -> &[Entity] {
        self.layered
            .as_deref()
            .map_or(&[], |layered| &layered.channels)
    }

    /// Whether `layer` is a channel of the stack read through, the
    /// loopback, [`control:`](crate::CONTROL), always among them.
    pub(crate) fn is_channel(&self, layer: &Entity) -> bool {
        *layer == crate::control_layer() || self.channels().contains(layer)
    }

    /// Whether a stack's own layer is read here, as a memory layer named
    /// [`session:`](crate::SESSION): then that is what `session:` is, and
    /// the primary's session overlay is read as a part of the primary.
    fn holds_own_layer(&self) -> bool {
        self.memories()
            .iter()
            .any(|(layer, _)| *layer == session_layer())
    }

    /// The primary's session store when it is read as
    /// [`session:`](crate::SESSION), live: the first line's overlay.
    /// `None` for a read over no line, and for one through a stack, whose
    /// own layer is `session:`, a memory layer read with the ticks
    /// applied.
    pub(crate) fn session_store(&self) -> Option<&Ephemeral> {
        if self.holds_own_layer() {
            return None;
        }
        self.sources.first().map(Source::overlay)
    }

    /// The name a row of the line at `index` binds its `in` to: the
    /// primary is [`memory:`](crate::MEMORY); a durable layer of a
    /// session is named as it is declared, `memory:<name>`; another line
    /// is named by its subject.
    fn line_layer(&self, index: usize) -> Entity {
        if index == 0 {
            return memory_layer();
        }
        match self.names().get(index) {
            Some(Some(layer)) => layer.clone(),
            _ => self.sources[index].as_view().subject().did().this(),
        }
    }

    /// The name a row of the line at `index`'s session overlay binds its
    /// `in` to: the primary's is [`session:`](crate::SESSION), but
    /// through a stack, whose own layer is `session:`, it is a part of
    /// the primary, [`memory:`](crate::MEMORY); another line's is read as
    /// the line's own.
    fn overlay_layer(&self, index: usize) -> Entity {
        if index == 0 && !self.holds_own_layer() {
            session_layer()
        } else if index == 0 {
            memory_layer()
        } else {
            self.line_layer(index)
        }
    }

    /// Read `layers` above the lines too: a transaction's writes, held
    /// so reading them costs a range read however many there are.
    pub(crate) fn with_layers(mut self, layers: Vec<Staged>) -> Self {
        self.layers = layers;
        self
    }

    /// What a read settlement observes of this environment: the lines'
    /// heads, then the revision of every overlay read with them (see
    /// [`overlays`](Self::overlays)).
    pub(crate) fn observation(&self) -> ReadObservation {
        ReadObservation {
            heads: self
                .sources
                .iter()
                .map(|source| source.revision().cloned())
                .collect(),
            overlays: self.overlays().map(Ephemeral::revision).collect(),
        }
    }

    /// Every overlay this environment reads: each line's session overlay,
    /// then the query's own. What names a read's layers names each of
    /// them by its revision, so none can be read without keying the
    /// caches built from it.
    fn overlays(&self) -> impl Iterator<Item = &Ephemeral> {
        self.sources
            .iter()
            .map(Source::overlay)
            .chain(self.memories().iter().map(|(_, memory)| memory))
            .chain(once(&self.overlay))
    }

    /// The layers' choosing writes a read of `input` meets, each cell
    /// as the layer's [`ReadSettlement`] leaves it: the settlement the
    /// commit applies, kept per layer and observation and advanced only
    /// over the writes it has not settled.
    ///
    /// [`ReadSettlement`]: super::transaction::ReadSettlement
    async fn settle_within(
        &self,
        input: &Selector<Constrained>,
    ) -> Result<Vec<(usize, Vec<CellSettlement>)>, DialogCorpusError> {
        let layers = self.layers();
        let mut settled: Vec<(usize, Vec<CellSettlement>)> = Vec::new();
        let mut observed: Option<ReadObservation> = None;
        for (index, layer) in layers.iter().enumerate() {
            if !layer.has_successions() {
                continue;
            }
            let cells = layer.electing_cells_within(input);
            if cells.is_empty() {
                continue;
            }
            let observed = observed.get_or_insert_with(|| self.observation());
            let failed = |error: CommitError| DialogCorpusError::Storage(error.to_string());
            let mut settlement = layer
                .take_settlement(observed)
                .unwrap_or_else(|| super::transaction::ReadSettlement::new(self.pending_edition()));
            // A settlement that failed partway is not kept: the next
            // read starts it again.
            let composite = Composite {
                sources: self.sources.clone(),
                memories: self.memories().to_vec(),
                names: self.names().to_vec(),
                channels: self.channels().to_vec(),
            };
            Box::pin(settlement.advance(&composite, &self.overlay, layer, self.env))
                .await
                .map_err(failed)?;
            let settlements: Vec<CellSettlement> = cells
                .iter()
                .filter_map(|cell| settlement.settlement_of(cell, layer))
                .collect();
            layer.put_settlement(observed, settlement);
            settled.push((index, settlements));
        }
        Ok(settled)
    }

    /// The edition a commit on this environment's line would mint: what
    /// a write the line has not committed yet stands at.
    pub(crate) fn pending_edition(&self) -> Edition {
        self.sources
            .first()
            .and_then(|source| source.revision())
            .map(|revision| revision.edition.successor())
            .unwrap_or(Edition::GENESIS)
    }

    /// Record every selector this environment executes into
    /// `demand`. Used by subscriptions to capture the evaluation's
    /// demand cover.
    pub(crate) fn with_demand(mut self, demand: crate::Demand) -> Self {
        self.demand = Some(demand);
        self
    }

    /// Attach a subscription's retained fixpoint for `concept`:
    /// when that concept's rules resolve recursive, evaluation
    /// continues the retained answer table instead of recomputing.
    pub(crate) fn with_fixpoint(mut self, concept: Entity, continuation: Continuation) -> Self {
        self.fixpoint = Some((concept, continuation));
        self
    }

    /// Record a selector's demanded range under `manifest`, when
    /// recording is on.
    #[inline(always)]
    fn record_demand(&self, selector: &Selector<Constrained>, manifest: &Manifest) {
        if let Some(demand) = &self.demand {
            demand.record(selector, manifest);
        }
    }

    /// Note a rule-discovery read: recorded as rule demand when
    /// recording is on, and kept so a rule set assembled from it can
    /// replay it for a later subscription.
    ///
    /// Every rule read names a rule attribute: a rule set is keyed by the
    /// rule facts of its layers ([`RuleKey`]), so a read of any other
    /// attribute would let a write the key does not see change the set.
    /// Test builds check it.
    fn read_rules(&self, selector: &Selector<Constrained>, manifest: &Manifest) {
        #[cfg(test)]
        assert!(
            selector.attribute().is_some_and(is_rule_attribute),
            "a rule read outside the rule attributes: {selector:?}"
        );
        if let Some(demand) = &self.demand {
            demand.record_rules(selector, manifest);
        }
        self.reads
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((selector.clone(), manifest.clone()));
    }
}

impl QueryEnv<'_> {
    /// The layers a read merges above the lines: the writes as held,
    /// settled cell by cell as a read meets them
    /// ([`settle_within`](Self::settle_within)).
    fn layers(&self) -> &Vec<Staged> {
        &self.layers
    }

    /// The lines' formats and the tombstones keyed under them, resolved
    /// from the lines' tree roots on first use (see [`Format`]).
    async fn format(&self) -> Result<&Format, DialogCorpusError> {
        self.format
            .get_or_try_init(|| async {
                let layers = self.layers();
                let mut lines = Vec::with_capacity(self.sources.len());
                for source in &self.sources {
                    lines.push(line_manifest(&source.as_view(), self.env).await?);
                }
                // With no line there is no tree, and the overlay is read
                // as a new tree would order it.
                let manifest = lines.first().cloned().unwrap_or_default();
                let keys = if lines.iter().all(|line| *line == manifest) {
                    MergeKeys::Stored
                } else {
                    MergeKeys::Fields
                };
                let lines = self
                    .sources
                    .iter()
                    .zip(lines)
                    .map(|(source, line)| {
                        let changes = self.overlay.tombstones(&line);
                        // Every set is shared, never merged, so this costs
                        // nothing per query however much the layers or the
                        // session hold.
                        let mut staged = Hidden::default().facts(changes);
                        for layer in layers {
                            staged = staged.facts(layer.tombstones(&line));
                        }
                        let mut tombstones =
                            staged.clone().facts(source.overlay().tombstones(&line));
                        for (_, memory) in self.memories() {
                            tombstones = tombstones.facts(memory.tombstones(&line));
                        }
                        LineFormat {
                            manifest: line,
                            staged,
                            tombstones,
                        }
                    })
                    .collect();
                // Only a memory layer's stream is filtered with these, so a
                // read with none skips them.
                let mut staged = Hidden::default();
                if !self.memories().is_empty() {
                    staged = staged.facts(self.overlay.tombstones(&manifest));
                    for layer in &self.layers {
                        staged = staged.facts(layer.tombstones(&manifest));
                    }
                }
                Ok(Format {
                    manifest,
                    keys,
                    lines,
                    staged,
                })
            })
            .await
    }
}

impl Clone for QueryEnv<'_> {
    fn clone(&self) -> Self {
        Self {
            sources: self.sources.clone(),
            layered: self.layered.clone(),
            overlay: self.overlay.clone(),
            layers: self.layers.clone(),
            format: self.format.clone(),
            demand: self.demand.clone(),
            reads: self.reads.clone(),
            fixpoint: self.fixpoint.clone(),
            fetches: self.fetches.clone(),
            memo: Memo::default(),
            env: self.env,
        }
    }
}

/// Execute a select against a single line, transparently routing through
/// a branch's remote upstream when configured. Extracted as a freestanding
/// helper so every line in a [`QueryEnv`] shares the exact same read path
/// (a transaction query is itself a single-line `QueryEnv`).
///
/// The line is only borrowed while the scan is set up: the returned
/// stream borrows nothing but the env. The setup runs here rather than
/// inside the stream so the stream boxed per scan holds only the scan,
/// not the setup's futures alongside it (together they came to 16 KiB,
/// allocated and copied for every scan a query ran), and the scan is
/// built in its box ([`Select::execute_boxed`](crate::Select)).
pub(crate) async fn select_from_source<'a>(
    source: &Source,
    env: &'a Erased,
    input: Selector<Constrained>,
) -> Result<FactStream<'a>, DialogCorpusError> {
    let select = crate::Select::from_view(source.as_view(), input);
    // Concurrent reads of one digest share fetch-and-hydrate through
    // the env's own `Load` (see `crate::load_or_hydrate`), with every
    // other evaluation in the process.
    let reads: &dyn TreeEnv = env;
    Ok(select.execute_boxed(reads).await?)
}

// Deliberately implemented for the *same* lifetime on both sides
// (`QueryEnv<'a>: Provider<Select<'a>>`), never a decoupled pair
// (`impl<'a, 's> ... where 'a: 's`). Auto-trait (`Send`) checking of
// a future that holds `&query_env` across an `await` erases every
// region, so the obligation resurfaces higher-ranked: a single
// erased lifetime (`for<'0> QueryEnv<'0>: Provider<Select<'0>>`)
// is provable by this impl, while a decoupled pair
// (`for<'0, '1> QueryEnv<'0>: Provider<Select<'1>>`) hits rustc's
// #100013 limitation and the poll future stops being `Send` on
// native. `QueryEnv` is covariant in `'a`, so call sites shrink
// `&QueryEnv<'a>` to `&QueryEnv<'s>` implicitly — the strict impl
// is what forces region inference to unify the two into one
// variable.
//
// Written out, not through `#[dialog_capability::provider]`: the
// answer is the one future [`QueryEnv::select`] makes, and not a
// future awaiting or wrapping it. A scan is issued per row of a join,
// and a future awaited in another, or wrapped by one, is built apart
// and moved into it, its whole state copied, every time.
impl<'a> Provider<Select<'a>> for QueryEnv<'a> {
    fn execute(
        &self,
        input: Selector<Constrained>,
    ) -> Resume<'_, Result<FactStream<'a>, DialogCorpusError>> {
        Resume::later(self.select::<Bare>(input))
    }
}

// The same lifetime on both sides, and written out, as
// `Provider<Select<'a>>` above.
impl<'a> Provider<SelectIn<'a>> for QueryEnv<'a> {
    fn execute(
        &self,
        input: Selector<Constrained>,
    ) -> Resume<'_, Result<Layered<'a>, DialogCorpusError>> {
        Resume::later(self.select::<Named>(input))
    }
}

/// How [`QueryEnv::select`] answers: with the rows bare, or each with
/// its layer. A type, not a flag, so each provider's future is the
/// read's own, and a read that names no layer never holds the layers.
trait Answer<'a>: ConditionalSend + 'static {
    /// Whether each stream's layer is named, for the rows to carry.
    const NAMED: bool;
    /// What the read answers with.
    type Out: ConditionalSend;
    /// The answer, from the rows bare: every read when the layers are
    /// not named, and the quarantined rows, read apart, in any case.
    fn rows(rows: FactStream<'a>) -> Self::Out;
    /// The answer, from the streams with their layers.
    fn layered(reads: Reads<'a>) -> Self::Out;
}

/// The rows bare: a [`Select`].
struct Bare;

impl<'a> Answer<'a> for Bare {
    const NAMED: bool = false;
    type Out = FactStream<'a>;

    fn rows(rows: FactStream<'a>) -> Self::Out {
        rows
    }

    fn layered(reads: Reads<'a>) -> Self::Out {
        merge_grouped(reads.streams, reads.manifest, reads.keys)
    }
}

/// Each row with its layer: a [`SelectIn`].
struct Named;

impl<'a> Answer<'a> for Named {
    const NAMED: bool = true;
    type Out = Layered<'a>;

    fn rows(rows: FactStream<'a>) -> Self::Out {
        Layered::primary(rows)
    }

    fn layered(reads: Reads<'a>) -> Self::Out {
        reads.layered()
    }
}

/// The streams a read that names its layers merges, in precedence
/// order (see [`QueryEnv::select`]), with the layer each is read from,
/// under the lines' format.
struct Reads<'a> {
    streams: Vec<FactStream<'a>>,
    layers: Vec<Entity>,
    manifest: Manifest,
    keys: MergeKeys,
}

impl<'a> Reads<'a> {
    /// The streams merged, each row with the index of its layer among
    /// the layers read (see [`Layered`]).
    fn layered(self) -> Layered<'a> {
        let Reads {
            streams,
            layers,
            manifest,
            keys,
        } = self;
        // Each stream's layer, as the index of its first stream: the
        // merge tells the streams apart, and a row carries the index
        // of its layer.
        let mut named: Vec<Entity> = Vec::new();
        let stamps: Vec<LayerIndex> = layers
            .into_iter()
            .map(
                |layer| match named.iter().position(|known| *known == layer) {
                    Some(at) => at,
                    None => {
                        named.push(layer);
                        named.len() - 1
                    }
                },
            )
            .map(|at| LayerIndex::try_from(at).expect("a read has fewer layers than u16::MAX"))
            .collect();
        let rows = merge_grouped_in(streams, manifest, keys);
        Layered {
            layers: named,
            rows: Box::pin(rows.map(move |row| row.map(|(view, index)| (view, stamps[index])))),
        }
    }
}

impl<'a> QueryEnv<'a> {
    /// A read of `input`: the quarantined rows read apart, or the
    /// streams the read merges, by precedence, newest standing first:
    /// the session overlays, the staged writes, the lines, and the
    /// per-query changes, which stand at no version. One fact can reach
    /// the merge from more than one stream (a value the line holds that
    /// the session overlay or a staged write holds too), and the merge
    /// keeps the first stream's row of such a fact.
    ///
    /// A read naming one layer (`in:` bound, [`Selector::layer`]) takes
    /// that layer's streams alone. Answered [`Named`], each stream comes
    /// with the layer it reads, for a read that binds each row's layer
    /// ([`SelectIn`]); answered [`Bare`], the read names no layer and
    /// pays nothing for one, per stream or per row.
    ///
    /// An `async move` block, not an `async fn`: the body of an `async
    /// fn` holds an argument it takes by value twice, as the argument
    /// and as the local it is moved into, and `input` is 304 bytes. The
    /// future is boxed per scan, a scan is issued per row of a join,
    /// and the block holds `input` once.
    #[allow(clippy::manual_async_fn)]
    fn select<A: Answer<'a>>(
        &self,
        input: Selector<Constrained>,
    ) -> impl Future<Output = Result<A::Out, DialogCorpusError>> + ConditionalSend + '_ {
        async move {
            if input.attribute() == Some(&*QUARANTINED) {
                let rows = Box::pin(self.select_quarantined(input)).await?;
                return Ok(A::rows(rows));
            }
            if input.attribute() == Some(&*INVALID) {
                let rows = Box::pin(self.select_invalid(input)).await?;
                return Ok(A::rows(rows));
            }
            let format = self.format().await?;
            let manifest = format.manifest.clone();
            self.record_demand(&input, &manifest);
            let wanted = input.layer().cloned();
            let named = A::NAMED || wanted.is_some();
            let mut sessions: Vec<FactStream<'a>> = Vec::new();
            let mut staged: Vec<FactStream<'a>> = Vec::new();
            let mut lines: Vec<FactStream<'a>> = Vec::with_capacity(self.sources.len());
            let mut changes: Vec<FactStream<'a>> = Vec::new();
            // The layer of every stream, by the group the stream goes in:
            // empty unless the read names them.
            let mut layers: Vec<(Group, Entity)> = Vec::new();
            // The layer a stream reads, when the read asks for it, and
            // whether the read takes the stream: every stream, unless the
            // read names one layer.
            let layer = |of: &dyn Fn() -> Entity| -> Option<Option<Entity>> {
                if !named {
                    return Some(None);
                }
                let layer = of();
                match &wanted {
                    Some(wanted) if *wanted != layer => None,
                    _ => Some(Some(layer)),
                }
            };

            // The choosing writes this read meets, settled cell by cell:
            // the line's claims they succeed, or succeed and write back, are
            // hidden from every line, and the values the cells already held
            // are hidden from the layers' own rows, as the commit would
            // write nothing for them.
            // Settled only when a staged layer has a choosing write: a read
            // that meets none takes nothing from the settlement, and does
            // not enter it.
            let settled = if self.layers().iter().any(Staged::has_successions) {
                self.settle_within(&input).await?
            } else {
                Vec::new()
            };
            let succeeded: Vec<&Fact> = settled
                .iter()
                .flat_map(|(_, cells)| {
                    cells
                        .iter()
                        .flat_map(|cell| cell.succeeded.iter().chain(cell.replaced.iter()))
                })
                .collect();
            let hidden_in = |line: &Manifest| -> Arc<HashSet<SortKey>> {
                Arc::new(succeeded.iter().map(|fact| sort_key(fact, line)).collect())
            };

            // Line streams — each filtered by tombstones from the
            // overlay's retracts so a `tx.retract(x)` (or any user-asserted
            // retract in `with(..)`) suppresses matching source facts, and
            // by the line's session tombstones. Each line's stream is
            // filtered with the tombstones keyed under its own manifest, and
            // borrows only `self.env`.
            for (index, (source, line)) in self.sources.iter().zip(&format.lines).enumerate() {
                let Some(layer) = layer(&|| self.line_layer(index)) else {
                    continue;
                };
                let raw = select_from_source(source, self.env, input.clone()).await?;
                let mut hidden = line.tombstones.within(&input);
                if !succeeded.is_empty() {
                    hidden = hidden.facts(hidden_in(&line.manifest));
                }
                lines.push(filter_hidden(raw, hidden, line.manifest.clone()));
                layers.extend(layer.map(|layer| (Group::Lines, layer)));
            }

            // Each line's session overlay, read live. Filtered by the
            // staged retracts only: the overlay's own tombstones hide facts
            // *beneath* it, never its own. Pushed only when it has rows,
            // for the same reason the per-query stream is below. An
            // overlay row is the newest fact of its cell: it stands past
            // the edition the next commit mints, above every committed row
            // and every staged write, so a cardinality-one read elects it,
            // until the session takes it back.
            // The edition the next commit mints, found only for a read that
            // meets an overlay or staged row: it reads the first line's
            // revision.
            let minted = Cell::new(None);
            let pending = || {
                minted.get().unwrap_or_else(|| {
                    let edition = self.pending_edition();
                    minted.set(Some(edition));
                    edition
                })
            };
            // Past every line's head, not only the first's: a join reads
            // lines of different lengths, and an overlay row is newer than
            // every committed row of every line it is read beside. Found
            // only for a read that meets an overlay row, since it reads
            // every line's revision.
            // One window for every in-memory store this read meets, so the
            // key range is built once, and not at all when they are empty.
            let window = Window::new(&input);
            let mut session = None;
            let newest = || {
                self.sources
                    .iter()
                    .filter_map(|source| source.revision())
                    .map(|revision| revision.edition.successor())
                    .fold(pending(), |newest, edition| newest.max(edition))
                    .successor()
            };
            for (index, (source, line)) in self.sources.iter().zip(&format.lines).enumerate() {
                let Some(layer) = layer(&|| self.overlay_layer(index)) else {
                    continue;
                };
                let rows = source.overlay().select_within(&window, &line.manifest);
                if rows.is_empty() {
                    continue;
                }
                let session = *session.get_or_insert_with(newest);
                let rows: FactStream<'a> = Box::pin(stream::iter(
                    rows.into_iter()
                        .map(move |fact| Ok(FactView::pending(fact, session))),
                ));
                sessions.push(filter_hidden(
                    rows,
                    line.staged.clone(),
                    line.manifest.clone(),
                ));
                layers.extend(layer.map(|layer| (Group::Sessions, layer)));
            }

            // Each memory layer, read live and filtered the same way: a
            // layer's own tombstones hide facts beneath it, never its own. Its
            // rows stand where a session overlay's do.
            for (name, memory) in self.memories() {
                let Some(layer) = layer(&|| name.clone()) else {
                    continue;
                };
                let rows = memory.select_within(&window, &manifest);
                if rows.is_empty() {
                    continue;
                }
                let session = *session.get_or_insert_with(newest);
                let rows: FactStream<'a> = Box::pin(stream::iter(
                    rows.into_iter()
                        .map(move |fact| Ok(FactView::pending(fact, session))),
                ));
                sessions.push(filter_hidden(rows, format.staged.clone(), manifest.clone()));
                layers.extend(layer.map(|layer| (Group::Sessions, layer)));
            }

            // Overlay stream — the per-query changes, read in the lines'
            // format so the rows order as the tree's own. The overlay always
            // carries facts (session metadata at minimum), but MATCHES the
            // typical fact selector rarely: a join's inner premise probes one
            // entity per outer binding, and pushing an empty overlay stream
            // anyway forced the k-way merge (and its per-row sort keys) on
            // every one of those probes. Push the overlay's materialized
            // result only when it has rows, so the single-source common case
            // flows through `merge_grouped`'s passthrough arm.
            if let Some(layer) = layer(&session_layer) {
                let overlay = self.overlay.select_within(&window, &manifest);
                if !overlay.is_empty() {
                    changes.push(Box::pin(stream::iter(
                        overlay.into_iter().map(|fact| Ok(fact.into())),
                    )));
                    layers.extend(layer.map(|layer| (Group::Changes, layer)));
                }
            }

            // Staged layers — a range read each, in the lines' format, and
            // pushed only when they have rows, for the same reason. A staged
            // row is a write the transaction will commit, so it stands at
            // the edition that commit mints, equal to every other write of
            // the transaction: a read over the transaction elects as a read
            // after the commit will.
            for (index, store) in self.layers().iter().enumerate() {
                let Some(layer) = layer(&|| store.layer().cloned().unwrap_or_else(memory_layer))
                else {
                    continue;
                };
                let rows = store.select_within(&window, &manifest);
                if rows.is_empty() {
                    continue;
                }
                let held: HashSet<SortKey> = settled
                    .iter()
                    .filter(|(settled_index, _)| *settled_index == index)
                    .flat_map(|(_, cells)| cells.iter())
                    .flat_map(|cell| cell.held.iter().chain(cell.succeeded.iter()))
                    .map(|fact| sort_key(fact, &manifest))
                    .collect();
                let pending = pending();
                let rows: FactStream<'a> = Box::pin(stream::iter(
                    rows.into_iter()
                        .map(move |fact| Ok(FactView::pending(fact, pending))),
                ));
                if held.is_empty() {
                    staged.push(rows);
                } else {
                    staged.push(filter_hidden(
                        rows,
                        Hidden::default().facts(Arc::new(held)),
                        manifest.clone(),
                    ));
                }
                layers.extend(layer.map(|layer| (Group::Staged, layer)));
            }

            // Most reads meet the lines alone, and take their streams as
            // they are.
            let streams: Vec<FactStream<'a>> =
                if sessions.is_empty() && staged.is_empty() && changes.is_empty() {
                    lines
                } else {
                    sessions
                        .into_iter()
                        .chain(staged)
                        .chain(lines)
                        .chain(changes)
                        .collect()
                };
            if !A::NAMED {
                return Ok(A::rows(merge_grouped(streams, manifest, format.keys)));
            }
            // The layers in the streams' order: the groups are gathered in
            // another order than they are merged in.
            layers.sort_by_key(|(group, _)| *group);
            let layers = layers.into_iter().map(|(_, layer)| layer).collect();
            Ok(A::layered(Reads {
                streams,
                layers,
                manifest,
                keys: format.keys,
            }))
        }
    }
}

/// The groups a read's streams go into the merge by, in that order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Group {
    Sessions,
    Staged,
    Lines,
    Changes,
}

// A range-size estimate for the planner's merge-versus-fold choice:
// the range's edge paths per line, summed. The `Changes` overlay is not
// consulted (small, and irrelevant to the order-of-magnitude answer a
// strategy heuristic needs). Summing lines is an upper bound — a fact
// on two lines counts twice — which is the safe direction for a "how
// broad is this range" question. `None` from every line (all empty)
// yields `None`.
#[dialog_capability::provider]
impl Provider<Estimate> for QueryEnv<'_> {
    async fn execute(
        &self,
        input: Selector<Constrained>,
    ) -> Result<Option<u64>, DialogCorpusError> {
        let mut total: Option<u64> = None;
        for source in &self.sources {
            let select = crate::Select::from_view(source.as_view(), input.clone());
            if let Some(estimate) = select.estimate(self.env as &dyn TreeEnv).await? {
                total = Some(total.unwrap_or(0).saturating_add(estimate));
            }
        }
        Ok(total)
    }
}

// A `Preload` hint forwards to the underlying env's ambient queue —
// the enqueue half of speculative replication; whichever driven
// evaluation stream polls next does the fetching (see
// `crate::repository::fetch`). Forwarding is the whole point of the
// ambient design: every construction site (plain queries,
// subscriptions, transaction queries) emits hints with no per-path
// wiring, and the env's budget decides whether anyone listens.
//
// The one exception is an env none of whose lines has a remote: the
// job a hint becomes warms this query's own lines, and with nowhere to
// fetch from it does nothing (see `warm_source`). Such an env refuses
// hints, which also tells an evaluator to stop composing them: a
// selection that hands each scan many rows otherwise queues a hint per
// row only for it to be dropped.
#[dialog_capability::provider]
impl Provider<Preload> for QueryEnv<'_> {
    async fn execute(&self, input: PreloadRequest) -> bool {
        // Warming ahead of demand has something to do only for a line whose
        // misses hydrate from somewhere.
        let fetches = *self.fetches.get_or_init(|| {
            let routes = crate::Routes::of(self.env);
            self.sources
                .iter()
                .any(|source| routes.fetches(source.as_view().subject().did()))
        });
        if !fetches {
            return false;
        }
        Provider::<Preload>::execute(self.env, input).await
    }
}

/// Whether `load` asks for a block anywhere the query reads (the
/// resolver's wildcard archive, see [`dialog_query::resolver::anywhere`])
/// rather than in one catalog.
fn anywhere(subject: &dialog_capability::Did) -> bool {
    Subject::from(subject.clone()).is_any()
}

// The spilled-value load behind `tree/value`: the same line-by-line
// walk as `Load`, reading each line's spill lane (its blob store, then
// the block catalog for values spilled before they moved to blobs,
// hydrating as a fact scan does). Spilled values are not nodes, so the
// node cache is not consulted. A load of one archive's value is that
// archive's, read through the env.
#[dialog_capability::provider]
impl<'a> Provider<BlobLoad> for QueryEnv<'a> {
    async fn execute(&self, load: BlobLoad) -> Result<Option<Buffer>, ArchiveError> {
        if !anywhere(load.archive.subject()) {
            return load.perform(self.env).await;
        }
        for source in &self.sources {
            let line = BlobLoad {
                archive: source.as_view().archive(),
                ..load.clone()
            };
            if let Some(bytes) = line.perform(self.env).await? {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }
}

// A load of a block anywhere the query reads (the resolver's wildcard
// archive) is tried line by line, each line's catalog in turn; a load of
// one catalog's block is that catalog's, read through the env.
#[dialog_capability::provider]
impl<'a> Provider<ArchiveLoad> for QueryEnv<'a> {
    async fn execute(&self, load: ArchiveLoad) -> Result<Option<Buffer>, ArchiveError> {
        if !anywhere(load.catalog.subject()) {
            return load.perform(self.env).await;
        }
        for source in &self.sources {
            let line = ArchiveLoad {
                catalog: source.as_view().catalog(),
                ..load.clone()
            };
            if let Some(block) = line.perform(self.env).await? {
                return Ok(Some(block));
            }
        }
        Ok(None)
    }
}

// A node read anywhere the query reads is tried line by line, each line's
// catalog in turn, through the env: the env answers from the node cache it
// holds for the line's repository, its store, or a hydration, so a resolver
// join that re-resolves the same reference once per outer row reads the
// node once. A read of one catalog's node is that catalog's.
/// A node is its hash, whichever line holds it, so the lines the env
/// answers at once are taken first and the rest waited on only when none
/// of those had it.
impl<'a> Provider<nodes::Read<CorpusNode>> for QueryEnv<'a> {
    fn execute(
        &self,
        read: nodes::Read<CorpusNode>,
    ) -> Resume<'_, Result<Option<CorpusNode>, DialogSearchTreeError>> {
        if !anywhere(read.catalog.subject()) {
            return read.perform(self.env);
        }
        let mut pending = Vec::new();
        for source in &self.sources {
            let mut line = read.clone();
            line.catalog = source.as_view().catalog();
            match line.perform(self.env).into_now() {
                Ok(Ok(Some(node))) => return Resume::now(Ok(Some(node))),
                Ok(Ok(None)) => {}
                Ok(Err(error)) => return Resume::now(Err(error)),
                Err(reading) => pending.push(reading),
            }
        }
        if pending.is_empty() {
            return Resume::now(Ok(None));
        }
        Resume::later(async move {
            for reading in pending {
                if let Some(node) = reading.await? {
                    return Ok(Some(node));
                }
            }
            Ok(None)
        })
    }
}

impl<'a> Provider<nodes::Preload<CorpusNode>> for QueryEnv<'a> {
    fn execute(&self, preload: nodes::Preload<CorpusNode>) -> Resume<'_, ()> {
        if !anywhere(preload.catalog.subject()) {
            return preload.perform(self.env);
        }
        let mut pending = Vec::new();
        for source in &self.sources {
            let mut line = preload.clone();
            line.catalog = source.as_view().catalog();
            if let Err(preloading) = line.perform(self.env).into_now() {
                pending.push(preloading);
            }
        }
        if pending.is_empty() {
            return Resume::now(());
        }
        Resume::later(async move {
            for preloading in pending {
                preloading.await;
            }
        })
    }
}

// Nodes are written where the commit says, through the env.
impl<'a> Provider<nodes::Write<CorpusNode>> for QueryEnv<'a> {
    fn execute(
        &self,
        write: nodes::Write<CorpusNode>,
    ) -> Resume<'_, Result<(), DialogSearchTreeError>> {
        write.perform(self.env)
    }
}

/// How rules are looked up: by an attribute they derive
/// (`dialog.rule/derives`, keyed by the attribute's `on:` entity). A
/// rule's `conclusion` fact is kept for tooling and is not read here:
/// every install writes the `derives` index, and a rule an earlier
/// release installed without it is inert until
/// [`Branch::upgrade_rules`](crate::Branch::upgrade_rules).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Index {
    Deriving,
}

impl Index {
    fn selector(self, key: &Entity) -> Selector<Constrained> {
        match self {
            Index::Deriving => derives_selector(key),
        }
    }
}

impl<'a> QueryEnv<'a> {
    /// Read a `dialog.rule/*` selector against a single line's committed
    /// tree only (NOT the overlay) and collect the matching facts.
    /// The durable layer's reads must be tree-only so the head-keyed
    /// discovery cache stays correct — overlay rules are handled
    /// separately, fresh, by the transient layer.
    async fn select_tree(
        &self,
        source: &Source,
        selector: Selector<Constrained>,
    ) -> Result<Vec<Fact>, DialogCorpusError> {
        Ok(self
            .select_tree_standing(source, selector)
            .await?
            .into_iter()
            .map(|(fact, _)| fact)
            .collect())
    }

    /// [`select_tree`](Self::select_tree), each fact with the
    /// standing of the commit that wrote it.
    async fn select_tree_standing(
        &self,
        source: &Source,
        selector: Selector<Constrained>,
    ) -> Result<Vec<(Fact, Standing)>, DialogCorpusError> {
        // Rule-discovery reads are demand too: a rule committed
        // later for a subscribed concept lands in this range and
        // must re-trigger the subscription. Recorded as *rule*
        // demand: a hit here invalidates the whole result, not one
        // entity's slice.
        let manifest = &self.format().await?.manifest;
        self.read_rules(&selector, manifest);
        // Rule bodies are hydrated from the full fact, so this read
        // genuinely needs owned rows; it is head-cached, not per-query hot.
        let rows = select_from_source(source, self.env, selector)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut facts = Vec::with_capacity(rows.len());
        for row in rows {
            let fact = match row.to_owned() {
                Ok(fact) => fact,
                Err(DialogCorpusError::CorruptEntry(reason)) => {
                    tracing::warn!(%reason, "ignoring corrupt stored row");
                    continue;
                }
                Err(error) => return Err(error),
            };
            let standing = Standing::new(row.standing(), Claim::from(fact.clone()).cause().clone());
            facts.push((fact, standing));
        }
        Ok(facts)
    }

    /// The rules under `index` at `key` held in `overlay`: a line's
    /// session overlay or the query's own. Two range reads each, and a
    /// body is hydrated once per content address. Recorded as rule
    /// demand, so a subscription re-evaluates when an overlay rule for
    /// the concept or attribute arrives or goes.
    fn overlay_rules(
        &self,
        overlay: &Ephemeral,
        index: Index,
        key: &Entity,
        manifest: &Manifest,
    ) -> Result<Vec<(DeductiveRule, Installed)>, EvaluationError> {
        let conclusions = index.selector(key);
        self.read_rules(&conclusions, manifest);
        let entities = rule_entities(overlay.scan(&conclusions));
        let mut rules = Vec::with_capacity(entities.len());
        for rule_entity in entities {
            let sources = source_selector(&rule_entity);
            self.read_rules(&sources, manifest);
            let Some(bytes) = source_bytes(overlay.scan(&sources)) else {
                continue;
            };
            if let Some(rule) = self.pending_body(rule_entity, &bytes)? {
                rules.push((rule, Installed::Pending));
            }
        }
        Ok(rules)
    }

    /// The rules a staged layer holds under `index` at `key`: two range
    /// reads, noted as an overlay's are. A body that does not hydrate is
    /// skipped.
    fn staged_rules(
        &self,
        layer: &Staged,
        index: Index,
        key: &Entity,
        manifest: &Manifest,
    ) -> Vec<(DeductiveRule, Installed)> {
        let conclusions = index.selector(key);
        self.read_rules(&conclusions, manifest);
        let entities = rule_entities(layer.scan(&conclusions));
        let mut rules = Vec::with_capacity(entities.len());
        for rule_entity in entities {
            let sources = source_selector(&rule_entity);
            self.read_rules(&sources, manifest);
            if let Some(bytes) = source_bytes(layer.scan(&sources))
                && let Ok(Some(rule)) = self.pending_body(rule_entity, &bytes)
            {
                rules.push((rule, Installed::Pending));
            }
        }
        rules
    }

    /// The rule stored under `rule_entity` as `bytes`, the bytes this
    /// read found, from the first line's body cache when the same bytes
    /// were decoded before. A body under an entity it does not hash to is
    /// forged, corrupt, or stored by an earlier release (which
    /// `Branch::upgrade_rules` re-installs), and is inert, as at commit.
    /// So is a body this release does not decode: one spelling the picks
    /// 0.3 and 0.4 wrote, which the upgrade rewrites where it can, or one
    /// in a later format. No read fails over it.
    fn pending_body(
        &self,
        rule_entity: Entity,
        bytes: &[u8],
    ) -> Result<Option<DeductiveRule>, EvaluationError> {
        let cache = self
            .sources
            .first()
            .map(|source| source.as_view().rule_cache());
        if let Some(body) = cache
            .as_ref()
            .and_then(|cache| cache.body(&rule_entity, bytes))
        {
            return Ok(Some(body));
        }
        let Ok(rule) = hydrate(bytes) else {
            return Ok(None);
        };
        if !rule.stored_as(&rule_entity) {
            return Ok(None);
        }
        if let Some(cache) = cache {
            cache.record_body(rule_entity, bytes, rule.clone());
        }
        Ok(Some(rule))
    }

    /// The committed rule entities under `index` at `key` on `source`,
    /// each with the standing of the commit indexing it: discovery
    /// alone, no body read. Cached per (key, head); a head move
    /// (commit/pull) re-scans.
    async fn durable_rule_entities(
        &self,
        source: &Source,
        index: Index,
        key: &Entity,
    ) -> Result<Vec<(Entity, Installed)>, EvaluationError> {
        let cache = source.as_view().rule_cache();
        let head = source.revision();
        let discovered = head.and_then(|h| match index {
            Index::Deriving => cache.derived(key, h),
        });
        if let Some(entities) = discovered {
            // Served from the head's discovery, the read is still one the
            // rule set assembled from it depends on.
            self.read_rules(&index.selector(key), &self.format().await?.manifest);
            return Ok(entities);
        }
        // The moment any resolution scans cold, the whole
        // `dialog.rule/*` region is committed work: this
        // concept's rules read it now, and every concept its
        // rule bodies reference reads it next (rule premises
        // recurse). The region is small — rules, not facts —
        // so hint both spans whole and let the ambient driver
        // replicate them level-parallel while this walk
        // demand-reads; closure depth then finds it local.
        for attribute in [conclusion_attr(), derives_attr(), source_attr()] {
            let listening = Provider::<Preload>::execute(
                self,
                PreloadRequest {
                    selector: Selector::new().the(attribute),
                    likelihood: Likelihood::Likely,
                },
            )
            .await;
            if !listening {
                break;
            }
        }
        let claims = self
            .select_tree_standing(source, index.selector(key))
            .await
            .map_err(|e| EvaluationError::Store(format!("rule index lookup: {e:?}")))?;
        let entities: Vec<(Entity, Installed)> = claims
            .into_iter()
            .map(|(claim, standing)| (claim.of, Installed::Committed(standing)))
            .collect();
        if let Some(head) = head {
            match index {
                Index::Deriving => {
                    cache.record_derived(key.clone(), head.clone(), entities.clone())
                }
            }
        }
        Ok(entities)
    }

    async fn durable_rules(
        &self,
        source: &Source,
        index: Index,
        key: &Entity,
    ) -> Result<Vec<(DeductiveRule, Installed)>, EvaluationError> {
        let rule_entities = self.durable_rule_entities(source, index, key).await?;

        // Each rule's `dialog.rule/source` is read from this line, which
        // is what says the rule is there; only decoding those bytes is
        // cached. Concurrently, because the reads are independent point
        // selects and a head move with N installed rules must not pay N
        // sequential round trips (blocks already in flight join through
        // the env's `Hydrate` flight).
        let rules = try_join_all(rule_entities.into_iter().map(
            |(rule_entity, installed)| async move {
                let source_claims = self
                    .select_tree(source, source_selector(&rule_entity))
                    .await
                    .map_err(|e| EvaluationError::Store(format!("rule source lookup: {e:?}")))?;
                let Some(bytes) = source_bytes(source_claims) else {
                    return Ok::<_, EvaluationError>(None);
                };
                Ok(self
                    .pending_body(rule_entity, &bytes)?
                    .map(|body| (body, installed)))
            },
        ))
        .await?;
        Ok(rules.into_iter().flatten().collect())
    }
}

#[dialog_capability::provider]
impl Provider<SelectRules> for QueryEnv<'_> {
    /// Resolve a concept's deductive rules by unioning across layers:
    /// each line is a durable layer (committed `dialog.rule/*`, head-cached),
    /// the overlay is a transient layer (uncommitted `dialog.rule/*`, fresh).
    /// The implicit per-descriptor rule is assembled once on top.
    ///
    /// The resolved rule set is checked against the program analysis
    /// of its dependency closure: an ill-stratified closure fails
    /// here (exactly like [`RuleRegistry::acquire`]), and a concept
    /// on a (stratified) cycle gets the analysis attached so
    /// evaluation runs the semi-naive fixpoint instead of recursing
    /// top-down unboundedly.
    ///
    /// [`RuleRegistry::acquire`]: dialog_query::session::RuleRegistry::acquire
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        let concept = input.this();

        // An assembled rule set depends only on the rule facts of the
        // layers it was resolved from, so while none of those has changed
        // the last one assembled stands: `key` names every layer's rule
        // facts, a line by its rules root and every overlay and staged
        // layer by their hash, so no layer can hold a rule the key does
        // not see, and a write of plain facts leaves it. A query recording
        // what it reads reuses the set as well, and records the rule
        // reads that assembled it as its own.
        let key = self.rule_key();
        let cache = self
            .sources
            .first()
            .map(|source| source.as_view().rule_cache());
        if let Some((bundle, demanded)) =
            cache.as_ref().and_then(|cache| cache.bundle(&input, &key))
        {
            #[cfg(test)]
            assert_eq!(
                rule_spellings(&bundle),
                rule_spellings(&self.isolated().assemble_bundle(&input).await?),
                "a cached rule set must be the one its layers assemble"
            );
            if let Some(demand) = &self.demand {
                demanded.record(demand);
            }
            return Ok(self.continuing(&concept, bundle));
        }
        let first_read = self
            .reads
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len();
        let bundle = self.assemble_bundle(&input).await?;
        if let Some(cache) = cache {
            cache.record_bundle(
                input.clone(),
                key,
                bundle.clone(),
                self.reads_since(first_read),
            );
        }
        Ok(self.continuing(&concept, bundle))
    }
}

impl QueryEnv<'_> {
    /// The rule set of `input` assembled from the layers: resolved, its
    /// closure analyzed, the rules the analysis sets aside left out, and
    /// the analysis attached when the concept is recursive.
    async fn assemble_bundle(
        &self,
        input: &ConceptDescriptor,
    ) -> Result<ConceptRules, EvaluationError> {
        // Plan cache rides a line (peers share content-addressed plans;
        // any line's cache is correct). The overlay-only query has no
        // line, so it falls back to a private cache.
        let plan_cache = self
            .sources
            .first()
            .map(|source| source.as_view().plan_cache())
            .unwrap_or_default();
        let bundle = self.resolve_bundle(input, plan_cache).await?;
        let analysis = self.program_analysis(input, &bundle).await?;
        analysis.check(input)?;
        let bundle = bundle.without(analysis.quarantined());
        Ok(if analysis.is_recursive(&ProgramAnalysis::node(input)) {
            bundle.with_recursion(analysis)
        } else {
            bundle
        })
    }

    /// Every rule-discovery read noted since `first`: what an entry
    /// assembled since then was read from.
    fn reads_since(&self, first: usize) -> Vec<RuleRead> {
        self.reads
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(first..)
            .map(<[_]>::to_vec)
            .unwrap_or_default()
    }

    /// This environment with nothing recording its reads: what a test
    /// assembles afresh, to compare with what a cache served, without
    /// that work joining the demand or the reads of what is served.
    #[cfg(test)]
    fn isolated(&self) -> Self {
        let mut isolated = self.clone();
        isolated.demand = None;
        isolated.reads = Arc::default();
        isolated
    }
}

/// The rules of `bundle` by identity, in an order that does not depend
/// on how they were installed: what two assemblies of one rule set agree
/// on. A rule is named by its content address, the hash of its canonical
/// spelling, so fresh local names do not tell two assemblies apart; a
/// rule without one (an implicit rule) by the concept it concludes.
#[cfg(test)]
fn rule_spellings(bundle: &ConceptRules) -> Vec<String> {
    let mut spellings: Vec<String> = bundle
        .rules()
        .map(|rule| match rule.try_this() {
            Some(entity) => entity.to_string(),
            None => format!("implicit {}", rule.conclusion().this()),
        })
        .collect();
    spellings.sort();
    spellings
}

/// The entity of the attribute concept of `field`'s relation read
/// plainly, with no listed domain: what a concept's derived fields are
/// keyed by.
fn relation_concept(field: &ConceptFieldDescriptor) -> Entity {
    ConceptDescriptor::of_attribute(&ConceptFieldDescriptor::required(
        field.descriptor().clone().plain(),
    ))
    .this()
}

/// The `dialog.rule/quarantined` attribute, compared on every select.
static QUARANTINED: LazyLock<CorpusRelation> = LazyLock::new(quarantined_attr);

/// The `dialog.rule/invalid` attribute, compared on every select.
static INVALID: LazyLock<CorpusRelation> = LazyLock::new(invalid_attr);

/// Why a stored rule body is not read, or `None` when it is: a rule
/// this release decodes, stored under the identity it hashes to. The
/// body's format is probed before it is decoded, so a body an older or
/// a newer release wrote is told apart from one that is no rule at all.
fn invalid_reason(entity: &Entity, bytes: &[u8]) -> Option<String> {
    use dialog_query::rule::body::{FORMAT, format_of};
    match format_of(bytes) {
        Some(format) if format > FORMAT => {
            return Some(format!(
                "newer: the body is in format {format}, and this release reads up to {FORMAT}"
            ));
        }
        Some(format) if format < FORMAT => {
            // A body an earlier format spelled the same way still reads.
            if hydrate(bytes).is_ok_and(|rule| rule.stored_as(entity))
                || hydrate_inductive(bytes).is_ok()
            {
                return None;
            }
            return Some(format!(
                "older: the body is in format {format}, which `upgrade_rules` rewrites to {FORMAT}"
            ));
        }
        _ => {}
    }
    match hydrate(bytes) {
        Ok(rule) if rule.stored_as(entity) => None,
        Ok(_) => Some("forged: the body does not hash to the identity it is stored under".into()),
        Err(error) => match hydrate_inductive(bytes) {
            Ok(_) => None,
            Err(_) => Some(format!("undecodable: {error}")),
        },
    }
}

impl<'a> QueryEnv<'a> {
    /// The rule facts of the layers this query reads rules from.
    fn rule_key(&self) -> RuleKey {
        let held = |(index, rules): (usize, Option<Blake3Hash>)| Some((index, rules?));
        RuleKey {
            lines: self
                .sources
                .iter()
                .map(|source| {
                    let source = source.as_view();
                    source.rule_cache().rules_root(source.root())
                })
                .collect(),
            overlays: self
                .overlays()
                .map(Ephemeral::rules)
                .enumerate()
                .filter_map(held)
                .collect(),
            staged: self
                .layers
                .iter()
                .map(Staged::rules)
                .enumerate()
                .filter_map(held)
                .collect(),
        }
    }

    /// `dialog.rule/quarantined` rows: one per rule the program analysis
    /// sets aside, of the rule and valued with the concept whose cycle
    /// it closed, narrowed by the selector's entity and value. Nothing
    /// stored under the attribute is read: the rows are the analysis's,
    /// answered over every rule the layers hold.
    async fn select_quarantined(
        &self,
        input: Selector<Constrained>,
    ) -> Result<FactStream<'a>, DialogCorpusError> {
        let quarantined = self
            .quarantined()
            .await
            .map_err(|error| DialogCorpusError::Storage(format!("quarantine: {error}")))?;
        let rows: Vec<Fact> = quarantined
            .into_iter()
            .map(|quarantine| Fact {
                the: QUARANTINED.clone(),
                of: quarantine.rule,
                is: Value::Entity(quarantine.concept),
                cause: None,
            })
            .filter(|row| {
                input.entity().is_none_or(|of| *of == row.of)
                    && input.value().is_none_or(|is| *is == row.is)
            })
            .collect();
        Ok(Box::pin(stream::iter(
            rows.into_iter().map(|row| Ok(row.into())),
        )))
    }

    /// `dialog.rule/invalid` rows: one per stored rule body the layers
    /// hold and this release does not read, of the rule's entity and
    /// valued with why (see [`invalid_reason`]), narrowed by the
    /// selector's entity and value. Nothing stored under the attribute
    /// is read. Such a rule is left out of every evaluation, and this is
    /// where an application learns that it was, and why.
    async fn select_invalid(
        &self,
        input: Selector<Constrained>,
    ) -> Result<FactStream<'a>, DialogCorpusError> {
        let bodies = self
            .rule_bodies()
            .await
            .map_err(|error| DialogCorpusError::Storage(format!("invalid rules: {error}")))?;
        let mut seen: HashSet<Entity> = HashSet::new();
        let mut rows: Vec<Fact> = bodies
            .into_iter()
            .filter(|(entity, _)| seen.insert(entity.clone()))
            .filter_map(|(entity, bytes)| {
                Some(Fact {
                    the: INVALID.clone(),
                    is: Value::String(invalid_reason(&entity, &bytes)?),
                    of: entity,
                    cause: None,
                })
            })
            .filter(|row| {
                input.entity().is_none_or(|of| *of == row.of)
                    && input.value().is_none_or(|is| *is == row.is)
            })
            .collect();
        rows.sort_by(|a, b| a.of.cmp(&b.of));
        Ok(Box::pin(stream::iter(
            rows.into_iter().map(|row| Ok(row.into())),
        )))
    }

    /// Every rule the program analysis sets aside, over every deductive
    /// rule the layers hold, sorted by rule and concept. Kept per rule
    /// key like an assembled rule set, and recorded as rule demand, so
    /// a subscription re-evaluates when a rule is installed or retracted.
    async fn quarantined(&self) -> Result<Vec<Quarantine>, EvaluationError> {
        let key = self.rule_key();
        let cache = self
            .sources
            .first()
            .map(|source| source.as_view().rule_cache());
        if let Some((quarantined, demanded)) =
            cache.as_ref().and_then(|cache| cache.quarantined(&key))
        {
            #[cfg(test)]
            assert_eq!(
                quarantined,
                self.isolated().assemble_quarantined().await?,
                "the rules a cache sets aside must be the ones its layers set aside"
            );
            if let Some(demand) = &self.demand {
                demanded.record(demand);
            }
            return Ok(quarantined);
        }
        let first_read = self
            .reads
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len();
        let quarantined = self.assemble_quarantined().await?;
        if let Some(cache) = cache {
            cache.record_quarantined(key, quarantined.clone(), self.reads_since(first_read));
        }
        Ok(quarantined)
    }

    /// Every rule the program analysis sets aside, assembled from the
    /// layers.
    async fn assemble_quarantined(&self) -> Result<Vec<Quarantine>, EvaluationError> {
        // Each attribute some rule concludes roots an analysis; a cycle
        // lies in the closure of every attribute on it, so the union
        // covers every cycle once.
        let mut seen: HashSet<Entity> = HashSet::new();
        let mut concluded: Vec<ConceptDescriptor> = Vec::new();
        for rule in self.every_rule().await? {
            for (_, field) in rule.conclusion().with().iter() {
                let descriptor = ConceptDescriptor::of_attribute(field);
                if seen.insert(descriptor.this()) {
                    concluded.push(descriptor);
                }
            }
        }
        // The analysis reads premises and never plans.
        let plan_cache = PlanCache::default();
        let mut quarantined: Vec<Quarantine> = Vec::new();
        for descriptor in &concluded {
            let bundle = self.resolve_bundle(descriptor, plan_cache.clone()).await?;
            let analysis = self.program_analysis(descriptor, &bundle).await?;
            for quarantine in analysis.quarantined() {
                if !quarantined.contains(quarantine) {
                    quarantined.push(quarantine.clone());
                }
            }
        }
        quarantined.sort_by(|a, b| (&a.rule, &a.concept).cmp(&(&b.rule, &b.concept)));
        Ok(quarantined)
    }

    /// Every deductive rule the layers hold, once each: each line's
    /// committed rules and session overlay, the staged layers and the
    /// per-query changes. A body under an entity it does not hash to is
    /// inert, as at resolution.
    async fn every_rule(&self) -> Result<Vec<DeductiveRule>, EvaluationError> {
        let bodies = self.rule_bodies().await?;
        let cache = self
            .sources
            .first()
            .map(|source| source.as_view().rule_cache());
        let mut rules: Vec<DeductiveRule> = Vec::new();
        let mut seen: HashSet<Entity> = HashSet::new();
        for (entity, bytes) in bodies {
            if !seen.insert(entity.clone()) {
                continue;
            }
            let cached = cache.as_ref().and_then(|cache| cache.body(&entity, &bytes));
            let rule = match cached {
                Some(rule) => rule,
                // Inductive rules share the attribute and do not decode.
                None => match hydrate(&bytes) {
                    Ok(rule) if rule.stored_as(&entity) => rule,
                    _ => continue,
                },
            };
            rules.push(rule);
        }
        Ok(rules)
    }

    /// Every stored rule body the layers hold, by the entity it is
    /// stored under: each line's committed rules and session overlay,
    /// the staged layers and the per-query changes. An entity a later
    /// layer also holds appears once per layer, the line's first.
    async fn rule_bodies(&self) -> Result<Vec<(Entity, Vec<u8>)>, EvaluationError> {
        let attribute = source_attr();
        let selector = Selector::new().the(attribute.clone());
        let manifest = &self.format().await?.manifest;
        let mut bodies: Vec<(Entity, Vec<u8>)> = Vec::new();
        let mut take = |facts: Vec<Fact>| {
            for fact in facts {
                if let Value::Bytes(bytes) = fact.is {
                    bodies.push((fact.of, bytes));
                }
            }
        };
        for source in &self.sources {
            take(
                self.select_tree(source, selector.clone())
                    .await
                    .map_err(|e| EvaluationError::Store(format!("rule source scan: {e:?}")))?,
            );
            self.read_rules(&selector, manifest);
            take(source.overlay().scan(&selector));
        }
        for layer in &self.layers {
            take(layer.scan(&selector));
        }
        take(self.overlay.scan(&selector));
        Ok(bodies)
    }
}

impl BodyMemo for QueryEnv<'_> {
    fn memo(&self) -> Option<&Memo> {
        Some(&self.memo)
    }
}

impl QueryEnv<'_> {
    /// `bundle` carrying this query's retained fixpoint, when a polling
    /// subscription is evaluating `concept` recursively. Attached per
    /// query, never cached: it belongs to the subscription.
    fn continuing(&self, concept: &Entity, bundle: ConceptRules) -> ConceptRules {
        match &self.fixpoint {
            Some((entity, continuation)) if entity == concept && bundle.recursion().is_some() => {
                bundle.with_continuation(continuation.clone())
            }
            _ => bundle,
        }
    }
}

impl<'a> QueryEnv<'a> {
    /// Every rule under `index` at `key`, unioned across layers: each
    /// line's durable layer (committed, head-cached) and session
    /// overlay, the per-query overlay, and the staged layers, the last
    /// three read fresh. Each rule comes with when it was installed: a
    /// committed rule at the standing of the commit indexing it, any
    /// other as pending, newer than every commit.
    #[tracing::instrument(skip_all, name = "resolve_rules")]
    async fn resolve_rules(
        &self,
        index: Index,
        key: &Entity,
    ) -> Result<Vec<(DeductiveRule, Installed)>, EvaluationError> {
        let mut rules: Vec<(DeductiveRule, Installed)> = Vec::new();
        let manifest = &self.format().await?.manifest;
        for source in &self.sources {
            rules.extend(self.durable_rules(source, index, key).await?);
            rules.extend(self.overlay_rules(source.overlay(), index, key, manifest)?);
        }
        for (_, memory) in self.memories() {
            rules.extend(self.overlay_rules(memory, index, key, manifest)?);
        }
        rules.extend(self.overlay_rules(&self.overlay, index, key, manifest)?);
        for layer in &self.layers {
            rules.extend(self.staged_rules(layer, index, key, manifest));
        }
        Ok(rules)
    }

    /// The head of `rule` re-spelled onto the attribute concept
    /// `attribute`, cached on the first line by content address.
    fn head_for(
        &self,
        rule: &DeductiveRule,
        on: &Entity,
    ) -> Result<Option<DeductiveRule>, EvaluationError> {
        let cache = self
            .sources
            .first()
            .map(|source| source.as_view().rule_cache());
        let identity = rule.try_this();
        if let (Some(cache), Some(identity)) = (&cache, &identity)
            && let Some(head) = cache.head(identity, on)
        {
            return Ok(Some(head));
        }
        let head = head_onto(rule, on)?;
        if let (Some(cache), Some(identity), Some(head)) = (cache, identity, &head) {
            cache.record_head(identity, on.clone(), head.clone());
        }
        Ok(head)
    }

    /// Whether some rule derives the relation `the` names, as
    /// [`resolve_bundle`](Self::resolve_bundle) would find one for the
    /// attribute concept over it: a built-in, or a rule under the
    /// relation's `derives` key on any layer. Only the index is read
    /// from the key the attribute spells; no concept is described or
    /// hashed and no body is hydrated, which is what a commit asks once
    /// per relation it writes.
    pub(crate) async fn rules_derive(&self, the: &CorpusRelation) -> Result<bool, EvaluationError> {
        let Some(on) = on_entity(the) else {
            return Ok(false);
        };
        if builtin_derives(&on) {
            return Ok(true);
        }
        let selector = Index::Deriving.selector(&on);
        let manifest = &self.format().await?.manifest;
        for source in &self.sources {
            if !self
                .durable_rule_entities(source, Index::Deriving, &on)
                .await?
                .is_empty()
            {
                return Ok(true);
            }
            self.read_rules(&selector, manifest);
            if !source.overlay().scan(&selector).is_empty() {
                return Ok(true);
            }
        }
        self.read_rules(&selector, manifest);
        if !self.overlay.scan(&selector).is_empty() {
            return Ok(true);
        }
        Ok(self
            .layers
            .iter()
            .any(|layer| !layer.scan(&selector).is_empty()))
    }

    /// The rule bundle for `descriptor`, resolved from every layer.
    ///
    /// A built-in concept is exact: nothing stores or derives its
    /// attributes besides the engine, so its rules install as written.
    /// An attribute concept is the relation of its attribute: the
    /// implicit scan plus the head of every rule deriving the
    /// attribute, found by the `derives` index and by the built-ins.
    /// Any other concept selects: its rule reads each derived attribute
    /// through the attribute concept and the rest from stored facts, so
    /// a rule written against a wider or narrower concept still answers
    /// the attributes it derives. Neither path reads a rule's
    /// `conclusion` (see [`Index`]).
    #[tracing::instrument(skip_all, name = "resolve_bundle")]
    async fn resolve_bundle(
        &self,
        descriptor: &ConceptDescriptor,
        plan_cache: PlanCache,
    ) -> Result<ConceptRules, EvaluationError> {
        let concept = descriptor.this();

        if let Some((_, field)) = descriptor.attribute_field() {
            let canonical = ConceptDescriptor::of_attribute(field);
            let mut bundle = ConceptRules::with_plan_cache(&canonical, plan_cache);
            for head in builtin_deriving(&concept) {
                bundle.install(head);
            }
            // The one source rule every head comes from, if it is one
            // and none of them folds: while nothing is stored under the
            // attribute, that rule re-headed onto the concept is its
            // whole answer and needs no election.
            let mut sole: Option<Option<DeductiveRule>> = None;
            let mut note = |head: &DeductiveRule| {
                sole = Some(match (sole.take(), head.origin()) {
                    // Ruled out once, ruled out for good: a later head
                    // split from a source never reinstates it.
                    (Some(None), _) | (_, None) => None,
                    (None, Some(origin)) => Some(origin.rule.clone()),
                    (Some(Some(known)), Some(origin)) if known.same(&origin.rule) => Some(known),
                    (Some(Some(_)), Some(_)) => None,
                });
            };
            // The rules deriving the relation the field reads, found by
            // the `derives` index, whatever type or cardinality this
            // read declares over the relation: the key is the relation's
            // `on:` entity.
            let single = ConceptDescriptor::of_attribute(&ConceptFieldDescriptor::required(
                field.descriptor().clone().plain(),
            ));
            if let Some(on) = derives_keys(&single).into_iter().next() {
                for (rule, installed) in self.resolve_rules(Index::Deriving, &on).await? {
                    if let Some(head) = self.head_for(&rule, &on)? {
                        note(&head);
                        bundle.install_at(head, installed);
                    }
                }
            }
            if builtin_deriving(&concept).is_empty()
                && let Some(Some(source)) = sole
                && let Some(attribute) = field.the().attribute()
                && let Some(covering) = source
                    .covering(&canonical)
                    .map_err(|error| EvaluationError::Store(format!("covering rule: {error}")))?
            {
                return Ok(bundle.with_exact(Exact {
                    rule: covering,
                    attributes: vec![attribute],
                }));
            }
            return Ok(bundle);
        }

        // A built-in concept is a closed view: its rows are tuples over
        // other entities (an upstream's name, subject and peer flattened
        // onto the branch tracking it), which per-attribute selection
        // would pair across upstreams. Nothing stores or derives its
        // attributes besides the engine, so it evaluates as written.
        let builtins = builtin(&concept);
        if !builtins.is_empty() {
            return Ok(assemble(descriptor, builtins, plan_cache));
        }

        let mut derived: HashSet<Entity> = HashSet::new();
        let mut deriving: HashSet<Entity> = HashSet::new();
        // The one source rule deriving every derived attribute, if it is
        // one: the concept's exact evaluation while nothing is stored
        // under them. A built-in head, a reducing rule, a second source
        // or a keyed collection rules it out.
        let mut sole: Option<Option<(DeductiveRule, Vec<dialog_corpus::Relation>)>> = None;
        for (_, field) in descriptor.with().iter() {
            let attribute = ConceptDescriptor::of_attribute(field);
            // Keyed by the relation's own attribute concept, read
            // plainly, which is what a legacy rule concluded.
            let entity = relation_concept(field);
            let builtins = builtin_deriving(&entity);
            let mut rules = builtins.clone();
            for on in derives_keys(&attribute) {
                rules.extend(
                    self.resolve_rules(Index::Deriving, &on)
                        .await?
                        .into_iter()
                        .map(|(rule, _)| rule),
                );
            }
            if !rules.is_empty() {
                derived.insert(entity.clone());
            }
            if !rules.is_empty() {
                let candidate = match (&sole, field.the().attribute()) {
                    (Some(None), _) | (_, None) => None,
                    _ if !builtins.is_empty() => None,
                    (current, Some(attribute)) => {
                        let mut found: Option<(DeductiveRule, Vec<dialog_corpus::Relation>)> =
                            current.clone().flatten();
                        let mut ok = true;
                        for rule in &rules {
                            if !rule.reduce().is_empty() {
                                ok = false;
                                break;
                            }
                            match &mut found {
                                None => found = Some((rule.clone(), Vec::new())),
                                Some((known, _)) if known.same(rule) => {}
                                Some(_) => {
                                    ok = false;
                                    break;
                                }
                            }
                        }
                        if ok {
                            if let Some((_, attributes)) = &mut found {
                                attributes.push(attribute);
                            }
                            found
                        } else {
                            None
                        }
                    }
                };
                sole = Some(candidate);
            }
            deriving.extend(rules.iter().filter_map(DeductiveRule::try_this));
        }
        // Nothing derived: the descriptor's own implicit rule, whose
        // plans it memoizes, is the selecting rule.
        let bundle = if derived.is_empty() {
            ConceptRules::with_plan_cache(descriptor, plan_cache)
        } else {
            // The selecting rule and the covering rule are functions of
            // the concept, the derived attributes and the sole source:
            // built once per branch and kept.
            let cache = self
                .sources
                .first()
                .map(|source| source.as_view().rule_cache());
            let mut derived_key: Vec<Entity> = derived.iter().cloned().collect();
            derived_key.sort();
            let sole = sole.flatten();
            let sole_key = sole.as_ref().and_then(|(rule, _)| rule.try_this());
            let selecting = match cache
                .as_ref()
                .and_then(|cache| cache.selecting(descriptor, &derived_key, &sole_key))
            {
                Some(selecting) => selecting,
                None => {
                    let rule = DeductiveRule::selecting(descriptor, &|field| {
                        derived.contains(&relation_concept(field))
                    })
                    .map_err(|error| EvaluationError::Store(format!("selecting rule: {error}")))?;
                    let exact = match sole {
                        Some((source, attributes)) => source
                            .covering(descriptor)
                            .map_err(|error| {
                                EvaluationError::Store(format!("covering rule: {error}"))
                            })?
                            .map(|rule| Exact { rule, attributes }),
                        None => None,
                    };
                    let selecting = Selecting {
                        descriptor: descriptor.clone(),
                        rule,
                        exact,
                    };
                    if let Some(cache) = cache {
                        cache.record_selecting(derived_key, sole_key, selecting.clone());
                    }
                    selecting
                }
            };
            let bundle = ConceptRules::with_implicit(selecting.rule, true, plan_cache);
            match selecting.exact {
                Some(exact) => bundle.with_exact(exact),
                None => bundle,
            }
        };
        Ok(bundle)
    }

    /// The program analysis over the rule set reachable from `root`:
    /// every concept referenced (transitively) by a resolved rule's
    /// concept premises contributes its own resolved rules, so
    /// cycles that span concepts — including ones closed entirely by
    /// durable rules — are visible.
    ///
    /// Per-concept rule discovery is head-cached
    /// ([`durable_rules`](Self::durable_rules)), so the walk is
    /// cheap after the first query at a given head.
    async fn program_analysis(
        &self,
        root: &ConceptDescriptor,
        root_bundle: &ConceptRules,
    ) -> Result<Arc<ProgramAnalysis>, EvaluationError> {
        fn referenced(bundle: &ConceptRules, queue: &mut Vec<ConceptDescriptor>) {
            for rule in bundle.rules() {
                for premise in rule.analysis().premises() {
                    match premise {
                        Premise::Assert(Proposition::Concept(query))
                        | Premise::Unless(Negation(Proposition::Concept(query))) => {
                            queue.push(query.predicate.clone());
                        }
                        _ => {}
                    }
                }
            }
        }

        let mut entries: Vec<(Entity, ConceptDescriptor, ConceptRules)> = Vec::new();
        let mut seen = HashSet::new();
        let mut queue = Vec::new();

        seen.insert(ProgramAnalysis::node(root));
        referenced(root_bundle, &mut queue);
        entries.push((
            ProgramAnalysis::node(root),
            root.clone(),
            root_bundle.clone(),
        ));

        // Level by level: everything a frontier references is known
        // needed, so each level's concepts resolve their rules
        // concurrently instead of paying one round trip per concept.
        // Only closure depth remains a sequential cost, and the span
        // hints `durable_rules` emits usually make deeper levels local.
        while !queue.is_empty() {
            let mut frontier = Vec::new();
            while let Some(descriptor) = queue.pop() {
                let entity = ProgramAnalysis::node(&descriptor);
                if seen.insert(entity.clone()) {
                    frontier.push((entity, descriptor));
                }
            }
            let resolved =
                try_join_all(frontier.into_iter().map(|(entity, descriptor)| async move {
                    // The analysis reads premises and never plans, so
                    // these bundles share the root's cache rather than
                    // allocating one each.
                    let bundle = self
                        .resolve_bundle(&descriptor, root_bundle.plan_cache().clone())
                        .await?;
                    Ok::<_, EvaluationError>((entity, descriptor, bundle))
                }))
                .await?;
            for (entity, descriptor, bundle) in resolved {
                referenced(&bundle, &mut queue);
                entries.push((entity, descriptor, bundle));
            }
        }

        // The attribute concepts some rule derives: what a concept the
        // walk never resolved would read through.
        let derived: HashSet<Entity> = entries
            .iter()
            .filter(|(_, descriptor, bundle)| {
                descriptor.attribute_field().is_some() && !bundle.installed().is_empty()
            })
            .map(|(entity, _, _)| entity.clone())
            .collect();

        Ok(Arc::new(ProgramAnalysis::analyze_with(
            entries.iter().map(|(entity, _, bundle)| (entity, bundle)),
            derived,
        )))
    }
}

impl Branch {
    /// Open a query over this branch.
    ///
    /// Returns a [`QueryLayer`] rooted at the branch. Use
    /// [`with`](QueryLayer::with) to fold in a [`Statement`]'s
    /// changes, [`join`](QueryLayer::join) to add another branch or a
    /// [`Changes`] overlay, then [`select`](QueryLayer::select) +
    /// `.perform(&env)`. Schema metadata is auto-injected at perform
    /// time — no manual overlay needed.
    pub fn query(&self) -> QueryLayer<'_> {
        QueryLayer::from(self)
    }

    /// Open a query over this branch with `statement` folded into the
    /// overlay in one step. Shorthand for `self.query().with(stmt)`.
    pub fn with<S: Statement>(&self, statement: S) -> QueryLayer<'_> {
        self.query().with(statement)
    }
}

/// Layered deductive-rule resolution — exhaustive coverage of the
/// caching invariants. Each test isolates one behaviour the durable
/// (committed, head-cached) and transient (overlay, fresh) layers must
/// satisfy.
#[cfg(test)]
mod rule_tests {
    use dialog_effects::archive::Load as ArchiveLoad;
    use dialog_effects::blob::Load as BlobLoad;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::{Counting, connect, test_repo};
    use crate::{Branch, Repository, Transaction};
    use dialog_corpus::Update as _;
    use dialog_credentials::Credential;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_peer::{Peer, Session};
    use dialog_query::concept::descriptor::{ConceptConclusion, ConceptDescriptor};
    use dialog_query::concept::query::ConceptQuery;
    use dialog_query::rule::DeductiveRuleDescriptor;
    use dialog_query::{DeductiveRule, Parameters, Term, the};
    use dialog_storage::provider::storage::VolatileSpace;

    /// Conclusion concept `employee` (one `name` field). Derived — no
    /// `employee` fact is ever written; rows come only from rules.
    fn employee_descriptor() -> ConceptDescriptor {
        serde_json::from_value(serde_json::json!({
            "with": { "name": { "the": "org/employee-name", "as": "text:" } }
        }))
        .expect("employee descriptor parses")
    }

    /// A deductive rule: an `employee` is anyone with an
    /// `org/person-name` fact, projected as `employee-name`.
    fn employee_from_person() -> DeductiveRule {
        rule_with_person_attr("org/person-name")
    }

    /// Same shape but reading a different person attribute — a *distinct*
    /// rule body, so a distinct content-addressed identity.
    fn rule_with_person_attr(attr: &str) -> DeductiveRule {
        let json = serde_json::json!({
            "deduce": { "with": { "name": { "the": "org/employee-name", "as": "text:" } } },
            "when": [{
                "assert": { "with": { "name": { "the": attr, "as": "text:" } } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } }
                }
            }]
        });
        let d: DeductiveRuleDescriptor = serde_json::from_value(json).expect("descriptor parses");
        d.compile().expect("rule compiles")
    }

    /// Query `employee` and return the derived entities.
    async fn query_employees<Env>(branch: &Branch, operator: &Env) -> anyhow::Result<Vec<Entity>>
    where
        Env: Provider<BlobRead>
            + dialog_capability::Provider<Get>
            + dialog_capability::Provider<Put>
            + dialog_capability::Provider<Resolve>
            + dialog_capability::Provider<Identify>
            + dialog_capability::Provider<ArchiveLoad>
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + dialog_capability::Provider<BlobLoad>
            + dialog_capability::Provider<dialog_corpus::Preload>
            + dialog_capability::Provider<dialog_corpus::Speculation>
            + dialog_capability::Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("name".into(), Term::var("name"));
        let query = ConceptQuery {
            predicate: employee_descriptor(),
            terms,
        };
        let rows: Vec<ConceptConclusion> = branch
            .query()
            .select(query)
            .perform(operator)
            .try_vec()
            .await?;
        Ok(rows.iter().map(|c| c.entity().clone()).collect())
    }

    /// Query `employee` through a transaction's view.
    async fn transaction_employees(
        tx: &Transaction<&Branch>,
        operator: &Peer<VolatileSpace, Session>,
    ) -> anyhow::Result<Vec<Entity>> {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("name".into(), Term::var("name"));
        let rows: Vec<ConceptConclusion> = tx
            .query()
            .select(ConceptQuery {
                predicate: employee_descriptor(),
                terms,
            })
            .perform(operator)
            .try_vec()
            .await?;
        Ok(rows.iter().map(|c| c.entity().clone()).collect())
    }

    /// Query `staff` (one `name` field over `org/staff-name`) and return
    /// the derived entities.
    async fn staff(
        branch: &Branch,
        operator: &Peer<VolatileSpace, Session>,
    ) -> anyhow::Result<Vec<Entity>> {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("name".into(), Term::var("name"));
        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({
            "with": { "name": { "the": "org/staff-name", "as": "text:" } }
        }))?;
        let rows: Vec<ConceptConclusion> = branch
            .query()
            .select(ConceptQuery { predicate, terms })
            .perform(operator)
            .try_vec()
            .await?;
        Ok(rows.iter().map(|c| c.entity().clone()).collect())
    }

    /// A concept over `org/person-name` under the field `field`.
    fn person_under(field: &str) -> ConceptDescriptor {
        serde_json::from_value(serde_json::json!({
            "with": { field: { "the": "org/person-name", "as": "text:" } }
        }))
        .expect("descriptor parses")
    }

    /// Names read through a concept with a single field `field`.
    async fn names_under<Env>(
        branch: &Branch,
        operator: &Env,
        field: &str,
    ) -> anyhow::Result<Vec<String>>
    where
        Env: Provider<BlobRead>
            + dialog_capability::Provider<Get>
            + dialog_capability::Provider<Put>
            + dialog_capability::Provider<Resolve>
            + dialog_capability::Provider<Identify>
            + dialog_capability::Provider<ArchiveLoad>
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + dialog_capability::Provider<BlobLoad>
            + dialog_capability::Provider<dialog_corpus::Preload>
            + dialog_capability::Provider<dialog_corpus::Speculation>
            + dialog_capability::Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert(field.into(), Term::var("value"));
        let query = ConceptQuery {
            predicate: person_under(field),
            terms,
        };
        let rows: Vec<ConceptConclusion> = branch
            .query()
            .select(query)
            .perform(operator)
            .try_vec()
            .await?;
        rows.iter()
            .map(|row| Ok(row.get::<String>(field)?))
            .collect()
    }

    /// A rule concluding both `employee-name` and `employee-role` from
    /// a person, as one head.
    fn employee_pair_rule() -> DeductiveRule {
        let json = serde_json::json!({
            "deduce": { "with": {
                "name": { "the": "org/employee-name", "as": "text:" },
                "role": { "the": "org/employee-role", "as": "text:" }
            }},
            "when": [{
                "assert": { "with": {
                    "name": { "the": "org/person-name", "as": "text:" },
                    "role": { "the": "org/person-role", "as": "text:" }
                }},
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } },
                    "role": { "?": { "name": "role" } }
                }
            }]
        });
        let d: DeductiveRuleDescriptor = serde_json::from_value(json).expect("descriptor parses");
        d.compile().expect("rule compiles")
    }

    /// A rule deriving one `org/<to>` attribute from one `org/<from>`
    /// attribute, both under the field `field`.
    fn projection(from: &str, to: &str, field: &str) -> DeductiveRule {
        let json = serde_json::json!({
            "deduce": { "with": { field: { "the": to, "as": "text:" } } },
            "when": [{
                "assert": { "with": { field: { "the": from, "as": "text:" } } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    field: { "?": { "name": field } }
                }
            }]
        });
        let d: DeductiveRuleDescriptor = serde_json::from_value(json).expect("descriptor parses");
        d.compile().expect("rule compiles")
    }

    /// Rows of a concept with the given `(field, attribute)` columns,
    /// as `(entity, values in field order)`.
    async fn rows_of<Env>(
        branch: &Branch,
        operator: &Env,
        fields: &[(&str, &str)],
    ) -> anyhow::Result<Vec<(Entity, Vec<String>)>>
    where
        Env: Provider<BlobRead>
            + dialog_capability::Provider<Get>
            + dialog_capability::Provider<Put>
            + dialog_capability::Provider<Resolve>
            + dialog_capability::Provider<Identify>
            + dialog_capability::Provider<ArchiveLoad>
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + dialog_capability::Provider<BlobLoad>
            + dialog_capability::Provider<dialog_corpus::Preload>
            + dialog_capability::Provider<dialog_corpus::Speculation>
            + dialog_capability::Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        let with: serde_json::Map<String, serde_json::Value> = fields
            .iter()
            .map(|(field, the)| {
                (
                    field.to_string(),
                    serde_json::json!({ "the": the, "as": "text:" }),
                )
            })
            .collect();
        let predicate: ConceptDescriptor =
            serde_json::from_value(serde_json::json!({ "with": with }))?;
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        for (field, _) in fields {
            terms.insert(field.to_string(), Term::var(*field));
        }
        let rows: Vec<ConceptConclusion> = branch
            .query()
            .select(ConceptQuery { predicate, terms })
            .perform(operator)
            .try_vec()
            .await?;
        let mut out = Vec::new();
        for row in rows {
            let mut values = Vec::new();
            for (field, _) in fields {
                values.push(row.get::<String>(field)?);
            }
            out.push((row.entity().clone(), values));
        }
        out.sort_by_key(|(entity, values)| format!("{entity}{values:?}"));
        Ok(out)
    }

    /// A rule derives one relation per head attribute: a concept over
    /// one of a two-attribute head's attributes sees the derivation,
    /// though the rule was never written against it.
    #[dialog_common::test]
    async fn it_sees_a_superset_rule_from_a_subset_concept() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(
                the!("org/person-role")
                    .of(alice.clone())
                    .is("admin".to_string()),
            )
            .assert(employee_pair_rule())
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        assert_eq!(
            rows_of(&branch, &operator, &[("name", "org/employee-name")]).await?,
            vec![(alice.clone(), vec!["Alice".to_string()])],
            "the name half of the head reaches a concept over the name alone"
        );
        assert_eq!(
            rows_of(&branch, &operator, &[("role", "org/employee-role")]).await?,
            vec![(alice.clone(), vec!["admin".to_string()])],
        );
        assert_eq!(
            rows_of(
                &branch,
                &operator,
                &[("name", "org/employee-name"), ("role", "org/employee-role")]
            )
            .await?,
            vec![(alice, vec!["Alice".to_string(), "admin".to_string()])],
            "and the concept the rule was written against still sees both"
        );
        Ok(())
    }

    /// Attributes derived by different rules join on the entity, as
    /// stored attributes do: neither rule knows the concept reading
    /// them together.
    #[dialog_common::test]
    async fn it_joins_attributes_derived_by_different_rules() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(
                the!("org/person-role")
                    .of(alice.clone())
                    .is("admin".to_string()),
            )
            .assert(
                the!("org/person-name")
                    .of(bob.clone())
                    .is("Bob".to_string()),
            )
            .assert(projection("org/person-name", "org/employee-name", "name"))
            .assert(projection("org/person-role", "org/employee-role", "role"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        assert_eq!(
            rows_of(
                &branch,
                &operator,
                &[("name", "org/employee-name"), ("role", "org/employee-role")]
            )
            .await?,
            vec![(alice, vec!["Alice".to_string(), "admin".to_string()])],
            "bob has a derived name but no derived role, so no employee row"
        );
        Ok(())
    }

    /// A derived value competes with a stored one under the attribute's
    /// election: one value per entity leaves a cardinality-one
    /// attribute concept, and the newer standing wins.
    #[dialog_common::test]
    async fn it_elects_between_a_stored_and_a_derived_value() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("org/employee-name")
                    .of(alice.clone())
                    .is("Stored".to_string()),
            )
            .assert(projection("org/person-name", "org/employee-name", "name"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Derived".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        assert_eq!(
            rows_of(&branch, &operator, &[("name", "org/employee-name")]).await?,
            vec![(alice.clone(), vec!["Derived".to_string()])],
            "the derived value stands as recent as the fact it came from, which is newer"
        );

        branch
            .transaction()
            .assert(
                the!("org/employee-name")
                    .of(alice.clone())
                    .is("Restored".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            rows_of(&branch, &operator, &[("name", "org/employee-name")]).await?,
            vec![(alice, vec!["Restored".to_string()])],
            "a newer stored value wins the election back"
        );
        Ok(())
    }

    /// Two concepts over the same attributes under different field names
    /// share an identity, since identity ignores field names. Each must
    /// still be answered by its own implicit rule: querying one first
    /// must not leave the other planned over the first one's fields.
    #[dialog_common::test]
    async fn it_answers_concepts_that_differ_only_in_field_names() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(Entity::new()?)
                    .is("Alice".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        assert_eq!(person_under("name").this(), person_under("label").this());
        assert_eq!(
            names_under(&branch, &operator, "name").await?,
            vec!["Alice"]
        );
        assert_eq!(
            names_under(&branch, &operator, "label").await?,
            vec!["Alice"]
        );
        Ok(())
    }

    // ----- (1) committed rule resolves via the durable (tree) layer ----

    #[dialog_common::test]
    async fn it_resolves_a_committed_rule() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(employee_from_person())
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        // refresh handle so the durable layer sees the new head
        let branch = repo.branch("main").open().perform(&operator).await?;

        let employees = query_employees(&branch, &operator).await?;
        assert!(employees.contains(&alice), "committed rule must resolve");
        Ok(())
    }

    /// Resolving a concept's rules cold hints the whole `dialog.rule/*`
    /// region (conclusion and source spans) into the env's ambient
    /// queue: the region is committed work the moment any resolution
    /// scans it, and warming it whole makes every deeper closure
    /// level's discovery and hydration local. The query's own driven
    /// stream executes the hints, leaving nothing pending.
    ///
    /// The branch tracks a remote, since hints are only taken from a
    /// query whose lines can fetch; the remote is never reached, because
    /// every block the query reads is local.
    #[dialog_common::test]
    async fn it_hints_the_rule_region_when_resolving_cold() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let env = Counting::new(operator);
        let branch = repo.branch("main").open().perform(&env).await?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of("id:alice".parse::<Entity>()?)
                    .is("Alice".to_string()),
            )
            .assert(employee_from_person())
            .commit()
            .publish()
            .perform(&env)
            .await?;
        let site = dialog_remote_s3::Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("bucket")
            .build()?;
        let origin = connect("origin", site, repo.did(), &env).await?;
        let remote_main = origin.branch("main").open().perform(&env).await?;
        branch.set_upstream(&remote_main).perform(&env).await?;
        let branch = repo.branch("main").open().perform(&env).await?;

        env.reset();
        let employees = query_employees(&branch, &env).await?;
        assert!(
            employees.contains(&"id:alice".parse()?),
            "committed rule must resolve"
        );
        assert!(
            env.count("Preload") >= 2,
            "cold rule resolution hints the conclusion and source spans"
        );
        let queue = Provider::<Speculation>::execute(&env, ()).await;
        assert_eq!(queue.pending(), 0, "the query's own stream drove the hints");
        Ok(())
    }

    /// A query whose lines have no remote reads only local blocks, so
    /// there is nothing to warm ahead of it: it takes no hints, and
    /// resolves the same.
    #[dialog_common::test]
    async fn it_takes_no_hints_over_local_lines() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let env = Counting::new(operator);
        let branch = repo.branch("main").open().perform(&env).await?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of("id:alice".parse::<Entity>()?)
                    .is("Alice".to_string()),
            )
            .assert(employee_from_person())
            .commit()
            .publish()
            .perform(&env)
            .await?;
        let branch = repo.branch("main").open().perform(&env).await?;

        env.reset();
        let employees = query_employees(&branch, &env).await?;
        assert!(
            employees.contains(&"id:alice".parse()?),
            "committed rule must resolve"
        );
        assert_eq!(env.count("Preload"), 0, "a local query forwards no hints");
        let queue = Provider::<Speculation>::execute(&env, ()).await;
        assert_eq!(queue.pending(), 0, "nothing was queued");
        Ok(())
    }

    /// A department's highest salary: a concept whose field reads the
    /// relation the salary rules derive, `org/dept-salary`, and picks
    /// its greatest claim.
    fn dept_top() -> anyhow::Result<ConceptDescriptor> {
        Ok(serde_json::from_value(serde_json::json!({ "with": {
            "top": { "the": "org/dept-salary", "as": "natural:", "pick": "max" }
        }}))?)
    }

    /// A committed rule derives a relation a ranked field reads: the
    /// rule stores, discovers and hydrates through the `db.rule/*`
    /// rail, and a field picking `max` over its relation chooses among
    /// the candidates it derives over committed facts.
    #[dialog_common::test]
    async fn it_elects_over_a_relation_a_committed_rule_derives() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // dept-salary(dept, salary) :- dept(employee, dept), salary(employee, salary)
        let rule = {
            let json = serde_json::json!({
                "deduce": { "with": {
                    "salary": { "the": "org/dept-salary", "as": "natural:", "cardinality": "many" }
                }},
                "when": [{
                    "assert": { "with": {
                        "dept": { "the": "org/dept", "as": "entity:" },
                        "salary": { "the": "org/salary", "as": "natural:" }
                    }},
                    "where": {
                        "this": { "?": { "name": "employee" } },
                        "dept": { "?": { "name": "this" } },
                        "salary": { "?": { "name": "salary" } }
                    }
                }]
            });
            let descriptor: DeductiveRuleDescriptor =
                serde_json::from_value(json).expect("descriptor parses");
            descriptor.compile().expect("rule compiles")
        };
        let dept_top = dept_top()?;

        let dept: Entity = "id:dept-a".parse()?;
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        branch
            .transaction()
            .assert(the!("org/dept").of(alice.clone()).is(dept.clone()))
            .assert(the!("org/salary").of(alice.clone()).is(3u32))
            .assert(the!("org/dept").of(bob.clone()).is(dept.clone()))
            .assert(the!("org/salary").of(bob.clone()).is(4u32))
            .assert(&rule)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("dept"));
        terms.insert("top".into(), Term::var("top"));
        let rows: Vec<ConceptConclusion> = branch
            .query()
            .select(ConceptQuery {
                predicate: dept_top,
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(rows.len(), 1, "one elected row for the department");
        assert_eq!(*rows[0].entity(), dept);
        assert_eq!(
            rows[0].get::<u64>("top")?,
            4,
            "the query chose among what the hydrated rule derives"
        );
        Ok(())
    }

    /// A ranked field reads every rule's candidates: the committed rule is found
    /// first and the query's overlay rule after it, and a rule folding
    /// their relation with `max` chooses among what both derive per
    /// entity, never taking either rule alone for the whole answer.
    #[dialog_common::test]
    async fn it_elects_over_a_committed_and_an_overlay_rule_together() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let salary = serde_json::json!({ "with": {
            "salary": { "the": "org/dept-salary", "as": "natural:", "cardinality": "many" }
        }});
        let committed: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": salary,
            "when": [{
                "assert": { "with": {
                    "dept": { "the": "org/dept", "as": "entity:" },
                    "salary": { "the": "org/salary", "as": "natural:" }
                }},
                "where": {
                    "this": { "?": { "name": "employee" } },
                    "dept": { "?": { "name": "this" } },
                    "salary": { "?": { "name": "salary" } }
                }
            }]
        }))?;
        let committed = committed.compile()?;
        let overlay: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": salary,
            "when": [{
                "assert": { "with": {
                    "flat": { "the": "org/flat-total", "as": "natural:" }
                }},
                "where": {
                    "this": { "?": { "name": "this" } },
                    "flat": { "?": { "name": "salary" } }
                }
            }]
        }))?;
        let overlay = overlay.compile()?;
        let dept_top = dept_top()?;

        let dept_a: Entity = "id:dept-a".parse()?;
        let dept_b: Entity = "id:dept-b".parse()?;
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        branch
            .transaction()
            .assert(the!("org/dept").of(alice.clone()).is(dept_a.clone()))
            .assert(the!("org/salary").of(alice.clone()).is(3u32))
            .assert(the!("org/dept").of(bob.clone()).is(dept_a.clone()))
            .assert(the!("org/salary").of(bob.clone()).is(4u32))
            .assert(the!("org/flat-total").of(dept_b.clone()).is(9u32))
            .assert(&committed)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("dept"));
        terms.insert("top".into(), Term::var("top"));
        let rows: Vec<ConceptConclusion> = branch
            .query()
            .with(&overlay)
            .select(ConceptQuery {
                predicate: dept_top,
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?;
        let mut tops: Vec<(Entity, u64)> = rows
            .iter()
            .map(|row| Ok((row.entity().clone(), row.get::<u64>("top")?)))
            .collect::<anyhow::Result<_>>()?;
        tops.sort();
        assert_eq!(
            tops,
            vec![(dept_a, 4), (dept_b, 9)],
            "the committed and the overlay rule both contribute"
        );
        Ok(())
    }

    // ----- (7) no rules => implicit-only, empty -----------------------

    #[dialog_common::test]
    async fn it_returns_empty_when_no_rules() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        branch
            .transaction()
            .assert(the!("org/person-name").of(alice).is("Alice".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // No rule stored, no `employee` fact: nothing matches.
        assert!(query_employees(&branch, &operator).await?.is_empty());
        Ok(())
    }

    // ----- (2) overlay rule resolves via the transient layer ----------

    #[dialog_common::test]
    async fn it_resolves_an_overlay_rule() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Rule lives only in the overlay (uncommitted) — must still resolve.
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("name".into(), Term::var("name"));
        let rows: Vec<ConceptConclusion> = branch
            .query()
            .with(employee_from_person())
            .select(ConceptQuery {
                predicate: employee_descriptor(),
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert!(rows.iter().any(|c| *c.entity() == alice));
        Ok(())
    }

    // ----- (3) overlay rule resolves AFTER a prior query (head fixed) --
    // The regression for the bug we hit: a prior query of the concept
    // populates the discovery cache (empty) at the current head; an
    // overlay rule must NOT be masked by that cache (head hasn't moved).

    #[dialog_common::test]
    async fn it_resolves_overlay_rule_after_prior_query() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Prime the durable discovery cache with an empty result.
        assert!(query_employees(&branch, &operator).await?.is_empty());

        // Now add the rule via the overlay (head unchanged) — must resolve.
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("name".into(), Term::var("name"));
        let rows: Vec<ConceptConclusion> = branch
            .query()
            .with(employee_from_person())
            .select(ConceptQuery {
                predicate: employee_descriptor(),
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert!(
            rows.iter().any(|c| *c.entity() == alice),
            "overlay rule must resolve despite a prior cached query at the same head"
        );
        Ok(())
    }

    // ----- (9) overlay rule does NOT leak into a later plain query ----

    #[dialog_common::test]
    async fn it_does_not_leak_overlay_rule_into_later_query() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Query WITH the overlay rule — resolves.
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("name".into(), Term::var("name"));
        let with_overlay: Vec<ConceptConclusion> = branch
            .query()
            .with(employee_from_person())
            .select(ConceptQuery {
                predicate: employee_descriptor(),
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert!(with_overlay.iter().any(|c| *c.entity() == alice));

        // A subsequent PLAIN query (no overlay) must NOT see it — the
        // transient layer is per-query; nothing was committed.
        assert!(
            query_employees(&branch, &operator).await?.is_empty(),
            "overlay rule must not persist into a later plain query"
        );
        Ok(())
    }

    /// A query carrying a rule of its own is served the rule set an
    /// earlier query with the same rule assembled, as a query over
    /// committed rules is: what the query holds is named by its
    /// revision, so nothing has to be read fresh.
    #[dialog_common::test]
    async fn it_reuses_the_rule_set_of_a_query_with_the_same_rule() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let employees = || async {
            let mut terms = Parameters::new();
            terms.insert("this".into(), Term::var("this"));
            terms.insert("name".into(), Term::var("name"));
            branch
                .query()
                .with(employee_from_person())
                .select(ConceptQuery {
                    predicate: employee_descriptor(),
                    terms,
                })
                .perform(&operator)
                .try_vec()
                .await
        };

        let first: Vec<ConceptConclusion> = employees().await?;
        assert!(first.iter().any(|c| *c.entity() == alice));
        let reused = branch.rule_cache().reused();

        let second: Vec<ConceptConclusion> = employees().await?;
        assert!(second.iter().any(|c| *c.entity() == alice));
        assert!(
            branch.rule_cache().reused() > reused,
            "the second query must be served the rule set the first assembled"
        );
        Ok(())
    }

    /// A branch holding `alice` as a person and the committed rule
    /// deriving employees from persons.
    async fn branch_with_employee_rule(
        operator: &Peer<VolatileSpace, Session>,
        repo: &Repository<Credential>,
    ) -> anyhow::Result<(Branch, Entity)> {
        let alice: Entity = "id:alice".parse()?;
        repo.branch("main")
            .open()
            .perform(operator)
            .await?
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(employee_from_person())
            .commit()
            .publish()
            .perform(operator)
            .await?;
        Ok((repo.branch("main").open().perform(operator).await?, alice))
    }

    /// A session-overlay write that installs no rule leaves every rule
    /// set standing: the next query is served the set the last one
    /// assembled instead of rebuilding it.
    #[dialog_common::test]
    async fn it_reuses_the_rule_set_across_an_overlay_write_without_rules() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, alice) = branch_with_employee_rule(&operator, &repo).await?;

        assert!(query_employees(&branch, &operator).await?.contains(&alice));
        let reused = branch.rule_cache().reused();

        branch.overlay().assert(
            the!("org/person-role")
                .of(alice.clone())
                .is("Engineer".to_string()),
        )?;
        assert!(query_employees(&branch, &operator).await?.contains(&alice));
        assert!(
            branch.rule_cache().reused() > reused,
            "an overlay write without a rule must not rebuild the rule set"
        );
        Ok(())
    }

    /// A commit that installs no rule leaves every rule set standing,
    /// though it moves the head and the tree root.
    #[dialog_common::test]
    async fn it_reuses_the_rule_set_across_a_commit_without_rules() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, alice) = branch_with_employee_rule(&operator, &repo).await?;

        assert!(query_employees(&branch, &operator).await?.contains(&alice));
        let reused = branch.rule_cache().reused();

        let bob: Entity = "id:bob".parse()?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(bob.clone())
                    .is("Bob".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let employees = query_employees(&branch, &operator).await?;
        assert!(employees.contains(&alice) && employees.contains(&bob));
        assert!(
            branch.rule_cache().reused() > reused,
            "a commit without a rule must not rebuild the rule set"
        );
        Ok(())
    }

    /// A commit that installs a rule after commits that installed none
    /// moves the rules root they carried: the next query sees the new
    /// rule, though the rule set before it was served across those
    /// commits.
    #[dialog_common::test]
    async fn it_sees_a_rule_committed_after_commits_without_rules() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, alice) = branch_with_employee_rule(&operator, &repo).await?;
        assert_eq!(
            query_employees(&branch, &operator).await?,
            vec![alice.clone()]
        );

        let carol: Entity = "id:carol".parse()?;
        branch
            .transaction()
            .assert(
                the!("org/contractor-name")
                    .of(carol.clone())
                    .is("Carol".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let reused = branch.rule_cache().reused();
        assert_eq!(
            query_employees(&branch, &operator).await?,
            vec![alice.clone()]
        );
        assert!(
            branch.rule_cache().reused() > reused,
            "the commit without a rule carried the rules root"
        );

        branch
            .transaction()
            .assert(rule_with_person_attr("org/contractor-name"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let employees = query_employees(&branch, &operator).await?;
        assert!(
            employees.contains(&alice) && employees.contains(&carol),
            "the committed rule is seen: {employees:?}"
        );
        Ok(())
    }

    /// A rule installed by a commit that does not classify its batch (a
    /// raw commit, as a pull or a merge lands one) is seen, though the
    /// commit before it carried the rules root: the carried root names
    /// the head it was carried to, and any other head is its own rules
    /// root.
    #[dialog_common::test]
    async fn it_sees_a_rule_a_head_move_it_did_not_classify_installed() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, alice) = branch_with_employee_rule(&operator, &repo).await?;
        let carol: Entity = "id:carol".parse()?;
        branch
            .transaction()
            .assert(
                the!("org/contractor-name")
                    .of(carol.clone())
                    .is("Carol".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            query_employees(&branch, &operator).await?,
            vec![alice.clone()]
        );

        let mut changes = dialog_corpus::Changes::new();
        rule_with_person_attr("org/contractor-name").assert(&mut changes);
        branch
            .commit(stream::iter(changes.into_instructions()))
            .perform(&operator)
            .await?;
        let employees = query_employees(&branch, &operator).await?;
        assert!(
            employees.contains(&alice) && employees.contains(&carol),
            "the rule the raw commit installed is seen: {employees:?}"
        );
        Ok(())
    }

    /// A subscription to a concept whose field is read under a ranked
    /// type, derived by rules, resolves it under two spellings: its own
    /// fields and its relation's attribute concept (field `is`). The two
    /// share an identity; each keeps its own rule set, so the re-poll
    /// after a commit does not evict one with the other.
    #[dialog_common::test]
    async fn it_keeps_a_rule_set_per_spelling_of_a_concept() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let cases = ["case:active", "case:invited"];
        let rule = |case: &str, flag: &str| -> anyhow::Result<DeductiveRule> {
            let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
                "deduce": { "with": { "status": { "the": "account/status", "as": cases } } },
                "when": [
                    {
                        "assert": { "with": { "flag": { "the": flag, "as": "text:" } } },
                        "where": { "this": { "?": { "name": "this" } }, "flag": { "?": { "name": "flag" } } }
                    },
                    { "assert": "==", "where": { "this": { "?": { "name": "status" } }, "is": case } }
                ]
            }))?;
            Ok(descriptor.compile()?)
        };
        let account: Entity = "id:account".parse()?;
        repo.branch("main")
            .open()
            .perform(&operator)
            .await?
            .transaction()
            .assert(rule("case:active", "account/active")?)
            .assert(rule("case:invited", "account/invited")?)
            .assert(
                the!("account/invited")
                    .of(account.clone())
                    .is("yes".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let status: ConceptDescriptor = serde_json::from_value(serde_json::json!({
            "with": { "status": { "the": "account/status", "as": cases } }
        }))?;
        let query = || {
            let mut terms = Parameters::new();
            terms.insert("this".into(), Term::var("this"));
            terms.insert("status".into(), Term::var("status"));
            ConceptQuery {
                predicate: status.clone(),
                terms,
            }
        };

        let mut subscription = branch.subscribe(query());
        let first = subscription.poll(&operator).await?;
        assert!(first.is_some_and(|delta| delta.asserted.len() == 1));
        let mut promote = dialog_corpus::Changes::new();
        promote.associate(
            "account/active".parse()?,
            account.clone(),
            Value::String("yes".into()),
            dialog_corpus::Cardinality::One,
        );
        branch
            .transaction()
            .assert(promote)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        subscription.poll(&operator).await?;
        assert_eq!(
            branch.rule_cache().spellings(&status.this()),
            2,
            "the re-poll resolves both spellings, and each keeps its own set"
        );
        Ok(())
    }

    /// Two repositories opened through one environment share what rule
    /// resolution decodes (a body decoded for one is not decoded again for
    /// the other), but each assembles its own rule set, keyed by its own
    /// lines' rules. The plan cache is shared the same way, through the
    /// same environment-wide handle, but goes unobserved here: a
    /// `PlanCache` offers no way to inspect its entries, and one added
    /// for a test alone would widen `dialog-query`'s API.
    #[dialog_common::test]
    async fn it_shares_decoded_rules_between_repositories_but_not_rule_sets() -> anyhow::Result<()>
    {
        let (operator, profile) = test_session_with_peer().await;
        let ours = test_repo(&operator, &profile).await;
        let theirs = test_repo(&operator, &profile).await;
        let (our_branch, alice) = branch_with_employee_rule(&operator, &ours).await?;
        let (their_branch, _) = branch_with_employee_rule(&operator, &theirs).await?;
        assert_eq!(
            query_employees(&our_branch, &operator).await?,
            vec![alice.clone()]
        );

        let rule = employee_from_person();
        let entity = rule.try_this().expect("a compiled rule has an identity");
        assert!(
            their_branch
                .rule_cache()
                .body(&entity, &rule.encode())
                .is_some(),
            "the body our query decoded is theirs to reuse"
        );
        let concept = employee_descriptor().this();
        assert_eq!(their_branch.rule_cache().spellings(&concept), 0);
        assert_eq!(
            query_employees(&their_branch, &operator).await?,
            vec![alice]
        );
        assert_eq!(their_branch.rule_cache().spellings(&concept), 1);
        assert_eq!(our_branch.rule_cache().spellings(&concept), 1);
        Ok(())
    }

    /// A transaction whose writes install no rule reads through the rule
    /// set a query of its branch assembled.
    #[dialog_common::test]
    async fn it_reuses_the_branch_rule_set_in_a_transaction_without_rules() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, alice) = branch_with_employee_rule(&operator, &repo).await?;

        assert!(query_employees(&branch, &operator).await?.contains(&alice));
        let reused = branch.rule_cache().reused();

        let bob: Entity = "id:bob".parse()?;
        let tx = branch.transaction().assert(
            the!("org/person-name")
                .of(bob.clone())
                .is("Bob".to_string()),
        );
        let employees = transaction_employees(&tx, &operator).await?;
        assert!(employees.contains(&alice) && employees.contains(&bob));
        assert!(
            branch.rule_cache().reused() > reused,
            "a transaction without a rule must not rebuild the rule set"
        );
        Ok(())
    }

    /// A rule arriving in the session overlay for a concept in another
    /// concept's closure is seen by the next query of the other, though
    /// that query's rule set was assembled before the rule arrived.
    #[dialog_common::test]
    async fn it_sees_an_overlay_rule_in_the_closure_of_a_cached_rule_set() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let alice: Entity = "id:alice".parse()?;
        repo.branch("main")
            .open()
            .perform(&operator)
            .await?
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(projection("org/employee-name", "org/staff-name", "name"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        assert!(staff(&branch, &operator).await?.is_empty());
        branch.overlay().assert(employee_from_person())?;
        assert_eq!(staff(&branch, &operator).await?, vec![alice]);
        Ok(())
    }

    /// A rule a transaction stages is read by the transaction's query,
    /// though a query of its branch assembled the rule set without it.
    #[dialog_common::test]
    async fn it_sees_a_staged_rule_after_a_cached_rule_set() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let alice: Entity = "id:alice".parse()?;
        repo.branch("main")
            .open()
            .perform(&operator)
            .await?
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        assert!(query_employees(&branch, &operator).await?.is_empty());
        let tx = branch.transaction().assert(employee_from_person());
        assert_eq!(transaction_employees(&tx, &operator).await?, vec![alice]);
        Ok(())
    }

    /// A rule whose source bytes are forged (another rule's, stored under
    /// its entity) is inert even after the genuine body under that entity
    /// was decoded: the body cache decodes bytes a read found, and never
    /// stands in for them.
    #[dialog_common::test]
    async fn it_keeps_forged_bytes_inert_after_the_genuine_body_is_cached() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let alice: Entity = "id:alice".parse()?;
        repo.branch("main")
            .open()
            .perform(&operator)
            .await?
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let mut genuine = Changes::new();
        employee_from_person().assert(&mut genuine);
        // The bytes of another rule: they decode, but not to a rule stored
        // under this entity.
        let other = Value::Bytes(rule_with_person_attr("org/person-alias").encode());
        let mut forged = Changes::new();
        for (of, the, change) in genuine.iter() {
            let dialog_corpus::Change::Assert(value, cardinality) = change else {
                continue;
            };
            let value = if *the == source_attr() {
                other.clone()
            } else {
                value.clone()
            };
            forged.associate(the.clone(), of.clone(), value, *cardinality);
        }

        let employees = |overlay: Changes| {
            let branch = &branch;
            let operator = &operator;
            async move {
                let mut terms = Parameters::new();
                terms.insert("this".into(), Term::var("this"));
                terms.insert("name".into(), Term::var("name"));
                let rows: Vec<ConceptConclusion> = branch
                    .query()
                    .with(overlay)
                    .select(ConceptQuery {
                        predicate: employee_descriptor(),
                        terms,
                    })
                    .perform(operator)
                    .try_vec()
                    .await?;
                anyhow::Ok(rows.iter().map(|c| c.entity().clone()).collect::<Vec<_>>())
            }
        };

        assert_eq!(employees(genuine).await?, vec![alice]);
        assert!(
            employees(forged).await?.is_empty(),
            "forged bytes under a cached entity must stay inert"
        );
        Ok(())
    }

    // ----- (5) discovery cache invalidated when the head moves --------
    // Query once (caches empty at head H0), then commit a rule (head ->
    // H1); the next query must re-scan and resolve it.

    #[dialog_common::test]
    async fn it_invalidates_discovery_on_head_move() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Cache empty at the current head.
        assert!(query_employees(&branch, &operator).await?.is_empty());

        // Commit the rule on the SAME handle → its head advances.
        branch
            .transaction()
            .assert(employee_from_person())
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // The same handle (head moved) must now re-scan and resolve.
        let employees = query_employees(&branch, &operator).await?;
        assert!(
            employees.contains(&alice),
            "a committed rule must resolve after the head advances (discovery re-scan)"
        );
        Ok(())
    }

    // ----- (6) hydration cache: same rule entity reused, distinct rules
    // distinguished. Two different rule bodies (distinct this()) both
    // resolve; re-querying reuses the cached compiled bodies.

    #[dialog_common::test]
    async fn it_resolves_two_distinct_rules_and_reuses_bodies() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let r1 = rule_with_person_attr("org/person-name");
        let r2 = rule_with_person_attr("org/contractor-name");
        assert_ne!(
            r1.this(),
            r2.this(),
            "distinct bodies ⇒ distinct identities"
        );

        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(
                the!("org/contractor-name")
                    .of(bob.clone())
                    .is("Bob".to_string()),
            )
            .assert(&r1)
            .assert(&r2)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Both rules conclude `employee`; Alice (person) and Bob
        // (contractor) both surface.
        let first = query_employees(&branch, &operator).await?;
        assert!(first.contains(&alice) && first.contains(&bob));

        // Second query on the same handle reuses cached bodies (no panic,
        // same result) — exercises the hydration-cache reuse path.
        let second = query_employees(&branch, &operator).await?;
        assert_eq!(first.len(), second.len());
        assert!(second.contains(&alice) && second.contains(&bob));
        Ok(())
    }

    // ----- (8) committed + overlay union: both resolve together -------

    #[dialog_common::test]
    async fn it_unions_committed_and_overlay_rules() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        // Commit rule #1 (person) + a person fact + a contractor fact.
        let r1 = rule_with_person_attr("org/person-name");
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(
                the!("org/contractor-name")
                    .of(bob.clone())
                    .is("Bob".to_string()),
            )
            .assert(&r1)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Rule #2 (contractor) only in the overlay.
        let r2 = rule_with_person_attr("org/contractor-name");
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("name".into(), Term::var("name"));
        let rows: Vec<ConceptConclusion> = branch
            .query()
            .with(&r2)
            .select(ConceptQuery {
                predicate: employee_descriptor(),
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?;
        let entities: Vec<Entity> = rows.iter().map(|c| c.entity().clone()).collect();
        assert!(
            entities.contains(&alice),
            "committed rule contributes Alice"
        );
        assert!(entities.contains(&bob), "overlay rule contributes Bob");
        Ok(())
    }

    /// The `employee` query binding every field to a variable.
    fn employees() -> ConceptQuery {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("name".into(), Term::var("name"));
        ConceptQuery {
            predicate: employee_descriptor(),
            terms,
        }
    }

    /// A rule asserted into the branch's session overlay resolves like
    /// a committed one: session-held rules are a rule source of their
    /// own, read fresh every query and never head-cached.
    #[dialog_common::test]
    async fn it_resolves_a_rule_held_in_the_session_overlay() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let bob: Entity = "id:bob".parse()?;
        branch
            .transaction()
            .assert(
                the!("org/contractor-name")
                    .of(bob.clone())
                    .is("Bob".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let before: Vec<ConceptConclusion> = branch
            .select(employees())
            .perform(&operator)
            .try_vec()
            .await?;
        assert!(before.is_empty(), "no rule, no employees");

        branch
            .overlay()
            .assert(rule_with_person_attr("org/contractor-name"))?;
        let after: Vec<ConceptConclusion> = branch
            .select(employees())
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(
            after.iter().map(|c| c.entity().clone()).collect::<Vec<_>>(),
            vec![bob],
            "the session rule concludes Bob"
        );

        branch.overlay().clear();
        let cleared: Vec<ConceptConclusion> = branch
            .select(employees())
            .perform(&operator)
            .try_vec()
            .await?;
        assert!(
            cleared.is_empty(),
            "dropping the session rule drops its conclusions"
        );
        Ok(())
    }

    /// A session rule arriving after a subscription evaluated must
    /// reach it: session rule reads are recorded as rule demand, so
    /// the rule's instant lands in the cover and the poll re-evaluates.
    /// Without that record the instant falls outside the cover and the
    /// poll wrongly reports nothing changed.
    #[dialog_common::test]
    async fn it_propagates_a_session_rule_to_a_subscription() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let bob: Entity = "id:bob".parse()?;
        branch
            .transaction()
            .assert(
                the!("org/contractor-name")
                    .of(bob.clone())
                    .is("Bob".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let mut subscription = branch.subscribe(employees());
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert!(initial.asserted.is_empty(), "no rule, no employees");

        branch
            .overlay()
            .assert(rule_with_person_attr("org/contractor-name"))?;
        let delta = subscription
            .poll(&operator)
            .await?
            .expect("the session rule reaches the subscription");
        assert_eq!(
            delta
                .asserted
                .iter()
                .map(|c| c.entity().clone())
                .collect::<Vec<_>>(),
            vec![bob]
        );

        branch.overlay().clear();
        let delta = subscription
            .poll(&operator)
            .await?
            .expect("dropping the session rule reaches the subscription");
        assert!(delta.asserted.is_empty());
        assert_eq!(delta.retracted.len(), 1);
        Ok(())
    }

    /// A subscription over a rule-derived concept stays quiet when the
    /// overlay changes facts it never reads: the session stamp lands
    /// outside both its fact cover and its rule-discovery cover, so the
    /// poll neither recomputes nor maintains.
    #[dialog_common::test]
    async fn it_ignores_unrelated_overlay_writes_under_a_rule_backed_subscription()
    -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(employee_from_person())
            .assert(
                the!("org/person-name")
                    .of(Entity::new()?)
                    .is("Alice".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let mut subscription = branch.subscribe(employees());
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert_eq!(
            initial.asserted.len(),
            1,
            "the committed rule derives Alice"
        );

        for path in ["/", "/hub", "/space"] {
            let site = Entity::new()?;
            branch
                .overlay()
                .assert(the!("xyz.tonk.site/path").of(site).is(path.to_string()))?;
            assert!(
                subscription.poll(&operator).await?.is_none(),
                "an unrelated session stamp changes nothing"
            );
        }
        assert_eq!(subscription.recomputes(), 1);
        assert_eq!(subscription.maintenances(), 0);
        Ok(())
    }

    // ----- (4) discovery cache keys on head: a stale handle (head not
    // advanced) keeps using its cached discovery and does NOT pick up a
    // rule committed via another handle until it refreshes. This proves
    // the cache actually gates on head (not re-scanning every query).

    #[dialog_common::test]
    async fn it_keeps_discovery_cached_until_head_advances() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let alice: Entity = "id:alice".parse()?;
        repo.branch("main")
            .open()
            .perform(&operator)
            .await?
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // Handle A: query once → caches empty discovery at head H0.
        let handle_a = repo.branch("main").open().perform(&operator).await?;
        assert!(query_employees(&handle_a, &operator).await?.is_empty());

        // Handle B (independent handle) commits the rule → branch head -> H1.
        repo.branch("main")
            .open()
            .perform(&operator)
            .await?
            .transaction()
            .assert(employee_from_person())
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // Handle A's head is still H0 (it didn't do the commit), so its
        // cached empty discovery stands — it does NOT see the new rule.
        assert!(
            query_employees(&handle_a, &operator).await?.is_empty(),
            "a handle at the old head must keep its cached discovery"
        );

        // A fresh handle (at H1) does see it — confirms the rule really is
        // committed, and the staleness above is the cache, not missing data.
        let handle_c = repo.branch("main").open().perform(&operator).await?;
        assert!(
            query_employees(&handle_c, &operator)
                .await?
                .contains(&alice)
        );
        Ok(())
    }

    // ----- (11) multi-branch: durable rules from each joined branch ----

    #[dialog_common::test]
    async fn it_unions_rules_across_joined_branches() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        // `main` holds a person + the person rule.
        let alice: Entity = "id:alice".parse()?;
        let r_person = rule_with_person_attr("org/person-name");
        repo.branch("main")
            .open()
            .perform(&operator)
            .await?
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(&r_person)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // A second branch holds a contractor + the contractor rule.
        let bob: Entity = "id:bob".parse()?;
        let r_contractor = rule_with_person_attr("org/contractor-name");
        repo.branch("other")
            .open()
            .perform(&operator)
            .await?
            .transaction()
            .assert(
                the!("org/contractor-name")
                    .of(bob.clone())
                    .is("Bob".to_string()),
            )
            .assert(&r_contractor)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let main = repo.branch("main").open().perform(&operator).await?;
        let other = repo.branch("other").open().perform(&operator).await?;

        // Query across both branches — each is a durable layer, so both
        // rules (and both their input facts) participate.
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("name".into(), Term::var("name"));
        let rows: Vec<ConceptConclusion> = main
            .query()
            .join(&other)
            .select(ConceptQuery {
                predicate: employee_descriptor(),
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?;
        let entities: Vec<Entity> = rows.iter().map(|c| c.entity().clone()).collect();
        assert!(entities.contains(&alice), "main's rule contributes Alice");
        assert!(entities.contains(&bob), "other's rule contributes Bob");
        Ok(())
    }

    // ===== cache INVALIDATION ========================================

    // A committed rule that is later RETRACTED must stop resolving: the
    // retract moves the head, so the discovery cache re-scans and finds
    // the rule's `dialog.rule/*` facts gone. The inverse of the
    // head-move-adds case.

    #[dialog_common::test]
    async fn it_invalidates_discovery_when_a_rule_is_retracted() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        let rule = employee_from_person();
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(&rule)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // Resolves while committed (also primes the cache at this head).
        assert!(query_employees(&branch, &operator).await?.contains(&alice));

        // Retract the rule's facts on the same handle → head advances.
        branch
            .transaction()
            .retract(&rule)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // Re-scan at the new head finds no rule → no rows.
        assert!(
            query_employees(&branch, &operator).await?.is_empty(),
            "a retracted rule must stop resolving (discovery re-scan at new head)"
        );
        Ok(())
    }

    // Changing a rule's BODY produces a new content-addressed rule
    // entity; the hydration cache (keyed by that entity) must not serve
    // the old compiled body for the new entity. Here two distinct
    // bodies share the SAME conclusion concept: both must resolve their
    // own input attribute, proving no cross-contamination.

    #[dialog_common::test]
    async fn it_does_not_reuse_a_body_across_distinct_rule_entities() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // v1 reads org/person-name; commit it + a matching person fact.
        let alice: Entity = "id:alice".parse()?;
        let v1 = rule_with_person_attr("org/person-name");
        branch
            .transaction()
            .assert(
                the!("org/person-name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(&v1)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        // Prime the hydration cache for v1.
        assert!(query_employees(&branch, &operator).await?.contains(&alice));

        // v2 reads org/agent-name (a DIFFERENT body ⇒ different this()).
        // Commit it + a matching agent fact. v2 must resolve via its OWN
        // body, not v1's cached one.
        let carol: Entity = "id:carol".parse()?;
        let v2 = rule_with_person_attr("org/agent-name");
        assert_ne!(v1.this(), v2.this());
        branch
            .transaction()
            .assert(
                the!("org/agent-name")
                    .of(carol.clone())
                    .is("Carol".to_string()),
            )
            .assert(&v2)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // Both resolve, each via its own (correctly distinct) compiled body.
        let employees = query_employees(&branch, &operator).await?;
        assert!(
            employees.contains(&alice),
            "v1 still resolves its person input"
        );
        assert!(
            employees.contains(&carol),
            "v2 resolves its agent input via its own body, not v1's cached one"
        );
        Ok(())
    }

    /// Tonk's `space/presence` shape: a concept whose optional field
    /// reads a relation derived by rules whose bodies read the concept
    /// itself for a required field. Two rules offer cases, every space
    /// with a subject `case:remote` and the one whose subject has a
    /// replica `case:replicated`, and the field elects among them with
    /// `top`, the first listed best: the better-ranked case wins, and
    /// no rule negates anything.
    #[dialog_common::test]
    async fn it_derives_an_optional_field_from_a_rule_reading_the_concept() -> anyhow::Result<()> {
        use dialog_query::rule::deductive::descriptor::DeductiveRuleDescriptor;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let cases = serde_json::json!(["case:replicated", "case:remote"]);
        let space = serde_json::json!({ "with": {
            "subject": { "the": "load.space/subject", "as": "entity:" },
            "presence": {
                "the": "load.space/case",
                "as": cases,
                "cardinality": "many",
                "optional": true,
                "pick": "top"
            }
        } });
        let replica = serde_json::json!({ "with": {
            "subject": { "the": "load.replica/subject", "as": "entity:" },
            "profile": { "the": "load.replica/profile", "as": "entity:" }
        } });
        let case = serde_json::json!({ "with": {
            "case": { "the": "load.space/case", "as": "entity:", "cardinality": "many" }
        } });
        let remote: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": case,
            "when": [
                { "assert": space, "where": {
                    "this": { "?": { "name": "this" } },
                    "subject": { "?": { "name": "subject" } } } },
                { "assert": "==", "where": {
                    "this": { "?": { "name": "case" } },
                    "is": "case:remote" } }
            ]
        }))?;
        let remote = remote.compile()?;
        let replicated: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": case,
            "when": [
                { "assert": space, "where": {
                    "this": { "?": { "name": "this" } },
                    "subject": { "?": { "name": "subject" } } } },
                { "assert": replica, "where": {
                    "subject": { "?": { "name": "subject" } } } },
                { "assert": "==", "where": {
                    "this": { "?": { "name": "case" } },
                    "is": "case:replicated" } }
            ]
        }))?;
        let replicated = replicated.compile()?;
        let here: Entity = "id:space-here".parse()?;
        let away: Entity = "id:space-away".parse()?;
        let device: Entity = "id:device".parse()?;
        branch
            .transaction()
            .assert(&remote)
            .assert(&replicated)
            .assert(
                the!("load.space/subject")
                    .of(here.clone())
                    .is("id:subject-here".parse::<Entity>()?),
            )
            .assert(
                the!("load.space/subject")
                    .of(away.clone())
                    .is("id:subject-away".parse::<Entity>()?),
            )
            .assert(
                the!("load.replica/subject")
                    .of("id:replica".parse::<Entity>()?)
                    .is("id:subject-here".parse::<Entity>()?),
            )
            .assert(
                the!("load.replica/profile")
                    .of("id:replica".parse::<Entity>()?)
                    .is(device),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // Read through the concept itself: each space carries the
        // best-ranked case a rule derives for it.
        let predicate: ConceptDescriptor = serde_json::from_value(space.clone())?;
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("subject".into(), Term::var("subject"));
        terms.insert("presence".into(), Term::var("presence"));
        let mut rows: Vec<(Entity, Option<String>)> = branch
            .select(ConceptQuery { predicate, terms })
            .perform(&operator)
            .try_vec()
            .await?
            .into_iter()
            .map(|row| {
                let presence = row
                    .get::<Entity>("presence")
                    .ok()
                    .map(|value| value.to_string());
                (row.entity().clone(), presence)
            })
            .collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (away, Some("case:remote".to_string())),
                (here, Some("case:replicated".to_string())),
            ]
        );
        Ok(())
    }
}

#[cfg(test)]
mod resolver_tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{Counting, test_repo};
    use crate::{Branch, Repository};
    use dialog_common::Blake3Hash;
    use dialog_corpus::{Entity, Value};
    use dialog_peer::Peer;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::concept::query::ConceptQuery;
    use dialog_query::query::Output as _;
    use dialog_query::rule::DeductiveRuleDescriptor;
    use dialog_query::{
        ConceptConclusion, Parameters, ResolverConclusion, ResolverQuery, Term, the,
    };
    use dialog_storage::provider::storage::VolatileSpace;
    use std::collections::BTreeSet;

    type Operator = Peer<VolatileSpace, dialog_peer::Session>;

    /// The handle of the branch's root node.
    fn root_of(branch: &Branch) -> Entity {
        let revision = branch.revision().expect("branch has a revision");
        Entity::from_node(revision.tree.hash()).expect("a node entity")
    }

    /// Commit a handful of facts and return the branch and its root's
    /// handle.
    async fn committed_branch(
        repo: &Repository<impl dialog_capability::Principal>,
        operator: &Operator,
    ) -> anyhow::Result<(Branch, Entity)> {
        let branch = repo.branch("main").open().perform(operator).await?;
        let mut tx = branch.transaction();
        for at in 0..8 {
            tx = tx.assert(
                the!("test/name")
                    .of(Entity::new()?)
                    .is(format!("entry-{at}")),
            );
        }
        tx.commit().publish().perform(operator).await?;
        let root = root_of(&branch);
        Ok((branch, root))
    }

    /// Commit `count` facts with padded values: enough leaf weight
    /// forces an index root, so spans and descent run against a real
    /// multi-level tree.
    async fn committed_wide_branch(
        repo: &Repository<impl dialog_capability::Principal>,
        operator: &Operator,
        count: usize,
    ) -> anyhow::Result<(Branch, Entity)> {
        let branch = repo.branch("main").open().perform(operator).await?;
        let pad = "p".repeat(160);
        let mut tx = branch.transaction();
        for at in 0..count {
            tx = tx.assert(
                the!("test/name")
                    .of(Entity::new()?)
                    .is(format!("entry-{at}-{pad}")),
            );
        }
        tx.commit().publish().perform(operator).await?;
        let root = root_of(&branch);
        Ok((branch, root))
    }

    /// The resolver `name` over the handle `of`, with each of `slots`
    /// bound to a variable of its own name.
    fn ask(name: &str, of: &Entity, slots: &[&str]) -> ResolverQuery {
        let mut wanted = serde_json::Map::new();
        for slot in slots {
            wanted.insert(
                slot.to_string(),
                serde_json::json!({ "?": { "name": slot } }),
            );
        }
        let query: ResolverQuery =
            serde_json::from_value(serde_json::json!({ "assert": name, "where": wanted }))
                .unwrap_or_else(|error| panic!("{name} deserializes: {error}"));
        let mut terms = Parameters::new();
        terms.insert("of".into(), Term::from(of.clone()).into());
        query.with_parameters(&terms)
    }

    const NODE: &[&str] = &[
        "kind",
        "size",
        "count",
        "scale",
        "novelty",
        "version",
        "fanout-exponent",
        "max-separator",
        "inline-n",
        "spill-prefix",
        "max-segment",
        "frame-ceiling-factor",
        "anchor-selector",
        "entry-overhead",
        "key-overhead",
        "link-overhead",
    ];
    const SPAN: &[&str] = &[
        "span",
        "at",
        "node",
        "separator",
        "until",
        "scale",
        "rank",
        "novelty",
    ];
    const ENTRY: &[&str] = &[
        "entry",
        "at",
        "key",
        "rank",
        "state",
        "retraction",
        "origin",
        "edition",
        "cause",
        "collapsed",
        "supersedes",
        "spill",
        "blob",
        "blob-size",
        "blob-version",
        "span",
    ];

    async fn rows(
        branch: &Branch,
        operator: &Operator,
        query: ResolverQuery,
    ) -> Vec<ResolverConclusion> {
        branch
            .query()
            .select(query)
            .perform(operator)
            .try_vec()
            .await
            .expect("the resolver answers")
    }

    /// [`rows`] through an environment that counts block reads.
    async fn counted(
        branch: &Branch,
        env: &Counting<Operator>,
        query: ResolverQuery,
    ) -> Vec<ResolverConclusion> {
        branch
            .query()
            .select(query)
            .perform(env)
            .try_vec()
            .await
            .expect("the resolver answers")
    }

    fn unsigned(row: &ResolverConclusion, slot: &str) -> u128 {
        match row.get(slot) {
            Some(Value::UnsignedInt(value)) => *value,
            other => panic!("expected unsigned `{slot}`, got {other:?}"),
        }
    }

    fn text(row: &ResolverConclusion, slot: &str) -> String {
        match row.get(slot) {
            Some(Value::String(value)) => value.clone(),
            other => panic!("expected string `{slot}`, got {other:?}"),
        }
    }

    fn bytes(row: &ResolverConclusion, slot: &str) -> Vec<u8> {
        match row.get(slot) {
            Some(Value::Bytes(value)) => value.clone(),
            other => panic!("expected bytes `{slot}`, got {other:?}"),
        }
    }

    fn entity(row: &ResolverConclusion, slot: &str) -> Entity {
        match row.get(slot) {
            Some(Value::Entity(value)) => value.clone(),
            other => panic!("expected entity `{slot}`, got {other:?}"),
        }
    }

    /// Every node the root reaches, by reading spans one node at a time.
    async fn reachable(branch: &Branch, operator: &Operator, root: &Entity) -> BTreeSet<Entity> {
        let mut all = BTreeSet::new();
        let mut frontier = vec![root.clone()];
        while let Some(next) = frontier.pop() {
            for span in rows(branch, operator, ask("tree/span", &next, &["node"])).await {
                frontier.push(entity(&span, "node"));
            }
            all.insert(next);
        }
        all
    }

    /// A rule that names its nodes by a fact, `probe/known`, and reads
    /// `resolver` over each, concluding `conclusion`.
    fn known(conclusion: serde_json::Value, premise: serde_json::Value) -> DeductiveRuleDescriptor {
        serde_json::from_value(serde_json::json!({
            "deduce": { "with": conclusion },
            "when": [
                {
                    "assert": { "with": {
                        "known": { "the": "probe/known", "as": "text:" }
                    }},
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "known": { "?": { "name": "known" } }
                    }
                },
                premise
            ]
        }))
        .expect("a rule descriptor")
    }

    /// `tree/node` says what a node is and the format it was written
    /// under, every field of it, in one row.
    #[dialog_common::test]
    async fn it_reads_what_a_node_is() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, root) = committed_branch(&repo, &operator).await?;

        let answered = rows(&branch, &operator, ask("tree/node", &root, NODE)).await;
        assert_eq!(answered.len(), 1, "one row per node: {answered:?}");
        let row = &answered[0];
        assert!(["index", "segment"].contains(&text(row, "kind").as_str()));
        assert!(unsigned(row, "size") > 0, "the node has a byte size");
        assert!(unsigned(row, "count") > 0, "the node has slots");
        for slot in NODE {
            assert!(row.contains_key(*slot), "`{slot}` is yielded: {row:?}");
        }
        assert!(unsigned(row, "fanout-exponent") > 0, "a tree branches");
        assert!(unsigned(row, "version") > 0, "a node has a layout");
        Ok(())
    }

    /// `tree/block` yields the node's block as stored: it hashes to the
    /// node's handle and is as long as `tree/node` says.
    #[dialog_common::test]
    async fn it_reads_a_nodes_block() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, root) = committed_branch(&repo, &operator).await?;

        let node = rows(&branch, &operator, ask("tree/node", &root, &["size"])).await;
        let block = rows(&branch, &operator, ask("tree/block", &root, &["bytes"])).await;
        assert_eq!(block.len(), 1, "one row per node");
        let block = bytes(&block[0], "bytes");
        assert_eq!(block.len() as u128, unsigned(&node[0], "size"));
        assert_eq!(
            Some(*Blake3Hash::hash(&block).as_bytes()),
            root.node_hash(),
            "the block is the one the handle names"
        );
        Ok(())
    }

    /// A write to a tree with an index root can ride the root as a
    /// buffered operation rather than reach a segment. `tree/entry` of
    /// the root yields each such operation whole, naming the span it is
    /// buffered against, and the root's `novelty` counts them.
    #[dialog_common::test]
    async fn it_reads_the_operations_an_index_buffers() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, _) = committed_wide_branch(&repo, &operator, 800).await?;
        branch
            .transaction()
            .assert(the!("test/name").of(Entity::new()?).is("late".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let root = root_of(&branch);

        let node = rows(&branch, &operator, ask("tree/node", &root, NODE)).await;
        assert_eq!(text(&node[0], "kind"), "index");
        let buffered = unsigned(&node[0], "novelty");
        assert!(buffered > 0, "the late write rides the root");
        let spans: Vec<Entity> = rows(&branch, &operator, ask("tree/span", &root, SPAN))
            .await
            .iter()
            .map(|row| entity(row, "span"))
            .collect();
        let entries = rows(&branch, &operator, ask("tree/entry", &root, ENTRY)).await;
        assert_eq!(entries.len() as u128, buffered, "one row per buffered op");
        for row in &entries {
            assert!(
                spans.contains(&entity(row, "span")),
                "a buffered op names a span of the node: {row:?}"
            );
            assert_eq!(text(row, "state"), "asserted");
        }
        Ok(())
    }

    /// A tree wide enough for an index root: one span per child, the
    /// ranges contiguous, each span named by its node and position and
    /// each child by a handle, and the descent reaches segments whose
    /// entries come whole, one row each.
    #[dialog_common::test]
    async fn it_descends_a_multi_level_tree() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, root) = committed_wide_branch(&repo, &operator, 800).await?;

        let node = rows(&branch, &operator, ask("tree/node", &root, NODE)).await;
        assert_eq!(
            text(&node[0], "kind"),
            "index",
            "800 padded entries outgrow a segment"
        );
        let spans = rows(&branch, &operator, ask("tree/span", &root, SPAN)).await;
        assert_eq!(
            spans.len() as u128,
            unsigned(&node[0], "count"),
            "one span per child"
        );
        assert_eq!(
            rows(&branch, &operator, ask("tree/entry", &root, ENTRY))
                .await
                .len() as u128,
            unsigned(&node[0], "novelty"),
            "an index's entries are the operations it buffers"
        );
        assert!(
            bytes(&spans[0], "separator").is_empty(),
            "the first span starts at the least key"
        );
        assert!(
            bytes(spans.last().unwrap(), "until").is_empty(),
            "the last span runs to the end"
        );
        for pair in spans.windows(2) {
            assert_eq!(
                bytes(&pair[0], "until"),
                bytes(&pair[1], "separator"),
                "a span ends where the next begins"
            );
        }
        let root_hash = base58::ToBase58::to_base58(&root.node_hash().unwrap()[..]);
        for (at, span) in spans.iter().enumerate() {
            assert_eq!(unsigned(span, "at"), at as u128);
            assert_eq!(
                entity(span, "span").as_str(),
                format!("span:{root_hash}/{at}"),
                "a span is named by its node and its position"
            );
            assert!(
                entity(span, "node").node_hash().is_some(),
                "a child is a handle"
            );
        }

        let mut entries = 0;
        let mut frontier: Vec<Entity> = spans.iter().map(|span| entity(span, "node")).collect();
        while let Some(child) = frontier.pop() {
            let child_node = rows(
                &branch,
                &operator,
                ask("tree/node", &child, &["kind", "count"]),
            )
            .await;
            assert_eq!(child_node.len(), 1, "a child answers tree/node");
            let below = rows(&branch, &operator, ask("tree/span", &child, &["node"])).await;
            if text(&child_node[0], "kind") == "segment" {
                assert!(below.is_empty(), "a segment has no spans");
                let found = rows(&branch, &operator, ask("tree/entry", &child, ENTRY)).await;
                assert_eq!(
                    found.len() as u128,
                    unsigned(&child_node[0], "count"),
                    "one row per entry"
                );
                let named: BTreeSet<Entity> =
                    found.iter().map(|row| entity(row, "entry")).collect();
                assert_eq!(named.len(), found.len(), "no two entries share an entity");
                for (at, entry) in found.iter().enumerate() {
                    assert_eq!(unsigned(entry, "at"), at as u128);
                    assert!(!bytes(entry, "key").is_empty(), "an entry has a key");
                    assert!(entry.contains_key("rank"), "and its key a rank");
                    assert_eq!(text(entry, "state"), "asserted");
                }
                entries += found.len();
            } else {
                frontier.extend(below.iter().map(|span| entity(span, "node")));
            }
        }
        assert!(
            entries >= 800,
            "the descent reaches every fact's entries: {entries}"
        );
        Ok(())
    }

    /// Every entry of every segment the root reaches.
    async fn entries_under(
        branch: &Branch,
        operator: &Operator,
        root: &Entity,
    ) -> Vec<ResolverConclusion> {
        let mut entries = Vec::new();
        for node in reachable(branch, operator, root).await {
            entries.extend(rows(branch, operator, ask("tree/entry", &node, ENTRY)).await);
        }
        entries
    }

    /// An entry comes whole. A value past the inline threshold spills,
    /// and its entry names it as an `asset:` that `tree/value` reads
    /// back; an inline value's entry has no `spill` at all. A versioned
    /// commit's entries carry their claim's origin and edition.
    #[dialog_common::test]
    async fn it_reads_an_entry_whole() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let big = "x".repeat(10 * 1024);
        branch
            .transaction()
            .assert(the!("test/big").of(Entity::new()?).is(big.clone()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let root = root_of(&branch);
        let entries = entries_under(&branch, &operator, &root).await;
        assert!(!entries.is_empty(), "the committed tree has entries");
        assert!(
            entries
                .iter()
                .any(|row| !bytes(row, "origin").is_empty() && row.contains_key("edition")),
            "a versioned commit stamps its claims: {entries:?}"
        );

        let spill = entries
            .iter()
            .find_map(|row| match row.get("spill") {
                Some(Value::Entity(asset)) => Some(asset.clone()),
                _ => None,
            })
            .expect("the 10 KiB value spilled");
        assert!(
            spill.as_str().starts_with("asset:"),
            "a spill is an asset: {spill}"
        );

        let values = rows(
            &branch,
            &operator,
            ask("tree/value", &spill, &["size", "bytes"]),
        )
        .await;
        assert_eq!(values.len(), 1, "one row per value");
        let size = unsigned(&values[0], "size");
        assert!(
            size >= big.len() as u128,
            "the value is whole ({size} bytes)"
        );
        assert_eq!(bytes(&values[0], "bytes").len() as u128, size);

        // A node's handle names no spilled value, and a spilled value's
        // names no node.
        assert!(
            rows(&branch, &operator, ask("tree/value", &root, &["size"]))
                .await
                .is_empty()
        );
        assert!(
            rows(&branch, &operator, ask("tree/node", &spill, &["kind"]))
                .await
                .is_empty()
        );

        // Enough small facts to fill segments: their entries have no
        // spill, and none is a blob record.
        let (branch, root) = committed_wide_branch(&repo, &operator, 800).await?;
        let entries = entries_under(&branch, &operator, &root).await;
        assert!(
            entries.iter().any(|row| !row.contains_key("spill")),
            "an inline value's entry has no spill"
        );
        assert!(
            entries.iter().all(|row| !row.contains_key("blob")
                && !row.contains_key("blob-size")
                && !row.contains_key("blob-version")),
            "an entry that is no blob record has no blob slots"
        );
        Ok(())
    }

    /// However many resolvers a query asks of one node, the environment
    /// reads its block once at most: a node it already keeps is not read
    /// again at all.
    #[dialog_common::test]
    async fn it_reads_a_node_once_for_every_part_asked() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, root) = committed_wide_branch(&repo, &operator, 800).await?;

        let env = Counting::new(operator.clone());
        counted(&branch, &env, ask("tree/node", &root, NODE)).await;
        counted(&branch, &env, ask("tree/span", &root, SPAN)).await;
        counted(&branch, &env, ask("tree/block", &root, &["bytes"])).await;
        assert!(
            env.block_reads() <= 1,
            "three parts of one node are one read of its block at most: {}",
            env.block_reads()
        );
        Ok(())
    }

    /// A handle that names no node the environment can reach, an entity
    /// that is no handle, and a value that is no entity each yield no
    /// rows and no error.
    #[dialog_common::test]
    async fn it_yields_nothing_for_what_is_not_a_reachable_node() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, root) = committed_branch(&repo, &operator).await?;

        let unknown = Entity::from_node(&[7u8; 32])?;
        let other: Entity = "user:alice".parse()?;
        for handle in [&unknown, &other] {
            let answered = rows(&branch, &operator, ask("tree/node", handle, NODE)).await;
            assert!(answered.is_empty(), "no rows for {handle}: {answered:?}");
        }

        let reference = base58::ToBase58::to_base58(&root.node_hash().unwrap()[..]);
        let mut terms = Parameters::new();
        terms.insert("of".into(), Term::from(Value::String(reference)).into());
        let by_string = ask("tree/node", &root, NODE).with_parameters(&terms);
        assert!(
            rows(&branch, &operator, by_string).await.is_empty(),
            "a node is asked for by its handle, not by a string of its hash"
        );

        let mut terms = Parameters::new();
        terms.insert("of".into(), Term::<Value>::blank().into());
        let blank = ask("tree/node", &root, NODE).with_parameters(&terms);
        assert!(
            rows(&branch, &operator, blank).await.is_empty(),
            "a resolver given no handle finds no node of its own accord"
        );
        Ok(())
    }

    /// Content addressing keeps history navigable: after a second
    /// commit the old root still answers, and the new root differs.
    #[dialog_common::test]
    async fn it_keeps_old_roots_readable() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, first) = committed_branch(&repo, &operator).await?;

        branch
            .transaction()
            .assert(the!("test/name").of(Entity::new()?).is("later".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let second = root_of(&branch);
        assert_ne!(first, second, "the root moved");
        for root in [&first, &second] {
            let answered = rows(&branch, &operator, ask("tree/node", root, &["kind"])).await;
            assert_eq!(answered.len(), 1, "root {root} answers");
        }
        Ok(())
    }

    /// A transaction's query reads through the same environment, so the
    /// resolvers answer in its as-if-committed view too.
    #[dialog_common::test]
    async fn it_serves_resolvers_in_transaction_queries() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, root) = committed_branch(&repo, &operator).await?;

        let tx = branch.transaction();
        let answered: Vec<ResolverConclusion> = tx
            .query()
            .select(ask("tree/node", &root, &["kind"]))
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(answered.len(), 1, "a transaction's query serves tree/node");
        Ok(())
    }

    /// A rule whose body is a resolver alone cannot be installed: nothing
    /// in it says which nodes, and a resolver does not go looking.
    #[dialog_common::test]
    fn it_refuses_a_rule_that_names_no_nodes() {
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": {
                "kind": { "the": "probe/node-kind", "as": "text:" }
            }},
            "when": [{
                "assert": "tree/node",
                "where": {
                    "of": { "?": { "name": "this" } },
                    "kind": { "?": { "name": "kind" } }
                }
            }]
        }))
        .expect("a rule descriptor");
        let refused = descriptor
            .compile()
            .expect_err("the rule needs its node given");
        assert!(
            format!("{refused:?}").contains("RequiredBindings"),
            "refused for the handle nothing binds: {refused:?}"
        );
    }

    /// The branch's root reaches a rule as a handle: `dialog.branch/tree`
    /// is the root's entity, and a rule joins it into `tree/span` and
    /// each child on into `tree/node`, deriving one row per child on the
    /// child's own entity.
    #[dialog_common::test]
    async fn it_derives_a_row_per_child_from_the_root() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, _) = committed_wide_branch(&repo, &operator, 800).await?;

        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": {
                "parent": { "the": "probe/child-parent", "as": "entity:" },
                "kind": { "the": "probe/child-kind", "as": "text:" }
            }},
            "when": [
                {
                    "assert": { "with": {
                        "tree": { "the": "dialog.branch/tree", "as": "entity:" }
                    }},
                    "where": {
                        "this": { "?": { "name": "branch" } },
                        "tree": { "?": { "name": "parent" } }
                    }
                },
                {
                    "assert": "tree/span",
                    "where": {
                        "of": { "?": { "name": "parent" } },
                        "node": { "?": { "name": "this" } }
                    }
                },
                {
                    "assert": "tree/node",
                    "where": {
                        "of": { "?": { "name": "this" } },
                        "kind": { "?": { "name": "kind" } }
                    }
                }
            ]
        }))?;
        let rule = descriptor.compile().expect("the rule compiles");
        let conclusion = rule.conclusion().clone();
        branch
            .transaction()
            .assert(&rule)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // Installing the rule committed, so read the root it left.
        let root = root_of(&branch);
        let spans = rows(&branch, &operator, ask("tree/span", &root, &["node"])).await;
        assert!(spans.len() > 1, "the root is an index: {spans:?}");
        let expected: BTreeSet<Entity> = spans.iter().map(|span| entity(span, "node")).collect();

        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("parent".into(), Term::var("parent"));
        terms.insert("kind".into(), Term::var("kind"));
        let derived: Vec<ConceptConclusion> = branch
            .query()
            .select(ConceptQuery {
                predicate: conclusion,
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?;
        let children: BTreeSet<Entity> = derived.iter().map(|row| row.entity().clone()).collect();
        assert_eq!(derived.len(), spans.len(), "one row per child: {derived:?}");
        assert_eq!(children, expected, "each row is on its own child");
        for row in &derived {
            let parent: Entity = row.get("parent")?;
            assert_eq!(parent, root, "every child names the root as its parent");
        }
        Ok(())
    }

    /// A concept of nodes whose rule names its nodes by a fact: the rule
    /// installs with no resolver listing anything, answers for the nodes
    /// the facts name and for no other, and looks one up by `this`.
    ///
    /// That a node no fact names is never read is the planner's doing
    /// and is pinned where a plan can be seen
    /// (`it_is_planned_after_the_fact_that_names_its_node` in
    /// `dialog-query`): here every node of a tree this replica wrote is
    /// already kept by the environment, so no read of one would show.
    #[dialog_common::test]
    async fn it_reads_only_the_nodes_a_fact_names() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, _) = committed_wide_branch(&repo, &operator, 800).await?;

        let rule = known(
            serde_json::json!({ "kind": { "the": "probe/node-kind", "as": "text:" } }),
            serde_json::json!({
                "assert": "tree/node",
                "where": {
                    "of": { "?": { "name": "this" } },
                    "kind": { "?": { "name": "kind" } }
                }
            }),
        )
        .compile()
        .expect("a rule that names its nodes compiles");
        let conclusion = rule.conclusion().clone();
        branch
            .transaction()
            .assert(&rule)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // Name two nodes of the tree as it now stands: its root and one
        // child. The facts are session facts, so naming them moves no root.
        let root = root_of(&branch);
        let children: Vec<Entity> = rows(&branch, &operator, ask("tree/span", &root, &["node"]))
            .await
            .iter()
            .map(|span| entity(span, "node"))
            .collect();
        assert!(children.len() > 2, "the tree has nodes no fact will name");
        let named = children[0].clone();
        let unnamed = children[1].clone();
        branch
            .overlay()
            .assert(the!("probe/known").of(root.clone()).is("yes".to_string()))?
            .assert(the!("probe/known").of(named.clone()).is("yes".to_string()))?;

        let select = |this: Term<Entity>| {
            let mut terms = Parameters::new();
            terms.insert("this".into(), this.into());
            terms.insert("kind".into(), Term::var("kind"));
            ConceptQuery {
                predicate: conclusion.clone(),
                terms,
            }
        };
        let answer = async |this: Term<Entity>| -> anyhow::Result<BTreeSet<Entity>> {
            let derived: Vec<ConceptConclusion> = branch
                .query()
                .select(select(this))
                .perform(&operator)
                .try_vec()
                .await?;
            Ok(derived.iter().map(|row| row.entity().clone()).collect())
        };

        assert_eq!(
            answer(Term::var("this")).await?,
            BTreeSet::from([root, named.clone()]),
            "asked for everything, the nodes the facts name"
        );
        assert_eq!(
            answer(Term::from(named.clone())).await?,
            BTreeSet::from([named]),
            "a named node is its one row"
        );
        assert!(
            answer(Term::from(unnamed)).await?.is_empty(),
            "a node no fact names is no row"
        );
        Ok(())
    }

    /// A node's children are read through the node: a concept keyed by
    /// the parent lists the children of one node from that node's block
    /// alone, and reads none of the children.
    #[dialog_common::test]
    async fn it_lists_the_children_of_a_given_node_from_its_block() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, _) = committed_wide_branch(&repo, &operator, 800).await?;

        let rule = known(
            serde_json::json!({
                "child": { "the": "probe/node-child", "as": "entity:", "pick": "all" }
            }),
            serde_json::json!({
                "assert": "tree/span",
                "where": {
                    "of": { "?": { "name": "this" } },
                    "node": { "?": { "name": "child" } }
                }
            }),
        )
        .compile()
        .expect("a rule keyed by the parent compiles");
        let conclusion = rule.conclusion().clone();
        branch
            .transaction()
            .assert(&rule)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let root = root_of(&branch);
        branch
            .overlay()
            .assert(the!("probe/known").of(root.clone()).is("yes".to_string()))?;

        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::from(root.clone()).into());
        terms.insert("child".into(), Term::var("child"));
        let env = Counting::new(operator.clone());
        let derived: Vec<ConceptConclusion> = branch
            .query()
            .select(ConceptQuery {
                predicate: conclusion,
                terms,
            })
            .perform(&env)
            .try_vec()
            .await?;
        let mut children = BTreeSet::new();
        for row in &derived {
            children.insert(row.get::<Entity>("child")?);
        }
        assert!(children.len() > 1, "the root is an index: {derived:?}");
        assert!(
            env.block_reads() <= 1,
            "the node's own block at most; no child's is read: {}",
            env.block_reads()
        );

        let spans = rows(&branch, &operator, ask("tree/span", &root, &["node"])).await;
        let expected: BTreeSet<Entity> = spans.iter().map(|span| entity(span, "node")).collect();
        assert_eq!(children, expected, "every child of the node, and no other");
        Ok(())
    }
}

#[cfg(test)]
mod ordered_relation_tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::test_repo;
    use dialog_corpus::position::{Bias, Position, insert};
    use dialog_corpus::{
        Directory, Entity, Fact, FactViewStream as _, Relation as CorpusRelation, Selector,
        Sequence, Symbol, Value,
    };
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::AttributeStatement;
    use dialog_query::attribute::Relation;
    use futures_util::TryStreamExt as _;
    use std::ops::RangeBounds;
    use std::str::FromStr as _;

    /// Derive the position for `member` in the given range, biased by
    /// the member's entity reference.
    fn place(member: &Entity, range: impl RangeBounds<Position>) -> Position {
        let bias = Bias::derive(member.to_string().as_bytes());
        insert(&bias, range).expect("position derives")
    }

    /// A membership fact: `[list  test.list/<position>  member]`.
    fn membership(list: &Entity, position: &Position, member: &Entity) -> AttributeStatement {
        let domain = Symbol::from_str("test.list").expect("domain parses");
        let attribute = CorpusRelation::compose(&domain, position.clone()).expect("attribute fits");
        AttributeStatement {
            the: Relation::from(attribute),
            of: list.clone(),
            is: Value::Entity(member.clone()),
            cause: None,
            cardinality: None,
        }
    }

    /// An ordered collection encoded as position-bearing attributes
    /// comes back from ONE prefix range scan already sorted — appends,
    /// prepend-free insertion between neighbors and all.
    #[dialog_common::test]
    async fn it_reads_ordered_members_from_one_scan() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let list = Entity::new()?;
        let apples = Entity::new()?;
        let bananas = Entity::new()?;
        let milk = Entity::new()?;
        let bread = Entity::new()?;

        // Build the list by appending, then wedge bread between apples
        // and bananas — the classic insert-in-the-middle.
        let at_apples = place(&apples, ..);
        let at_bananas = place(&bananas, &at_apples..);
        let at_milk = place(&milk, &at_bananas..);
        let at_bread = place(&bread, &at_apples..&at_bananas);

        branch
            .transaction()
            .assert(membership(&list, &at_apples, &apples))
            .assert(membership(&list, &at_bananas, &bananas))
            .assert(membership(&list, &at_milk, &milk))
            .assert(membership(&list, &at_bread, &bread))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // A dictionary entry lives in the same domain: the disjoint
        // name shapes (symbols start lowercase, positions uppercase)
        // let one scan serve both.
        let domain = Symbol::from_str("test.list")?;
        let title = CorpusRelation::compose(&domain, Symbol::from_str("title")?)?;
        branch
            .transaction()
            .assert(AttributeStatement {
                the: Relation::from(title),
                of: list.clone(),
                is: Value::String("Groceries".into()),
                cause: None,
                cardinality: None,
            })
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // One contiguous range scan of the list's domain.
        let members: Vec<Fact> = branch
            .claims()
            .select(Selector::new().of(list.clone()).with_domain(&domain))
            .perform(&operator)
            .await?
            .owned()
            .try_collect()
            .await?;

        let attributes: Vec<String> = members.iter().map(|fact| fact.the.to_string()).collect();
        let mut sorted = attributes.clone();
        sorted.sort();
        assert_eq!(attributes, sorted, "the scan streams in position order");

        // Classify the scan by name shape: named entries into a
        // directory, position-named members into a sequence.
        let mut fields: Directory<Value> = Directory::new();
        let mut sequence: Sequence<Value> = Sequence::new();
        for fact in &members {
            let named = fields.admit(&fact.the, fact.is.clone());
            let ordered = sequence.admit(&fact.the, fact.is.clone());
            assert!(named != ordered, "every entry lands in exactly one");
        }

        assert_eq!(
            fields.get(&Symbol::from_str("title")?),
            Some(&Value::String("Groceries".into())),
            "the dictionary entry reads by name"
        );

        let expected: Vec<Value> = [&apples, &bread, &bananas, &milk]
            .into_iter()
            .map(|member| Value::Entity(member.clone()))
            .collect();
        let values: Vec<Value> = sequence.values().cloned().collect();
        assert_eq!(values, expected, "members arrive in list order");

        // The sequence's edge positions are the bounds for the next
        // insertion: append after the last member.
        assert_eq!(sequence.first_position(), Some(&at_apples));
        assert_eq!(sequence.last_position(), Some(&at_milk));
        let eggs = Entity::new()?;
        let at_eggs = place(&eggs, sequence.last_position().expect("nonempty")..);
        assert!(
            at_eggs > at_milk,
            "the appended position sorts after every member"
        );
        Ok(())
    }
}
