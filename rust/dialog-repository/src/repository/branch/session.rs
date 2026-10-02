use dialog_effects::blob::Read as BlobRead;
use std::cell::Cell;
use std::collections::HashSet;

use dialog_artifacts::history::Edition;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStream, ArtifactView, Changes, DialogArtifactsError,
    Entity, Estimate, Likelihood, Preload, PreloadRequest, Select, SortKey, Speculation, Statement,
    sort_key,
};
use dialog_artifacts::{LoadBlob, Relation as ArtifactsRelation, Standing, Value};
use dialog_capability::{Capability, Fork, Provider};
use dialog_common::{Buffer, ConditionalSync, Held, Holds};
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::{Identify, Operator, OperatorExt as _};
use dialog_effects::memory::Resolve;
use dialog_query::attribute::AttributeDescriptor;
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
use dialog_search_tree::{DialogSearchTreeError, LoadBlock, Manifest, PersistentNode};
use futures_util::future::try_join_all;
use futures_util::{TryStreamExt as _, stream};
use std::sync::{Arc, LazyLock, Mutex};
use tokio::sync::OnceCell;

use crate::CommitError;
use crate::REGISTRY;
use crate::layer::{Hidden, MergeKeys, filter_hidden, merge_grouped, tombstones_from};
use crate::repository::branch::select::line_manifest;
use crate::repository::fetch::Driven;
use crate::repository::source::{Source, SourceRef};
use crate::repository::{CellSettlement, ReadObservation};
use crate::rules::{
    LayerRoots, RuleRead, Selecting, assemble, builtin, builtin_derives, builtin_deriving,
    conclusion_attr, derives_attr, derives_keys, derives_selector, has_overlay_rules, head_onto,
    hydrate, overlay_rules_deriving, quarantined_attr, rule_entities, source_attr, source_bytes,
    source_selector,
};
use crate::schema::{
    Branch as BranchConcept, DidExt as _, Replica, Session, SessionBranch, session,
};
use crate::{Branch, Hydrate, NetworkedIndex, RemoteSite, Snapshot, Staged};

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
/// # fn example<Q: Application>(branch: &Branch, snapshot: &Snapshot, query: Q, facts: dialog_artifacts::Changes) {
/// let layer = branch
///     .query()
///     .join(snapshot)                 // another line
///     .with(facts);                   // user-asserted overlay facts
/// let staged = layer.select(query);   // `.perform(&env)` injects metadata
/// # let _ = staged;
/// # }
/// ```
#[derive(Default, Clone)]
pub struct QueryLayer<'a> {
    sources: Vec<SourceRef<'a>>,
    changes: Changes,
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
        self.sources.extend(other.sources);
        other.changes.assert(&mut self.changes);
        self
    }

    /// The branches this layer reads from, in join order.
    pub fn branches(&self) -> Vec<&'a Branch> {
        self.sources
            .iter()
            .filter_map(|source| match source {
                SourceRef::Branch(branch) => Some(*branch),
                SourceRef::Snapshot(_) => None,
            })
            .collect()
    }

    /// The snapshots this layer reads from, in join order.
    pub fn snapshots(&self) -> Vec<&'a Snapshot> {
        self.sources
            .iter()
            .filter_map(|source| match source {
                SourceRef::Snapshot(snapshot) => Some(*snapshot),
                SourceRef::Branch(_) => None,
            })
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
    /// DIDs the schema entities are derived from.
    pub fn metadata(&self, operator: &Capability<Operator>) -> Changes {
        Changes::clone(&self.shared_metadata(operator))
    }

    /// [`metadata`](Self::metadata), shared with every other query over
    /// the same branch, profile, operator and head.
    fn shared_metadata(&self, operator: &Capability<Operator>) -> Arc<Changes> {
        // Every query folds this in, and for a layer over one branch it
        // depends only on the profile, the operator and the head, so the
        // branch keeps it: deriving it hashes and base58-renders entities
        // and re-parses both DIDs each time.
        if let [SourceRef::Branch(branch)] = self.sources.as_slice() {
            return branch.layer_metadata(operator, || self.derive_metadata(operator));
        }
        Arc::new(self.derive_metadata(operator))
    }

    /// Derive what [`metadata`](Self::metadata) folds in.
    fn derive_metadata(&self, operator: &Capability<Operator>) -> Changes {
        let mut changes = Changes::new();

        let mut branch_entities = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            if let Some(entity) = source.metadata(operator, &mut changes) {
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
        for source in &self.sources {
            let subject = source.subject();
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

        changes
    }

    /// The full per-query overlay: this layer's own
    /// [`changes`](Self::changes) with [`metadata`](Self::metadata)
    /// folded in. This is exactly what `.select(..).perform(..)`
    /// queries against alongside the branch streams.
    ///
    /// A layer that adds no changes of its own queries the metadata
    /// alone, which is then the branch's shared copy rather than a new
    /// one rebuilt fact by fact for every query.
    pub fn overlay(&self, operator: &Capability<Operator>) -> Arc<Changes> {
        let metadata = self.shared_metadata(operator);
        if self.changes.is_empty() {
            return metadata;
        }
        let mut overlay = self.changes.clone();
        Changes::clone(&metadata).assert(&mut overlay);
        Arc::new(overlay)
    }

    /// Stage a query application. Call `.perform(&operator)` to execute.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        SelectQuery {
            layer: self.clone(),
            query,
        }
    }
}

// A line's session overlay ([`Branch::overlay`], [`Snapshot::overlay`])
// is not folded here: [`QueryEnv`] reads it live at every evaluation,
// so every read path — `select`, `query`, transaction queries,
// subscription evaluations — sees session facts with no per-path
// wiring and no snapshot to go stale.
impl<'a> From<SourceRef<'a>> for QueryLayer<'a> {
    fn from(source: SourceRef<'a>) -> Self {
        Self {
            sources: vec![source],
            changes: Changes::new(),
        }
    }
}

impl<'a> From<&'a Branch> for QueryLayer<'a> {
    fn from(branch: &'a Branch) -> Self {
        Self::from(SourceRef::from(branch))
    }
}

impl<'a> From<&'a Snapshot> for QueryLayer<'a> {
    fn from(snapshot: &'a Snapshot) -> Self {
        Self::from(SourceRef::from(snapshot))
    }
}

impl From<Changes> for QueryLayer<'_> {
    fn from(changes: Changes) -> Self {
        Self {
            sources: Vec::new(),
            changes,
        }
    }
}

/// A query command ready to be performed against an environment.
pub struct SelectQuery<'a, Q> {
    layer: QueryLayer<'a>,
    query: Q,
}

impl<'a, Q> SelectQuery<'a, Q> {
    pub(crate) fn new(source: impl Into<SourceRef<'a>>, query: Q) -> Self {
        Self {
            layer: QueryLayer::from(source.into()),
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
            + Provider<Hydrate>
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
                .map_err(|e| DialogArtifactsError::Storage(format!("identify: {e}")))?;

            let overlay = layer.overlay(&operator);
            let sources: Vec<Source> =
                layer.sources.iter().map(|source| source.to_source()).collect();
            let query_env = QueryEnv::new(sources.clone(), overlay, env);
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
    Provider<BlobRead>
    + Provider<Get>
    + Provider<Put>
    + Provider<Resolve>
    + Provider<Hydrate>
    + Provider<Preload>
    + Provider<Fork<RemoteSite, Resolve>>
    + Holds
    + ConditionalSync
{
}

impl<T> Capabilities for T where
    T: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Hydrate>
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
    /// All overlay facts — caller-asserted + auto-injected metadata —
    /// merged into one batch. Queried via [`Changes::select`] under the
    /// lines' format.
    changes: Arc<Changes>,
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
    /// [`SourceRef::fetches`](crate::repository::source::SourceRef)):
    /// preload hints are refused when none can.
    fetches: bool,
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
    /// Build a runtime env from already-resolved parts: the lines to
    /// read, the per-query overlay (caller changes + injected metadata),
    /// and the underlying capability env. The tombstones are lifted
    /// on first read, once the lines' format is known: the per-query
    /// retracts, keyed under each line's manifest.
    ///
    /// `Branch::query`, `Snapshot::query`, and the transaction-query
    /// paths all construct through here so there is exactly one query
    /// env — a transaction query is just a single-line `QueryEnv`.
    /// Deductive-rule resolution is built in (a durable layer per line,
    /// its session overlay, and the per-query changes as a transient
    /// layer), so the paths can never diverge on it.
    pub(crate) fn new(
        sources: Vec<Source>,
        changes: impl Into<Arc<Changes>>,
        env: &'a Erased,
    ) -> Self {
        let changes = changes.into();
        let fetches = sources.iter().any(|source| source.as_ref().fetches());
        Self {
            sources,
            changes,
            layers: Vec::new(),
            format: Arc::new(OnceCell::new()),
            demand: None,
            reads: Arc::new(Mutex::new(Vec::new())),
            fixpoint: None,
            fetches,
            memo: Memo::default(),
            env,
        }
    }

    /// Read `layers` above the lines too: a transaction's writes, held
    /// so reading them costs a range read however many there are.
    pub(crate) fn with_layers(mut self, layers: Vec<Staged>) -> Self {
        self.layers = layers;
        self
    }

    /// What a read settlement observes of this environment: the lines'
    /// heads, their session overlays and the metadata read with.
    pub(crate) fn observation(&self) -> ReadObservation {
        ReadObservation {
            heads: self
                .sources
                .iter()
                .map(|source| source.as_ref().revision())
                .collect(),
            overlays: self
                .sources
                .iter()
                .map(|source| source.as_ref().overlay().revision())
                .collect(),
            metadata: self.changes.clone(),
        }
    }

    /// The layers' choosing writes a read of `input` meets, each cell
    /// as the layer's [`ReadSettlement`] leaves it: the settlement the
    /// commit applies, kept per layer and observation and advanced only
    /// over the writes it has not settled.
    ///
    /// [`ReadSettlement`]: super::transaction::ReadSettlement
    async fn settle_within(
        &self,
        input: &ArtifactSelector<Constrained>,
    ) -> Result<Vec<(usize, Vec<CellSettlement>)>, DialogArtifactsError> {
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
            let failed = |error: CommitError| DialogArtifactsError::Storage(error.to_string());
            let mut settlement = layer
                .take_settlement(observed)
                .unwrap_or_else(|| super::transaction::ReadSettlement::new(self.pending_edition()));
            // A settlement that failed partway is not kept: the next
            // read starts it again.
            Box::pin(settlement.advance(&self.sources, &self.changes, layer, self.env))
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
            .and_then(|source| source.as_ref().revision())
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
    fn record_demand(&self, selector: &ArtifactSelector<Constrained>, manifest: &Manifest) {
        if let Some(demand) = &self.demand {
            demand.record(selector, manifest);
        }
    }

    /// Note a rule-discovery read: recorded as rule demand when
    /// recording is on, and kept so a rule set assembled from it can
    /// replay it for a later subscription.
    fn read_rules(&self, selector: &ArtifactSelector<Constrained>, manifest: &Manifest) {
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
    async fn format(&self) -> Result<&Format, DialogArtifactsError> {
        self.format
            .get_or_try_init(|| async {
                let layers = self.layers();
                let mut lines = Vec::with_capacity(self.sources.len());
                for source in &self.sources {
                    lines.push(line_manifest(source.as_ref(), self.env).await?);
                }
                // With no line there is no tree, and the overlay is read
                // as a new tree would order it.
                let manifest = lines.first().cloned().unwrap_or_default();
                let keys = if lines.iter().all(|line| *line == manifest) {
                    MergeKeys::Stored
                } else {
                    MergeKeys::Fields
                };
                let shared = Arc::new(tombstones_from(&self.changes, &manifest));
                let lines = self
                    .sources
                    .iter()
                    .zip(lines)
                    .map(|(source, line)| {
                        let changes = if line == manifest {
                            shared.clone()
                        } else {
                            Arc::new(tombstones_from(&self.changes, &line))
                        };
                        // Every set is shared, never merged, so this costs
                        // nothing per query however much the layers or the
                        // session hold.
                        let mut staged = Hidden::default().facts(changes);
                        for layer in layers {
                            staged = staged.facts(layer.tombstones(&line));
                        }
                        let tombstones = staged
                            .clone()
                            .facts(source.as_ref().overlay().tombstones(&line));
                        LineFormat {
                            manifest: line,
                            staged,
                            tombstones,
                        }
                    })
                    .collect();
                Ok(Format {
                    manifest,
                    keys,
                    lines,
                })
            })
            .await
    }
}

impl Clone for QueryEnv<'_> {
    fn clone(&self) -> Self {
        Self {
            sources: self.sources.clone(),
            changes: self.changes.clone(),
            layers: self.layers.clone(),
            format: self.format.clone(),
            demand: self.demand.clone(),
            reads: self.reads.clone(),
            fixpoint: self.fixpoint.clone(),
            fetches: self.fetches,
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
    source: SourceRef<'_>,
    env: &'a Erased,
    input: ArtifactSelector<Constrained>,
) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
    let select = crate::Select::from_source(source, input);
    let remote = source.fallback();
    // Concurrent reads of one digest share fetch-and-hydrate through
    // the env's own `Hydrate` flight (see `crate::Hydrate`), with
    // every other evaluation in the process.
    let store = NetworkedIndex::new(env, select.catalog(), remote);
    Ok(select.execute_boxed(store).await?)
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
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
impl<'a> Provider<Select<'a>> for QueryEnv<'a> {
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        if input.attribute() == Some(&*QUARANTINED) {
            return Box::pin(self.select_quarantined(input)).await;
        }
        let format = self.format().await?;
        let manifest = format.manifest.clone();
        self.record_demand(&input, &manifest);
        // One fact can reach the merge from more than one stream: a value
        // the line holds that the session overlay or a staged write holds
        // too. The merge keeps the first stream's row of such a fact, so
        // the streams go in by precedence, newest standing first: the
        // session overlays, the staged writes, the lines, and the
        // per-query changes, which stand at no version.
        let mut sessions: Vec<ArtifactStream<'a>> = Vec::new();
        let mut staged: Vec<ArtifactStream<'a>> = Vec::new();
        let mut lines: Vec<ArtifactStream<'a>> = Vec::with_capacity(self.sources.len());
        let mut changes: Vec<ArtifactStream<'a>> = Vec::new();

        // The choosing writes this read meets, settled cell by cell:
        // the line's claims they succeed, or succeed and write back, are
        // hidden from every line, and the values the cells already held
        // are hidden from the layers' own rows, as the commit would
        // write nothing for them.
        let settled = self.settle_within(&input).await?;
        let succeeded: Vec<&Artifact> = settled
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
        for (source, line) in self.sources.iter().zip(&format.lines) {
            let raw = select_from_source(source.as_ref(), self.env, input.clone()).await?;
            let mut hidden = line.tombstones.within(&input);
            if !succeeded.is_empty() {
                hidden = hidden.facts(hidden_in(&line.manifest));
            }
            lines.push(filter_hidden(raw, hidden, line.manifest.clone()));
        }

        // Each line's session overlay, read live. Filtered by the
        // staged retracts only: the overlay's own tombstones hide facts
        // *beneath* it, never its own. Pushed only when it has rows,
        // for the same reason the per-query stream is below. An
        // overlay row is the newest fact of its cell: it stands past
        // the edition the next commit mints, above every committed row
        // and every staged write, so a read elects it under `last` and
        // ranks it with the rest under any other pick, until the
        // session takes it back.
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
        let mut session = None;
        for (source, line) in self.sources.iter().zip(&format.lines) {
            let rows = source.as_ref().overlay().select(&input, &line.manifest);
            if rows.is_empty() {
                continue;
            }
            let session = *session.get_or_insert_with(|| {
                self.sources
                    .iter()
                    .filter_map(|source| source.as_ref().revision())
                    .map(|revision| revision.edition.successor())
                    .fold(pending(), |newest, edition| newest.max(edition))
                    .successor()
            });
            let rows: ArtifactStream<'a> = Box::pin(stream::iter(
                rows.into_iter()
                    .map(move |fact| Ok(ArtifactView::pending(fact, session))),
            ));
            sessions.push(filter_hidden(
                rows,
                line.staged.clone(),
                line.manifest.clone(),
            ));
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
        let overlay = self.changes.select(&input, &manifest);
        if !overlay.is_empty() {
            changes.push(Box::pin(stream::iter(
                overlay.into_iter().map(|fact| Ok(fact.into())),
            )));
        }

        // Staged layers — a range read each, in the lines' format, and
        // pushed only when they have rows, for the same reason. A staged
        // row is a write the transaction will commit, so it stands at
        // the edition that commit mints, equal to every other write of
        // the transaction: a read over the transaction elects as a read
        // after the commit will.
        for (index, layer) in self.layers().iter().enumerate() {
            let rows = layer.select(&input, &manifest);
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
            let rows: ArtifactStream<'a> = Box::pin(stream::iter(
                rows.into_iter()
                    .map(move |fact| Ok(ArtifactView::pending(fact, pending))),
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
        }

        // Most reads meet the lines alone, and take their streams as
        // they are.
        let streams: Vec<ArtifactStream<'a>> =
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
        Ok(merge_grouped(streams, manifest, format.keys))
    }
}

// A range-size estimate for the planner's merge-versus-fold choice:
// the range's edge paths per line, summed. The `Changes` overlay is not
// consulted (small, and irrelevant to the order-of-magnitude answer a
// strategy heuristic needs). Summing lines is an upper bound — a fact
// on two lines counts twice — which is the safe direction for a "how
// broad is this range" question. `None` from every line (all empty)
// yields `None`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Estimate> for QueryEnv<'_> {
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<Option<u64>, DialogArtifactsError> {
        let mut total: Option<u64> = None;
        for source in &self.sources {
            let select = crate::Select::from_source(source.as_ref(), input.clone());
            let remote = source.as_ref().fallback();
            let store = NetworkedIndex::new(self.env, select.catalog(), remote);
            if let Some(estimate) = select.estimate(store).await? {
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
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Preload> for QueryEnv<'_> {
    async fn execute(&self, input: PreloadRequest) -> bool {
        if !self.fetches {
            return false;
        }
        Provider::<Preload>::execute(self.env, input).await
    }
}

// The spilled-value load behind `tree/value`: the same line-by-line
// walk as `Load`, reading each line's spill lane (its blob store, then
// the block catalog for values spilled before they moved to blobs,
// with the remote fallback a fact scan uses). Spilled values are not
// nodes, so the node cache is not consulted.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<'a> Provider<LoadBlob> for QueryEnv<'a> {
    async fn execute(
        &self,
        LoadBlob { hash }: LoadBlob,
    ) -> Result<Option<Buffer>, DialogArtifactsError> {
        for source in &self.sources {
            let source = source.as_ref();
            let store = NetworkedIndex::new(self.env, source.archive().index(), source.fallback());
            if let Some(bytes) = store.load_blob(&hash).await? {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }
}

// The idempotent block-load behind resolver premises (`tree/node` &
// co). No demand is recorded: the block behind a hash is
// content-addressed and can never change, so no tree diff could ever
// invalidate a row derived from it — the soundness argument lives in
// `dialog_artifacts::inspect`. Reads go through each line's archive
// catalog capability with the same remote fallback a fact scan uses;
// the first line that has the block wins (content addressing makes
// them interchangeable), and a block absent everywhere contributes
// nothing. Reads go through the line's shared node cache (the same
// one the eager root probe in `select.rs` and `Subscription::touched`
// use): a resolver join re-resolves the same reference once per outer
// row, and a raw backend get would re-fetch that identical immutable
// block every time.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<'a> Provider<LoadBlock> for QueryEnv<'a> {
    async fn execute(&self, load: LoadBlock) -> Result<Option<Buffer>, DialogSearchTreeError> {
        for source in &self.sources {
            let source = source.as_ref();
            let remote = source.fallback();
            let store = NetworkedIndex::new(self.env, source.archive().index(), remote);
            let cache = source.node_cache();
            if let Some(node) = cache.get_cached(&load.hash) {
                return Ok(Some(node.buffer().clone()));
            }
            if let Some(block) = load.clone().perform(&store).await? {
                // A block that checks as a node joins the cache; any other
                // block is returned as it is, for the caller to read.
                if let Ok(node) = PersistentNode::try_from(block.clone()) {
                    cache.insert(load.hash.clone(), node);
                }
                return Ok(Some(block));
            }
        }
        Ok(None)
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
    fn selector(self, key: &Entity) -> ArtifactSelector<Constrained> {
        match self {
            Index::Deriving => derives_selector(key),
        }
    }
}

/// The rules a staged layer holds under `index` at `key`: two range
/// reads, as a session's are. The layer is per query, so it is read
/// fresh and records no demand; a body that does not hydrate is
/// skipped, as an overlay's is.
fn staged_rules(layer: &Staged, index: Index, key: &Entity) -> Vec<(DeductiveRule, Installed)> {
    let entities = rule_entities(layer.scan(&index.selector(key)));
    let mut rules = Vec::with_capacity(entities.len());
    for rule_entity in entities {
        if let Some(bytes) = source_bytes(layer.scan(&source_selector(&rule_entity)))
            && let Ok(rule) = hydrate(&bytes)
            && rule.stored_as(&rule_entity)
        {
            rules.push((rule, Installed::Pending));
        }
    }
    rules
}

impl<'a> QueryEnv<'a> {
    /// Read a `dialog.rule/*` selector against a single line's committed
    /// tree only (NOT the overlay) and collect the matching artifacts.
    /// The durable layer's reads must be tree-only so the head-keyed
    /// discovery cache stays correct — overlay rules are handled
    /// separately, fresh, by the transient layer.
    async fn select_tree(
        &self,
        source: &Source,
        selector: ArtifactSelector<Constrained>,
    ) -> Result<Vec<Artifact>, DialogArtifactsError> {
        Ok(self
            .select_tree_standing(source, selector)
            .await?
            .into_iter()
            .map(|(artifact, _)| artifact)
            .collect())
    }

    /// [`select_tree`](Self::select_tree), each artifact with the
    /// standing of the commit that wrote it.
    async fn select_tree_standing(
        &self,
        source: &Source,
        selector: ArtifactSelector<Constrained>,
    ) -> Result<Vec<(Artifact, Standing)>, DialogArtifactsError> {
        // Rule-discovery reads are demand too: a rule committed
        // later for a subscribed concept lands in this range and
        // must re-trigger the subscription. Recorded as *rule*
        // demand: a hit here invalidates the whole result, not one
        // entity's slice.
        let manifest = &self.format().await?.manifest;
        self.read_rules(&selector, manifest);
        // Rule bodies are hydrated from the full artifact, so this read
        // genuinely needs owned rows; it is head-cached, not per-query hot.
        let rows = select_from_source(source.as_ref(), self.env, selector)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut artifacts = Vec::with_capacity(rows.len());
        for row in rows {
            let artifact = match row.to_owned() {
                Ok(artifact) => artifact,
                Err(DialogArtifactsError::CorruptEntry(reason)) => {
                    tracing::warn!(%reason, "ignoring corrupt stored row");
                    continue;
                }
                Err(error) => return Err(error),
            };
            let standing = Standing {
                version: row.standing(),
                cause: Claim::from(artifact.clone()).cause().clone(),
            };
            artifacts.push((artifact, standing));
        }
        Ok(artifacts)
    }

    /// The rules under `index` at `key` held in `source`'s session
    /// overlay: session-asserted `dialog.rule/*` facts, read fresh (the
    /// overlay is in memory and never head-cached). Recorded as rule
    /// demand, so a subscription re-evaluates when a session rule for
    /// the concept or attribute arrives or goes.
    fn session_rules(
        &self,
        source: &Source,
        index: Index,
        key: &Entity,
        manifest: &Manifest,
    ) -> Result<Vec<(DeductiveRule, Installed)>, EvaluationError> {
        let overlay = source.as_ref().overlay();
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
            // A body under an entity it does not hash to is forged,
            // corrupt, or stored by an earlier release: inert, as at
            // commit.
            let rule = hydrate(&bytes)?;
            if rule.stored_as(&rule_entity) {
                rules.push((rule, Installed::Pending));
            }
        }
        Ok(rules)
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
        let cache = source.as_ref().rule_cache();
        let head = source.as_ref().revision();
        let discovered = head.as_ref().and_then(|h| match index {
            Index::Deriving => cache.derived(key, h),
        });
        if let Some(entities) = discovered {
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
                    selector: ArtifactSelector::new().the(attribute),
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
        if let Some(head) = head.clone() {
            match index {
                Index::Deriving => cache.record_derived(key.clone(), head, entities.clone()),
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
        let cache = source.as_ref().rule_cache();
        let rule_entities = self.durable_rule_entities(source, index, key).await?;

        // Hydration: reuse cached bodies (content-addressed, never
        // stale) and fetch + compile the rest from each rule's
        // `dialog.rule/source` — concurrently, because the fetches are
        // independent point selects and a head move with N installed
        // rules must not pay N sequential round trips (blocks already
        // in flight join through the env's `Hydrate` flight).
        let cache = &cache;
        let rules = try_join_all(rule_entities.into_iter().map(
            |(rule_entity, installed)| async move {
                if let Some(body) = cache.body(&rule_entity) {
                    return Ok::<_, EvaluationError>(Some((body, installed)));
                }
                let source_claims = self
                    .select_tree(source, source_selector(&rule_entity))
                    .await
                    .map_err(|e| EvaluationError::Store(format!("rule source lookup: {e:?}")))?;
                let Some(bytes) = source_bytes(source_claims) else {
                    return Ok(None);
                };
                let body = hydrate(&bytes)?;
                // A body under an entity it does not hash to is forged,
                // corrupt, or stored by an earlier release
                // (`Branch::upgrade_rules` re-installs those): inert, as at
                // commit.
                if !body.stored_as(&rule_entity) {
                    return Ok(None);
                }
                cache.record_body(rule_entity, body.clone());
                Ok(Some((body, installed)))
            },
        ))
        .await?;
        Ok(rules.into_iter().flatten().collect())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
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

        // An assembled rule set depends only on the committed layers it was
        // resolved from, so while none has moved the last one assembled
        // stands. Not when rules are read fresh: from the query's overlay,
        // or from a line's session overlay, which moves without moving its
        // root. A query recording what it reads reuses the set as well,
        // and records the rule reads that assembled it as its own.
        let roots = self.layer_roots();
        let cache = self
            .sources
            .first()
            .map(|source| source.as_ref().rule_cache())
            .filter(|_| !has_overlay_rules(&self.changes));
        if let Some((bundle, reads)) = cache
            .as_ref()
            .and_then(|cache| cache.bundle(&input, &roots))
        {
            if let Some(demand) = &self.demand {
                for (selector, manifest) in &reads {
                    demand.record_rules(selector, manifest);
                }
            }
            return Ok(self.continuing(&concept, bundle));
        }
        let first_read = self
            .reads
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len();

        // Plan cache rides a line (peers share content-addressed plans;
        // any line's cache is correct). The overlay-only query has no
        // line, so it falls back to a private cache.
        let plan_cache = self
            .sources
            .first()
            .map(|source| source.as_ref().plan_cache())
            .unwrap_or_default();

        let bundle = self.resolve_bundle(&input, plan_cache).await?;
        let analysis = self.program_analysis(&input, &bundle).await?;
        analysis.check(&input)?;
        let bundle = bundle.without(analysis.quarantined());
        let bundle = if analysis.is_recursive(&ProgramAnalysis::node(&input)) {
            bundle.with_recursion(analysis)
        } else {
            bundle
        };
        if let Some(cache) = cache {
            let reads = self
                .reads
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .get(first_read..)
                .map(<[_]>::to_vec)
                .unwrap_or_default();
            cache.record_bundle(input.clone(), roots, bundle.clone(), reads);
        }
        Ok(self.continuing(&concept, bundle))
    }
}

/// The entity of the attribute concept of `field`'s relation read
/// under no pick: what a rule installed before the `derives` index
/// concluded, and what a concept's derived fields are keyed by, so a
/// reader's pick never changes which rules it finds.
fn relation_concept(field: &ConceptFieldDescriptor) -> Entity {
    ConceptDescriptor::of_attribute(&ConceptFieldDescriptor::required(
        field.descriptor().clone().without_pick(),
    ))
    .this()
}

/// The `dialog.rule/quarantined` attribute, compared on every select.
static QUARANTINED: LazyLock<ArtifactsRelation> = LazyLock::new(quarantined_attr);

impl<'a> QueryEnv<'a> {
    /// The roots of the layers this query reads rules from.
    fn layer_roots(&self) -> LayerRoots {
        LayerRoots {
            lines: self
                .sources
                .iter()
                .map(|source| source.as_ref().root())
                .collect(),
            overlays: self
                .sources
                .iter()
                .map(|source| source.as_ref().overlay().revision())
                .collect(),
            staged: self.layers.iter().map(Staged::generation).collect(),
        }
    }

    /// `dialog.rule/quarantined` rows: one per rule the program analysis
    /// sets aside, of the rule and valued with the concept whose cycle
    /// it closed, narrowed by the selector's entity and value. Nothing
    /// stored under the attribute is read: the rows are the analysis's,
    /// answered over every rule the layers hold.
    async fn select_quarantined(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        let quarantined = self
            .quarantined()
            .await
            .map_err(|error| DialogArtifactsError::Storage(format!("quarantine: {error}")))?;
        let rows: Vec<Artifact> = quarantined
            .into_iter()
            .map(|quarantine| Artifact {
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

    /// Every rule the program analysis sets aside, over every deductive
    /// rule the layers hold, sorted by rule and concept. Kept per layer
    /// roots like an assembled rule set, and recorded as rule demand, so
    /// a subscription re-evaluates when a rule is installed or retracted.
    async fn quarantined(&self) -> Result<Vec<Quarantine>, EvaluationError> {
        let roots = self.layer_roots();
        let cache = self
            .sources
            .first()
            .map(|source| source.as_ref().rule_cache())
            .filter(|_| !has_overlay_rules(&self.changes));
        if let Some((quarantined, reads)) =
            cache.as_ref().and_then(|cache| cache.quarantined(&roots))
        {
            if let Some(demand) = &self.demand {
                for (selector, manifest) in &reads {
                    demand.record_rules(selector, manifest);
                }
            }
            return Ok(quarantined);
        }
        let first_read = self
            .reads
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len();

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

        if let Some(cache) = cache {
            let reads = self
                .reads
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .get(first_read..)
                .map(<[_]>::to_vec)
                .unwrap_or_default();
            cache.record_quarantined(roots, quarantined.clone(), reads);
        }
        Ok(quarantined)
    }

    /// Every deductive rule the layers hold, once each: each line's
    /// committed rules and session overlay, the staged layers and the
    /// per-query changes. A body under an entity it does not hash to is
    /// inert, as at resolution.
    async fn every_rule(&self) -> Result<Vec<DeductiveRule>, EvaluationError> {
        let attribute = source_attr();
        let selector = ArtifactSelector::new().the(attribute.clone());
        let manifest = &self.format().await?.manifest;
        let mut bodies: Vec<(Entity, Vec<u8>)> = Vec::new();
        let mut take = |artifacts: Vec<Artifact>| {
            for artifact in artifacts {
                if let Value::Bytes(bytes) = artifact.is {
                    bodies.push((artifact.of, bytes));
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
            take(source.as_ref().overlay().scan(&selector));
        }
        for layer in &self.layers {
            take(layer.scan(&selector));
        }
        for (entity, changed, change) in self.changes.iter() {
            if *changed == attribute
                && let dialog_artifacts::Change::Assert(Value::Bytes(bytes), _) = change
            {
                bodies.push((entity.clone(), bytes.clone()));
            }
        }

        let cache = self
            .sources
            .first()
            .map(|source| source.as_ref().rule_cache());
        let mut rules: Vec<DeductiveRule> = Vec::new();
        let mut seen: HashSet<Entity> = HashSet::new();
        for (entity, bytes) in bodies {
            if !seen.insert(entity.clone()) {
                continue;
            }
            let cached = cache.as_ref().and_then(|cache| cache.body(&entity));
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
            rules.extend(self.session_rules(source, index, key, manifest)?);
        }
        rules.extend(match index {
            Index::Deriving => overlay_rules_deriving(&self.changes, key)
                .into_iter()
                .map(|rule| (rule, Installed::Pending)),
        });
        for layer in &self.layers {
            rules.extend(staged_rules(layer, index, key));
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
            .map(|source| source.as_ref().rule_cache());
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
    /// relation's `derives` key on any layer, with the rules installed
    /// before the index existed folded into the committed key. Only the
    /// index is read from the key the attribute spells; no concept is
    /// described or hashed and no body is hydrated, which is what a
    /// commit asks once per relation it writes.
    pub(crate) async fn rules_derive(
        &self,
        the: &ArtifactsRelation,
    ) -> Result<bool, EvaluationError> {
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
            if !source.as_ref().overlay().scan(&selector).is_empty() {
                return Ok(true);
            }
        }
        if !overlay_rules_deriving(&self.changes, &on).is_empty() {
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
    /// attribute, found by the `derives` index, by the built-ins, and,
    /// for rules installed before the index existed, by the concept
    /// they conclude. Any other concept selects: its rule reads each
    /// derived attribute through the attribute concept and the rest
    /// from stored facts, and a rule concluding exactly this concept
    /// without a `derives` index installs as written beside it.
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
            // A ranked chain scans every relation it lists and takes the
            // rules deriving any of them; it is never exact.
            for scan in ConceptRules::chain_scans(field) {
                note(&scan);
                bundle.install(scan);
            }
            // The rules deriving each relation the field reads, found
            // by the `derives` index, whatever type or pick this read
            // declares over the relation. A rule installed before the
            // index existed concluded the attribute concept of the
            // relation itself, read under no pick: that is the
            // entity it is found by.
            for relation in field.descriptor().relations() {
                let single = ConceptDescriptor::of_attribute(&ConceptFieldDescriptor::required(
                    AttributeDescriptor::over(
                        relation.clone(),
                        "",
                        field.cardinality(),
                        field.descriptor().content_type(),
                    ),
                ));
                let Some(on) = derives_keys(&single).into_iter().next() else {
                    continue;
                };
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
        let mut sole: Option<Option<(DeductiveRule, Vec<dialog_artifacts::Relation>)>> = None;
        for (_, field) in descriptor.with().iter() {
            let attribute = ConceptDescriptor::of_attribute(field);
            // Keyed by the relation's own attribute concept, read under
            // no pick, which is what a legacy rule concluded.
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
            // A field whose pick is not the plain stored read is read
            // through its attribute concept whether or not a rule
            // derives it, so its candidates are gathered and elected.
            if !rules.is_empty() || field.descriptor().reads_elected() {
                derived.insert(entity.clone());
            }
            if !rules.is_empty() {
                let candidate = match (&sole, field.the().attribute()) {
                    (Some(None), _) | (_, None) => None,
                    _ if !builtins.is_empty() => None,
                    (current, Some(attribute)) => {
                        let mut found: Option<(DeductiveRule, Vec<dialog_artifacts::Relation>)> =
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
                .map(|source| source.as_ref().rule_cache());
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
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::Branch;
    use crate::helpers::{Counting, connect, test_repo};
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::concept::descriptor::{ConceptConclusion, ConceptDescriptor};
    use dialog_query::concept::query::ConceptQuery;
    use dialog_query::rule::DeductiveRuleDescriptor;
    use dialog_query::{DeductiveRule, Parameters, Term, the};

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
            + dialog_capability::Provider<crate::Hydrate>
            + dialog_capability::Provider<dialog_artifacts::Preload>
            + dialog_capability::Provider<dialog_artifacts::Speculation>
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
            + dialog_capability::Provider<crate::Hydrate>
            + dialog_capability::Provider<dialog_artifacts::Preload>
            + dialog_capability::Provider<dialog_artifacts::Speculation>
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
            + dialog_capability::Provider<crate::Hydrate>
            + dialog_capability::Provider<dialog_artifacts::Preload>
            + dialog_capability::Provider<dialog_artifacts::Speculation>
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

    /// A committed rule derives a relation a query elects over: the
    /// rule stores, discovers and hydrates through the `db.rule/*`
    /// rail, and a read of its relation under `max` chooses among the
    /// candidates it derives over committed facts, found by the
    /// relation whatever pick the read declares over it.
    #[dialog_common::test]
    async fn it_elects_over_a_relation_a_committed_rule_derives() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // dept-salary(dept, salary) :- dept(employee, dept), salary(employee, salary)
        let rule = {
            let json = serde_json::json!({
                "deduce": { "with": {
                    "salary": { "the": "org/dept-salary", "as": "natural:", "pick": "all" }
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
        let dept_top: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "top": { "the": "org/dept-salary", "as": "natural:", "pick": "max" }
        }}))?;

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

    /// An election reads every rule's candidates: the committed rule is
    /// found first and the query's overlay rule after it, and a `max`
    /// read over their relation chooses among what both derive per
    /// entity, never taking either rule alone for the whole answer.
    #[dialog_common::test]
    async fn it_elects_over_a_committed_and_an_overlay_rule_together() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let salary = serde_json::json!({ "with": {
            "salary": { "the": "org/dept-salary", "as": "natural:", "pick": "all" }
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
        let dept_top: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "top": { "the": "org/dept-salary", "as": "natural:", "pick": "max" }
        }}))?;

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
    /// is derived by rules whose bodies read the concept itself for a
    /// required field. The field lists its cases best first, so every
    /// space with a subject is `case:remote` and the one whose subject
    /// has a replica is `case:replicated`, the better-ranked case
    /// winning: no rule negates anything.
    #[dialog_common::test]
    async fn it_derives_an_optional_field_from_a_rule_reading_the_concept() -> anyhow::Result<()> {
        use dialog_query::rule::deductive::descriptor::DeductiveRuleDescriptor;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let cases = serde_json::json!(["case:replicated", "case:remote"]);
        let space = serde_json::json!({ "with": {
            "subject": { "the": "load.space/subject", "as": "entity:" },
            "presence": { "the": "load.space/presence", "as": cases, "optional": true }
        } });
        let replica = serde_json::json!({ "with": {
            "subject": { "the": "load.replica/subject", "as": "entity:" },
            "profile": { "the": "load.replica/profile", "as": "entity:" }
        } });
        let presence = serde_json::json!({ "with": {
            "presence": { "the": "load.space/presence", "as": "entity:" }
        } });
        let remote: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": presence,
            "when": [
                { "assert": space, "where": {
                    "this": { "?": { "name": "this" } },
                    "subject": { "?": { "name": "subject" } } } },
                { "assert": "==", "where": {
                    "this": { "?": { "name": "presence" } },
                    "is": "case:remote" } }
            ]
        }))?;
        let remote = remote.compile()?;
        let replicated: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": presence,
            "when": [
                { "assert": space, "where": {
                    "this": { "?": { "name": "this" } },
                    "subject": { "?": { "name": "subject" } } } },
                { "assert": replica, "where": {
                    "subject": { "?": { "name": "subject" } } } },
                { "assert": "==", "where": {
                    "this": { "?": { "name": "presence" } },
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

        // The relation read under the ranked cases.
        let ranked: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "presence": { "the": "load.space/presence", "as": cases }
        } }))?;
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("presence".into(), Term::var("presence"));
        let mut presences: Vec<(Entity, String)> = branch
            .select(ConceptQuery {
                predicate: ranked,
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?
            .into_iter()
            .map(|row| {
                Ok((
                    row.entity().clone(),
                    row.get::<Entity>("presence")?.to_string(),
                ))
            })
            .collect::<anyhow::Result<_>>()?;
        presences.sort();
        assert_eq!(
            presences,
            vec![
                (away.clone(), "case:remote".to_string()),
                (here.clone(), "case:replicated".to_string()),
            ]
        );

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

    use crate::helpers::test_repo;
    use crate::{Branch, Repository};
    use base58::ToBase58;
    use dialog_artifacts::{Entity, Value};
    use dialog_peer::Peer;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::query::Output as _;
    use dialog_query::{
        ResolverConclusion, ResolverQuery, Term, TreeEntryQuery, TreeKeyQuery, TreeNodeQuery,
        TreeSpanQuery, TreeValueQuery, the,
    };
    use dialog_storage::provider::storage::VolatileSpace;

    /// Commit a handful of facts and return the branch + the committed
    /// root's node reference (base58, exactly as `dialog.branch/tree`
    /// carries it).
    async fn committed_branch(
        repo: &Repository<impl dialog_capability::Principal>,
        operator: &Peer<VolatileSpace, dialog_peer::Session>,
    ) -> anyhow::Result<(Branch, String)> {
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
        let revision = branch
            .revision()
            .expect("branch has a revision after commit");
        let tree_bytes: &[u8] = revision.tree.hash();
        Ok((branch, ToBase58::to_base58(tree_bytes)))
    }

    /// `tree/node` with every output free, over a constant reference.
    fn tree_node(reference: &str) -> ResolverQuery {
        ResolverQuery::TreeNode(TreeNodeQuery {
            of: Term::from(Value::String(reference.into())).into(),
            kind: Term::var("kind"),
            size: Term::var("size"),
            count: Term::var("count"),
            scale: Term::var("scale"),
            novelty: Term::var("novelty"),
        })
    }

    fn tree_span(reference: &str) -> ResolverQuery {
        ResolverQuery::TreeSpan(TreeSpanQuery {
            of: Term::from(Value::String(reference.into())).into(),
            at: Term::var("at"),
            node: Term::var("node"),
            separator: Term::var("separator"),
            until: Term::var("until"),
            scale: Term::var("scale"),
            rank: Term::var("rank"),
            novelty: Term::var("novelty"),
        })
    }

    fn tree_entry(reference: &str) -> ResolverQuery {
        ResolverQuery::TreeEntry(TreeEntryQuery {
            of: Term::from(Value::String(reference.into())).into(),
            at: Term::var("at"),
            key: Term::var("key"),
            state: Term::var("state"),
            retraction: Term::var("retraction"),
            origin: Term::var("origin"),
            edition: Term::var("edition"),
            cause: Term::var("cause"),
            collapsed: Term::var("collapsed"),
            supersedes: Term::var("supersedes"),
            spill: Term::var("spill"),
        })
    }

    fn tree_value(reference: &str) -> ResolverQuery {
        ResolverQuery::TreeValue(TreeValueQuery {
            of: Term::from(Value::String(reference.into())).into(),
            size: Term::var("size"),
            bytes: Term::var("bytes"),
        })
    }

    fn tree_key(reference: &str) -> ResolverQuery {
        ResolverQuery::TreeKey(TreeKeyQuery {
            of: Term::from(Value::String(reference.into())).into(),
            at: Term::var("at"),
            key: Term::var("key"),
            rank: Term::var("rank"),
        })
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

    /// Commit `count` facts with padded values and return the branch +
    /// root reference: enough leaf weight forces an index root, so the
    /// span/descent surface runs against a real multi-level tree.
    async fn committed_wide_branch(
        repo: &Repository<impl dialog_capability::Principal>,
        operator: &Peer<VolatileSpace, dialog_peer::Session>,
        count: usize,
    ) -> anyhow::Result<(Branch, String)> {
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
        let revision = branch
            .revision()
            .expect("branch has a revision after commit");
        let tree_bytes: &[u8] = revision.tree.hash();
        Ok((branch, ToBase58::to_base58(tree_bytes)))
    }

    /// A blank `of` (a query that never names the node reference) is
    /// scheduled but matches nothing: zero rows, no error. The planner
    /// only refuses an unbound *variable* input.
    #[dialog_common::test]
    async fn it_yields_nothing_for_a_blank_reference() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, _root) = committed_branch(&repo, &operator).await?;

        let rows: Vec<ResolverConclusion> = branch
            .query()
            .select(ResolverQuery::TreeNode(TreeNodeQuery {
                of: Term::<Value>::blank().into(),
                kind: Term::var("kind"),
                size: Term::var("size"),
                count: Term::var("count"),
                scale: Term::var("scale"),
                novelty: Term::var("novelty"),
            }))
            .perform(&operator)
            .try_vec()
            .await?;
        assert!(
            rows.is_empty(),
            "a blank reference matches nothing: {rows:?}"
        );
        Ok(())
    }

    /// A tree wide enough for an index root runs the whole span
    /// surface end to end: one contiguous span row per child, and the
    /// descent chain (span node -> tree/node -> tree/key) reaches real
    /// segment entries.
    #[dialog_common::test]
    async fn it_descends_a_multi_level_tree() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, root) = committed_wide_branch(&repo, &operator, 800).await?;

        let node: Vec<ResolverConclusion> = branch
            .query()
            .select(tree_node(&root))
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(
            text(&node[0], "kind"),
            "index",
            "800 padded entries must outgrow one segment: {node:?}"
        );
        let children = unsigned(&node[0], "count") as usize;

        let spans: Vec<ResolverConclusion> = branch
            .query()
            .select(tree_span(&root))
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(spans.len(), children, "one span row per child");
        for pair in spans.windows(2) {
            assert_eq!(
                pair[0].get("until"),
                pair[1].get("separator"),
                "spans tile the key space: {spans:?}"
            );
        }

        // Descend to the leaves whatever the depth: the fixture's
        // entities are random, so the tree's shape is probabilistic and
        // a child may itself be an index (the depth-3 draw that made
        // the one-level version of this walk flaky).
        let mut entries = 0usize;
        let mut frontier: Vec<String> = spans.iter().map(|span| text(span, "node")).collect();
        while let Some(child) = frontier.pop() {
            let child_node: Vec<ResolverConclusion> = branch
                .query()
                .select(tree_node(&child))
                .perform(&operator)
                .try_vec()
                .await?;
            assert_eq!(child_node.len(), 1, "child answers tree/node");
            if text(&child_node[0], "kind") == "segment" {
                let keys: Vec<ResolverConclusion> = branch
                    .query()
                    .select(tree_key(&child))
                    .perform(&operator)
                    .try_vec()
                    .await?;
                assert_eq!(
                    keys.len(),
                    unsigned(&child_node[0], "count") as usize,
                    "one key row per segment entry"
                );
                entries += keys.len();
            } else {
                let child_spans: Vec<ResolverConclusion> = branch
                    .query()
                    .select(tree_span(&child))
                    .perform(&operator)
                    .try_vec()
                    .await?;
                frontier.extend(child_spans.iter().map(|span| text(span, "node")));
            }
        }
        assert!(
            entries >= 800,
            "the leaves carry at least the committed facts, got {entries}"
        );
        Ok(())
    }

    /// The flagship join shape: a premise binds the node reference as
    /// a VARIABLE and the resolver consumes it — through a deductive
    /// rule, so this also covers resolver premises in rule bodies and
    /// the planner ordering the resolver after its binding premise.
    #[dialog_common::test]
    async fn it_joins_resolvers_on_a_bound_reference() -> anyhow::Result<()> {
        use dialog_query::concept::query::ConceptQuery;
        use dialog_query::rule::DeductiveRuleDescriptor;
        use dialog_query::{ConceptConclusion, Parameters};

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, root) = committed_branch(&repo, &operator).await?;

        // A committed fact carries the root reference; the rule joins
        // it into `tree/node` through the `?root` variable.
        let probe = Entity::new()?;
        branch
            .transaction()
            .assert(the!("probe/tree").of(probe.clone()).is(root.clone()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": {
                "root": { "the": "probe/described-root", "as": "text:" },
                "kind": { "the": "probe/described-kind", "as": "text:" }
            }},
            "when": [
                {
                    "assert": { "with": {
                        "root": { "the": "probe/tree", "as": "text:" }
                    }},
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "root": { "?": { "name": "root" } }
                    }
                },
                {
                    "assert": "tree/node",
                    "where": {
                        "of": { "?": { "name": "root" } },
                        "kind": { "?": { "name": "kind" } }
                    }
                }
            ]
        }))?;
        let rule = descriptor.compile().expect("the joining rule compiles");
        let conclusion = rule.conclusion().clone();
        branch
            .transaction()
            .assert(&rule)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("root".into(), Term::var("root"));
        terms.insert("kind".into(), Term::var("kind"));
        let rows: Vec<ConceptConclusion> = branch
            .query()
            .select(ConceptQuery {
                predicate: conclusion,
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(rows.len(), 1, "the join yields one row: {rows:?}");
        Ok(())
    }

    /// The committed root answers `tree/node` through the ordinary
    /// query path: one row, a real kind, a positive size, and a count.
    #[dialog_common::test]
    async fn it_reads_the_root_node_through_resolvers() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, root) = committed_branch(&repo, &operator).await?;

        let rows: Vec<ResolverConclusion> = branch
            .query()
            .select(tree_node(&root))
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(rows.len(), 1, "one row per node: {rows:?}");
        let row = &rows[0];
        let kind = text(row, "kind");
        assert!(kind == "index" || kind == "segment", "kind set: {row:?}");
        assert!(unsigned(row, "size") > 0, "node has a byte size");
        assert!(unsigned(row, "count") > 0, "node has slots");
        Ok(())
    }

    /// Structure is consistent between resolvers: a segment's
    /// `tree/key` rows match its count and `tree/span` yields nothing;
    /// an index's `tree/span` rows match its count, chain into
    /// contiguous ranges, and each child answers `tree/node` in turn
    /// (the descent chain).
    #[dialog_common::test]
    async fn it_descends_consistently() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, root) = committed_branch(&repo, &operator).await?;

        let node: Vec<ResolverConclusion> = branch
            .query()
            .select(tree_node(&root))
            .perform(&operator)
            .try_vec()
            .await?;
        let count = unsigned(&node[0], "count") as usize;

        let spans: Vec<ResolverConclusion> = branch
            .query()
            .select(tree_span(&root))
            .perform(&operator)
            .try_vec()
            .await?;
        let keys: Vec<ResolverConclusion> = branch
            .query()
            .select(tree_key(&root))
            .perform(&operator)
            .try_vec()
            .await?;

        match text(&node[0], "kind").as_str() {
            "segment" => {
                assert_eq!(keys.len(), count, "one key row per entry");
                assert!(spans.is_empty(), "a segment has no spans");
                assert!(
                    keys.iter().all(|row| matches!(row.get("key"), Some(Value::Bytes(bytes)) if !bytes.is_empty())),
                    "keys carry bytes: {keys:?}"
                );
            }
            "index" => {
                assert_eq!(spans.len(), count, "one span row per child");
                assert!(keys.is_empty(), "an index has no entries");
                // Spans tile the key space: each span ends where the
                // next begins, and the outer bounds are open.
                for pair in spans.windows(2) {
                    assert_eq!(
                        pair[0].get("until"),
                        pair[1].get("separator"),
                        "spans are contiguous: {spans:?}"
                    );
                }
                let child = text(&spans[0], "node");
                let child_node: Vec<ResolverConclusion> = branch
                    .query()
                    .select(tree_node(&child))
                    .perform(&operator)
                    .try_vec()
                    .await?;
                assert_eq!(child_node.len(), 1, "the child answers tree/node");
            }
            other => panic!("unexpected kind {other}"),
        }
        Ok(())
    }

    /// Absent (well-formed but unknown) and malformed references
    /// contribute nothing — zero rows, no error.
    #[dialog_common::test]
    async fn it_yields_nothing_for_absent_or_malformed_references() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, _root) = committed_branch(&repo, &operator).await?;

        let absent = ToBase58::to_base58(&[7u8; 32][..]);
        for reference in [absent.as_str(), "not-base58-!!!", ""] {
            let rows: Vec<ResolverConclusion> = branch
                .query()
                .select(tree_node(reference))
                .perform(&operator)
                .try_vec()
                .await?;
            assert!(rows.is_empty(), "no rows for {reference:?}: {rows:?}");
        }
        Ok(())
    }

    /// Content addressing keeps history navigable: after a second
    /// commit the old root still answers, and the new root differs.
    #[dialog_common::test]
    async fn it_keeps_old_roots_queryable() -> anyhow::Result<()> {
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
        let revision = branch.revision().expect("second revision");
        let tree_bytes: &[u8] = revision.tree.hash();
        let second = ToBase58::to_base58(tree_bytes);
        assert_ne!(first, second, "the root moved");

        for reference in [&first, &second] {
            let rows: Vec<ResolverConclusion> = branch
                .query()
                .select(tree_node(reference))
                .perform(&operator)
                .try_vec()
                .await?;
            assert_eq!(rows.len(), 1, "root {reference} answers");
        }
        Ok(())
    }

    /// Transaction queries construct the same `QueryEnv`, so the tree
    /// resolvers are available in the as-if-committed view too.
    #[dialog_common::test]
    async fn it_serves_resolvers_in_transaction_queries() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, root) = committed_branch(&repo, &operator).await?;

        let tx = branch.transaction();
        let rows: Vec<ResolverConclusion> = tx
            .query()
            .select(tree_node(&root))
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(rows.len(), 1, "tx query serves tree/node: {rows:?}");
        Ok(())
    }

    /// A value past the inline threshold spills to a content-addressed
    /// block; `tree/entry` names its reference and `tree/value` reads
    /// it back — and versioned commits stamp claim metadata (origin,
    /// edition) that `tree/entry` surfaces, which is what makes the
    /// history region legible.
    #[dialog_common::test]
    async fn it_reads_spilled_values_and_claim_metadata() -> anyhow::Result<()> {
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
        let revision = branch.revision().expect("committed");
        let tree_bytes: &[u8] = revision.tree.hash();
        let root = ToBase58::to_base58(tree_bytes);

        // Walk every segment reachable from the root, collecting entry
        // rows — shape-agnostic (the root may be a segment or index).
        let mut queue = vec![root];
        let mut entries: Vec<ResolverConclusion> = Vec::new();
        while let Some(node) = queue.pop() {
            entries.extend(
                branch
                    .query()
                    .select(tree_entry(&node))
                    .perform(&operator)
                    .try_vec()
                    .await?,
            );
            for span in branch
                .query()
                .select(tree_span(&node))
                .perform(&operator)
                .try_vec()
                .await?
            {
                queue.push(text(&span, "node"));
            }
        }
        assert!(!entries.is_empty(), "the committed tree has entries");

        assert!(
            entries.iter().any(
                |row| matches!(row.get("origin"), Some(Value::Bytes(bytes)) if !bytes.is_empty())
            ),
            "versioned commits stamp claim versions: {entries:?}"
        );

        let spill = entries
            .iter()
            .find_map(|row| match row.get("spill") {
                Some(Value::String(reference)) if !reference.is_empty() => Some(reference.clone()),
                _ => None,
            })
            .expect("the 10 KiB value spilled");

        let values: Vec<ResolverConclusion> = branch
            .query()
            .select(tree_value(&spill))
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(values.len(), 1, "one row per value block");
        let size = unsigned(&values[0], "size");
        assert!(
            size >= big.len() as u128,
            "the block holds the whole value ({size} bytes)"
        );
        match values[0].get("bytes") {
            Some(Value::Bytes(bytes)) => assert_eq!(bytes.len() as u128, size),
            other => panic!("expected value bytes, got {other:?}"),
        }
        Ok(())
    }
}

#[cfg(test)]
mod ordered_relation_tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::test_repo;
    use dialog_artifacts::position::{Bias, Position, insert};
    use dialog_artifacts::{
        Artifact, ArtifactSelector, ArtifactViewStream as _, Directory, Entity,
        Relation as ArtifactsRelation, Sequence, Symbol, Value,
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
        let attribute =
            ArtifactsRelation::compose(&domain, position.clone()).expect("attribute fits");
        AttributeStatement {
            the: Relation::from(attribute),
            of: list.clone(),
            is: Value::Entity(member.clone()),
            cause: None,
            cardinality: None,
            pick: None,
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
        let title = ArtifactsRelation::compose(&domain, Symbol::from_str("title")?)?;
        branch
            .transaction()
            .assert(AttributeStatement {
                the: Relation::from(title),
                of: list.clone(),
                is: Value::String("Groceries".into()),
                cause: None,
                cardinality: None,
                pick: None,
            })
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // One contiguous range scan of the list's domain.
        let members: Vec<Artifact> = branch
            .claims()
            .select(
                ArtifactSelector::new()
                    .of(list.clone())
                    .with_domain(&domain),
            )
            .perform(&operator)
            .await?
            .owned()
            .try_collect()
            .await?;

        let attributes: Vec<String> = members
            .iter()
            .map(|artifact| artifact.the.to_string())
            .collect();
        let mut sorted = attributes.clone();
        sorted.sort();
        assert_eq!(attributes, sorted, "the scan streams in position order");

        // Classify the scan by name shape: named entries into a
        // directory, position-named members into a sequence.
        let mut fields: Directory<Value> = Directory::new();
        let mut sequence: Sequence<Value> = Sequence::new();
        for artifact in &members {
            let named = fields.admit(&artifact.the, artifact.is.clone());
            let ordered = sequence.admit(&artifact.the, artifact.is.clone());
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
