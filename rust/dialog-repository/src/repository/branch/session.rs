use std::collections::HashSet;

use dialog_artifacts::inspect::Load;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStream, ArtifactViewStream as _, Changes,
    DialogArtifactsError, Entity, Select, SortKey, Statement,
};
use dialog_capability::{Capability, Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::{Identify, Operator, OperatorExt as _};
use dialog_effects::memory::Resolve;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::fixpoint::Continuation;
use dialog_query::concept::query::{ConceptRules, PlanCache};
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output};
use dialog_query::session::ProgramAnalysis;
use dialog_query::source::SelectRules;
use dialog_query::{DeductiveRule, Negation, Premise, Proposition};
use dialog_search_tree::Buffer;
use dialog_storage::{Blake3Hash, StorageBackend};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use std::sync::Arc;

use crate::layer::{filter_tombstones, merge_grouped, tombstones_from};
use crate::rules::{
    assemble, builtin, conclusion_selector, hydrate, overlay_rules, rule_entities, source_bytes,
    source_selector,
};
use crate::schema::{DidExt as _, Session, SessionBranch, session};
use crate::{
    Branch, NetworkedIndex, RemoteFallback, RemoteSite, RepositoryArchiveExt as _,
    RepositoryMemoryExt, Upstream,
};

/// A composable query over one or more branches plus an in-memory
/// overlay.
///
/// `branch.query()` returns a `QueryLayer` rooted at that branch.
/// From there:
///
/// - [`with`](Self::with) folds any [`Statement`] (a concept
///   instance, an attribute expression, a [`Changes`] batch) into the
///   overlay — its asserts/replaces surface alongside branch facts,
///   its retracts tombstone matching branch facts.
/// - [`join`](Self::join) merges in another branch or `QueryLayer`.
/// - [`select`](Self::select) stages a query; `.perform(&env)` runs it.
///
/// All branches in the layer are peers — there is no distinguished
/// "primary". A query reads the union of every branch's facts plus
/// the overlay.
///
/// # Auto-injected schema metadata
///
/// At `.perform(env)` the layer resolves the operator's identity via
/// [`Identify`] and folds in [`metadata`](Self::metadata): one
/// [`Origin`](crate::schema::Replica) + [`Branch`](crate::schema::Branch)
/// (+ [`BranchRevision`](crate::schema::BranchRevision) when committed)
/// per branch, plus a single [`Session`]. Callers don't pass the
/// profile or operator DID, and nothing is written to any branch's
/// tree.
///
/// ```ignore
/// branch.query()
///     .join(&other_branch)            // another branch source
///     .with(custom_concept_instance)  // user-asserted overlay facts
///     .select(query)
///     .perform(&env);                 // metadata auto-injected
/// ```
#[derive(Default, Clone)]
pub struct QueryLayer<'a> {
    branches: Vec<&'a Branch>,
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

    /// Merge another layer in: union the branches, fold the other
    /// layer's changes via its `Statement` impl. Accepts anything
    /// convertible into a `QueryLayer` — a `&Branch` or a `Changes`.
    pub fn join(mut self, other: impl Into<QueryLayer<'a>>) -> Self {
        let other = other.into();
        self.branches.extend(other.branches);
        other.changes.assert(&mut self.changes);
        self
    }

    /// The branches this layer reads from.
    pub fn branches(&self) -> &[&'a Branch] {
        &self.branches
    }

    /// The caller-supplied overlay changes (no auto-injected metadata).
    pub fn changes(&self) -> &Changes {
        &self.changes
    }

    /// The schema-metadata [`Changes`] for this layer: every branch's
    /// [`BranchMetadata`](super::metadata::BranchMetadata) plus a
    /// single [`Session`] (with one cardinality-many
    /// `dialog.session/branch` per branch in scope).
    ///
    /// `operator` (from [`Identify`]) supplies the profile + operator
    /// DIDs the schema entities are derived from.
    pub fn metadata(&self, operator: &Capability<Operator>) -> Changes {
        let mut changes = Changes::new();

        let mut branch_entities = Vec::with_capacity(self.branches.len());
        for branch in &self.branches {
            let metadata = branch.metadata(operator);
            branch_entities.push(metadata.branch.this.clone());
            metadata.assert(&mut changes);
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
    pub fn overlay(&self, operator: &Capability<Operator>) -> Changes {
        let mut overlay = self.changes.clone();
        self.metadata(operator).assert(&mut overlay);
        overlay
    }

    /// Stage a query application. Call `.perform(&operator)` to execute.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        SelectQuery {
            layer: self.clone(),
            query,
        }
    }
}

// Folds the branch's transient session overlay
// ([`Branch::overlay`]): every read path — `branch.select`,
// `branch.query`, transaction queries, subscription evaluations —
// constructs through here, so session facts participate in all of
// them with no per-path wiring.
impl<'a> From<&'a Branch> for QueryLayer<'a> {
    fn from(branch: &'a Branch) -> Self {
        Self {
            branches: vec![branch],
            changes: branch.overlay().changes(),
        }
    }
}

impl From<Changes> for QueryLayer<'_> {
    fn from(changes: Changes) -> Self {
        Self {
            branches: Vec::new(),
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
    pub(crate) fn new(branch: &'a Branch, query: Q) -> Self {
        Self {
            layer: QueryLayer::from(branch),
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
    /// tombstones, and unions every branch stream (tombstone-filtered)
    /// with the overlay.
    pub fn perform<Env>(self, env: &'a Env) -> impl Output<Q::Conclusion> + 'a
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
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
            let tombstones = Arc::new(tombstones_from(&overlay));

            let branches = layer.branches.iter().map(|&branch| branch.clone()).collect();
            let query_env = QueryEnv::new(branches, overlay, tombstones, env);
            let results = Box::pin(query.perform(&query_env));
            for await result in results {
                yield result?;
            }
        }
    }
}

/// The runtime environment that bridges the layer's branches and
/// per-query overlay changes into the query engine's Provider bounds.
///
/// Built fresh on each `.perform(env)`; the environment reference
/// is never captured on the layer itself.
pub(crate) struct QueryEnv<'a, Env> {
    /// Owned (cheaply cloned: shared caches) so the env's only
    /// lifetime is the underlying `env` reference. A poll/evaluation
    /// can then type its `QueryEnv` with the *named* env lifetime
    /// instead of a generator-local borrow — which is what keeps the
    /// enclosing future `Send`-general on native (two independent
    /// erased lifetimes in `QueryEnv<'0>: Provider<Select<'1>>` hit
    /// rustc's #100013 limitation; a named lifetime does not).
    branches: Vec<Branch>,
    /// All overlay facts — caller-asserted + auto-injected metadata —
    /// merged into one batch. Queried via `Provider<Select> for Changes`.
    changes: Changes,
    /// `sort_key`s of every retracted fact in `changes`. Each branch
    /// stream is filtered against these before the merge so retracts
    /// in the overlay suppress matching facts in the source.
    tombstones: Arc<HashSet<SortKey>>,
    /// When present, every selector this environment executes —
    /// fact scans and rule-discovery reads alike — records its
    /// demanded range here. Subscriptions use the recorded cover to
    /// gate re-evaluation.
    demand: Option<crate::Demand>,
    /// A polling subscription's retained fixpoint for one concept:
    /// attached to that concept's resolved rules so a recursive
    /// evaluation continues (or rebuilds into) the retained answer
    /// table instead of computing a throwaway one.
    fixpoint: Option<(Entity, Continuation)>,
    env: &'a Env,
}

impl<'a, Env> QueryEnv<'a, Env> {
    /// Build a runtime env from already-resolved parts: the branches to
    /// read, the per-query overlay (caller changes + injected metadata),
    /// the tombstones lifted from it, and the underlying capability env.
    ///
    /// Both `Branch::query` and the transaction-query path construct
    /// through here so there is exactly one query env — a transaction
    /// query is just a single-branch `QueryEnv`. Deductive-rule
    /// resolution is built in (a durable layer per branch + the overlay
    /// as a transient layer), so the two paths can never diverge on it.
    pub(crate) fn new(
        branches: Vec<Branch>,
        changes: Changes,
        tombstones: Arc<HashSet<SortKey>>,
        env: &'a Env,
    ) -> Self {
        Self {
            branches,
            changes,
            tombstones,
            demand: None,
            fixpoint: None,
            env,
        }
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

    /// Record a selector's demanded range, when recording is on.
    fn record_demand(&self, selector: &ArtifactSelector<Constrained>) {
        if let Some(demand) = &self.demand {
            demand.record(selector);
        }
    }
}

impl<Env> Clone for QueryEnv<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            branches: self.branches.clone(),
            changes: self.changes.clone(),
            tombstones: self.tombstones.clone(),
            demand: self.demand.clone(),
            fixpoint: self.fixpoint.clone(),
            env: self.env,
        }
    }
}

/// Execute a select against a single branch, transparently routing through
/// the branch's remote upstream when configured. Extracted as a freestanding
/// helper so every branch in a [`QueryEnv`] shares the exact same branch-read
/// path (a transaction query is itself a single-branch `QueryEnv`).
///
/// Takes the branch by value (a cheap clone: shared caches) and moves
/// it into the returned stream, so the stream borrows only the env —
/// errors surface as the stream's first item.
pub(crate) fn select_from_branch<'a, Env>(
    branch: Branch,
    env: &'a Env,
    input: ArtifactSelector<Constrained>,
) -> ArtifactStream<'a>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    Box::pin(async_stream::try_stream! {
        let select = branch.claims().select(input);

        let remote = match branch.upstream() {
            Some(Upstream::Remote { remote: name, .. }) => {
                let loaded = branch
                    .subject()
                    .remote(name.clone())
                    .load()
                    .perform(env)
                    .await;
                RemoteFallback::from_load(name, loaded)
            }
            _ => RemoteFallback::None,
        };

        let store = NetworkedIndex::new(env, select.catalog(), remote);
        let stream = select.execute(store).await?;
        for await artifact in stream {
            yield artifact?;
        }
    })
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
impl<'a, Env> Provider<Select<'a>> for QueryEnv<'a, Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        self.record_demand(&input);
        let mut streams: Vec<ArtifactStream<'a>> = Vec::with_capacity(self.branches.len() + 1);

        // Branch streams — each filtered by tombstones from the
        // overlay's retracts so a `tx.retract(x)` (or any user-asserted
        // retract in `with(..)`) suppresses matching source facts. Each
        // owns its branch clone and borrows only `self.env`.
        for branch in &self.branches {
            let raw = select_from_branch(branch.clone(), self.env, input.clone());
            streams.push(filter_tombstones(raw, self.tombstones.clone()));
        }

        // Overlay stream — Changes itself is a Provider<Select>. The
        // overlay always carries facts (session metadata at minimum), but
        // MATCHES the typical fact selector rarely: a join's inner premise
        // probes one entity per outer binding, and pushing an empty overlay
        // stream anyway forced the k-way merge (and its per-row sort keys)
        // on every one of those probes. Peek the overlay's materialized
        // result and push it only when it has rows, so the single-source
        // common case flows through `merge_grouped`'s passthrough arm.
        let mut overlay = Provider::<Select<'a>>::execute(&self.changes, input).await?;
        match futures_util::StreamExt::next(&mut overlay).await {
            None => {}
            Some(first) => {
                streams.push(Box::pin(stream::iter(vec![first]).chain(overlay)));
            }
        }

        Ok(merge_grouped(streams))
    }
}

// The idempotent block-load behind resolver premises (`tree/node` &
// co). No demand is recorded: the block behind a hash is
// content-addressed and can never change, so no tree diff could ever
// invalidate a row derived from it — the soundness argument lives in
// `dialog_artifacts::inspect`. Reads go through each branch's archive
// catalog capability with the same remote fallback a fact scan uses;
// the first branch that has the block wins (content addressing makes
// them interchangeable), and a block absent everywhere contributes
// nothing. Reads go through the branch's shared node cache (the same
// one the eager root probe in `select.rs` and `Subscription::touched`
// use): a resolver join re-resolves the same reference once per outer
// row, and a raw backend get would re-fetch that identical immutable
// block every time.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<'a, Env> Provider<Load> for QueryEnv<'a, Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    async fn execute(&self, input: Blake3Hash) -> Result<Option<Vec<u8>>, DialogArtifactsError> {
        for branch in &self.branches {
            let remote = match branch.upstream() {
                Some(Upstream::Remote { remote: name, .. }) => {
                    let loaded = branch
                        .subject()
                        .remote(name.clone())
                        .load()
                        .perform(self.env)
                        .await;
                    RemoteFallback::from_load(name, loaded)
                }
                _ => RemoteFallback::None,
            };
            let store = NetworkedIndex::new(self.env, branch.archive().index(), remote);
            let cached = branch
                .node_cache()
                .get_or_fetch(&NodeHash::from(input), async |hash| {
                    StorageBackend::get(&store, hash.as_bytes())
                        .await
                        .map(|bytes| bytes.map(Buffer::from))
                })
                .await?;
            if let Some(buffer) = cached {
                return Ok(Some(buffer.into_vec()));
            }
        }
        Ok(None)
    }
}

impl<'a, Env> QueryEnv<'a, Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    /// Read a `dialog.rule/*` selector against a single branch's committed
    /// tree only (NOT the overlay) and collect the matching artifacts.
    /// The durable layer's reads must be tree-only so the head-keyed
    /// discovery cache stays correct — overlay rules are handled
    /// separately, fresh, by the transient layer.
    async fn select_tree(
        &self,
        branch: &Branch,
        selector: ArtifactSelector<Constrained>,
    ) -> Result<Vec<Artifact>, DialogArtifactsError> {
        // Rule-discovery reads are demand too: a rule committed
        // later for a subscribed concept lands in this range and
        // must re-trigger the subscription. Recorded as *rule*
        // demand: a hit here invalidates the whole result, not one
        // entity's slice.
        if let Some(demand) = &self.demand {
            demand.record_rules(&selector);
        }
        // Rule bodies are hydrated from the full artifact, so this read
        // genuinely needs owned rows; it is head-cached, not per-query hot.
        select_from_branch(branch.clone(), self.env, selector)
            .owned()
            .try_collect()
            .await
    }

    /// The durable rules concluding `concept` on `branch`: the committed
    /// `dialog.rule/*` rules, read from the tree and cached by branch head
    /// (re-scanned only when the head moves), with hydrated bodies
    /// cached by content-addressed rule entity.
    async fn durable_rules(
        &self,
        branch: &Branch,
        concept: &Entity,
    ) -> Result<Vec<DeductiveRule>, EvaluationError> {
        let cache = branch.rule_cache();
        let head = branch.revision();

        // Discovery: which rule entities conclude this concept (committed).
        // Cached per (concept, head); a head move (commit/pull) re-scans.
        let rule_entities = match head.as_ref().and_then(|h| cache.discovered(concept, h)) {
            Some(entities) => entities,
            None => {
                let claims = self
                    .select_tree(branch, conclusion_selector(concept))
                    .await
                    .map_err(|e| {
                        EvaluationError::Store(format!("rule conclusion lookup: {e:?}"))
                    })?;
                let entities = rule_entities(claims);
                if let Some(head) = head.clone() {
                    cache.record_discovery(concept.clone(), head, entities.clone());
                }
                entities
            }
        };

        // Hydration: reuse cached bodies (content-addressed, never stale),
        // fetch + compile the rest from each rule's `dialog.rule/source`.
        let mut rules = Vec::with_capacity(rule_entities.len());
        for rule_entity in rule_entities {
            if let Some(body) = cache.body(&rule_entity) {
                rules.push(body);
                continue;
            }
            let source_claims = self
                .select_tree(branch, source_selector(&rule_entity))
                .await
                .map_err(|e| EvaluationError::Store(format!("rule source lookup: {e:?}")))?;
            let Some(source) = source_bytes(source_claims) else {
                continue;
            };
            let body = hydrate(&source)?;
            cache.record_body(rule_entity, body.clone());
            rules.push(body);
        }
        Ok(rules)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env> Provider<SelectRules> for QueryEnv<'_, Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    /// Resolve a concept's deductive rules by unioning across layers:
    /// each branch is a durable layer (committed `dialog.rule/*`, head-cached),
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
        let mut rules: Vec<DeductiveRule> = Vec::new();

        // Built-in rules first: the derived version-control concepts
        // (schema::Revision / schema::RevisionParent, plus the
        // recursive schema::RevisionAncestor closure over the parent
        // edges) are concluded from signed `dialog.db/revision`
        // records by fixed rules — nothing is stored under
        // `dialog.revision/*`.
        rules.extend(builtin(&concept));

        // Durable layers — one per branch.
        for branch in &self.branches {
            rules.extend(self.durable_rules(branch, &concept).await?);
        }
        // Transient layer — the per-query overlay, read fresh.
        rules.extend(overlay_rules(&self.changes, &concept));

        // Plan cache rides a branch (peers share content-addressed plans;
        // any branch's cache is correct). The overlay-only query has no
        // branch, so it falls back to a private cache.
        let plan_cache = self
            .branches
            .first()
            .map(|branch| branch.plan_cache())
            .unwrap_or_default();

        let bundle = assemble(&input, rules, plan_cache);
        let analysis = self.program_analysis(&input, &bundle).await?;
        analysis.check(&input)?;
        Ok(if analysis.is_recursive(&concept) {
            let bundle = bundle.with_recursion(analysis);
            match &self.fixpoint {
                Some((entity, continuation)) if *entity == concept => {
                    bundle.with_continuation(continuation.clone())
                }
                _ => bundle,
            }
        } else {
            bundle
        })
    }
}

impl<'a, Env> QueryEnv<'a, Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
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

        let mut entries: Vec<(Entity, ConceptRules)> = Vec::new();
        let mut seen = HashSet::new();
        let mut queue = Vec::new();

        seen.insert(root.this());
        referenced(root_bundle, &mut queue);
        entries.push((root.this(), root_bundle.clone()));

        while let Some(descriptor) = queue.pop() {
            let entity = descriptor.this();
            if !seen.insert(entity.clone()) {
                continue;
            }
            let mut rules: Vec<DeductiveRule> = builtin(&entity);
            for branch in &self.branches {
                rules.extend(self.durable_rules(branch, &entity).await?);
            }
            rules.extend(overlay_rules(&self.changes, &entity));
            let bundle = assemble(&descriptor, rules, PlanCache::default());
            referenced(&bundle, &mut queue);
            entries.push((entity, bundle));
        }

        Ok(Arc::new(ProgramAnalysis::analyze(
            entries.iter().map(|(entity, bundle)| (entity, bundle)),
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
    use crate::helpers::test_repo;
    use dialog_operator::helpers::test_operator_with_profile;
    use dialog_query::concept::descriptor::{ConceptConclusion, ConceptDescriptor};
    use dialog_query::concept::query::ConceptQuery;
    use dialog_query::rule::DeductiveRuleDescriptor;
    use dialog_query::{DeductiveRule, Parameters, Term, the};

    /// Conclusion concept `employee` (one `name` field). Derived — no
    /// `employee` fact is ever written; rows come only from rules.
    fn employee_descriptor() -> ConceptDescriptor {
        serde_json::from_value(serde_json::json!({
            "with": { "name": { "the": "org/employee-name", "as": "Text" } }
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
            "deduce": { "with": { "name": { "the": "org/employee-name", "as": "Text" } } },
            "when": [{
                "assert": { "with": { "name": { "the": attr, "as": "Text" } } },
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
        Env: dialog_capability::Provider<Get>
            + dialog_capability::Provider<Put>
            + dialog_capability::Provider<Resolve>
            + dialog_capability::Provider<Identify>
            + dialog_capability::Provider<Fork<RemoteSite, Get>>
            + dialog_capability::Provider<Fork<RemoteSite, Resolve>>
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

    // ----- (1) committed rule resolves via the durable (tree) layer ----

    #[dialog_common::test]
    async fn it_resolves_a_committed_rule() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
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
            .perform(&operator)
            .await?;
        // refresh handle so the durable layer sees the new head
        let branch = repo.branch("main").open().perform(&operator).await?;

        let employees = query_employees(&branch, &operator).await?;
        assert!(employees.contains(&alice), "committed rule must resolve");
        Ok(())
    }

    /// A *reducing* rule stores, discovers, and hydrates through the
    /// same `db.rule/*` rail: the committed rule's reduce block
    /// survives the durable layer round trip, and queries evaluate
    /// its fold over committed facts.
    #[dialog_common::test]
    async fn it_resolves_a_committed_reducing_rule() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // dept-total(this, total: sum(?salary)) grouped by department.
        let rule = {
            let json = serde_json::json!({
                "deduce": { "with": {
                    "total": { "the": "org/dept-total", "as": "UnsignedInteger" }
                }},
                "when": [{
                    "assert": { "with": {
                        "dept": { "the": "org/dept", "as": "Entity" },
                        "salary": { "the": "org/salary", "as": "UnsignedInteger" }
                    }},
                    "where": {
                        "this": { "?": { "name": "employee" } },
                        "dept": { "?": { "name": "this" } },
                        "salary": { "?": { "name": "salary" } }
                    }
                }],
                "reduce": {
                    "total": { "apply": "sum", "of": { "?": { "name": "salary" } } }
                }
            });
            let descriptor: DeductiveRuleDescriptor =
                serde_json::from_value(json).expect("descriptor parses");
            descriptor.compile().expect("reducing rule compiles")
        };
        let dept_total = rule.conclusion().clone();

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
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("dept"));
        terms.insert("total".into(), Term::var("total"));
        let rows: Vec<ConceptConclusion> = branch
            .query()
            .select(ConceptQuery {
                predicate: dept_total,
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(rows.len(), 1, "one folded row for the department");
        assert_eq!(*rows[0].entity(), dept);
        assert_eq!(
            rows[0].get::<u64>("total")?,
            7,
            "the hydrated reduce block folded the committed salaries"
        );
        Ok(())
    }

    // ----- (7) no rules => implicit-only, empty -----------------------

    #[dialog_common::test]
    async fn it_returns_empty_when_no_rules() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        branch
            .transaction()
            .assert(the!("org/person-name").of(alice).is("Alice".to_string()))
            .commit()
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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

    // ----- (4) discovery cache keys on head: a stale handle (head not
    // advanced) keeps using its cached discovery and does NOT pick up a
    // rule committed via another handle until it refreshes. This proves
    // the cache actually gates on head (not re-scanning every query).

    #[dialog_common::test]
    async fn it_keeps_discovery_cached_until_head_advances() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
            .perform(&operator)
            .await?;

        // Resolves while committed (also primes the cache at this head).
        assert!(query_employees(&branch, &operator).await?.contains(&alice));

        // Retract the rule's facts on the same handle → head advances.
        branch
            .transaction()
            .retract(&rule)
            .commit()
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
        let (operator, profile) = test_operator_with_profile().await;
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
}

#[cfg(test)]
mod resolver_tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::test_repo;
    use crate::{Branch, Repository};
    use base58::ToBase58;
    use dialog_artifacts::{Entity, Value};
    use dialog_operator::Operator;
    use dialog_operator::helpers::test_operator_with_profile;
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
        operator: &Operator<VolatileSpace>,
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
        tx.commit().perform(operator).await?;
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
        operator: &Operator<VolatileSpace>,
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
        tx.commit().perform(operator).await?;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, root) = committed_branch(&repo, &operator).await?;

        // A committed fact carries the root reference; the rule joins
        // it into `tree/node` through the `?root` variable.
        let probe = Entity::new()?;
        branch
            .transaction()
            .assert(the!("probe/tree").of(probe.clone()).is(root.clone()))
            .commit()
            .perform(&operator)
            .await?;

        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": {
                "root": { "the": "probe/described-root", "as": "Text" },
                "kind": { "the": "probe/described-kind", "as": "Text" }
            }},
            "when": [
                {
                    "assert": { "with": {
                        "root": { "the": "probe/tree", "as": "Text" }
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let (branch, first) = committed_branch(&repo, &operator).await?;

        branch
            .transaction()
            .assert(the!("test/name").of(Entity::new()?).is("later".to_string()))
            .commit()
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let big = "x".repeat(10 * 1024);
        branch
            .transaction()
            .assert(the!("test/big").of(Entity::new()?).is(big.clone()))
            .commit()
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
        Artifact, ArtifactSelector, ArtifactViewStream as _, Attribute, Directory, Entity,
        Sequence, Symbol, Value,
    };
    use dialog_operator::helpers::test_operator_with_profile;
    use dialog_query::AttributeStatement;
    use dialog_query::attribute::The;
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
        let attribute = Attribute::compose(&domain, position.clone()).expect("attribute fits");
        AttributeStatement {
            the: The::from(attribute),
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
        let (operator, profile) = test_operator_with_profile().await;
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
            .perform(&operator)
            .await?;

        // A dictionary entry lives in the same domain: the disjoint
        // name shapes (symbols start lowercase, positions uppercase)
        // let one scan serve both.
        let domain = Symbol::from_str("test.list")?;
        let title = Attribute::compose(&domain, Symbol::from_str("title")?)?;
        branch
            .transaction()
            .assert(AttributeStatement {
                the: The::from(title),
                of: list.clone(),
                is: Value::String("Groceries".into()),
                cause: None,
                cardinality: None,
            })
            .commit()
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
