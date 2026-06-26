use std::collections::HashSet;
use std::sync::Arc;

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStream, Changes, DialogArtifactsError, Select, SortKey,
    Statement,
};
use dialog_capability::{Capability, Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::{Identify, Operator, OperatorExt as _};
use dialog_effects::memory::Resolve;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output};
use dialog_query::source::SelectRules;
use futures_util::TryStreamExt as _;

use crate::layer::{filter_tombstones, merge_grouped, tombstones_from};
use crate::rules::{RuleClaims, RuleSource};
use crate::schema::{DidExt as _, Session, SessionBranch, session};
use crate::{Branch, NetworkedIndex, RemoteSite, RepositoryMemoryExt, Upstream};

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
/// [`Origin`](crate::schema::Origin) + [`Branch`](crate::schema::Branch)
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
    rule_source: Option<Arc<dyn RuleSource>>,
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
    pub fn with<S: Statement>(mut self, statement: S) -> Self {
        statement.assert(&mut self.changes);
        self
    }

    /// Install a [`RuleSource`](crate::RuleSource) so the session
    /// resolves deductive rules stored as facts, not just the implicit
    /// per-descriptor rule. Without this, queries see only implicit
    /// rules (unchanged default behavior). Chainable.
    pub fn with_rules(mut self, source: Arc<dyn RuleSource>) -> Self {
        self.rule_source = Some(source);
        self
    }

    /// Merge another layer in: union the branches, fold the other
    /// layer's changes via its `Statement` impl. Accepts anything
    /// convertible into a `QueryLayer` — a `&Branch` or a `Changes`.
    pub fn join(mut self, other: impl Into<QueryLayer<'a>>) -> Self {
        let other = other.into();
        self.branches.extend(other.branches);
        other.changes.assert(&mut self.changes);
        self.rule_source = self.rule_source.or(other.rule_source);
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

impl<'a> From<&'a Branch> for QueryLayer<'a> {
    fn from(branch: &'a Branch) -> Self {
        Self {
            branches: vec![branch],
            changes: Changes::new(),
            rule_source: None,
        }
    }
}

impl From<Changes> for QueryLayer<'_> {
    fn from(changes: Changes) -> Self {
        Self {
            branches: Vec::new(),
            changes,
            rule_source: None,
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
            let tombstones = tombstones_from(&overlay);

            let query_env =
                QueryEnv::new(layer.branches, overlay, tombstones, layer.rule_source, env);
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
    branches: Vec<&'a Branch>,
    /// All overlay facts — caller-asserted + auto-injected metadata —
    /// merged into one batch. Queried via `Provider<Select> for Changes`.
    changes: Changes,
    /// `sort_key`s of every retracted fact in `changes`. Each branch
    /// stream is filtered against these before the merge so retracts
    /// in the overlay suppress matching facts in the source.
    tombstones: HashSet<SortKey>,
    /// Optional consumer-supplied resolver for deductive rules stored
    /// as facts. `None` means only implicit per-descriptor rules.
    rule_source: Option<Arc<dyn RuleSource>>,
    env: &'a Env,
}

impl<'a, Env> QueryEnv<'a, Env> {
    /// Build a runtime env from already-resolved parts: the branches to
    /// read, the per-query overlay (caller changes + injected metadata),
    /// the tombstones lifted from it, an optional rule source, and the
    /// underlying capability env.
    ///
    /// Both `Branch::query` and the transaction-query path construct
    /// through here so there is exactly one query env — a transaction
    /// query is just a single-branch `QueryEnv`, which makes the two
    /// paths impossible to diverge (e.g. on rule resolution).
    pub(crate) fn new(
        branches: Vec<&'a Branch>,
        changes: Changes,
        tombstones: HashSet<SortKey>,
        rule_source: Option<Arc<dyn RuleSource>>,
        env: &'a Env,
    ) -> Self {
        Self {
            branches,
            changes,
            tombstones,
            rule_source,
            env,
        }
    }
}

impl<Env> Clone for QueryEnv<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            branches: self.branches.clone(),
            changes: self.changes.clone(),
            tombstones: self.tombstones.clone(),
            rule_source: self.rule_source.clone(),
            env: self.env,
        }
    }
}

/// Execute a select against a single branch, transparently routing through
/// the branch's remote upstream when configured. Extracted as a freestanding
/// helper so every branch in a [`QueryEnv`] shares the exact same branch-read
/// path (a transaction query is itself a single-branch `QueryEnv`).
pub(crate) async fn select_from_branch<'a, Env>(
    branch: &'a Branch,
    env: &'a Env,
    input: ArtifactSelector<Constrained>,
) -> Result<ArtifactStream<'a>, DialogArtifactsError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let select = branch.claims().select(input);

    let remote = match branch.upstream() {
        Some(Upstream::Remote { remote: name, .. }) => {
            branch.subject().remote(name).load().perform(env).await.ok()
        }
        _ => None,
    };

    let store = NetworkedIndex::new(env, select.catalog(), remote);
    let stream = select.execute(store).await?;
    Ok(Box::pin(stream))
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
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
        let mut streams: Vec<ArtifactStream<'a>> = Vec::with_capacity(self.branches.len() + 1);

        // Branch streams — each filtered by tombstones from the
        // overlay's retracts so a `tx.retract(x)` (or any user-asserted
        // retract in `with(..)`) suppresses matching source facts.
        for branch in &self.branches {
            let raw = select_from_branch(branch, self.env, input.clone()).await?;
            streams.push(filter_tombstones(raw, self.tombstones.clone()));
        }

        // Overlay stream — Changes itself is a Provider<Select>.
        streams.push(Provider::<Select<'a>>::execute(&self.changes, input).await?);

        Ok(merge_grouped(streams))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env> RuleClaims for QueryEnv<'_, Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    async fn select_claims(
        &self,
        selector: ArtifactSelector<Constrained>,
    ) -> Result<Vec<Artifact>, DialogArtifactsError> {
        // Read rule facts through the same branch + overlay union as
        // any other fact, so rules asserted via `.with(..)` are visible.
        let stream = Provider::<Select<'_>>::execute(self, selector).await?;
        stream.try_collect().await
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
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        // Every concept carries its implicit per-descriptor rule. When a
        // `RuleSource` is installed, let it extend that with deductive
        // rules stored as facts (read through `self` as `RuleClaims`).
        let rules = ConceptRules::new(&input);
        match &self.rule_source {
            Some(source) => source.resolve(&input, rules, self).await,
            None => Ok(rules),
        }
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

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::{test_operator_with_profile, test_repo};
    use dialog_artifacts::Entity;
    use dialog_query::query::Output;
    use dialog_query::{Concept, Query, Term};
    use std::sync::atomic::{AtomicBool, Ordering};

    mod people {
        /// `test/name` attribute used by the rule-source seam tests.
        #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
        #[domain("test")]
        pub struct Name(
            /// The person's name string.
            pub String,
        );
    }

    /// A simple concept used to exercise the rule-source seam.
    #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Person {
        /// The person entity.
        pub this: Entity,
        /// Their `test/name` attribute.
        pub name: people::Name,
    }

    /// Records whether `resolve` was called and returns `rules`
    /// unchanged — enough to prove the seam routes through it.
    struct SpyRuleSource {
        called: Arc<AtomicBool>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl RuleSource for SpyRuleSource {
        async fn resolve(
            &self,
            _concept: &ConceptDescriptor,
            rules: ConceptRules,
            _claims: &dyn RuleClaims,
        ) -> Result<ConceptRules, EvaluationError> {
            self.called.store(true, Ordering::SeqCst);
            Ok(rules)
        }
    }

    #[dialog_common::test]
    async fn it_invokes_the_installed_rule_source() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        branch
            .transaction()
            .assert(Person {
                this: alice.clone(),
                name: people::Name("Alice".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        let called = Arc::new(AtomicBool::new(false));
        let source = Arc::new(SpyRuleSource {
            called: called.clone(),
        });

        let results: Vec<Person> = branch
            .query()
            .with_rules(source)
            .select(Query::<Person> {
                this: alice.clone().into(),
                name: Term::var("name"),
            })
            .perform(&operator)
            .try_vec()
            .await?;

        assert!(
            called.load(Ordering::SeqCst),
            "installed RuleSource must be consulted during the query"
        );
        // Resolver returned rules unchanged, so the implicit rule still
        // resolves the concept normally.
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].this, alice);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_resolves_without_a_rule_source() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        branch
            .transaction()
            .assert(Person {
                this: alice.clone(),
                name: people::Name("Alice".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        // No `.with_rules(..)` — implicit-only path, unchanged behavior.
        let results: Vec<Person> = branch
            .query()
            .select(Query::<Person> {
                this: alice.clone().into(),
                name: Term::var("name"),
            })
            .perform(&operator)
            .try_vec()
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].this, alice);
        Ok(())
    }
}
