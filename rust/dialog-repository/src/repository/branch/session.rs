use std::collections::HashSet;

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    ArtifactSelector, ArtifactStream, Changes, DialogArtifactsError, Select, SortKey, Statement,
};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::{Identify, OperatorExt as _};
use dialog_effects::memory::Resolve;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output};
use dialog_query::source::SelectRules;

use crate::layer::{filter_tombstones, merge_grouped, tombstones_from};
use crate::{Branch, NetworkedIndex, RemoteSite, RepositoryMemoryExt, Upstream};

/// A composable overlay on top of a query session.
///
/// Carries the two things you can layer onto a session:
///
/// - extra `&Branch` references — their facts get unioned into the
///   merge alongside the primary branch's.
/// - a [`Changes`] batch — in-memory facts (asserts/replaces) get
///   surfaced via `Provider<Select> for Changes`; retracts are lifted
///   into tombstones that filter matching source facts from every
///   branch stream before the merge.
///
/// Construct from a `&Branch`, a `Changes`, or by `.with(stmt)`-ing
/// any [`Statement`] into a fresh layer. Combine layers with
/// [`join`](Self::join). The same shape lives on
/// [`QuerySession::with`] / [`QuerySession::join`] for direct session
/// composition.
#[derive(Default, Clone)]
pub struct QueryLayer<'a> {
    branches: Vec<&'a Branch>,
    changes: Changes,
}

impl<'a> QueryLayer<'a> {
    /// An empty layer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Assert a [`Statement`] into this layer's changes.
    ///
    /// `Changes` itself implements `Statement`, so `.with(changes)`
    /// folds an existing batch in. Any concept instance, attribute
    /// expression, or other `Statement` works too.
    pub fn with<S: Statement>(mut self, statement: S) -> Self {
        statement.assert(&mut self.changes);
        self
    }

    /// Merge another layer in: union the branches, fold the other
    /// layer's changes via its `Statement` impl.
    pub fn join(mut self, other: impl Into<QueryLayer<'a>>) -> Self {
        let other = other.into();
        self.branches.extend(other.branches);
        other.changes.assert(&mut self.changes);
        self
    }

    /// The branches layered onto this overlay.
    pub fn branches(&self) -> &[&'a Branch] {
        &self.branches
    }

    /// The pending changes on this overlay.
    pub fn changes(&self) -> &Changes {
        &self.changes
    }
}

impl<'a> From<&'a Branch> for QueryLayer<'a> {
    fn from(branch: &'a Branch) -> Self {
        Self {
            branches: vec![branch],
            changes: Changes::new(),
        }
    }
}

impl<'a> From<Changes> for QueryLayer<'a> {
    fn from(changes: Changes) -> Self {
        Self {
            branches: Vec::new(),
            changes,
        }
    }
}

/// A query session on a branch.
///
/// Created by [`Branch::query`]. Composes the primary branch with an
/// optional [`QueryLayer`] overlay (extra branches + in-memory
/// changes), then evaluates queries via `.select(q).perform(&env)`.
///
/// # Auto-injected schema metadata
///
/// At `.perform(env)` time the session resolves the operator's
/// identity via [`Identify`] and synthesizes a [`Changes`] overlay of
/// [`schema`](crate::schema) facts: one [`Origin`](crate::schema::Origin)
/// per branch in scope, one [`Branch`](crate::schema::Branch) per
/// branch, [`BranchRevision`](crate::schema::BranchRevision) when
/// committed, plus one [`Session`](crate::schema::Session) for the
/// whole query (with a cardinality-many `dialog.session/branch`
/// attribute listing the branches). Callers don't pass the profile
/// or operator DID; nothing is written to any branch's tree.
///
/// ```ignore
/// branch.query()
///     .join(&other_branch)                // extra branch in the overlay
///     .with(custom_concept_instance)      // user-asserted facts
///     .select(query)
///     .perform(&env);                     // metadata auto-injected
/// ```
#[derive(Clone)]
pub struct QuerySession<'a> {
    primary: Option<&'a Branch>,
    layer: QueryLayer<'a>,
}

impl<'a> QuerySession<'a> {
    /// Open a session whose only initial source is the given changes
    /// batch (no primary branch). Used by ad-hoc query callers that
    /// only have an in-memory `Changes` to query.
    pub fn from_changes(changes: Changes) -> Self {
        Self {
            primary: None,
            layer: QueryLayer::from(changes),
        }
    }

    /// Assert a [`Statement`] into the session's overlay changes.
    /// Chainable.
    pub fn with<S: Statement>(mut self, statement: S) -> Self {
        self.layer = self.layer.with(statement);
        self
    }

    /// Join another [`QueryLayer`] (or anything convertible into one
    /// — `&Branch`, `Changes`) into the session's overlay.
    pub fn join(mut self, other: impl Into<QueryLayer<'a>>) -> Self {
        self.layer = self.layer.join(other);
        self
    }

    /// The session's primary branch, if any.
    pub fn primary(&self) -> Option<&'a Branch> {
        self.primary
    }

    /// The overlay layered on top of the primary.
    pub fn layer(&self) -> &QueryLayer<'a> {
        &self.layer
    }

    /// Select with a query application. Call `.perform(&operator)` to execute.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        SelectQuery {
            primary: self.primary,
            layer: self.layer.clone(),
            query,
        }
    }
}

/// A query command ready to be performed against an environment.
pub struct SelectQuery<'a, Q> {
    primary: Option<&'a Branch>,
    layer: QueryLayer<'a>,
    query: Q,
}

impl<'a, Q> SelectQuery<'a, Q> {
    pub(crate) fn new(branch: &'a Branch, query: Q) -> Self {
        Self {
            primary: Some(branch),
            layer: QueryLayer::new(),
            query,
        }
    }
}

impl<'a, Q: Application> SelectQuery<'a, Q> {
    /// Execute the query, returning a stream of results.
    ///
    /// Before evaluating, this resolves the operator's profile +
    /// operator DIDs via [`Identify`] and synthesizes the
    /// [`schema`](crate::schema) metadata overlay (Origin / Branch /
    /// BranchRevision per branch + a single Session). The metadata
    /// changes are merged with the session's own overlay changes;
    /// any retracts in the combined batch are lifted into tombstones
    /// that filter matching facts out of each branch's stream before
    /// the union.
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
        let SelectQuery {
            primary,
            layer,
            query,
        } = self;
        async_stream::try_stream! {
            // Resolve identity once for the whole query.
            let operator = Identify
                .perform(env)
                .await
                .map_err(|e| DialogArtifactsError::Storage(format!("identify: {e}")))?;
            let profile = operator.profile().clone();
            let operator_did = operator.did();

            // Build the metadata overlay and merge it with the
            // caller-supplied layer's changes.
            let mut combined_changes = super::metadata::synthesize(
                primary,
                layer.branches(),
                &profile,
                &operator_did,
            )?;
            layer.changes.clone().assert(&mut combined_changes);
            let tombstones = tombstones_from(&combined_changes);

            let query_env = QueryEnv {
                primary,
                branches: layer.branches,
                changes: combined_changes,
                tombstones,
                env,
            };
            let results = Box::pin(query.perform(&query_env));
            for await result in results {
                yield result?;
            }
        }
    }
}

/// The runtime environment that bridges a primary branch, any
/// joined branches, and the per-query overlay changes into the
/// query engine's Provider bounds.
///
/// Built fresh on each `.perform(env)`; the environment reference
/// is never captured on the session itself.
pub(crate) struct QueryEnv<'a, Env> {
    primary: Option<&'a Branch>,
    branches: Vec<&'a Branch>,
    /// All overlay facts — user-asserted + auto-injected metadata —
    /// merged into one batch. Queried via [`Provider<Select> for Changes`](dialog_artifacts::Changes).
    changes: Changes,
    /// `sort_key`s of every retracted fact in `changes`. Each branch
    /// stream is filtered against these before the merge so retracts
    /// in the overlay suppress matching facts in the source.
    tombstones: HashSet<SortKey>,
    env: &'a Env,
}

impl<Env> Clone for QueryEnv<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            primary: self.primary,
            branches: self.branches.clone(),
            changes: self.changes.clone(),
            tombstones: self.tombstones.clone(),
            env: self.env,
        }
    }
}

/// Execute a select against a single branch, transparently routing through
/// the branch's remote upstream when configured. Extracted as a freestanding
/// helper so both [`QueryEnv`] and the transaction-time
/// [`TransactionEnv`](crate::transaction_query::TransactionEnv) share the
/// exact same branch-read path.
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
        let mut streams: Vec<ArtifactStream<'a>> =
            Vec::with_capacity(self.primary.is_some() as usize + self.branches.len() + 1);

        // Branch streams — filtered by tombstones from the overlay's
        // retracts so a `tx.retract(x)` (or any user-asserted retract
        // in `with(...)`) suppresses matching source facts.
        if let Some(branch) = self.primary {
            let raw = select_from_branch(branch, self.env, input.clone()).await?;
            streams.push(filter_tombstones(raw, self.tombstones.clone()));
        }
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
impl<Env: ConditionalSync> Provider<SelectRules> for QueryEnv<'_, Env> {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        // No user-installable rules in this design — the overlay only
        // carries facts. Rules come from the implicit rule each
        // `ConceptDescriptor` carries.
        Ok(ConceptRules::new(&input))
    }
}

impl Branch {
    /// Open a query session on this branch.
    ///
    /// The session starts with no overlay — pure branch query. Use
    /// [`with`](QuerySession::with) to fold in a [`Statement`]'s
    /// changes, or [`join`](QuerySession::join) to add another branch
    /// or a [`Changes`] / [`QueryLayer`]. At `.perform(env)` time the
    /// branch's schema metadata is auto-injected — no manual overlay
    /// needed.
    pub fn query(&self) -> QuerySession<'_> {
        QuerySession {
            primary: Some(self),
            layer: QueryLayer::new(),
        }
    }

    /// Open a query session and immediately fold the given statement
    /// into its overlay.
    ///
    /// Shorthand for `self.query().with(stmt)`. Lets callers compose
    /// in one step:
    ///
    /// ```ignore
    /// branch.with(my_synthetic_facts).select(q).perform(&env);
    /// ```
    pub fn with<S: Statement>(&self, statement: S) -> QuerySession<'_> {
        self.query().with(statement)
    }
}
