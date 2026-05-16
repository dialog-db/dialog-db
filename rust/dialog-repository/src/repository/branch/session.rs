use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    ArtifactSelector, ArtifactStream, DialogArtifactsError, Select, Statement,
};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::Resolve;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;
use dialog_query::overlay::Overlay;
use dialog_query::query::{Application, Output};
use dialog_query::rule::When;
use dialog_query::rule::deductive::DeductiveRule;
use dialog_query::source::SelectRules;

use crate::{Branch, NetworkedIndex, RemoteSite, RepositoryMemoryExt, Upstream};

/// A query session on a branch.
///
/// Created by [`Branch::query`]. The session is a composition surface:
/// - `.assert(stmt)` / `.retract(stmt)` push facts into an in-memory
///   [`Overlay`] using the same [`Statement`] API as [`Transaction`].
/// - `.install(rule_fn)` / `.register(rule)` add deductive rules to the
///   overlay so they merge per-concept with the implicit rules.
/// - `.overlay(layer)` layers another source (a `&Branch` or a pre-built
///   `Overlay`) on top. Layers compose incrementally; selects union from
///   every layer at perform time.
///
/// All layering is captured on the session itself — no environment is
/// referenced until `.select(...).perform(env)` runs.
pub struct QuerySession<'a> {
    primary: &'a Branch,
    branches: Vec<&'a Branch>,
    overlay: Overlay,
}

/// A source that can be added as an overlay layer on a [`QuerySession`].
///
/// Implemented for `&'a Branch` (adds another branch's facts to the union)
/// and `Overlay` (merges in-memory facts and rules into the session's
/// accumulator). Avoids exposing the layer enum or capturing the env.
pub trait OverlayLayer<'a> {
    /// Apply this layer to the given session.
    fn apply(self, session: QuerySession<'a>) -> Result<QuerySession<'a>, EvaluationError>;
}

impl<'a> OverlayLayer<'a> for &'a Branch {
    fn apply(self, mut session: QuerySession<'a>) -> Result<QuerySession<'a>, EvaluationError> {
        session.branches.push(self);
        Ok(session)
    }
}

impl<'a> OverlayLayer<'a> for Overlay {
    fn apply(self, mut session: QuerySession<'a>) -> Result<QuerySession<'a>, EvaluationError> {
        session.overlay = session.overlay.extend(self)?;
        Ok(session)
    }
}

impl<'a> QuerySession<'a> {
    /// Register a pre-built deductive rule on the session overlay.
    pub fn register(mut self, rule: DeductiveRule) -> Result<Self, EvaluationError> {
        self.overlay = self.overlay.register(rule)?;
        Ok(self)
    }

    /// Install a deductive rule from a function.
    pub fn install<M, W>(mut self, rule: impl Fn(M) -> W) -> Result<Self, EvaluationError>
    where
        M: Application + Default + Into<ConceptDescriptor>,
        W: When,
    {
        let query = M::default();
        let concept: ConceptDescriptor = query.clone().into();
        let when = rule(query).into_premises();
        let premises = when.into_vec();
        let rule =
            DeductiveRule::new(concept, premises).map_err(|e| EvaluationError::Planning {
                message: e.to_string(),
            })?;
        self.overlay = self.overlay.register(rule)?;
        Ok(self)
    }

    /// Assert a [`Statement`] into the session's overlay — same shape as
    /// [`Transaction::assert`](super::Transaction::assert), but the result
    /// is queryable in-memory rather than committed to storage.
    pub fn assert<S: Statement>(mut self, statement: S) -> Self {
        self.overlay = self.overlay.assert(statement);
        self
    }

    /// Retract a [`Statement`] from the session's overlay.
    pub fn retract<S: Statement>(mut self, statement: S) -> Self {
        self.overlay = self.overlay.retract(statement);
        self
    }

    /// Layer another source on top of this session.
    ///
    /// Accepts any [`OverlayLayer`] — currently a `&Branch` (its data is
    /// unioned at perform time) or an [`Overlay`] (merged into the
    /// session's in-memory accumulator). Chainable.
    ///
    /// ```ignore
    /// let main = repo.branch("main").load().perform(&op).await?;
    /// let feature = repo.branch("feature/x").load().perform(&op).await?;
    /// let results = feature.query()
    ///     .overlay(&main)
    ///     .overlay(feature.metadata())
    ///     .assert(my_synthetic_fact)
    ///     .select(query)
    ///     .perform(&op);
    /// ```
    pub fn overlay<L: OverlayLayer<'a>>(self, layer: L) -> Result<Self, EvaluationError> {
        layer.apply(self)
    }

    /// Borrow the accumulated overlay.
    pub fn current_overlay(&self) -> &Overlay {
        &self.overlay
    }

    /// Borrow the extra branches layered on this session.
    pub fn layered_branches(&self) -> &[&'a Branch] {
        &self.branches
    }

    /// Select with a query application. Call `.perform(&operator)` to execute.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        SelectQuery {
            primary: self.primary,
            branches: self.branches.clone(),
            overlay: self.overlay.clone(),
            query,
        }
    }
}

/// A query command ready to be performed against an environment.
pub struct SelectQuery<'a, Q> {
    primary: &'a Branch,
    branches: Vec<&'a Branch>,
    overlay: Overlay,
    query: Q,
}

impl<'a, Q> SelectQuery<'a, Q> {
    pub(crate) fn new(branch: &'a Branch, query: Q) -> Self {
        Self {
            primary: branch,
            branches: Vec::new(),
            overlay: Overlay::new(),
            query,
        }
    }
}

impl<'a, Q: Application> SelectQuery<'a, Q> {
    /// Execute the query, returning a stream of results.
    pub fn perform<Env>(self, env: &'a Env) -> impl Output<Q::Conclusion> + 'a
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let composite = CompositeEnv {
            primary: self.primary,
            branches: self.branches,
            overlay: self.overlay,
            env,
        };
        let query = self.query;
        async_stream::try_stream! {
            let results = Box::pin(query.perform(&composite));
            for await result in results {
                yield result?;
            }
        }
    }
}

/// The runtime environment that bridges a primary branch plus any layered
/// branches and an in-memory overlay into the query engine's Provider bounds.
///
/// Built fresh on each `.perform(env)` so the environment reference is
/// never captured on the session itself.
pub(crate) struct CompositeEnv<'a, Env> {
    primary: &'a Branch,
    branches: Vec<&'a Branch>,
    overlay: Overlay,
    env: &'a Env,
}

impl<Env> Clone for CompositeEnv<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            primary: self.primary,
            branches: self.branches.clone(),
            overlay: self.overlay.clone(),
            env: self.env,
        }
    }
}

impl<'a, Env> CompositeEnv<'a, Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    async fn select_branch(
        branch: &'a Branch,
        env: &'a Env,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        let select = branch.claims().select(input);

        let remote = match branch.upstream() {
            Some(Upstream::Remote { remote: name, .. }) => branch
                .subject()
                .remote(name)
                .load()
                .perform(env)
                .await
                .ok(),
            _ => None,
        };

        let store = NetworkedIndex::new(env, select.catalog(), remote);
        let stream = select.execute(store).await?;
        Ok(Box::pin(stream))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<'a, Env> Provider<Select<'a>> for CompositeEnv<'a, Env>
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
        // Each branch produces a stream; collect them, then chain with the
        // in-memory overlay. Streams are returned eagerly (the inner state
        // is loaded up front) so chaining preserves correctness without
        // re-selecting.
        let mut streams: Vec<ArtifactStream<'a>> = Vec::with_capacity(self.branches.len() + 2);
        streams.push(Self::select_branch(self.primary, self.env, input.clone()).await?);
        for branch in &self.branches {
            streams.push(Self::select_branch(branch, self.env, input.clone()).await?);
        }
        streams.push(Provider::<Select<'a>>::execute(&self.overlay, input).await?);
        use futures_util::StreamExt;
        Ok(Box::pin(futures_util::stream::iter(streams).flatten()))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env: ConditionalSync> Provider<SelectRules> for CompositeEnv<'_, Env> {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        // Branches don't currently carry rules of their own, so all rules
        // come from the in-memory overlay (which also fills in the implicit
        // per-concept rule on first acquire).
        self.overlay.rules().acquire(&input)
    }
}

impl Branch {
    /// Open a query session on this branch.
    ///
    /// The session starts with an empty overlay and no layered branches.
    /// Use `.assert(...)` / `.retract(...)` to inject in-memory facts,
    /// `.install(...)` / `.register(...)` to attach deductive rules, and
    /// `.overlay(...)` to layer another `&Branch` or a pre-built `Overlay`.
    pub fn query(&self) -> QuerySession<'_> {
        QuerySession {
            primary: self,
            branches: Vec::new(),
            overlay: Overlay::new(),
        }
    }

    /// The branch metadata overlay — synthetic facts describing the branch
    /// (name, revision hash, upstream, hosting repository) under the
    /// `dialog.meta/*` attribute namespace.
    ///
    /// Compose with `branch.query().overlay(branch.metadata())?` to make
    /// branch internals queryable like any other fact.
    pub fn metadata(&self) -> Overlay {
        super::metadata::branch_metadata(self)
    }
}
