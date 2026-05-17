use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{ArtifactSelector, ArtifactStream, DialogArtifactsError, Select};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::Resolve;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;
use dialog_query::layer::{Layer, merge_grouped};
use dialog_query::query::{Application, Output};
use dialog_query::source::SelectRules;

use crate::{Branch, NetworkedIndex, RemoteSite, RepositoryMemoryExt, Upstream};

/// A query session on a branch.
///
/// Created by [`Branch::query`]. The session is a thin composition surface:
/// it layers additional sources on top of the primary branch via
/// `.with(layer)`, then runs queries with `.select(q).perform(&env)`.
///
/// Mutable state — synthetic facts, installed rules — lives on the
/// [`Layer`] *layers* the caller builds, not on the session. Build a
/// layer end-to-end, then attach it:
///
/// ```ignore
/// let metadata = branch.metadata();              // a pre-built layer
/// let synthetic = Layer::new()
///     .assert(my_concept_instance)
///     .install(|q: Query<Derived>| (...,))?;
///
/// branch.query()
///     .with(&other_branch)?                      // union with another branch
///     .with(metadata)?                           // union with branch metadata
///     .with(synthetic)?                          // union with in-memory layer
///     .select(query)
///     .perform(&env);
/// ```
///
/// No environment reference is captured on the session itself —
/// `CompositeEnv` is built fresh at `.perform(env)`.
pub struct QuerySession<'a> {
    primary: &'a Branch,
    branches: Vec<&'a Branch>,
    layer: Layer,
}

/// A source that can be layered onto a [`QuerySession`] via
/// [`QuerySession::with`].
///
/// Implemented for `&'a Branch` (adds another branch's facts to the union)
/// and [`Layer`] (merges in-memory facts and rules into the session's
/// accumulator). The trait keeps the session's composition API polymorphic
/// without capturing the env.
pub trait QueryLayer<'a> {
    /// Apply this layer to the given session.
    fn apply(self, session: QuerySession<'a>) -> Result<QuerySession<'a>, EvaluationError>;
}

impl<'a> QueryLayer<'a> for &'a Branch {
    fn apply(self, mut session: QuerySession<'a>) -> Result<QuerySession<'a>, EvaluationError> {
        session.branches.push(self);
        Ok(session)
    }
}

impl<'a> QueryLayer<'a> for Layer {
    fn apply(self, mut session: QuerySession<'a>) -> Result<QuerySession<'a>, EvaluationError> {
        session.layer = session.layer.extend(self)?;
        Ok(session)
    }
}

impl<'a> QuerySession<'a> {
    /// Layer another source on top of this session.
    ///
    /// Accepts any [`QueryLayer`] — currently a `&Branch` (its data is
    /// unioned at perform time) or a [`Layer`] (merged into the
    /// session's in-memory accumulator). Chainable.
    pub fn with<L: QueryLayer<'a>>(self, layer: L) -> Result<Self, EvaluationError> {
        layer.apply(self)
    }

    /// Borrow the extra branches layered on this session.
    pub fn layered_branches(&self) -> &[&'a Branch] {
        &self.branches
    }

    /// Borrow the merged [`Layer`] (the union of every `Layer` passed to
    /// [`with`](Self::with)).
    pub fn layer(&self) -> &Layer {
        &self.layer
    }

    /// Select with a query application. Call `.perform(&operator)` to execute.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        SelectQuery {
            primary: self.primary,
            branches: self.branches.clone(),
            layer: self.layer.clone(),
            query,
        }
    }
}

/// A query command ready to be performed against an environment.
pub struct SelectQuery<'a, Q> {
    primary: &'a Branch,
    branches: Vec<&'a Branch>,
    layer: Layer,
    query: Q,
}

impl<'a, Q> SelectQuery<'a, Q> {
    pub(crate) fn new(branch: &'a Branch, query: Q) -> Self {
        Self {
            primary: branch,
            branches: Vec::new(),
            layer: Layer::new(),
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
            layer: self.layer,
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
/// branches and an in-memory layer into the query engine's Provider bounds.
///
/// Built fresh on each `.perform(env)` so the environment reference is
/// never captured on the session itself.
pub(crate) struct CompositeEnv<'a, Env> {
    primary: &'a Branch,
    branches: Vec<&'a Branch>,
    layer: Layer,
    env: &'a Env,
}

impl<Env> Clone for CompositeEnv<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            primary: self.primary,
            branches: self.branches.clone(),
            layer: self.layer.clone(),
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
        // Each source independently yields items with same-`(the, of)`
        // consecutive (branches by tree key, `InMemoryFacts` by explicit
        // sort). A plain chain would separate cross-source items sharing a
        // key and break the cardinality-one sliding window in `only.rs`;
        // `merge_grouped` interleaves them so the invariant holds.
        let mut streams: Vec<ArtifactStream<'a>> = Vec::with_capacity(self.branches.len() + 2);
        streams.push(Self::select_branch(self.primary, self.env, input.clone()).await?);
        for branch in &self.branches {
            streams.push(Self::select_branch(branch, self.env, input.clone()).await?);
        }
        streams.push(Provider::<Select<'a>>::execute(&self.layer, input).await?);
        Ok(merge_grouped(streams))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env: ConditionalSync> Provider<SelectRules> for CompositeEnv<'_, Env> {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        // Branches don't currently carry rules of their own, so all rules
        // come from the in-memory layer (which also fills in the implicit
        // per-concept rule on first acquire).
        self.layer.rules().acquire(&input)
    }
}

impl Branch {
    /// Open a query session on this branch.
    ///
    /// The session starts empty — no layered branches, no in-memory facts.
    /// Use `.with(layer)` to attach a `&Branch` or a pre-built `Layer`
    /// (which is where you assert facts and install rules).
    pub fn query(&self) -> QuerySession<'_> {
        QuerySession {
            primary: self,
            branches: Vec::new(),
            layer: Layer::new(),
        }
    }

    /// The branch metadata layer — synthetic facts describing the branch
    /// (name, revision hash, upstream, hosting repository) under the
    /// `dialog.meta/*` attribute namespace.
    ///
    /// Compose with `branch.query().with(branch.metadata())?` to make
    /// branch internals queryable like any other fact.
    pub fn metadata(&self) -> Layer {
        super::metadata::branch_metadata(self)
    }
}
