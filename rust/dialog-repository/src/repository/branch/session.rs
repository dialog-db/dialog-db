use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{ArtifactSelector, ArtifactStream, DialogArtifactsError, Select};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::Resolve;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output};
use dialog_query::source::SelectRules;

use crate::layer::{VolatileLayer, merge_grouped};
use crate::{Branch, NetworkedIndex, RemoteSite, RepositoryMemoryExt, Upstream};

/// A query session on a branch.
///
/// Created by [`Branch::query`]. The session is a thin composition surface:
/// it layers additional sources on top of the primary branch via
/// `.with(layer)`, then runs queries with `.select(q).perform(&env)`.
///
/// Each source — the primary branch, every additional `&Branch`, every
/// [`VolatileLayer`] — is kept independent. At evaluation time their selects are
/// unioned via [`merge_grouped`] and their rules merged per concept.
/// Mutable state (synthetic facts, installed rules) lives on the
/// `VolatileLayer`s the caller builds, not on the session itself.
///
/// ```ignore
/// let metadata = branch.metadata();              // a pre-built layer
/// let synthetic = VolatileLayer::new()
///     .assert(my_concept_instance)
///     .install(|q: Query<Derived>| (...,))?;
///
/// branch.query()
///     .with(&other_branch)                       // union with another branch
///     .with(metadata)                            // union with branch metadata
///     .with(synthetic)                           // union with in-memory layer
///     .select(query)
///     .perform(&env);
/// ```
///
/// No environment reference is captured on the session itself —
/// `QueryEnv` is built fresh at `.perform(env)`.
pub struct QuerySession<'a> {
    primary: &'a Branch,
    branches: Vec<&'a Branch>,
    layers: Vec<VolatileLayer>,
}

/// A source that can be layered onto a [`QuerySession`] via
/// [`QuerySession::with`].
///
/// Implemented for `&'a Branch` (adds another branch's facts to the
/// union) and [`VolatileLayer`] (adds an in-memory fact + rule source).
/// Polymorphic without exposing how each layer is dispatched internally
/// and without capturing the env.
pub trait QueryLayer<'a> {
    /// Apply this layer to the given session.
    fn apply(self, session: QuerySession<'a>) -> QuerySession<'a>;
}

impl<'a> QueryLayer<'a> for &'a Branch {
    fn apply(self, mut session: QuerySession<'a>) -> QuerySession<'a> {
        session.branches.push(self);
        session
    }
}

impl<'a> QueryLayer<'a> for VolatileLayer {
    fn apply(self, mut session: QuerySession<'a>) -> QuerySession<'a> {
        session.layers.push(self);
        session
    }
}

impl<'a> QuerySession<'a> {
    /// Layer another source on top of this session.
    ///
    /// Accepts any [`QueryLayer`] — currently a `&Branch` (its data is
    /// unioned at perform time) or a [`VolatileLayer`] (kept independent and
    /// unioned at perform time). Chainable.
    pub fn with<L: QueryLayer<'a>>(self, layer: L) -> Self {
        layer.apply(self)
    }

    /// Borrow the extra branches layered on this session.
    pub fn layered_branches(&self) -> &[&'a Branch] {
        &self.branches
    }

    /// Borrow the layers (in `with`-call order) attached to this session.
    pub fn layers(&self) -> &[VolatileLayer] {
        &self.layers
    }

    /// Select with a query application. Call `.perform(&operator)` to execute.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        SelectQuery {
            primary: self.primary,
            branches: self.branches.clone(),
            layers: self.layers.clone(),
            query,
        }
    }
}

/// A query command ready to be performed against an environment.
pub struct SelectQuery<'a, Q> {
    primary: &'a Branch,
    branches: Vec<&'a Branch>,
    layers: Vec<VolatileLayer>,
    query: Q,
}

impl<'a, Q> SelectQuery<'a, Q> {
    pub(crate) fn new(branch: &'a Branch, query: Q) -> Self {
        Self {
            primary: branch,
            branches: Vec::new(),
            layers: Vec::new(),
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
        let query_env = QueryEnv {
            primary: self.primary,
            branches: self.branches,
            layers: self.layers,
            env,
        };
        let query = self.query;
        async_stream::try_stream! {
            let results = Box::pin(query.perform(&query_env));
            for await result in results {
                yield result?;
            }
        }
    }
}

/// The runtime environment that bridges a primary branch plus any
/// layered branches and in-memory layers into the query engine's
/// Provider bounds.
///
/// Built fresh on each `.perform(env)` so the environment reference is
/// never captured on the session itself.
pub(crate) struct QueryEnv<'a, Env> {
    primary: &'a Branch,
    branches: Vec<&'a Branch>,
    layers: Vec<VolatileLayer>,
    env: &'a Env,
}

impl<Env> Clone for QueryEnv<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            primary: self.primary,
            branches: self.branches.clone(),
            layers: self.layers.clone(),
            env: self.env,
        }
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
        // Every source independently yields artifacts with same-`(the, of)`
        // consecutive (branches by tree key, layers by their own tree
        // scan). `merge_grouped` interleaves them so cross-source items
        // sharing a key stay adjacent — the invariant the cardinality-one
        // sliding window in `only.rs` depends on.
        let mut streams: Vec<ArtifactStream<'a>> =
            Vec::with_capacity(1 + self.branches.len() + self.layers.len());
        streams.push(Self::select_branch(self.primary, self.env, input.clone()).await?);
        for branch in &self.branches {
            streams.push(Self::select_branch(branch, self.env, input.clone()).await?);
        }
        for layer in &self.layers {
            streams.push(Provider::<Select<'a>>::execute(layer, input.clone()).await?);
        }
        Ok(merge_grouped(streams))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env: ConditionalSync> Provider<SelectRules> for QueryEnv<'_, Env> {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        // Branches don't currently carry rules of their own. Each layer's
        // rule registry contributes alternative deductive rules per
        // concept; we acquire from each and merge installed rules
        // together. The implicit rule (auto-derived from the concept
        // descriptor) is the same across registries, so dedup happens
        // naturally via `ConceptRules::extend`.
        let mut iter = self.layers.iter();
        let mut acquired = match iter.next() {
            Some(layer) => layer.rules().acquire(&input)?,
            None => ConceptRules::new(&input),
        };
        for layer in iter {
            let more = layer.rules().acquire(&input)?;
            acquired.extend(&more);
        }
        Ok(acquired)
    }
}

impl Branch {
    /// Open a query session on this branch.
    ///
    /// The session starts empty — no layered branches, no layers.
    /// Use `.with(layer)` to attach a `&Branch` or a pre-built `VolatileLayer`
    /// (which is where you assert facts and install rules).
    pub fn query(&self) -> QuerySession<'_> {
        QuerySession {
            primary: self,
            branches: Vec::new(),
            layers: Vec::new(),
        }
    }

    /// The branch metadata layer — synthetic facts describing the branch
    /// (name, revision hash, upstream, hosting repository) under the
    /// `dialog.meta/*` attribute namespace.
    ///
    /// Compose with `branch.query().with(branch.metadata().await?)` to
    /// make branch internals queryable like any other fact. Async because
    /// committing the synthetic facts into the layer's prolly tree
    /// requires an await.
    pub async fn metadata(&self) -> Result<VolatileLayer, DialogArtifactsError> {
        super::metadata::branch_metadata(self).await
    }
}
