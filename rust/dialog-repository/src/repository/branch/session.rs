use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStream, DialogArtifactsError, Select, Value,
};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::Resolve;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;
use dialog_query::overlay::{Overlaid, Overlay};
use dialog_query::query::{Application, Output};
use dialog_query::rule::When;
use dialog_query::rule::deductive::DeductiveRule;
use dialog_query::session::RuleRegistry;
use dialog_query::source::SelectRules;

use crate::{Branch, NetworkedIndex, RemoteSite, RepositoryMemoryExt, Upstream};

/// A query session on a branch.
///
/// Created by [`Branch::query`]. Build up the session by installing rules
/// (`.install` / `.register`) and asserting overlay facts (`.assert` /
/// `.fact`), then run queries with `.select(q).perform(&operator)`.
///
/// The overlay accumulated on the session is unioned with the branch's
/// stored artifacts for every query: facts asserted here show up alongside
/// real facts, and rules installed here merge with the implicit per-concept
/// rules derived from descriptors.
pub struct QuerySession<'a> {
    branch: &'a Branch,
    overlay: Overlay,
}

impl<'a> QuerySession<'a> {
    /// Register a pre-built deductive rule.
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

    /// Assert an overlay artifact. The artifact is visible to subsequent
    /// queries through this session, unioned with the branch's stored facts.
    pub fn assert(mut self, artifact: Artifact) -> Self {
        self.overlay = self.overlay.assert(artifact);
        self
    }

    /// Assert an overlay artifact from triple parts.
    pub fn fact(
        mut self,
        the: impl AsRef<str>,
        of: impl AsRef<str>,
        is: impl Into<Value>,
    ) -> Result<Self, DialogArtifactsError> {
        self.overlay = self.overlay.fact(the, of, is)?;
        Ok(self)
    }

    /// Replace the entire overlay with the given one. Useful when composing
    /// a pre-built overlay (e.g. [`Branch::metadata`]).
    pub fn with_overlay(mut self, overlay: Overlay) -> Self {
        self.overlay = overlay;
        self
    }

    /// Borrow the overlay being built.
    pub fn overlay(&self) -> &Overlay {
        &self.overlay
    }

    /// Select with a query application. Call `.perform(&operator)` to execute.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        SelectQuery::with_overlay(self.branch, self.overlay.clone(), query)
    }
}

/// A query command ready to be performed against an environment.
pub struct SelectQuery<'a, Q> {
    branch: &'a Branch,
    overlay: Overlay,
    query: Q,
}

impl<'a, Q> SelectQuery<'a, Q> {
    pub(crate) fn new(branch: &'a Branch, query: Q) -> Self {
        Self {
            branch,
            overlay: Overlay::new(),
            query,
        }
    }

    fn with_overlay(branch: &'a Branch, overlay: Overlay, query: Q) -> Self {
        Self {
            branch,
            overlay,
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
        let branch_env = BranchEnv::new(self.branch, env);
        let env = Overlaid::new(branch_env, self.overlay);
        let query = self.query;
        async_stream::try_stream! {
            let results = Box::pin(query.perform(&env));
            for await result in results {
                yield result?;
            }
        }
    }
}

/// Bridges a Branch + Env for the query engine's Provider bounds.
///
/// This is the "primary" side of the overlay pair: it serves the branch's
/// stored claims and the implicit per-concept rules.
pub(crate) struct BranchEnv<'a, Env> {
    branch: &'a Branch,
    env: &'a Env,
    // An empty rule registry on the primary side. Overlay rules are merged
    // in by `Overlaid::execute(SelectRules)`. The registry's `acquire` will
    // create the implicit per-concept rule on first lookup, matching the
    // engine's expectation that `SelectRules` always returns a value.
    rules: RuleRegistry,
}

impl<'a, Env> BranchEnv<'a, Env> {
    pub(crate) fn new(branch: &'a Branch, env: &'a Env) -> Self {
        Self {
            branch,
            env,
            rules: RuleRegistry::new(),
        }
    }
}

impl<Env> Clone for BranchEnv<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            branch: self.branch,
            env: self.env,
            rules: self.rules.clone(),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<'a, Env> Provider<Select<'a>> for BranchEnv<'a, Env>
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
        let select = self.branch.claims().select(input);

        let remote = match self.branch.upstream() {
            Some(Upstream::Remote { remote: name, .. }) => self
                .branch
                .subject()
                .remote(name)
                .load()
                .perform(self.env)
                .await
                .ok(),
            _ => None,
        };

        let store = NetworkedIndex::new(self.env, select.catalog(), remote);
        let stream = select.execute(store).await?;
        Ok(Box::pin(stream))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env: ConditionalSync> Provider<SelectRules> for BranchEnv<'_, Env> {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        self.rules.acquire(&input)
    }
}

impl Branch {
    /// Open a query session on this branch.
    ///
    /// The session starts with an empty overlay. Use `.assert(...)` /
    /// `.fact(...)` to inject in-memory facts and `.install(...)` /
    /// `.register(...)` to attach deductive rules — both are merged with
    /// the branch's data during evaluation.
    pub fn query(&self) -> QuerySession<'_> {
        QuerySession {
            branch: self,
            overlay: Overlay::new(),
        }
    }

    /// The branch metadata overlay — synthetic facts describing the branch
    /// (name, revision hash, upstream, hosting repository) under the
    /// `dialog.meta/*` attribute namespace.
    ///
    /// Compose with `branch.query().with_overlay(branch.metadata())` to make
    /// branch internals queryable like any other fact.
    pub fn metadata(&self) -> Overlay {
        super::metadata::branch_metadata(self)
    }
}
