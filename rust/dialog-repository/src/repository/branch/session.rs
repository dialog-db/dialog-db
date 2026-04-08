use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{ArtifactSelector, ArtifactStream, DialogArtifactsError, Select};
use dialog_capability::Provider;
use dialog_capability::fork::Fork;
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output};
use dialog_query::rule::When;
use dialog_query::rule::deductive::DeductiveRule;
use dialog_query::session::RuleRegistry;
use dialog_query::source::SelectRules;
use dialog_remote_s3::S3;

use dialog_capability::Subject;
use dialog_remote_ucan_s3::UcanSite;

use super::Branch;
use crate::repository::archive::networked::NetworkedIndex;
use crate::repository::branch::upstream::UpstreamState;
use crate::repository::memory::MemoryExt;

/// A query session on a branch.
///
/// Created by [`Branch::session`]. Install rules with `.install()`,
/// then run queries with `.query(q).perform(&operator)`.
pub struct QuerySession<'a> {
    branch: &'a Branch,
    rules: RuleRegistry,
}

impl<'a> QuerySession<'a> {
    /// Register a pre-built deductive rule.
    pub fn register(mut self, rule: DeductiveRule) -> Result<Self, EvaluationError> {
        self.rules.register(rule)?;
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
        self.rules.register(rule)?;
        Ok(self)
    }

    /// Select with a query application. Call `.perform(&operator)` to execute.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        SelectQuery::with_rules(self.branch, self.rules.clone(), query)
    }
}

/// A query command ready to be performed against an environment.
pub struct SelectQuery<'a, Q> {
    branch: &'a Branch,
    rules: RuleRegistry,
    query: Q,
}

impl<'a, Q> SelectQuery<'a, Q> {
    pub(crate) fn new(branch: &'a Branch, query: Q) -> Self {
        Self {
            branch,
            rules: RuleRegistry::new(),
            query,
        }
    }

    fn with_rules(branch: &'a Branch, rules: RuleRegistry, query: Q) -> Self {
        Self {
            branch,
            rules,
            query,
        }
    }
}

impl<'a, Q: Application> SelectQuery<'a, Q> {
    /// Execute the query, returning a stream of results.
    pub fn perform<Env>(self, env: &'a Env) -> impl Output<Q::Conclusion> + 'a
    where
        Env: Provider<archive_fx::Get>
            + Provider<archive_fx::Put>
            + Provider<memory_fx::Resolve>
            + Provider<Fork<S3, archive_fx::Get>>
            + Provider<Fork<S3, memory_fx::Resolve>>
            + Provider<Fork<UcanSite, archive_fx::Get>>
            + Provider<Fork<UcanSite, memory_fx::Resolve>>
            + ConditionalSync
            + 'static,
    {
        let source = QueryEnv::new(self.branch, env, self.rules);
        let query = self.query;
        async_stream::try_stream! {
            let results = Box::pin(query.perform(&source));
            for await result in results {
                yield result?;
            }
        }
    }
}

/// Bridges a Branch + Env + RuleRegistry for the query engine's Provider bounds.
pub(crate) struct QueryEnv<'a, Env> {
    branch: &'a Branch,
    env: &'a Env,
    rules: RuleRegistry,
}

impl<'a, Env> QueryEnv<'a, Env> {
    pub(crate) fn new(branch: &'a Branch, env: &'a Env, rules: RuleRegistry) -> Self {
        Self { branch, env, rules }
    }
}

impl<Env> Clone for QueryEnv<'_, Env> {
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
impl<'a, Env> Provider<Select<'a>> for QueryEnv<'a, Env>
where
    Env: Provider<archive_fx::Get>
        + Provider<archive_fx::Put>
        + Provider<memory_fx::Resolve>
        + Provider<Fork<S3, archive_fx::Get>>
        + Provider<Fork<S3, memory_fx::Resolve>>
        + Provider<Fork<UcanSite, archive_fx::Get>>
        + Provider<Fork<UcanSite, memory_fx::Resolve>>
        + ConditionalSync
        + 'static,
{
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        let select = self.branch.claims().select(input);

        let remote = match self.branch.upstream() {
            Some(UpstreamState::Remote { name, .. }) => {
                Subject::from(self.branch.subject().clone())
                    .remote(name)
                    .load()
                    .perform(self.env)
                    .await
                    .ok()
            }
            _ => None,
        };

        let store = NetworkedIndex::new(self.env, select.catalog(), remote);
        let stream = select.execute(store).await?;
        Ok(Box::pin(stream))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env: ConditionalSync> Provider<SelectRules> for QueryEnv<'_, Env> {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        self.rules.acquire(&input)
    }
}

impl Branch {
    /// Open a query session on this branch.
    pub fn query(&self) -> QuerySession<'_> {
        QuerySession {
            branch: self,
            rules: RuleRegistry::new(),
        }
    }
}
