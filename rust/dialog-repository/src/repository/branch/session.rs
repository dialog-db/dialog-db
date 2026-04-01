use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{ArtifactSelector, ArtifactStream, DialogArtifactsError, Select};
use dialog_capability::Provider;
use dialog_capability::authority;
use dialog_capability::fork::Fork;
use dialog_capability::storage;
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

use super::Branch;
use crate::repository::archive::fallback::FallbackStore;
use crate::repository::branch::state::UpstreamState;

/// A query session on a branch.
///
/// Created by [`Branch::session`]. Install rules with `.install()`,
/// then run queries with `.query(q).perform(&operator)`.
pub struct Session<'a> {
    branch: &'a Branch,
    rules: RuleRegistry,
}

impl<'a> Session<'a> {
    /// Install a deductive rule.
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

    /// Start a query. Call `.perform(&operator)` on the result to execute.
    pub fn query<Q: Application>(&self, query: Q) -> QueryCommand<'_, Q> {
        QueryCommand {
            session: self,
            query,
        }
    }

    /// Alias for [`query`](Session::query).
    pub fn select<Q: Application>(&self, query: Q) -> QueryCommand<'_, Q> {
        self.query(query)
    }
}

/// A query command ready to be performed against an environment.
pub struct QueryCommand<'a, Q> {
    session: &'a Session<'a>,
    query: Q,
}

impl<'a, Q: Application> QueryCommand<'a, Q> {
    /// Execute the query, returning a stream of results.
    pub fn perform<Env>(self, env: &'a Env) -> impl Output<Q::Conclusion> + 'a
    where
        Env: Provider<archive_fx::Get>
            + Provider<archive_fx::Put>
            + Provider<memory_fx::Resolve>
            + Provider<Fork<S3, archive_fx::Get>>
            + Provider<Fork<S3, memory_fx::Resolve>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, archive_fx::Get>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Resolve>>
            + Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<storage::List>
            + Provider<storage::Get>
            + ConditionalSync
            + 'static,
    {
        let source = SessionEnv {
            session: self.session,
            env,
        };
        let query = self.query;
        async_stream::try_stream! {
            let results = Box::pin(query.perform(&source));
            for await result in results {
                yield result?;
            }
        }
    }
}

/// Internal type that bridges Session + Env for the query engine.
struct SessionEnv<'a, Env> {
    session: &'a Session<'a>,
    env: &'a Env,
}

impl<Env> Clone for SessionEnv<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            session: self.session,
            env: self.env,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<'a, Env> Provider<Select<'a>> for SessionEnv<'a, Env>
where
    Env: Provider<archive_fx::Get>
        + Provider<archive_fx::Put>
        + Provider<memory_fx::Resolve>
        + Provider<Fork<S3, archive_fx::Get>>
        + Provider<Fork<S3, memory_fx::Resolve>>
        + Provider<Fork<dialog_remote_ucan_s3::UcanSite, archive_fx::Get>>
        + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Resolve>>
        + Provider<authority::Identify>
        + Provider<authority::Sign>
        + Provider<storage::List>
        + Provider<storage::Get>
        + ConditionalSync
        + 'static,
{
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        let select = self.session.branch.select(input);

        let remote = match self.session.branch.upstream() {
            Some(UpstreamState::Remote { name, .. }) => self
                .session
                .branch
                .remote(name)
                .load()
                .perform(self.env)
                .await
                .ok(),
            _ => None,
        };

        let store = FallbackStore::new(self.env, select.catalog(), remote);
        let stream = select.execute(store).await?;
        Ok(Box::pin(stream))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env: ConditionalSync> Provider<SelectRules> for SessionEnv<'_, Env> {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        self.session.rules.acquire(&input)
    }
}

impl Branch {
    /// Open a query session on this branch.
    pub fn session(&self) -> Session<'_> {
        Session {
            branch: self,
            rules: RuleRegistry::new(),
        }
    }
}
