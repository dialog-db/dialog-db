use dialog_capability::Provider;
use dialog_capability::credential::{Allow, Authorize};
use dialog_capability::fork::Fork;
use dialog_common::ConditionalSync;
use dialog_effects::memory as memory_fx;
use dialog_remote_s3::S3;

use super::Branch;
use super::state::{BranchName, UpstreamState};
use crate::repository::error::RepositoryError;
use crate::repository::remote::RemoteBranch;
use crate::repository::remote::SiteName;
use crate::repository::revision::Revision;

/// Command struct for fetching the upstream branch's current revision.
///
/// Borrows `&Branch` (non-consuming). Reads the branch's upstream to
/// dispatch to local or remote fetch logic.
///
/// Does NOT modify local state — only reads from upstream.
pub struct Fetch<'a, Store> {
    branch: &'a Branch<Store>,
}

impl<'a, Store> Fetch<'a, Store> {
    pub(super) fn new(branch: &'a Branch<Store>) -> Self {
        Self { branch }
    }
}

impl<Store: Clone> Fetch<'_, Store> {
    /// Execute the fetch operation, returning the upstream revision.
    ///
    /// Returns `None` if the upstream has no revision yet.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>
            + Provider<Authorize<memory_fx::Resolve, Allow>>
            + Provider<Fork<S3, memory_fx::Resolve>>
            + ConditionalSync,
    {
        let upstream =
            self.branch
                .upstream()
                .ok_or_else(|| RepositoryError::BranchHasNoUpstream {
                    name: self.branch.name().clone(),
                })?;

        match &upstream {
            UpstreamState::Local { branch: name, .. } => fetch_local(self.branch, name, env).await,
            UpstreamState::Remote {
                name,
                branch: branch_name,
                subject,
                ..
            } => fetch_remote(self.branch, name, branch_name, subject, env).await,
        }
    }
}

/// Fetch the current revision from a local upstream branch.
///
/// Does NOT modify local state.
async fn fetch_local<Store: Clone, Env>(
    branch: &Branch<Store>,
    upstream_name: &BranchName,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    Env: Provider<memory_fx::Resolve>,
{
    let upstream = branch
        .load_branch(upstream_name.clone())
        .perform(env)
        .await?;

    Ok(Some(upstream.revision()))
}

/// Fetch the current revision from a remote upstream branch.
///
/// Does NOT modify local state. Looks up credentials from the persisted
/// `RemoteSite` configuration.
async fn fetch_remote<Store, Env>(
    branch: &Branch<Store>,
    remote: &SiteName,
    upstream_branch_name: &BranchName,
    upstream_subject: &dialog_capability::Did,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    Env: Provider<memory_fx::Resolve>
        + Provider<Authorize<memory_fx::Resolve, Allow>>
        + Provider<Fork<S3, memory_fx::Resolve>>
        + ConditionalSync,
{
    let remote_site = branch.load_remote(remote.clone()).perform(env).await?;

    let remote_branch = RemoteBranch::new(
        remote_site.name().clone(),
        remote_site.s3_address().clone(),
        upstream_subject.clone(),
        upstream_branch_name.clone(),
    );

    remote_branch.resolve(env).await
}

#[cfg(test)]
mod tests {
    use super::super::tests::{test_issuer, test_subject};
    use crate::artifacts::{Artifact, Instruction};
    use crate::repository::Repository;
    use crate::repository::branch::state::UpstreamState;
    use crate::repository::node_reference::NodeReference;
    use dialog_storage::provider::Volatile;
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_fetches_local_upstream_revision() -> anyhow::Result<()> {
        let env = Volatile::new();

        let repo = Repository::new(test_issuer().await, test_subject());

        let main = repo.open_branch("main").perform(&env).await?;
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: crate::Value::String("Main data".to_string()),
                cause: None,
            })]))
            .perform(&env)
            .await?;
        let main_revision = main.revision();

        let feature = repo.open_branch("feature").perform(&env).await?;
        feature
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: NodeReference::default(),
            })
            .perform(&env)
            .await?;

        let fetched = super::fetch_local(&feature, &"main".into(), &env).await?;

        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().tree(), main_revision.tree());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_does_not_modify_local_state_on_fetch() -> anyhow::Result<()> {
        let env = Volatile::new();

        let repo = Repository::new(test_issuer().await, test_subject());

        let main = repo.open_branch("main").perform(&env).await?;
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: crate::Value::String("Main data".to_string()),
                cause: None,
            })]))
            .perform(&env)
            .await?;

        let feature = repo.open_branch("feature").perform(&env).await?;
        feature
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: NodeReference::default(),
            })
            .perform(&env)
            .await?;

        let feature_revision_before = feature.revision();

        let _fetched = super::fetch_local(&feature, &"main".into(), &env).await?;

        assert_eq!(feature.revision(), feature_revision_before);

        Ok(())
    }
}
