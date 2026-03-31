use dialog_capability::fork::Fork;
use dialog_capability::{Provider, authority, storage};
use dialog_common::ConditionalSync;
use dialog_effects::memory as memory_fx;
use dialog_remote_s3::S3;

use super::Branch;
use super::state::{BranchName, UpstreamState};
use crate::repository::error::RepositoryError;
use crate::repository::remote::RemoteBranch;
use crate::repository::remote::RemoteName;
use crate::repository::revision::Revision;

/// Command struct for fetching the upstream branch's current revision.
///
/// Borrows `&Branch` (non-consuming). Reads the branch's upstream to
/// dispatch to local or remote fetch logic.
///
/// Does NOT modify local state — only reads from upstream.
pub struct Fetch<'a> {
    branch: &'a Branch,
}

impl<'a> Fetch<'a> {
    pub(super) fn new(branch: &'a Branch) -> Self {
        Self { branch }
    }
}

impl Fetch<'_> {
    /// Execute the fetch operation, returning the upstream revision.
    ///
    /// Returns `None` if the upstream has no revision yet.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>
            + Provider<Fork<S3, memory_fx::Resolve>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Resolve>>
            + Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<storage::List>
            + Provider<storage::Get>
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
                ..
            } => fetch_remote(self.branch, name, branch_name, env).await,
        }
    }
}

/// Fetch the current revision from a local upstream branch.
///
/// Does NOT modify local state.
async fn fetch_local<Env>(
    branch: &Branch,
    upstream_name: &BranchName,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    Env: Provider<memory_fx::Resolve>,
{
    let upstream = branch
        .branch(upstream_name.clone())
        .load()
        .perform(env)
        .await?;

    Ok(upstream.revision())
}

/// Fetch the current revision from a remote upstream branch.
///
/// Does NOT modify local state. Looks up credentials from the persisted
/// `RemoteSite` configuration.
async fn fetch_remote<Env>(
    branch: &Branch,
    remote: &RemoteName,
    upstream_branch_name: &BranchName,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    Env: Provider<memory_fx::Resolve>
        + Provider<Fork<S3, memory_fx::Resolve>>
        + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Resolve>>
        + Provider<authority::Identify>
        + Provider<authority::Sign>
        + Provider<storage::List>
        + Provider<storage::Get>
        + ConditionalSync,
{
    let remote_repo = branch.remote(remote.clone()).load().perform(env).await?;

    match remote_repo.address().address {
        crate::SiteAddress::S3(addr) => {
            let remote_branch = RemoteBranch::new(
                remote_repo.name().clone(),
                addr.clone(),
                remote_repo.did(),
                upstream_branch_name.clone(),
            );
            remote_branch.resolve(env).await
        }
        #[cfg(feature = "ucan")]
        crate::SiteAddress::Ucan(addr) => {
            let remote_branch = RemoteBranch::new(
                remote_repo.name().clone(),
                addr.clone(),
                remote_repo.did(),
                upstream_branch_name.clone(),
            );
            remote_branch.resolve(env).await
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::artifacts::{Artifact, Instruction};
    use crate::helpers::{test_operator, test_repo};
    use crate::repository::branch::state::UpstreamState;
    use crate::repository::node_reference::NodeReference;
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_fetches_local_upstream_revision() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: crate::Value::String("Main data".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        let main_revision = main.revision().expect("main should have a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: NodeReference::default(),
            })
            .perform(&operator)
            .await?;

        let fetched = super::fetch_local(&feature, &"main".into(), &operator).await?;

        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().tree(), main_revision.tree());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_does_not_modify_local_state_on_fetch() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: crate::Value::String("Main data".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: NodeReference::default(),
            })
            .perform(&operator)
            .await?;

        let feature_revision_before = feature.revision();

        let _fetched = super::fetch_local(&feature, &"main".into(), &operator).await?;

        // Fetch should not modify local state
        assert_eq!(feature.revision(), feature_revision_before);

        Ok(())
    }
}
