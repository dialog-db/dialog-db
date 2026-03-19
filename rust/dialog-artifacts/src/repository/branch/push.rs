use crate::RemoteAddress;
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_effects::remote::RemoteInvocation;
use futures_util::{StreamExt, TryStreamExt};

use super::Branch;
use super::novelty::novelty;
use super::state::{BranchName, UpstreamState};
use crate::repository::error::RepositoryError;
use crate::repository::remote::RemoteBranch;
use crate::repository::remote::SiteName;
use crate::repository::revision::Revision;

/// Command struct for pushing local changes to an upstream branch.
///
/// Borrows `&Branch` (non-consuming). Reads the branch's upstream to
/// dispatch to local or remote push logic.
pub struct Push<'a> {
    branch: &'a Branch,
}

impl<'a> Push<'a> {
    pub(super) fn new(branch: &'a Branch) -> Self {
        Self { branch }
    }
}

impl Push<'_> {
    /// Execute the push operation.
    ///
    /// Returns `Some(revision)` on success, `None` if the push could not
    /// fast-forward (diverged).
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, RepositoryError>
    where
        Env: Provider<archive_fx::Get>
            + Provider<archive_fx::Put>
            + Provider<memory_fx::Resolve>
            + Provider<memory_fx::Publish>
            + Provider<RemoteInvocation<archive_fx::Put, RemoteAddress>>
            + Provider<RemoteInvocation<memory_fx::Resolve, RemoteAddress>>
            + Provider<RemoteInvocation<memory_fx::Publish, RemoteAddress>>
            + ConditionalSync
            + 'static,
    {
        let upstream =
            self.branch
                .upstream()
                .ok_or_else(|| RepositoryError::BranchHasNoUpstream {
                    name: self.branch.name().clone(),
                })?;

        match &upstream {
            UpstreamState::Local { branch: name, .. } => push_local(self.branch, name, env).await,
            UpstreamState::Remote {
                name,
                branch: branch_name,
                subject,
                ..
            } => push_remote(self.branch, name, branch_name, subject, env).await,
        }
    }
}

/// Push local changes to a local upstream branch.
///
/// Fast-forward: if the upstream's tree matches our base (it hasn't diverged),
/// reset upstream to our revision and return success.
/// Diverged: return `Ok(None)`.
async fn push_local<Env>(
    branch: &Branch,
    upstream_name: &BranchName,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    Env: Provider<archive_fx::Get>
        + Provider<archive_fx::Put>
        + Provider<memory_fx::Resolve>
        + Provider<memory_fx::Publish>
        + ConditionalSync
        + 'static,
{
    let upstream = branch
        .load_branch(upstream_name.clone())
        .perform(env)
        .await?;

    let branch_revision = branch.revision();
    let branch_base = branch
        .upstream()
        .map(|u| u.tree().clone())
        .unwrap_or_default();

    if upstream.revision().tree() != &branch_base {
        return Ok(None);
    }

    upstream.reset(branch_revision.clone()).perform(env).await?;

    Ok(Some(branch_revision))
}

/// Push local changes to a remote upstream branch.
///
/// 1. Look up credentials from `RemoteSite`
/// 2. Build `RemoteBranch` from upstream state
/// 3. Compute novel nodes via `novelty()`
/// 4. Read each node's raw bytes from local archive, upload to remote
/// 5. Publish revision to remote
async fn push_remote<Env>(
    branch: &Branch,
    remote: &SiteName,
    upstream_branch_name: &BranchName,
    upstream_subject: &dialog_capability::Did,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    Env: Provider<archive_fx::Get>
        + Provider<archive_fx::Put>
        + Provider<memory_fx::Resolve>
        + Provider<memory_fx::Publish>
        + Provider<RemoteInvocation<archive_fx::Put, RemoteAddress>>
        + Provider<RemoteInvocation<memory_fx::Resolve, RemoteAddress>>
        + Provider<RemoteInvocation<memory_fx::Publish, RemoteAddress>>
        + ConditionalSync
        + 'static,
{
    let remote_site = branch.load_remote(remote.clone()).perform(env).await?;

    let remote_branch = RemoteBranch::new(
        remote_site.name().clone(),
        remote_site.address().clone(),
        upstream_subject.clone(),
        upstream_branch_name.clone(),
    );

    let branch_revision = branch.revision();
    let branch_base = branch
        .upstream()
        .map(|u| u.tree().clone())
        .unwrap_or_default();
    let catalog = branch.archive().index();

    // Maximum number of concurrent block uploads.
    const UPLOAD_CONCURRENCY: usize = 16;

    let nodes = novelty(
        *branch_base.hash(),
        *branch_revision.tree().hash(),
        env,
        catalog.clone(),
    );

    nodes
        .map(|node_result| {
            let catalog = &catalog;
            let remote_branch = &remote_branch;
            async move {
                let node = node_result.map_err(|e| RepositoryError::PushFailed {
                    cause: format!("Failed to compute novelty: {}", e),
                })?;

                let hash = *node.hash();

                let get_cap = catalog.clone().invoke(archive_fx::Get::new(hash));
                let bytes =
                    get_cap
                        .perform(env)
                        .await
                        .map_err(|e| RepositoryError::PushFailed {
                            cause: format!("Failed to read local block: {}", e),
                        })?;

                if let Some(bytes) = bytes {
                    remote_branch
                        .upload_block(hash, bytes, env)
                        .await
                        .map_err(|e| RepositoryError::PushFailed {
                            cause: format!("Failed to upload block: {}", e),
                        })?;
                }

                Ok(())
            }
        })
        .buffer_unordered(UPLOAD_CONCURRENCY)
        .try_collect::<()>()
        .await?;

    remote_branch
        .publish(branch_revision.clone(), env)
        .await
        .map_err(|e| RepositoryError::PushFailed {
            cause: format!("Failed to publish revision: {}", e),
        })?;

    Ok(Some(branch_revision))
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
    async fn it_pushes_to_local_upstream() -> anyhow::Result<()> {
        let env = Volatile::new();

        let repo = Repository::new(test_issuer().await, test_subject());

        let _main = repo.open_branch("main").perform(&env).await?;

        let feature = repo.open_branch("feature").perform(&env).await?;
        feature
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: NodeReference::default(),
            })
            .perform(&env)
            .await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: crate::Value::String("Alice".to_string()),
            cause: None,
        };
        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(&env)
            .await?;

        let feature_revision = feature.revision();

        let result = super::push_local(&feature, &"main".into(), &env).await?;
        assert!(result.is_some());

        let main_reloaded = repo.load_branch("main").perform(&env).await?;
        assert_eq!(main_reloaded.revision().tree(), feature_revision.tree());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_none_when_local_upstream_diverged() -> anyhow::Result<()> {
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

        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: crate::Value::String("feature@test.com".to_string()),
                cause: None,
            })]))
            .perform(&env)
            .await?;

        let result = super::push_local(&feature, &"main".into(), &env).await?;
        assert!(result.is_none(), "Push should return None when diverged");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_has_no_upstream_by_default() -> anyhow::Result<()> {
        let env = Volatile::new();

        let repo = Repository::new(test_issuer().await, test_subject());
        let branch = repo.open_branch("feature").perform(&env).await?;

        assert!(branch.upstream().is_none());

        Ok(())
    }
}
