use dialog_capability::fork::Fork;
use dialog_capability::{Provider, authority, storage};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_remote_s3::S3;

use super::Branch;
use super::novelty::novelty;
use super::state::{BranchName, UpstreamState};
use crate::repository::error::RepositoryError;
use crate::repository::remote::RemoteName;
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
            + Provider<Fork<S3, archive_fx::Put>>
            + Provider<Fork<S3, memory_fx::Resolve>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, archive_fx::Get>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Resolve>>
            + Provider<Fork<S3, memory_fx::Publish>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, archive_fx::Put>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Resolve>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Publish>>
            + Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<storage::List>
            + Provider<storage::Get>
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
                ..
            } => Box::pin(push_remote(self.branch, name, branch_name, env)).await,
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
        .branch(upstream_name.clone())
        .load()
        .perform(env)
        .await?;

    let branch_revision = match branch.revision() {
        Some(rev) => rev,
        None => return Ok(None),
    };
    let branch_base = branch
        .upstream()
        .map(|u| u.tree().clone())
        .unwrap_or_default();

    let upstream_tree = upstream
        .revision()
        .map(|r| r.tree().clone())
        .unwrap_or_default();

    if upstream_tree != branch_base {
        return Ok(None);
    }

    upstream.reset(branch_revision.clone()).perform(env).await?;

    Ok(Some(branch_revision))
}

/// Push local changes to a remote upstream branch.
///
/// Push local changes to a remote upstream branch.
///
/// 1. Load remote repository
/// 2. Compute novel nodes
/// 3. Upload blocks via remote archive
/// 4. Publish revision via remote branch
/// 5. Update local upstream state
async fn push_remote<Env>(
    branch: &Branch,
    remote: &RemoteName,
    upstream_branch_name: &BranchName,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    Env: Provider<archive_fx::Get>
        + Provider<archive_fx::Put>
        + Provider<memory_fx::Resolve>
        + Provider<memory_fx::Publish>
        + Provider<Fork<S3, archive_fx::Put>>
        + Provider<Fork<S3, memory_fx::Resolve>>
        + Provider<Fork<S3, memory_fx::Publish>>
        + Provider<Fork<dialog_remote_ucan_s3::UcanSite, archive_fx::Put>>
        + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Resolve>>
        + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Publish>>
        + Provider<authority::Identify>
        + Provider<authority::Sign>
        + Provider<storage::List>
        + Provider<storage::Get>
        + ConditionalSync
        + 'static,
{
    let branch_revision = match branch.revision() {
        Some(rev) => rev,
        None => return Ok(None),
    };
    let branch_base = branch
        .upstream()
        .map(|u| u.tree().clone())
        .unwrap_or_default();

    // Load remote repository and open remote branch
    let remote_repo = branch.remote(remote.clone()).load().perform(env).await?;
    let remote_branch = remote_repo
        .branch(upstream_branch_name.clone())
        .open()
        .perform(env)
        .await?;

    // Compute and upload novel blocks
    let nodes = novelty(
        *branch_base.hash(),
        *branch_revision.tree().hash(),
        env,
        branch.archive().index(),
    );

    let local_catalog = branch.archive().index();
    Box::pin(
        remote_repo
            .archive()
            .index()
            .upload(nodes, local_catalog)
            .perform(env),
    )
    .await?;

    // Publish revision to remote
    remote_branch
        .publish(branch_revision.clone())
        .perform(env)
        .await?;

    // Update local upstream state
    if let Some(upstream) = branch.upstream() {
        branch
            .upstream
            .publish(
                Some(upstream.with_tree(branch_revision.tree().clone())),
                env,
            )
            .await
            .map_err(|e| RepositoryError::PushFailed {
                cause: format!("Failed to update upstream state: {:?}", e),
            })?;
    }

    Ok(Some(branch_revision))
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator, test_repo};
    use crate::repository::branch::state::UpstreamState;
    use crate::repository::node_reference::NodeReference;
    use crate::{Artifact, Instruction};
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_pushes_to_local_upstream() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;

        let _main = repo.branch("main").open().perform(&operator).await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: NodeReference::default(),
            })
            .perform(&operator)
            .await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: crate::Value::String("Alice".to_string()),
            cause: None,
        };
        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(&operator)
            .await?;

        let feature_revision = feature.revision().expect("feature should have a revision");

        let result = super::push_local(&feature, &"main".into(), &operator).await?;
        assert!(result.is_some());

        let main_reloaded = repo.branch("main").load().perform(&operator).await?;
        let main_rev = main_reloaded
            .revision()
            .expect("main should have a revision after push");
        assert_eq!(main_rev.tree(), feature_revision.tree());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_none_when_local_upstream_diverged() -> anyhow::Result<()> {
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

        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: crate::Value::String("feature@test.com".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let result = super::push_local(&feature, &"main".into(), &operator).await?;
        assert!(result.is_none(), "Push should return None when diverged");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_has_no_upstream_by_default() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("feature").open().perform(&operator).await?;

        assert!(branch.upstream().is_none());

        Ok(())
    }
}
