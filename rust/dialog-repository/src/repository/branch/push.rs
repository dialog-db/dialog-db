use crate::repository::memory::MemoryExt;
use crate::repository::remote::address::RemoteSite;
use dialog_capability::Fork;
use dialog_capability::Provider;
use dialog_capability::Subject;
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::novelty::novelty;
use super::upstream::UpstreamState;
use crate::repository::archive::ArchiveExt as _;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

/// Command struct for pushing local changes to an upstream branch.
///
/// Borrows `&Branch` (non-consuming). Reads the branch's upstream to
/// dispatch to local or remote push logic.
pub struct Push<'a> {
    branch: &'a Branch,
}

impl<'a> Push<'a> {
    fn new(branch: &'a Branch) -> Self {
        Self { branch }
    }
}

impl Branch {
    /// Create a command to push local changes to the upstream branch.
    ///
    /// Reads the upstream configuration from branch state and dispatches
    /// to local or remote push logic.
    pub fn push(&self) -> Push<'_> {
        Push::new(self)
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
            + Provider<Fork<RemoteSite, archive_fx::Get>>
            + Provider<Fork<RemoteSite, archive_fx::Put>>
            + Provider<Fork<RemoteSite, memory_fx::Resolve>>
            + Provider<Fork<RemoteSite, memory_fx::Publish>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let upstream = branch
            .upstream()
            .ok_or_else(|| RepositoryError::BranchHasNoUpstream {
                name: branch.name().clone(),
            })?;

        match &upstream {
            UpstreamState::Local {
                branch: upstream_name,
                ..
            } => {
                // Fast-forward push to local upstream
                let upstream_branch = Subject::from(branch.subject().clone())
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

                let upstream_tree = upstream_branch
                    .revision()
                    .map(|r| r.tree.clone())
                    .unwrap_or_default();

                // Can only fast-forward if upstream hasn't diverged
                if upstream_tree != branch_base {
                    return Ok(None);
                }

                upstream_branch
                    .reset(branch_revision.clone())
                    .perform(env)
                    .await?;

                Ok(Some(branch_revision))
            }
            UpstreamState::Remote {
                name: remote_name,
                branch: upstream_branch_name,
                ..
            } => {
                let branch_revision = match branch.revision() {
                    Some(rev) => rev,
                    None => return Ok(None),
                };
                let branch_base = branch
                    .upstream()
                    .map(|u| u.tree().clone())
                    .unwrap_or_default();

                // Load remote repository and open remote branch
                let remote_repo = Subject::from(branch.subject().clone())
                    .remote(remote_name.clone())
                    .load()
                    .perform(env)
                    .await?;
                let remote_branch = remote_repo
                    .branch(upstream_branch_name.clone())
                    .open()
                    .perform(env)
                    .await?;

                // Compute and upload novel blocks
                let nodes = novelty(
                    *branch_base.hash(),
                    *branch_revision.tree.hash(),
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
                        .publish(Some(upstream.with_tree(branch_revision.tree.clone())))
                        .perform(env)
                        .await
                        .map_err(|e| RepositoryError::PushFailed {
                            cause: format!("Failed to update upstream state: {:?}", e),
                        })?;
                }

                Ok(Some(branch_revision))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use crate::repository::branch::upstream::UpstreamState;
    use crate::repository::node_reference::NodeReference;
    use crate::{Artifact, Instruction, Value};
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_pushes_to_local_upstream() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

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
            is: Value::String("Alice".to_string()),
            cause: None,
        };
        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(&operator)
            .await?;

        let feature_revision = feature.revision().expect("feature should have a revision");

        let result = feature.push().perform(&operator).await?;
        assert!(result.is_some());

        let main_reloaded = repo.branch("main").load().perform(&operator).await?;
        let main_rev = main_reloaded
            .revision()
            .expect("main should have a revision after push");
        assert_eq!(main_rev.tree, feature_revision.tree);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_none_when_local_upstream_diverged() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: Value::String("Main data".to_string()),
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
                is: Value::String("feature@example.com".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let result = feature.push().perform(&operator).await?;
        assert!(result.is_none(), "Push should return None when diverged");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_has_no_upstream_by_default() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("feature").open().perform(&operator).await?;

        assert!(branch.upstream().is_none());

        Ok(())
    }
}
