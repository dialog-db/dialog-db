use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::{Publish, Resolve};
use dialog_prolly_tree::{Tree, TreeDifference};
use futures_util::TryStreamExt;

use super::{Branch, Index, UpstreamState};
use crate::repository::archive::RepositoryArchiveExt as _;
use crate::repository::archive::local::LocalIndex;
use crate::repository::error::RepositoryError;
use crate::repository::memory::RepositoryMemoryExt;
use crate::repository::remote::RemoteSite;
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
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Put>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<Fork<RemoteSite, Publish>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let upstream = branch
            .upstream()
            .ok_or_else(|| RepositoryError::BranchHasNoUpstream {
                name: branch.name().to_string(),
            })?;

        match &upstream {
            UpstreamState::Local {
                branch: upstream_name,
                ..
            } => {
                // Fast-forward push to local upstream. Use `.open()` so
                // pushing into an upstream branch with no prior commits
                // still works — the upstream just starts at our tree.
                let upstream = branch
                    .subject()
                    .branch(upstream_name.clone())
                    .open()
                    .perform(env)
                    .await?;

                let revision = match branch.revision() {
                    Some(revision) => revision,
                    None => return Ok(None),
                };
                let base = branch
                    .upstream()
                    .map(|u| u.tree().clone())
                    .unwrap_or_default();

                let current = upstream
                    .revision()
                    .map(|r| r.tree.clone())
                    .unwrap_or_default();

                // Can only fast-forward if upstream hasn't diverged
                if current != base {
                    return Ok(None);
                }

                upstream.reset(revision.clone()).perform(env).await?;

                Ok(Some(revision))
            }
            UpstreamState::Remote {
                name: remote_name,
                branch: upstream_branch_name,
                ..
            } => {
                let revision = match branch.revision() {
                    Some(rev) => rev,
                    None => return Ok(None),
                };
                let base = branch
                    .upstream()
                    .map(|u| u.tree().clone())
                    .unwrap_or_default();

                // Load remote repository and open remote branch
                let remote = branch
                    .subject()
                    .remote(remote_name.clone())
                    .load()
                    .perform(env)
                    .await?;

                let upstream = remote
                    .branch(upstream_branch_name.clone())
                    .open()
                    .perform(env)
                    .await?;

                // Compute and upload novel blocks: tree nodes that exist
                // in the current tree but not in the base tree.
                let index = branch.archive().index();
                let store = LocalIndex::new(env, index.clone());
                let base: Index = Tree::from_hash(base.hash(), &store).await?;
                let current: Index = Tree::from_hash(revision.tree.hash(), &store).await?;
                let difference = TreeDifference::compute(&base, &current, &store, &store).await?;
                let novelty = difference.novel_nodes().map_err(Into::into);
                // Boxed because the upload future carries the full
                // stream type and would otherwise trip the
                // `clippy::large_futures` lint when embedded in the
                // surrounding `perform` future.
                let target = remote.archive().index();
                let upload = target.upload(novelty, index).perform(env);
                Box::pin(upload).await?;

                // Publish revision to remote
                upstream.publish(revision.clone()).perform(env).await?;

                // Update local upstream state
                if let Some(upstream) = branch.upstream() {
                    branch
                        .upstream
                        .publish(upstream.with_tree(revision.tree.clone()))
                        .perform(env)
                        .await
                        .map_err(|e| RepositoryError::PushFailed {
                            cause: format!("Failed to update upstream state: {:?}", e),
                        })?;
                }

                Ok(Some(revision))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use crate::repository::branch::UpstreamState;
    use crate::repository::tree::TreeReference;
    use anyhow::Result;

    use dialog_artifacts::{Artifact, Instruction, Value};
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_pushes_to_local_upstream() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let _main = repo.branch("main").open().perform(&operator).await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: TreeReference::default(),
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
    async fn it_returns_none_when_local_upstream_diverged() -> Result<()> {
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
                tree: TreeReference::default(),
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
    async fn it_has_no_upstream_by_default() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("feature").open().perform(&operator).await?;

        assert!(branch.upstream().is_none());

        Ok(())
    }
}
