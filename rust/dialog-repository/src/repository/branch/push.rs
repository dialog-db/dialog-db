use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::{Publish, Resolve};
use dialog_prolly_tree::{Tree, TreeDifference};
use futures_util::TryStreamExt;

use crate::{
    Branch, Index, LocalIndex, PushError, RemoteSite, RepositoryArchiveExt as _,
    RepositoryMemoryExt, Revision, Upstream,
};

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
    /// Push is fast-forward only:
    ///
    /// - `Ok(Some(revision))` — pushed; upstream now at `revision`.
    /// - `Ok(None)` — nothing to push (branch has no local revision).
    /// - `Err(PushError::NonFastForward)` — upstream has moved since
    ///   the last sync; pull to integrate before pushing again.
    ///
    /// For remote upstream, novel tree blocks are uploaded before the
    /// revision is published.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, PushError>
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
        let upstream_state = branch
            .upstream()
            .ok_or_else(|| PushError::BranchHasNoUpstream {
                branch: branch.name().to_string(),
            })?;

        let revision = match branch.revision() {
            Some(revision) => revision,
            None => return Ok(None),
        };
        let base = upstream_state.tree().clone();

        match &upstream_state {
            Upstream::Local {
                branch: upstream_name,
                ..
            } => {
                let target = branch
                    .subject()
                    .branch(upstream_name.clone())
                    .open()
                    .perform(env)
                    .await?;

                let current = target.revision().map(|r| r.tree).unwrap_or_default();
                if current != base {
                    return Err(PushError::NonFastForward {
                        branch: branch.name().to_string(),
                        expected: base,
                        actual: current,
                    });
                }

                target.reset(revision.clone()).perform(env).await?;
            }
            Upstream::Remote {
                remote: remote_name,
                branch: upstream_branch_name,
                ..
            } => {
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

                // Refresh the cache from the remote so our divergence
                // check sees the latest upstream tree, not whatever
                // was in our last snapshot.
                upstream.fetch().perform(env).await?;

                let current = upstream.revision().map(|r| r.tree).unwrap_or_default();
                if current != base {
                    return Err(PushError::NonFastForward {
                        branch: branch.name().to_string(),
                        expected: base,
                        actual: current,
                    });
                }

                // Upload tree nodes present in our current tree but not
                // in the base, so the remote can hydrate the new tree
                // before we publish the revision pointing at it.
                let index = branch.archive().index();
                let store = LocalIndex::new(env, index.clone());
                let base_tree: Index = Tree::from_hash(base.hash(), &store).await?;
                let current_tree: Index = Tree::from_hash(revision.tree.hash(), &store).await?;
                let difference =
                    TreeDifference::compute(&base_tree, &current_tree, &store, &store).await?;
                let novelty = difference.novel_nodes().map_err(Into::into);
                let remote_archive = remote.archive();
                let remote_index = remote_archive.index();
                let upload = remote_index.upload(novelty, index).perform(env);
                // Boxed because the upload future carries the full
                // stream type and produces large futures.
                Box::pin(upload).await?;

                upstream.publish(revision.clone()).perform(env).await?;
            }
        }

        // Advance our recorded sync point to the just-pushed tree.
        branch
            .upstream
            .publish(upstream_state.with_tree(revision.tree.clone()))
            .perform(env)
            .await?;

        Ok(Some(revision))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::PushError;
    use crate::helpers::{test_operator_with_profile, test_repo};
    use anyhow::Result;

    use dialog_artifacts::{Artifact, Instruction, Value};
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_pushes_to_local_upstream() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

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
    async fn it_errors_non_fast_forward_on_local_upstream_diverged() -> Result<()> {
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
        feature.set_upstream(&main).perform(&operator).await?;

        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: Value::String("feature@example.com".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let result = feature.push().perform(&operator).await;
        assert!(
            matches!(result, Err(PushError::NonFastForward { .. })),
            "Push should fail with NonFastForward when diverged, got: {result:?}"
        );

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

    #[dialog_common::test]
    async fn it_errors_pushing_branch_without_upstream() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("feature").open().perform(&operator).await?;

        let result = branch.push().perform(&operator).await;
        assert!(
            matches!(result, Err(PushError::BranchHasNoUpstream { .. })),
            "Push should fail with BranchHasNoUpstream, got: {result:?}"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_none_when_pushing_empty_branch() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let result = feature.push().perform(&operator).await?;
        assert!(result.is_none(), "Push with no revision should return None");

        Ok(())
    }
}
