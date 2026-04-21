use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::{Identify, OperatorExt};
use dialog_effects::memory::{Publish, Resolve};
use dialog_prolly_tree::{EMPT_TREE_HASH, Tree};

use crate::{
    Branch, Index, NetworkedIndex, PullError, RemoteRepository, RemoteSite,
    RepositoryArchiveExt as _, RepositoryMemoryExt, Revision, TreeReference, Upstream,
};

/// Command struct for pulling from upstream (auto-dispatches local/remote).
pub struct Pull<'a> {
    branch: &'a Branch,
}

impl<'a> Pull<'a> {
    fn new(branch: &'a Branch) -> Self {
        Self { branch }
    }
}

impl Branch {
    /// Pull from the configured upstream.
    pub fn pull(&self) -> Pull<'_> {
        Pull::new(self)
    }
}

impl Pull<'_> {
    /// Execute the pull operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, PullError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let upstream = branch
            .upstream()
            .ok_or_else(|| PullError::BranchHasNoUpstream {
                branch: branch.name().to_string(),
            })?;

        // Resolve the upstream's current revision and, when the
        // upstream is remote, keep a handle so the merge can fall back
        // to the remote archive for blocks that aren't local.
        let (upstream_revision, remote) = match upstream {
            Upstream::Local { branch: id, .. } => {
                let upstream_branch = branch.subject().branch(id).load().perform(env).await?;
                (upstream_branch.revision(), None)
            }
            Upstream::Remote {
                remote: name,
                branch: branch_name,
                ..
            } => {
                let remote = branch.subject().remote(name).load().perform(env).await?;
                let upstream = remote.branch(branch_name).open().perform(env).await?;
                (upstream.fetch().perform(env).await?, Some(remote))
            }
        };

        // Upstream has never received a revision yet — nothing to
        // merge in, so the pull is a no-op.
        let Some(upstream_revision) = upstream_revision else {
            return Ok(None);
        };

        // `base` is the upstream tree at our last sync point (the
        // divergence marker). If it equals the upstream's current
        // tree, the upstream hasn't moved and there's nothing to pull.
        let base = branch
            .upstream()
            .map(|u| u.tree().clone())
            .unwrap_or_default();

        if base == upstream_revision.tree {
            return Ok(None);
        }

        let local_revision = branch.revision();
        let local_tree_hash = local_revision
            .as_ref()
            .map(|revision| *revision.tree.hash())
            .unwrap_or(EMPT_TREE_HASH);

        // `NetworkedIndex` reads from the local archive first and,
        // when the upstream is remote, falls back to the remote
        // archive for blocks that haven't been replicated. With
        // `remote: None` it degrades to a plain local index.
        let store = NetworkedIndex::new(env, branch.archive().index(), remote);

        // Load the three trees: last-sync base, local current, and the
        // upstream revision we're merging in.
        let base: Index = Tree::from_hash(base.hash(), &store).await?;
        let local: Index = Tree::from_hash(&local_tree_hash, &store).await?;
        let mut merged: Index = Tree::from_hash(upstream_revision.tree.hash(), &store).await?;

        // Replay local changes (base → local) on top of the upstream
        // tree to produce the merged tree.
        let read_store = store.clone();
        let local_changes = base.differentiate(&local, &read_store, &read_store);
        let mut write_store = store;
        Box::pin(merged.integrate(local_changes, &mut write_store)).await?;

        let merged_tree = TreeReference::from(merged.hash().copied().unwrap_or(EMPT_TREE_HASH));

        let new_revision = match local_revision {
            // Merging produced the upstream tree verbatim
            // (fast-forward): adopt the upstream revision — there's
            // nothing novel to attribute.
            _ if merged_tree == upstream_revision.tree => upstream_revision.clone(),
            // Branch has no prior revision; adopt the upstream
            // revision directly (its identity still applies).
            None => upstream_revision.clone(),
            // Real three-way merge: mint a revision attributed to the
            // current authority combining both sides.
            Some(local) => {
                let authority = Identify.perform(env).await?;
                local.merge(
                    &upstream_revision,
                    merged_tree,
                    authority.did(),
                    authority.profile().clone(),
                )
            }
        };

        // Publish the merged revision as the branch's new head.
        branch
            .revision
            .publish(new_revision.clone())
            .perform(env)
            .await?;

        // Advance the recorded sync base to the upstream's tree we
        // just merged in, so the next pull/push uses it as the
        // divergence marker.
        if let Some(upstream) = branch.upstream() {
            branch
                .upstream
                .publish(upstream.with_tree(upstream_revision.tree.clone()))
                .perform(env)
                .await?;
        }

        Ok(Some(new_revision))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use anyhow::Result;

    use dialog_artifacts::{Artifact, Instruction, Value};
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_pulls_from_local_upstream_no_changes() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:seed".parse()?,
            is: Value::String("Seed".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let pulled = feature.pull().perform(&operator).await?;
        assert!(pulled.is_some());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_upstream_changes_without_local_changes() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: Value::String("Main data".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;
        let main_revision = main.revision().expect("main should have a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let pulled = feature.pull().perform(&operator).await?;
        assert!(pulled.is_some());
        let feature_rev = feature
            .revision()
            .expect("feature should have a revision after pull");
        assert_eq!(feature_rev.tree, main_revision.tree);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_and_merges_with_both_sides_changed() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: Value::String("Main data".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;
        let main_revision = main.revision().expect("main should have a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: Value::String("feature@test.com".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let pulled = feature.pull().perform(&operator).await?;
        assert!(pulled.is_some());
        let feature_rev = feature
            .revision()
            .expect("feature should have a revision after merge");
        assert_ne!(feature_rev.tree, main_revision.tree);
        Ok(())
    }
}
