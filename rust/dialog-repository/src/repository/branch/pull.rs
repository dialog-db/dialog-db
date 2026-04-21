use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::{Identify, OperatorExt};
use dialog_effects::memory::{Publish, Resolve};
use dialog_prolly_tree::{EMPT_TREE_HASH, Tree};
use dialog_storage::{Blake3Hash, ContentAddressedStorage, DialogStorageError, Encoder};
use futures_util::StreamExt;

use crate::{
    Branch, Index, LocalIndex, NetworkedIndex, PullError, RemoteSite, RepositoryArchiveExt as _,
    RepositoryMemoryExt, Revision, TreeReference, Upstream,
};

/// Command struct for merging an explicit upstream revision.
///
/// Performs a three-way merge between the current branch, the base
/// (last sync point), and the upstream revision.
pub struct PullLocal<'a> {
    branch: &'a Branch,
    upstream_revision: Revision,
}

impl<'a> PullLocal<'a> {
    fn new(branch: &'a Branch, upstream_revision: Revision) -> Self {
        Self {
            branch,
            upstream_revision,
        }
    }
}

impl PullLocal<'_> {
    /// Execute the merge, returning the new revision (or None if no changes).
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, PullError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let upstream_revision = self.upstream_revision;

        let branch_base = branch
            .upstream()
            .map(|u| u.tree().clone())
            .unwrap_or_default();

        if branch_base == upstream_revision.tree {
            return Ok(None);
        }

        let store = LocalIndex::new(env, branch.archive().index());
        three_way_merge(branch, upstream_revision, store, env).await
    }
}

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

    /// Merge an explicit upstream revision into this branch.
    pub fn merge(&self, upstream_revision: Revision) -> PullLocal<'_> {
        PullLocal::new(self, upstream_revision)
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

        match upstream {
            Upstream::Local { branch: id, .. } => {
                let upstream_branch = branch.subject().branch(id).load().perform(env).await?;

                match upstream_branch.revision() {
                    Some(rev) => branch.merge(rev).perform(env).await,
                    None => Ok(None),
                }
            }
            Upstream::Remote {
                remote: name,
                branch: branch_name,
                ..
            } => {
                let remote_repo = branch.subject().remote(name).load().perform(env).await?;

                let remote_branch = remote_repo.branch(branch_name).open().perform(env).await?;

                let upstream_revision = match remote_branch.fetch().perform(env).await? {
                    Some(rev) => rev,
                    None => return Ok(None),
                };

                let branch_base = branch
                    .upstream()
                    .map(|u| u.tree().clone())
                    .unwrap_or_default();

                if branch_base == upstream_revision.tree {
                    return Ok(None);
                }

                let store =
                    NetworkedIndex::new(env, branch.archive().index(), Some(remote_repo.clone()));

                let result = three_way_merge(branch, upstream_revision, store.clone(), env).await?;

                // Replicate all tree blocks to local storage.
                if let Some(rev) = branch.revision() {
                    let target: Index = Tree::from_hash(rev.tree.hash(), &store).await?;
                    let stream = target.stream(&store);
                    tokio::pin!(stream);
                    while let Some(r) = stream.next().await {
                        r?;
                    }
                }

                Ok(result)
            }
        }
    }
}

/// Three-way merge: applies changes between base and current onto the
/// upstream revision's tree. Publishes the resulting revision and updates
/// the upstream tracking state.
async fn three_way_merge<S>(
    branch: &Branch,
    upstream_revision: Revision,
    store: S,
    env: &(impl Provider<Resolve> + Provider<Publish> + Provider<Identify> + ConditionalSync),
) -> Result<Option<Revision>, PullError>
where
    S: ContentAddressedStorage<Hash = Blake3Hash, Error = DialogStorageError>
        + Encoder<Hash = Blake3Hash, Error = DialogStorageError>
        + Clone
        + ConditionalSync,
{
    let branch_base = branch
        .upstream()
        .map(|u| u.tree().clone())
        .unwrap_or_default();
    let branch_revision = branch.revision();

    let mut target: Index = Tree::from_hash(upstream_revision.tree.hash(), &store).await?;

    let base: Index = Tree::from_hash(branch_base.hash(), &store).await?;
    let current_tree_hash = branch_revision
        .as_ref()
        .map(|rev| *rev.tree.hash())
        .unwrap_or(EMPT_TREE_HASH);
    let current: Index = Tree::from_hash(&current_tree_hash, &store).await?;

    let diff_store = store.clone();
    let changes = base.differentiate(&current, &diff_store, &diff_store);
    let mut write_store = store;
    Box::pin(target.integrate(changes, &mut write_store)).await?;

    let tree = TreeReference::from(target.hash().copied().unwrap_or(EMPT_TREE_HASH));

    let new_revision = match branch_revision {
        // Merging produced the upstream tree verbatim (fast-forward): just
        // adopt the upstream revision — there's nothing novel to attribute.
        _ if tree == upstream_revision.tree => upstream_revision.clone(),
        // Branch has no prior revision to merge against; adopt the upstream
        // revision directly (its identity still applies).
        None => upstream_revision.clone(),
        // Real three-way merge: mint a revision attributed to the current
        // authority combining both sides.
        Some(base) => {
            let authority = Identify.perform(env).await?;
            base.merge(
                &upstream_revision,
                tree,
                authority.did(),
                authority.profile().clone(),
            )
        }
    };

    branch
        .revision
        .publish(new_revision.clone())
        .perform(env)
        .await?;

    if let Some(upstream) = branch.upstream() {
        branch
            .upstream
            .publish(upstream.with_tree(upstream_revision.tree.clone()))
            .perform(env)
            .await?;
    }

    Ok(Some(new_revision))
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
        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:seed".parse()?,
            is: Value::String("Seed".to_string()),
            cause: None,
        };
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(&operator)
            .await?;
        let upstream_revision = main.revision().expect("main should have a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        let pulled = feature.merge(upstream_revision).perform(&operator).await?;
        assert!(pulled.is_some());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_upstream_changes_without_local_changes() -> Result<()> {
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
        let main_revision = main.revision().expect("main should have a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        let pulled = feature
            .merge(main_revision.clone())
            .perform(&operator)
            .await?;
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
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: Value::String("Main data".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        let main_revision = main.revision().expect("main should have a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: Value::String("feature@test.com".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let pulled = feature
            .merge(main_revision.clone())
            .perform(&operator)
            .await?;
        assert!(pulled.is_some());
        let feature_rev = feature
            .revision()
            .expect("feature should have a revision after merge");
        assert_ne!(feature_rev.tree, main_revision.tree);
        Ok(())
    }
}
