use dialog_capability::fork::Fork;
use dialog_capability::{Policy, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::authority;
use dialog_effects::memory as memory_fx;
use dialog_prolly_tree::{EMPT_TREE_HASH, Tree};
use dialog_remote_s3::S3;
use futures_util::StreamExt;
use std::collections::HashSet;

use super::Branch;
use super::Index;
use super::state::UpstreamState;
use crate::DialogArtifactsError;
use crate::repository::archive::local::LocalIndex;
use crate::repository::archive::networked::NetworkedIndex;
use crate::repository::node_reference::NodeReference;
use crate::repository::revision::Revision;

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
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, DialogArtifactsError>
    where
        Env: Provider<archive_fx::Get>
            + Provider<archive_fx::Put>
            + Provider<memory_fx::Resolve>
            + Provider<memory_fx::Publish>
            + Provider<authority::Identify>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let upstream_revision = self.upstream_revision;

        let branch_base = branch
            .upstream()
            .map(|u| u.tree().clone())
            .unwrap_or_default();

        if branch_base == *upstream_revision.tree() {
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
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, DialogArtifactsError>
    where
        Env: Provider<archive_fx::Get>
            + Provider<archive_fx::Put>
            + Provider<memory_fx::Resolve>
            + Provider<memory_fx::Publish>
            + Provider<authority::Identify>
            + Provider<Fork<S3, archive_fx::Get>>
            + Provider<Fork<S3, memory_fx::Resolve>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, archive_fx::Get>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let upstream = branch.upstream().ok_or_else(|| {
            DialogArtifactsError::Storage(format!("Branch {} has no upstream", branch.name()))
        })?;

        match upstream {
            UpstreamState::Local { branch: id, .. } => {
                let upstream_branch = branch
                    .load_branch(id)
                    .perform(env)
                    .await
                    .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

                match upstream_branch.revision() {
                    Some(rev) => branch.merge(rev).perform(env).await,
                    None => Ok(None),
                }
            }
            UpstreamState::Remote {
                name,
                branch: branch_name,
                ..
            } => {
                let remote_repo = branch
                    .remote(name)
                    .load()
                    .perform(env)
                    .await
                    .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

                let remote_branch = remote_repo
                    .branch(branch_name)
                    .open()
                    .perform(env)
                    .await
                    .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

                let upstream_revision = remote_branch.fetch().perform(env).await.map_err(|e| {
                    DialogArtifactsError::Storage(format!("Failed to fetch remote: {:?}", e))
                })?;

                let upstream_revision = match upstream_revision {
                    Some(rev) => rev,
                    None => return Ok(None),
                };

                let branch_base = branch
                    .upstream()
                    .map(|u| u.tree().clone())
                    .unwrap_or_default();

                if branch_base == *upstream_revision.tree() {
                    return Ok(None);
                }

                let store =
                    NetworkedIndex::new(env, branch.archive().index(), Some(remote_repo.clone()));

                let result = three_way_merge(branch, upstream_revision, store.clone(), env).await?;

                // Replicate all tree blocks to local storage
                if let Some(rev) = branch.revision() {
                    let target: Index = Tree::from_hash(rev.tree().hash(), &store).await?;
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
    env: &(
         impl Provider<memory_fx::Resolve>
         + Provider<memory_fx::Publish>
         + Provider<authority::Identify>
         + ConditionalSync
     ),
) -> Result<Option<Revision>, DialogArtifactsError>
where
    S: dialog_storage::ContentAddressedStorage<
            Hash = dialog_storage::Blake3Hash,
            Error = dialog_storage::DialogStorageError,
        > + dialog_storage::Encoder<
            Hash = dialog_storage::Blake3Hash,
            Error = dialog_storage::DialogStorageError,
        > + Clone
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
        .map(|rev| *rev.tree().hash())
        .unwrap_or(EMPT_TREE_HASH);
    let current: Index = Tree::from_hash(&current_tree_hash, &store).await?;

    let diff_store = store.clone();
    let changes = base.differentiate(&current, &diff_store, &diff_store);
    let mut write_store = store;
    Box::pin(target.integrate(changes, &mut write_store)).await?;

    let hash = target.hash().cloned().unwrap_or(EMPT_TREE_HASH);

    let new_revision = if &hash == upstream_revision.tree.hash() {
        upstream_revision.clone()
    } else {
        let identify_cap = Subject::from(branch.subject().clone()).invoke(authority::Identify);
        let auth = identify_cap
            .perform(env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("Identify failed: {}", e)))?;

        let branch_period = branch_revision.as_ref().map(|r| r.period).unwrap_or(0);

        Revision {
            subject: auth.subject().clone(),
            issuer: authority::Operator::of(&auth).operator.clone(),
            authority: authority::Profile::of(&auth).profile.clone(),
            tree: NodeReference::from(hash),
            cause: HashSet::from([upstream_revision.tree().clone()]),
            period: upstream_revision.period.max(branch_period) + 1,
            moment: 0,
        }
    };

    branch
        .revision
        .publish(Some(new_revision.clone()), env)
        .await
        .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

    if let Some(upstream) = branch.upstream() {
        branch
            .upstream
            .publish(
                Some(upstream.with_tree(upstream_revision.tree.clone())),
                env,
            )
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;
    }

    Ok(Some(new_revision))
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use crate::{Artifact, Instruction};
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_pulls_from_local_upstream_no_changes() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:seed".parse()?,
            is: crate::Value::String("Seed".to_string()),
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
    async fn it_pulls_upstream_changes_without_local_changes() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

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
        let pulled = feature
            .merge(main_revision.clone())
            .perform(&operator)
            .await?;
        assert!(pulled.is_some());
        let feature_rev = feature
            .revision()
            .expect("feature should have a revision after pull");
        assert_eq!(feature_rev.tree(), main_revision.tree());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_and_merges_with_both_sides_changed() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

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
        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: crate::Value::String("feature@test.com".to_string()),
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
        assert_ne!(feature_rev.tree(), main_revision.tree());
        Ok(())
    }
}
