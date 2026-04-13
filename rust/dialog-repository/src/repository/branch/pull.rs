use dialog_capability::fork::Fork;
use dialog_capability::{Policy, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::authority;
use dialog_effects::memory as memory_fx;
use dialog_prolly_tree::{EMPT_TREE_HASH, Tree};
use dialog_remote_s3::S3;
use std::collections::HashSet;

use super::Branch;
use super::Index;
use super::state::UpstreamState;
use crate::DialogArtifactsError;
use crate::repository::archive::ContentAddressedStore;
use crate::repository::archive::fallback::FallbackStore;
use crate::repository::node_reference::NodeReference;
use crate::repository::remote::RemoteName;
use crate::repository::revision::Revision;

/// Command struct for pulling from a local upstream revision (legacy API).
///
/// This performs a three-way merge between the current branch, the base
/// (last sync point), and the upstream revision.
pub struct PullLocal<'a> {
    branch: &'a Branch,
    upstream_revision: Revision,
}

impl<'a> PullLocal<'a> {
    pub(super) fn new(branch: &'a Branch, upstream_revision: Revision) -> Self {
        Self {
            branch,
            upstream_revision,
        }
    }
}

impl PullLocal<'_> {
    /// Execute the pull operation, returning the new revision (or None if
    /// no changes).
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
        pull_local(self.branch, self.upstream_revision, env).await
    }
}

/// Command struct for pulling from upstream (auto-dispatches local/remote).
///
/// Borrows `&Branch`. Reads the branch's upstream to determine whether
/// to pull from a local or remote upstream.
pub struct Pull<'a> {
    branch: &'a Branch,
}

impl<'a> Pull<'a> {
    pub(super) fn new(branch: &'a Branch) -> Self {
        Self { branch }
    }
}

impl Pull<'_> {
    /// Execute the pull operation.
    ///
    /// For local upstreams, loads the upstream branch revision and performs
    /// a three-way merge. For remote upstreams, resolves the remote revision
    /// and merges using FallbackStore.
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
        let upstream = self.branch.upstream().ok_or_else(|| {
            DialogArtifactsError::Storage(format!("Branch {} has no upstream", self.branch.name()))
        })?;

        match upstream {
            UpstreamState::Local { branch: id, .. } => {
                let upstream_branch = self
                    .branch
                    .load_branch(id)
                    .perform(env)
                    .await
                    .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

                match upstream_branch.revision() {
                    Some(rev) => pull_local(self.branch, rev, env).await,
                    None => Ok(None),
                }
            }
            UpstreamState::Remote {
                name,
                branch: branch_name,
                ..
            } => pull_remote(self.branch, &name, &branch_name, env).await,
        }
    }
}

/// Perform a three-way merge from a local upstream revision.
///
/// Returns the new revision (or None if no changes).
async fn pull_local<Env>(
    branch: &Branch,
    upstream_revision: Revision,
    env: &Env,
) -> Result<Option<Revision>, DialogArtifactsError>
where
    Env: Provider<archive_fx::Get>
        + Provider<archive_fx::Put>
        + Provider<memory_fx::Resolve>
        + Provider<memory_fx::Publish>
        + Provider<authority::Identify>
        + ConditionalSync
        + 'static,
{
    let branch_base = branch
        .upstream()
        .map(|u| u.tree().clone())
        .unwrap_or_default();
    let branch_revision = branch.revision();

    if branch_base == *upstream_revision.tree() {
        return Ok(None);
    }

    let mut store = ContentAddressedStore::new(env, branch.archive().index());

    let mut target: Index = Tree::from_hash(upstream_revision.tree.hash(), &store).await?;

    let base: Index = Tree::from_hash(branch_base.hash(), &store).await?;

    let current_tree_hash = branch_revision
        .as_ref()
        .map(|rev| *rev.tree().hash())
        .unwrap_or(EMPT_TREE_HASH);

    let current: Index = Tree::from_hash(&current_tree_hash, &store).await?;

    let diff_store = store.clone();
    let changes = base.differentiate(&current, &diff_store, &diff_store);

    Box::pin(target.integrate(changes, &mut store)).await?;

    let hash = target.hash().cloned().unwrap_or(EMPT_TREE_HASH);

    if &hash == upstream_revision.tree.hash() {
        branch
            .revision
            .publish(Some(upstream_revision.clone()), env)
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

        Ok(Some(upstream_revision))
    } else {
        let identify_cap = Subject::from(branch.subject().clone()).invoke(authority::Identify);
        let auth = identify_cap
            .perform(env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("Identify failed: {}", e)))?;

        let branch_period = branch_revision.as_ref().map(|r| r.period).unwrap_or(0);

        let new_revision = Revision {
            subject: auth.subject().clone(),
            issuer: authority::Operator::of(&auth).operator.clone(),
            authority: authority::Profile::of(&auth).profile.clone(),
            tree: NodeReference::from(hash),
            cause: HashSet::from([upstream_revision.tree().clone()]),
            period: upstream_revision.period.max(branch_period) + 1,
            moment: 0,
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
}

/// Pull from a remote upstream: fetch, merge, update local state.
async fn pull_remote<Env>(
    branch: &Branch,
    remote: &RemoteName,
    upstream_branch_name: &super::state::BranchName,
    env: &Env,
) -> Result<Option<Revision>, DialogArtifactsError>
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
    // Load remote repository and fetch latest revision
    let remote_repo = branch
        .remote(remote.clone())
        .load()
        .perform(env)
        .await
        .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

    let remote_branch = remote_repo
        .branch(upstream_branch_name.clone())
        .open()
        .perform(env)
        .await
        .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

    let upstream_revision =
        remote_branch.fetch().perform(env).await.map_err(|e| {
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
    let branch_revision = branch.revision();

    if branch_base == *upstream_revision.tree() {
        return Ok(None);
    }

    // Use FallbackStore for three-way merge (reads fall through to remote)
    let mut store = FallbackStore::new(env, branch.archive().index(), Some(remote_repo.clone()));

    let mut target: Index = Tree::from_hash(upstream_revision.tree.hash(), &store).await?;

    let base: Index = Tree::from_hash(branch_base.hash(), &store).await?;

    let current_tree_hash = branch_revision
        .as_ref()
        .map(|rev| *rev.tree().hash())
        .unwrap_or(EMPT_TREE_HASH);

    let current: Index = Tree::from_hash(&current_tree_hash, &store).await?;

    let diff_store = store.clone();
    let changes = base.differentiate(&current, &diff_store, &diff_store);

    Box::pin(target.integrate(changes, &mut store)).await?;

    // Replicate all tree blocks to local storage
    {
        use futures_util::StreamExt;
        let replicate_store = store.clone();
        let stream = target.stream(&replicate_store);
        tokio::pin!(stream);
        while let Some(result) = stream.next().await {
            result?;
        }
    }

    let hash = target.hash().cloned().unwrap_or(EMPT_TREE_HASH);

    if &hash == upstream_revision.tree.hash() {
        branch
            .revision
            .publish(Some(upstream_revision.clone()), env)
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

        Ok(Some(upstream_revision))
    } else {
        let identify_cap = Subject::from(branch.subject().clone()).invoke(authority::Identify);
        let auth = identify_cap
            .perform(env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("Identify failed: {}", e)))?;

        let branch_period = branch_revision.as_ref().map(|r| r.period).unwrap_or(0);

        let new_revision = Revision {
            subject: auth.subject().clone(),
            issuer: authority::Operator::of(&auth).operator.clone(),
            authority: authority::Profile::of(&auth).profile.clone(),
            tree: NodeReference::from(hash),
            cause: HashSet::from([upstream_revision.tree().clone()]),
            period: upstream_revision.period.max(branch_period) + 1,
            moment: 0,
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

        // Commit something to main so we have a real upstream revision
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

        // Pull with same revision as upstream_revision — no changes since
        // feature's base is the default empty tree.
        let pulled = feature.merge(upstream_revision).perform(&operator).await?;

        // The pull should produce a result since the feature branch is empty
        // and the upstream has data.
        assert!(pulled.is_some());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_upstream_changes_without_local_changes() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: crate::Value::String("Main data".to_string()),
            cause: None,
        };
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
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
