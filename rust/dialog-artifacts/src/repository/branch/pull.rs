use dialog_capability::{Provider, credential::Authorize};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_prolly_tree::{EMPT_TREE_HASH, Tree};
use dialog_s3_credentials::s3::site::{S3Access, S3Invocation};
use std::collections::HashSet;

use super::Branch;
use super::Index;
use super::state::UpstreamState;
use crate::DialogArtifactsError;
use crate::repository::archive::ContentAddressedStore;
use crate::repository::archive::fallback::FallbackStore;
use crate::repository::node_reference::NodeReference;
use crate::repository::remote::{RemoteBranch, SiteName};
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
            + Provider<Authorize<archive_fx::Get, S3Access>>
            + Provider<S3Invocation<archive_fx::Get>>
            + Provider<Authorize<memory_fx::Resolve, S3Access>>
            + Provider<S3Invocation<memory_fx::Resolve>>
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

                pull_local(self.branch, upstream_branch.revision(), env).await
            }
            UpstreamState::Remote {
                name,
                branch: branch_name,
                subject,
                ..
            } => pull_remote(self.branch, &name, &branch_name, &subject, env).await,
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

    let mut target: Index = Tree::from_hash(upstream_revision.tree.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load upstream tree: {:?}", e))
        })?;

    let base: Index = Tree::from_hash(branch_base.hash(), &store)
        .await
        .map_err(|e| DialogArtifactsError::Storage(format!("Failed to load base tree: {:?}", e)))?;

    let current: Index = Tree::from_hash(branch_revision.tree.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load current tree: {:?}", e))
        })?;

    let diff_store = store.clone();
    let changes = base.differentiate(&current, &diff_store, &diff_store);

    Box::pin(target.integrate(changes, &mut store))
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to integrate changes: {:?}", e))
        })?;

    let hash = target.hash().cloned().unwrap_or(EMPT_TREE_HASH);

    if &hash == upstream_revision.tree.hash() {
        branch
            .revision
            .publish(upstream_revision.clone(), env)
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
        let issuer_did = branch.issuer().did();
        let new_revision = Revision {
            issuer: issuer_did,
            tree: NodeReference::from(hash),
            cause: HashSet::from([upstream_revision.tree().clone()]),
            period: upstream_revision.period.max(branch_revision.period) + 1,
            moment: 0,
        };

        branch
            .revision
            .publish(new_revision.clone(), env)
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

/// Perform a three-way merge pulling from a remote upstream.
///
/// Uses `FallbackStore` so that reads can fall through to the remote
/// when the local archive doesn't have the blocks. Looks up credentials
/// from the persisted `RemoteSite` configuration.
async fn pull_remote<Env>(
    branch: &Branch,
    remote: &SiteName,
    upstream_branch_name: &super::state::BranchName,
    upstream_subject: &dialog_capability::Did,
    env: &Env,
) -> Result<Option<Revision>, DialogArtifactsError>
where
    Env: Provider<archive_fx::Get>
        + Provider<archive_fx::Put>
        + Provider<memory_fx::Resolve>
        + Provider<memory_fx::Publish>
        + Provider<Authorize<archive_fx::Get, S3Access>>
        + Provider<S3Invocation<archive_fx::Get>>
        + Provider<Authorize<memory_fx::Resolve, S3Access>>
        + Provider<S3Invocation<memory_fx::Resolve>>
        + ConditionalSync
        + 'static,
{
    let remote_site = branch
        .load_remote(remote.clone())
        .perform(env)
        .await
        .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

    let remote_branch = RemoteBranch::new(
        remote_site.name().clone(),
        remote_site.site().clone(),
        upstream_subject.clone(),
        upstream_branch_name.clone(),
    );

    let upstream_revision = remote_branch
        .resolve(env)
        .await
        .map_err(|e| DialogArtifactsError::Storage(format!("Failed to resolve remote: {:?}", e)))?;

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

    let mut store = FallbackStore::new(env, branch.archive().index(), &remote_branch);

    let mut target: Index = Tree::from_hash(upstream_revision.tree.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load upstream tree: {:?}", e))
        })?;

    let base: Index = Tree::from_hash(branch_base.hash(), &store)
        .await
        .map_err(|e| DialogArtifactsError::Storage(format!("Failed to load base tree: {:?}", e)))?;

    let current: Index = Tree::from_hash(branch_revision.tree.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load current tree: {:?}", e))
        })?;

    let diff_store = store.clone();
    let changes = base.differentiate(&current, &diff_store, &diff_store);

    Box::pin(target.integrate(changes, &mut store))
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to integrate changes: {:?}", e))
        })?;

    let hash = target.hash().cloned().unwrap_or(EMPT_TREE_HASH);

    if &hash == upstream_revision.tree.hash() {
        branch
            .revision
            .publish(upstream_revision.clone(), env)
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
        let issuer_did = branch.issuer().did();
        let new_revision = Revision {
            issuer: issuer_did,
            tree: NodeReference::from(hash),
            cause: HashSet::from([upstream_revision.tree().clone()]),
            period: upstream_revision.period.max(branch_revision.period) + 1,
            moment: 0,
        };

        branch
            .revision
            .publish(new_revision.clone(), env)
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
    use super::super::tests::{test_issuer, test_subject};
    use crate::artifacts::{Artifact, Instruction};
    use crate::repository::Repository;
    use crate::repository::node_reference::NodeReference;
    use crate::repository::revision::Revision;
    use dialog_storage::provider::Volatile;
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_pulls_from_local_upstream_no_changes() -> anyhow::Result<()> {
        let env = Volatile::new();

        let repo = Repository::new(test_issuer().await, test_subject());

        let branch = repo.open_branch("feature").perform(&env).await?;

        let upstream_revision = Revision::new(repo.issuer().did());

        let pulled = branch.pull(upstream_revision).perform(&env).await?;

        assert!(pulled.is_none(), "No changes expected when base matches");
        assert_eq!(branch.revision().tree(), &NodeReference::default());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_upstream_changes_without_local_changes() -> anyhow::Result<()> {
        let env = Volatile::new();

        let repo = Repository::new(test_issuer().await, test_subject());

        let main = repo.open_branch("main").perform(&env).await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: crate::Value::String("Main data".to_string()),
            cause: None,
        };
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(&env)
            .await?;

        let main_revision = main.revision().clone();

        let feature = repo.open_branch("feature").perform(&env).await?;

        let pulled = feature.pull(main_revision.clone()).perform(&env).await?;

        assert!(pulled.is_some());
        assert_eq!(feature.revision().tree(), main_revision.tree());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_and_merges_with_both_sides_changed() -> anyhow::Result<()> {
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

        let main_revision = main.revision().clone();

        let feature = repo.open_branch("feature").perform(&env).await?;

        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: crate::Value::String("feature@test.com".to_string()),
                cause: None,
            })]))
            .perform(&env)
            .await?;

        let pulled = feature.pull(main_revision.clone()).perform(&env).await?;

        assert!(pulled.is_some());
        let merged_tree = feature.revision().tree().clone();
        assert_ne!(&merged_tree, main_revision.tree());

        Ok(())
    }
}
