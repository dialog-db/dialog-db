use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_effects::remote::RemoteInvocation;
use dialog_prolly_tree::{EMPT_TREE_HASH, Tree};
use crate::environment::Address;
use std::collections::HashSet;

use super::Branch;
use super::Index;
use super::state::UpstreamState;
use crate::DialogArtifactsError;
use crate::repository::archive::ContentAddressedStore;
use crate::repository::archive::fallback::FallbackStore;
use crate::repository::node_reference::NodeReference;
use crate::repository::remote::{RemoteBranch, RemoteSite};
use crate::repository::revision::Revision;

/// Command struct for pulling from a local upstream revision (legacy API).
///
/// This performs a three-way merge between the current branch, the base
/// (last sync point), and the upstream revision.
pub struct PullLocal {
    pub(super) branch: Branch,
    pub(super) upstream_revision: Revision,
}

impl PullLocal {
    /// Execute the pull operation, returning the updated branch and the
    /// new revision (or None if no changes).
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<(Branch, Option<Revision>), DialogArtifactsError>
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
/// Borrows the `Branch` (consuming). Reads `branch.state().upstream` to
/// determine whether to pull from a local or remote upstream.
pub struct Pull {
    pub(super) branch: Branch,
}

impl Pull {
    /// Execute the pull operation.
    ///
    /// For local upstreams, loads the upstream branch revision and performs
    /// a three-way merge. For remote upstreams, resolves the remote revision
    /// and merges using FallbackStore.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<(Branch, Option<Revision>), DialogArtifactsError>
    where
        Env: Provider<archive_fx::Get>
            + Provider<archive_fx::Put>
            + Provider<memory_fx::Resolve>
            + Provider<memory_fx::Publish>
            + Provider<RemoteInvocation<archive_fx::Get, Address>>
            + Provider<RemoteInvocation<memory_fx::Resolve, Address>>
            + ConditionalSync
            + 'static,
    {
        let state = self.branch.state();
        let upstream = state.upstream.as_ref().ok_or_else(|| {
            DialogArtifactsError::Storage(format!(
                "Branch {} has no upstream",
                self.branch.id()
            ))
        })?;

        match upstream.clone() {
            UpstreamState::Local { branch: id } => {
                let upstream_branch = Branch::load(
                    id,
                    self.branch.issuer().clone(),
                    self.branch.subject().clone(),
                )
                .perform(env)
                .await
                .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

                pull_local(self.branch, upstream_branch.revision(), env).await
            }
            UpstreamState::Remote {
                site,
                branch: id,
                subject,
            } => pull_remote(self.branch, &site, &id, &subject, env).await,
        }
    }
}

/// Perform a three-way merge from a local upstream revision.
///
/// Returns the updated branch and the new revision (or None if no changes).
pub(crate) async fn pull_local<Env>(
    branch: Branch,
    upstream_revision: Revision,
    env: &Env,
) -> Result<(Branch, Option<Revision>), DialogArtifactsError>
where
    Env: Provider<archive_fx::Get>
        + Provider<archive_fx::Put>
        + Provider<memory_fx::Resolve>
        + Provider<memory_fx::Publish>
        + ConditionalSync
        + 'static,
{
    let branch_base = branch.base();
    let branch_revision = branch.revision();

    if branch_base == *upstream_revision.tree() {
        return Ok((branch, None));
    }

    let mut store = ContentAddressedStore::new(env, branch.archive().index());

    let mut target: Index = Tree::from_hash(upstream_revision.tree.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load upstream tree: {:?}", e))
        })?;

    let base: Index = Tree::from_hash(branch_base.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load base tree: {:?}", e))
        })?;

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
        let branch = branch
            .reset(upstream_revision.clone())
            .perform(env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

        Ok((branch, Some(upstream_revision)))
    } else {
        let issuer_did = branch.issuer.did();
        let new_revision = Revision {
            issuer: issuer_did,
            tree: NodeReference(hash),
            cause: HashSet::from([upstream_revision.edition().map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to create edition: {:?}", e))
            })?]),
            period: upstream_revision.period.max(branch_revision.period) + 1,
            moment: 0,
        };

        let branch = branch
            .advance(new_revision.clone(), upstream_revision.tree.clone())
            .perform(env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

        Ok((branch, Some(new_revision)))
    }
}

/// Perform a three-way merge pulling from a remote upstream.
///
/// Uses `FallbackStore` so that reads can fall through to the remote
/// when the local archive doesn't have the blocks. Looks up credentials
/// from the persisted `RemoteSite` configuration.
async fn pull_remote<Env>(
    branch: Branch,
    site: &str,
    upstream_branch_id: &super::state::BranchId,
    upstream_subject: &dialog_capability::Did,
    env: &Env,
) -> Result<(Branch, Option<Revision>), DialogArtifactsError>
where
    Env: Provider<archive_fx::Get>
        + Provider<archive_fx::Put>
        + Provider<memory_fx::Resolve>
        + Provider<memory_fx::Publish>
        + Provider<RemoteInvocation<archive_fx::Get, Address>>
        + Provider<RemoteInvocation<memory_fx::Resolve, Address>>
        + ConditionalSync
        + 'static,
{
    let remote_site =
        RemoteSite::load(site, branch.subject(), env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

    let remote_branch = RemoteBranch {
        remote: remote_site.name().to_string(),
        site: remote_site.site().clone(),
        address: remote_site.address().clone(),
        subject: upstream_subject.clone(),
        branch: upstream_branch_id.clone(),
    };

    let upstream_revision = remote_branch.resolve(env).await.map_err(|e| {
        DialogArtifactsError::Storage(format!("Failed to resolve remote: {:?}", e))
    })?;

    let upstream_revision = match upstream_revision {
        Some(rev) => rev,
        None => return Ok((branch, None)),
    };

    let branch_base = branch.base();
    let branch_revision = branch.revision();

    if branch_base == *upstream_revision.tree() {
        return Ok((branch, None));
    }

    let mut store = FallbackStore::new(env, branch.archive().index(), &remote_branch);

    let mut target: Index = Tree::from_hash(upstream_revision.tree.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load upstream tree: {:?}", e))
        })?;

    let base: Index = Tree::from_hash(branch_base.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load base tree: {:?}", e))
        })?;

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
        let branch = branch
            .reset(upstream_revision.clone())
            .perform(env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

        Ok((branch, Some(upstream_revision)))
    } else {
        let issuer_did = branch.issuer.did();
        let new_revision = Revision {
            issuer: issuer_did,
            tree: NodeReference(hash),
            cause: HashSet::from([upstream_revision.edition().map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to create edition: {:?}", e))
            })?]),
            period: upstream_revision.period.max(branch_revision.period) + 1,
            moment: 0,
        };

        let branch = branch
            .advance(new_revision.clone(), upstream_revision.tree.clone())
            .perform(env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

        Ok((branch, Some(new_revision)))
    }
}

#[cfg(test)]
mod tests {
    use super::super::Branch;
    use super::super::tests::{test_issuer, test_subject};
    use crate::artifacts::{Artifact, Instruction};
    use crate::repository::node_reference::NodeReference;
    use crate::repository::revision::Revision;
    use dialog_storage::provider::Volatile;
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_pulls_from_local_upstream_no_changes() -> anyhow::Result<()> {
        let env = Volatile::new();
        let issuer = test_issuer().await;

        let branch = Branch::open("feature", issuer.clone(), test_subject())
            .perform(&env)
            .await?;

        let upstream_revision = Revision::new(issuer.did());

        let (branch, pulled) = branch.pull(upstream_revision).perform(&env).await?;

        assert!(pulled.is_none(), "No changes expected when base matches");
        assert_eq!(branch.revision().tree(), &NodeReference::default());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_upstream_changes_without_local_changes() -> anyhow::Result<()> {
        let env = Volatile::new();
        let issuer = test_issuer().await;

        let main = Branch::open("main", issuer.clone(), test_subject())
            .perform(&env)
            .await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: crate::Value::String("Main data".to_string()),
            cause: None,
        };
        let (main, _) = main
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(&env)
            .await?;

        let main_revision = main.revision().clone();

        let feature = Branch::open("feature", issuer, test_subject())
            .perform(&env)
            .await?;

        let (feature, pulled) = feature
            .pull(main_revision.clone())
            .perform(&env)
            .await?;

        assert!(pulled.is_some());
        assert_eq!(feature.revision().tree(), main_revision.tree());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_and_merges_with_both_sides_changed() -> anyhow::Result<()> {
        let env = Volatile::new();
        let issuer = test_issuer().await;

        let main = Branch::open("main", issuer.clone(), test_subject())
            .perform(&env)
            .await?;

        let (main, _) = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: crate::Value::String("Main data".to_string()),
                cause: None,
            })]))
            .perform(&env)
            .await?;

        let main_revision = main.revision().clone();

        let feature = Branch::open("feature", issuer.clone(), test_subject())
            .perform(&env)
            .await?;

        let (feature, _) = feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: crate::Value::String("feature@test.com".to_string()),
                cause: None,
            })]))
            .perform(&env)
            .await?;

        let (feature, pulled) = feature
            .pull(main_revision.clone())
            .perform(&env)
            .await?;

        assert!(pulled.is_some());
        let merged_tree = feature.revision().tree().clone();
        assert_ne!(&merged_tree, main_revision.tree());

        Ok(())
    }
}
