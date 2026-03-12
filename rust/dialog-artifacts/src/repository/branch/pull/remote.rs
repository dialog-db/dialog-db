use dialog_capability::{Did, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_effects::remote::RemoteInvocation;
use dialog_prolly_tree::{EMPT_TREE_HASH, Tree};
use std::collections::HashSet;

use crate::DialogArtifactsError;
use crate::repository::Site;
use crate::repository::archive::fallback::FallbackStore;
use crate::repository::branch::{Branch, Index};
use crate::repository::branch::state::BranchId;
use crate::repository::node_reference::NodeReference;
use crate::repository::remote::RemoteBranch;
use crate::repository::revision::Revision;

/// Perform a three-way merge pulling from a remote upstream.
///
/// Uses `FallbackStore` so that reads can fall through to the remote
/// when the local archive doesn't have the blocks.
pub(super) async fn pull<Env>(
    branch: Branch,
    site: &Site,
    upstream_branch_id: &BranchId,
    upstream_subject: &Did,
    env: &Env,
) -> Result<(Branch, Option<Revision>), DialogArtifactsError>
where
    Env: Provider<archive_fx::Get>
        + Provider<archive_fx::Put>
        + Provider<memory_fx::Resolve>
        + Provider<memory_fx::Publish>
        + Provider<RemoteInvocation<archive_fx::Get, Site>>
        + Provider<RemoteInvocation<memory_fx::Resolve, Site>>
        + ConditionalSync
        + 'static,
{
    let remote_branch = RemoteBranch {
        site: site.clone(),
        subject: upstream_subject.clone(),
        branch: upstream_branch_id.clone(),
    };

    // Resolve upstream revision from remote
    let upstream_revision = remote_branch.resolve(env).await.map_err(|e| {
        DialogArtifactsError::Storage(format!("Failed to resolve remote: {:?}", e))
    })?;

    let upstream_revision = match upstream_revision {
        Some(rev) => rev,
        None => return Ok((branch, None)),
    };

    let branch_base = branch.base();
    let branch_revision = branch.revision();

    // If upstream revision's tree matches our base, nothing to do
    if branch_base == *upstream_revision.tree() {
        return Ok((branch, None));
    }

    // Use FallbackStore for reads (local first, then remote)
    let mut store = FallbackStore::new(env, branch.archive().index(), &remote_branch);

    // Load upstream tree (may need remote reads)
    let mut target: Index = Tree::from_hash(upstream_revision.tree.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load upstream tree: {:?}", e))
        })?;

    // Load base tree (should be local)
    let base: Index = Tree::from_hash(branch_base.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load base tree: {:?}", e))
        })?;

    // Load current tree (should be local)
    let current: Index = Tree::from_hash(branch_revision.tree.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load current tree: {:?}", e))
        })?;

    // Compute local changes
    let diff_store = store.clone();
    let changes = base.differentiate(&current, &diff_store, &diff_store);

    // Integrate local changes into upstream tree
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
