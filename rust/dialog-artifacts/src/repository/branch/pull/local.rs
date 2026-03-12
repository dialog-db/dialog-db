use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_prolly_tree::{EMPT_TREE_HASH, Tree};
use std::collections::HashSet;

use crate::DialogArtifactsError;
use crate::repository::archive::ContentAddressedStore;
use crate::repository::branch::{Branch, Index};
use crate::repository::node_reference::NodeReference;
use crate::repository::revision::Revision;

/// Perform a three-way merge from a local upstream revision.
///
/// Returns the updated branch and the new revision (or None if no changes).
pub(crate) async fn pull<Env>(
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

    // If upstream revision's tree matches our base, nothing to do
    if branch_base == *upstream_revision.tree() {
        return Ok((branch, None));
    }

    let mut store = ContentAddressedStore::new(env, branch.archive().index());

    // Load upstream tree
    let mut target: Index = Tree::from_hash(upstream_revision.tree.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load upstream tree: {:?}", e))
        })?;

    // Load base tree (state at last sync)
    let base: Index = Tree::from_hash(branch_base.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load base tree: {:?}", e))
        })?;

    // Load current tree
    let current: Index = Tree::from_hash(branch_revision.tree.hash(), &store)
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to load current tree: {:?}", e))
        })?;

    // Compute local changes: what operations transform base into current.
    let diff_store = store.clone();
    let changes = base.differentiate(&current, &diff_store, &diff_store);

    // Integrate local changes into upstream tree
    Box::pin(target.integrate(changes, &mut store))
        .await
        .map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to integrate changes: {:?}", e))
        })?;

    // Get the hash of the integrated tree
    let hash = target.hash().cloned().unwrap_or(EMPT_TREE_HASH);

    // Check if integration actually changed the tree
    if &hash == upstream_revision.tree.hash() {
        // No local changes were integrated — adopt upstream directly
        let branch = branch
            .reset(upstream_revision.clone())
            .perform(env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

        Ok((branch, Some(upstream_revision)))
    } else {
        // Create new revision with integrated changes
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

        // Advance branch to merged revision with upstream's tree as base
        let branch = branch
            .advance(new_revision.clone(), upstream_revision.tree.clone())
            .perform(env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

        Ok((branch, Some(new_revision)))
    }
}
