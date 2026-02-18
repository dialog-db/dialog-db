use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_prolly_tree::{EMPT_TREE_HASH, Tree};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::{Branch, Index};
use crate::repository::archive::ContentAddressedStore;
use crate::repository::node_reference::NodeReference;
use crate::repository::revision::Revision;
use crate::DialogArtifactsError;

/// Command struct for pulling from a local upstream revision.
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
        env: Arc<Mutex<Env>>,
    ) -> Result<(Branch, Option<Revision>), DialogArtifactsError>
    where
        Env: Provider<archive_fx::Get>
            + Provider<archive_fx::Put>
            + Provider<memory_fx::Resolve>
            + Provider<memory_fx::Publish>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let upstream_revision = self.upstream_revision;
        let branch_base = branch.base();
        let branch_revision = branch.revision();

        // If upstream revision's tree matches our base, nothing to do
        if branch_base == *upstream_revision.tree() {
            return Ok((branch, None));
        }

        let archive = ContentAddressedStore::new(
            env.clone(),
            branch.archive().index(),
        );

        // Load upstream tree
        let mut target: Index<Env> =
            Tree::from_hash(upstream_revision.tree.hash(), archive.clone())
                .await
                .map_err(|e| {
                    DialogArtifactsError::Storage(format!("Failed to load upstream tree: {:?}", e))
                })?;

        // Load base tree (state at last sync)
        let base: Index<Env> = Tree::from_hash(branch_base.hash(), archive.clone())
            .await
            .map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to load base tree: {:?}", e))
            })?;

        // Load current tree
        let current: Index<Env> = Tree::from_hash(branch_revision.tree.hash(), archive)
            .await
            .map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to load current tree: {:?}", e))
            })?;

        // Compute local changes: what operations transform base into current
        let changes = base.differentiate(&current);

        // Integrate local changes into upstream tree
        target.integrate(changes).await.map_err(|e| {
            DialogArtifactsError::Storage(format!("Failed to integrate changes: {:?}", e))
        })?;

        // Get the hash of the integrated tree
        let hash = target.hash().cloned().unwrap_or(EMPT_TREE_HASH);

        // Check if integration actually changed the tree
        if &hash == upstream_revision.tree.hash() {
            // No local changes were integrated — adopt upstream directly
            let mut env = env.lock().await;
            let branch = branch
                .reset(upstream_revision.clone())
                .perform(&mut *env)
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
            let mut env = env.lock().await;
            let branch = branch
                .advance(new_revision.clone(), upstream_revision.tree.clone())
                .perform(&mut *env)
                .await
                .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

            Ok((branch, Some(new_revision)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{test_issuer, test_subject};
    use super::super::Branch;
    use crate::artifacts::{Artifact, Instruction};
    use crate::repository::node_reference::NodeReference;
    use crate::repository::revision::Revision;
    use futures_util::stream;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[dialog_common::test]
    async fn it_pulls_from_local_upstream_no_changes() -> anyhow::Result<()> {
        let env = Arc::new(Mutex::new(dialog_storage::provider::Volatile::new()));

        let issuer = test_issuer().await;

        let branch = Branch::open("feature", issuer.clone(), test_subject())
            .perform(&mut *env.lock().await)
            .await?;

        // Pull with upstream at same base — should be a no-op
        let upstream_revision = Revision::new(issuer.did());

        let (branch, pulled) = branch.pull(upstream_revision).perform(env.clone()).await?;

        assert!(pulled.is_none(), "No changes expected when base matches");
        assert_eq!(branch.revision().tree(), &NodeReference::default());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_upstream_changes_without_local_changes() -> anyhow::Result<()> {
        let env = Arc::new(Mutex::new(dialog_storage::provider::Volatile::new()));

        let issuer = test_issuer().await;

        // Create "main" branch and commit something
        let main = Branch::open("main", issuer.clone(), test_subject())
            .perform(&mut *env.lock().await)
            .await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: crate::Value::String("Main data".to_string()),
            cause: None,
        };
        let (main, _) = main
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(env.clone())
            .await?;

        let main_revision = main.revision().clone();

        // Create "feature" branch (empty, base = empty tree)
        let feature = Branch::open("feature", issuer, test_subject())
            .perform(&mut *env.lock().await)
            .await?;

        // Pull main's revision into feature (no local changes)
        let (feature, pulled) = feature
            .pull(main_revision.clone())
            .perform(env.clone())
            .await?;

        assert!(pulled.is_some());
        // Since feature had no local changes, it should adopt main's revision
        assert_eq!(feature.revision().tree(), main_revision.tree());

        Ok(())
    }
}
