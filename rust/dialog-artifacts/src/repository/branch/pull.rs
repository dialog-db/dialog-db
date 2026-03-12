use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_effects::remote::RemoteInvocation;

use super::Branch;
use crate::DialogArtifactsError;
use crate::repository::Site;
use crate::repository::revision::Revision;

pub(crate) mod local;
mod remote;

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
        local::pull(self.branch, self.upstream_revision, env).await
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
            + Provider<RemoteInvocation<archive_fx::Get, Site>>
            + Provider<RemoteInvocation<memory_fx::Resolve, Site>>
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
            crate::repository::branch::state::UpstreamState::Local { branch: id } => {
                // Load upstream branch revision, then three-way merge
                let upstream_branch = Branch::load(
                    id,
                    self.branch.issuer().clone(),
                    self.branch.subject().clone(),
                )
                .perform(env)
                .await
                .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

                local::pull(self.branch, upstream_branch.revision(), env).await
            }
            crate::repository::branch::state::UpstreamState::Remote {
                site,
                branch: id,
                subject,
            } => remote::pull(self.branch, &site, &id, &subject, env).await,
        }
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

        // Pull with upstream at same base — should be a no-op
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

        // Create "main" branch and commit something
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

        // Create "feature" branch (empty, base = empty tree)
        let feature = Branch::open("feature", issuer, test_subject())
            .perform(&env)
            .await?;

        // Pull main's revision into feature (no local changes)
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

        // Create "main" branch
        let main = Branch::open("main", issuer.clone(), test_subject())
            .perform(&env)
            .await?;

        // Commit to main
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

        // Create "feature" branch from same starting point
        let feature = Branch::open("feature", issuer.clone(), test_subject())
            .perform(&env)
            .await?;

        // Commit to feature (different entity)
        let (feature, _) = feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: crate::Value::String("feature@test.com".to_string()),
                cause: None,
            })]))
            .perform(&env)
            .await?;

        // Pull main's changes into feature (both sides changed)
        let (feature, pulled) = feature
            .pull(main_revision.clone())
            .perform(&env)
            .await?;

        assert!(pulled.is_some());
        // Tree should differ from both main and feature originals (merged)
        let merged_tree = feature.revision().tree().clone();
        assert_ne!(&merged_tree, main_revision.tree());

        Ok(())
    }
}
