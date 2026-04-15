use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::reference::BranchReference;
use super::upstream::UpstreamState;
use crate::repository::error::RepositoryError;
use crate::repository::memory::Cell;
use crate::repository::revision::Revision;

/// Command to load an existing branch, returning an error if not found.
pub struct LoadBranch {
    branch: BranchReference,
}

impl LoadBranch {
    /// Create from a branch reference.
    pub fn new(branch: BranchReference) -> Self {
        Self { branch }
    }

    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        let revision: Cell<Option<Revision>> = self.branch.cell("revision");
        revision.resolve().perform(env).await?;

        if revision.get().is_none() {
            return Err(RepositoryError::BranchNotFound {
                name: self.branch.name(),
            });
        }

        let upstream: Cell<Option<UpstreamState>> = self.branch.cell("upstream");
        upstream.resolve().perform(env).await?;

        Ok(Branch {
            reference: self.branch,
            revision,
            upstream,
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use crate::repository::error::RepositoryError;

    #[dialog_common::test]
    async fn it_loads_existing_branch() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let _ = repo.branch("main").open().perform(&operator).await?;
        let branch = repo.branch("main").load().perform(&operator).await?;

        assert_eq!(branch.name().as_str(), "main");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_loading_missing_branch() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let result = repo.branch("nonexistent").load().perform(&operator).await;

        assert!(matches!(
            result,
            Err(RepositoryError::BranchNotFound { .. })
        ));
        Ok(())
    }
}
