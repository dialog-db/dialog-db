use dialog_capability::{Did, Provider};
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::state::{BranchName, UpstreamState};
use crate::repository::cell::Cell;
use crate::repository::error::RepositoryError;
use crate::repository::memory::{BranchMemory, Memory};
use crate::repository::revision::Revision;

/// Command to load an existing branch, returning an error if not found.
pub struct LoadBranch {
    subject: Did,
    memory: Memory,
    branch_memory: BranchMemory,
}

impl LoadBranch {
    pub(crate) fn new(subject: Did, memory: Memory, branch_memory: BranchMemory) -> Self {
        // pub(crate): constructed by BranchReference and Branch::load_branch
        Self {
            subject,
            memory,
            branch_memory,
        }
    }

    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        let revision: Cell<Option<Revision>> = self.branch_memory.cell("revision");
        revision.resolve(env).await?;

        // The outer Option from Cell::get() tells us whether the cell exists
        // in storage. If it's None, the branch was never opened/created.
        if revision.get().is_none() {
            return Err(RepositoryError::BranchNotFound {
                name: self.branch_memory.name().clone(),
            });
        }

        let upstream: Cell<Option<UpstreamState>> = self.branch_memory.cell("upstream");
        upstream.resolve(env).await?;

        Ok(Branch {
            subject: self.subject,
            memory: self.memory,
            branch_memory: self.branch_memory,
            revision,
            upstream,
        })
    }
}

impl Branch {
    /// Load a sibling branch by name (internal use for pull/push/fetch).
    pub(crate) fn load_branch(&self, name: impl Into<BranchName>) -> LoadBranch {
        // pub(crate): used by pull, push, and fetch to resolve upstream branches
        let branch_memory = self.memory.branch(name.into());
        LoadBranch::new(self.subject.clone(), self.memory.clone(), branch_memory)
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
