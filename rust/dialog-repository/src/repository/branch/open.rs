use dialog_capability::{Did, Provider};
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::state::UpstreamState;
use crate::repository::cell::Cell;
use crate::repository::error::RepositoryError;
use crate::repository::memory::{BranchMemory, Memory};
use crate::repository::revision::Revision;

/// Command to open a branch, creating it with defaults if it doesn't exist.
pub struct OpenBranch {
    subject: Did,
    memory: Memory,
    branch_memory: BranchMemory,
}

impl OpenBranch {
    pub(crate) fn new(subject: Did, memory: Memory, branch_memory: BranchMemory) -> Self {
        // pub(crate): constructed by BranchReference
        Self {
            subject,
            memory,
            branch_memory,
        }
    }

    /// Execute the open operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve> + Provider<memory_fx::Publish>,
    {
        let revision: Cell<Option<Revision>> = self.branch_memory.cell("revision");
        revision.resolve(env).await?;
        // Publish None to mark the branch as existing if the cell is empty.
        if revision.get().is_none() {
            revision.publish(None, env).await?;
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

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};

    #[dialog_common::test]
    async fn it_opens_new_branch() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let branch = repo.branch("main").open().perform(&operator).await?;

        assert_eq!(branch.name().as_str(), "main");
        assert!(
            branch.revision().is_none(),
            "New branch should have no revision"
        );
        Ok(())
    }
}
