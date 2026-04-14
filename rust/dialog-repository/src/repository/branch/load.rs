use dialog_capability::{Did, Provider};
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::state::UpstreamState;
use crate::repository::cell::Cell;
use crate::repository::error::RepositoryError;
use crate::repository::memory::{Memory, Trace};
use crate::repository::revision::Revision;

/// Command to load an existing branch, returning an error if not found.
pub struct LoadBranch {
    subject: Did,
    memory: Memory,
    trace: Trace,
}

impl LoadBranch {
    pub(crate) fn new(subject: Did, memory: Memory, trace: Trace) -> Self {
        // pub(crate): called by BranchSelector and Branch::load_branch
        Self {
            subject,
            memory,
            trace,
        }
    }

    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        let revision: Cell<Option<Revision>> = self.trace.cell("revision");
        revision.resolve(env).await?;

        // The outer Option from Cell::get() tells us whether the cell exists
        // in storage. If it's None, the branch was never opened/created.
        if revision.get().is_none() {
            return Err(RepositoryError::BranchNotFound {
                name: self.trace.name().clone(),
            });
        }

        let upstream: Cell<Option<UpstreamState>> = self.trace.cell("upstream");
        upstream.resolve(env).await?;

        Ok(Branch {
            subject: self.subject,
            memory: self.memory,
            trace: self.trace,
            revision,
            upstream,
        })
    }
}

use super::state::BranchName;

impl Branch {
    /// Load a sibling branch by name (internal use for pull/push/fetch).
    pub(crate) fn load_branch(&self, name: impl Into<BranchName>) -> LoadBranch {
        // pub(crate): used by pull, push, and fetch to resolve upstream branches
        let trace = self.memory.trace(name.into());
        LoadBranch::new(self.subject.clone(), self.memory.clone(), trace)
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
