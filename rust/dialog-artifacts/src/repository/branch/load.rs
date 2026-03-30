use dialog_capability::{Did, Provider};
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::state::UpstreamState;
use crate::repository::cell::Cell;
use crate::repository::error::RepositoryError;
use crate::repository::memory::{Memory, Trace};
use crate::repository::revision::Revision;

/// Command to load an existing branch, returning an error if not found.
pub struct Load {
    subject: Did,
    memory: Memory,
    trace: Trace,
}

impl Load {
    pub(crate) fn new(subject: Did, memory: Memory, trace: Trace) -> Self {
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

#[cfg(test)]
mod tests {
    use super::super::tests::{test_operator, test_repo};
    use crate::repository::error::RepositoryError;

    #[dialog_common::test]
    async fn it_loads_existing_branch() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;

        let _ = repo.open_branch("main").perform(&operator).await?;
        let branch = repo.load_branch("main").perform(&operator).await?;

        assert_eq!(branch.name().as_str(), "main");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_loading_missing_branch() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;

        let result = repo.load_branch("nonexistent").perform(&operator).await;

        assert!(matches!(
            result,
            Err(RepositoryError::BranchNotFound { .. })
        ));
        Ok(())
    }
}
