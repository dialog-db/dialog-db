use dialog_capability::{Did, Provider};
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::state::UpstreamState;
use crate::repository::cell::Cell;
use crate::repository::error::RepositoryError;
use crate::repository::memory::{Memory, Trace};
use crate::repository::revision::Revision;

/// Command to open a branch, creating it with defaults if it doesn't exist.
pub struct Open {
    subject: Did,
    memory: Memory,
    trace: Trace,
}

impl Open {
    pub(crate) fn new(subject: Did, memory: Memory, trace: Trace) -> Self {
        Self {
            subject,
            memory,
            trace,
        }
    }

    /// Execute the open operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve> + Provider<memory_fx::Publish>,
    {
        let revision: Cell<Option<Revision>> = self.trace.cell("revision");
        revision.resolve(env).await?;
        // Publish None to mark the branch as existing if the cell is empty.
        if revision.get().is_none() {
            revision.publish(None, env).await?;
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

    #[dialog_common::test]
    async fn it_opens_new_branch() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;

        let branch = repo.open_branch("main").perform(&operator).await?;

        assert_eq!(branch.name().as_str(), "main");
        assert!(
            branch.revision().is_none(),
            "New branch should have no revision"
        );
        Ok(())
    }
}
