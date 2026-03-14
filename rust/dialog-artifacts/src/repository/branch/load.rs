use dialog_capability::{Did, Provider};
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::state::UpstreamState;
use crate::repository::cell::{Cell, CellOr};
use crate::repository::error::RepositoryError;
use crate::repository::memory::{Authorization, Memory, Trace};
use crate::repository::revision::Revision;

/// Command to load an existing branch, returning an error if not found.
pub struct Load {
    session: Authorization,
    subject: Did,
    memory: Memory,
    trace: Trace,
}

impl Load {
    pub(crate) fn new(session: Authorization, subject: Did, memory: Memory, trace: Trace) -> Self {
        Self {
            session,
            subject,
            memory,
            trace,
        }
    }
}

impl Load {
    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        let default_revision = Revision::new(self.session.did());
        let revision: CellOr<Revision> = self.trace.cell("revision").or(default_revision);
        revision.resolve(env).await?;

        if revision.inner().read_with(|opt| opt.is_none()) {
            return Err(RepositoryError::BranchNotFound {
                name: self.trace.name().clone(),
            });
        }

        let upstream: Cell<Option<UpstreamState>> = self.trace.cell("upstream");
        upstream.resolve(env).await?;

        Ok(Branch {
            session: self.session,
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
    use super::super::tests::{test_issuer, test_subject};
    use crate::repository::Repository;
    use crate::repository::error::RepositoryError;
    use dialog_storage::provider::Volatile;

    #[dialog_common::test]
    async fn it_loads_existing_branch() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::new(test_issuer().await, test_subject());

        let _ = repo.open_branch("main").perform(&env).await?;
        let branch = repo.load_branch("main").perform(&env).await?;

        assert_eq!(branch.name().as_str(), "main");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_loading_missing_branch() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::new(test_issuer().await, test_subject());

        let result = repo.load_branch("nonexistent").perform(&env).await;

        assert!(matches!(
            result,
            Err(RepositoryError::BranchNotFound { .. })
        ));
        Ok(())
    }
}
