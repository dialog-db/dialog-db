use dialog_capability::{Did, Provider, Subject};
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::memory;
use super::state::{BranchName, BranchState};
use crate::repository::cell::CellOr;
use crate::repository::credentials::Credentials;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

/// Command to load an existing branch, returning an error if not found.
pub struct Load {
    name: BranchName,
    issuer: Credentials,
    subject: Did,
}

impl Load {
    pub(super) fn new(name: BranchName, issuer: Credentials, subject: Did) -> Self {
        Self {
            name,
            issuer,
            subject,
        }
    }
}

impl Load {
    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        let default_state = BranchState::new(Revision::new(self.issuer.did()));
        let mem = memory::Memory::new(Subject::from(self.subject), self.name.clone());
        let cell: CellOr<BranchState> = mem.cell().or(default_state);
        cell.resolve(env).await?;
        if cell.inner().read_with(|opt| opt.is_none()) {
            return Err(RepositoryError::BranchNotFound { name: self.name });
        }
        Ok(Branch {
            name: self.name,
            issuer: self.issuer,
            cell,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::Branch;
    use super::super::tests::{test_issuer, test_subject};
    use crate::repository::error::RepositoryError;
    use dialog_storage::provider::Volatile;

    #[dialog_common::test]
    async fn it_loads_existing_branch() -> anyhow::Result<()> {
        let env = Volatile::new();
        let issuer = test_issuer().await;

        // First open creates
        let _ = Branch::open("main", issuer.clone(), test_subject())
            .perform(&env)
            .await?;

        // Load should find it
        let branch = Branch::load("main", issuer, test_subject())
            .perform(&env)
            .await?;

        assert_eq!(branch.name().as_str(), "main");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_loading_missing_branch() -> anyhow::Result<()> {
        let env = Volatile::new();

        let result = Branch::load("nonexistent", test_issuer().await, test_subject())
            .perform(&env)
            .await;

        assert!(matches!(
            result,
            Err(RepositoryError::BranchNotFound { .. })
        ));
        Ok(())
    }
}
