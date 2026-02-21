use dialog_capability::{Did, Provider, Subject};
use dialog_effects::memory as memory_fx;

use super::memory;
use super::state::{BranchId, BranchState};
use super::Branch;
use crate::repository::cell::CellOr;
use crate::repository::credentials::Credentials;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

/// Command to load an existing branch, returning an error if not found.
pub struct Load {
    pub(super) id: BranchId,
    pub(super) issuer: Credentials,
    pub(super) subject: Did,
}

impl Load {
    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &mut Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        let default_state =
            BranchState::new(self.id.clone(), Revision::new(self.issuer.did()), None);
        let mem = memory::Memory::new(Subject::from(self.subject), self.id.clone());
        let cell: CellOr<BranchState> = mem.cell().or(default_state);
        cell.resolve(env).await?;
        if cell.inner().read_with(|opt| opt.is_none()) {
            return Err(RepositoryError::BranchNotFound { id: self.id });
        }
        Ok(Branch {
            issuer: self.issuer,
            cell,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{test_issuer, test_subject};
    use super::super::Branch;
    use crate::repository::error::RepositoryError;

    #[dialog_common::test]
    async fn it_loads_existing_branch() -> anyhow::Result<()> {
        let mut env = dialog_storage::provider::Volatile::new();
        let issuer = test_issuer().await;

        // First open creates
        let _ = Branch::open("main", issuer.clone(), test_subject())
            .perform(&mut env)
            .await?;

        // Load should find it
        let branch = Branch::load("main", issuer, test_subject())
            .perform(&mut env)
            .await?;

        assert_eq!(branch.id().id(), "main");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_loading_missing_branch() -> anyhow::Result<()> {
        let mut env = dialog_storage::provider::Volatile::new();

        let result = Branch::load("nonexistent", test_issuer().await, test_subject())
            .perform(&mut env)
            .await;

        assert!(matches!(
            result,
            Err(RepositoryError::BranchNotFound { .. })
        ));
        Ok(())
    }
}
