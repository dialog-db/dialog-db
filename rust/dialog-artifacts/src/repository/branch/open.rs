use dialog_capability::{Did, Provider, Subject};
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::memory;
use super::state::{BranchName, BranchState};
use crate::repository::cell::CellOr;
use crate::repository::credentials::Credentials;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

/// Command to open a branch, creating it with defaults if it doesn't exist.
pub struct Open {
    pub(super) name: BranchName,
    pub(super) issuer: Credentials,
    pub(super) subject: Did,
}

impl Open {
    /// Execute the open operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve> + Provider<memory_fx::Publish>,
    {
        let default_state = BranchState::new(Revision::new(self.issuer.did()));
        let mem = memory::Memory::new(Subject::from(self.subject), self.name.clone());
        let cell: CellOr<BranchState> = mem.cell().or(default_state);
        cell.get_or_init(env).await?;
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
    use crate::repository::node_reference::NodeReference;
    use dialog_storage::provider::Volatile;

    #[dialog_common::test]
    async fn it_opens_new_branch() -> anyhow::Result<()> {
        let env = Volatile::new();

        let branch = Branch::open("main", test_issuer().await, test_subject())
            .perform(&env)
            .await?;

        assert_eq!(branch.name().as_str(), "main");
        assert_eq!(branch.revision().tree(), &NodeReference::default());
        Ok(())
    }
}
