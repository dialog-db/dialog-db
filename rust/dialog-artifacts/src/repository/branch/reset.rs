use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::state::BranchState;
use super::Branch;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

/// Command struct for resetting a branch to a given revision.
pub struct Reset {
    pub(super) branch: Branch,
    pub(super) revision: Revision,
}

impl Reset {
    /// Execute the reset operation, returning the updated branch.
    pub async fn perform<Env>(self, env: &mut Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory_fx::Publish>,
    {
        let branch = self.branch;
        let revision = self.revision;

        let new_state = BranchState {
            revision: revision.clone(),
            base: revision.tree.clone(),
            ..branch.state()
        };

        branch.cell.publish(new_state, env).await?;

        Ok(branch)
    }
}
