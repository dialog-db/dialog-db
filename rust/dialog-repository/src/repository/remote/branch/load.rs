//! Command to load an existing remote branch.

use crate::{
    BranchReference, LoadRemoteBranchError, OpenRemoteBranch, RemoteBranch, RemoteRepository,
};
use dialog_capability::Provider;
use dialog_effects::memory::Resolve;

/// Command to load an existing remote branch.
///
/// Behaves like [`OpenRemoteBranch`] but errors if the persisted
/// snapshot cache has no revision (the branch has never been fetched
/// locally).
pub struct LoadRemoteBranch {
    open: OpenRemoteBranch,
}

impl LoadRemoteBranch {
    /// Construct from an owned remote repository and a branch reference.
    pub(super) fn new(repository: RemoteRepository, branch: BranchReference) -> Self {
        Self {
            open: OpenRemoteBranch::new(repository, branch),
        }
    }

    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteBranch, LoadRemoteBranchError>
    where
        Env: Provider<Resolve>,
    {
        let branch = self.open.perform(env).await?;
        if branch.revision().is_none() {
            return Err(LoadRemoteBranchError::NotFound {
                name: branch.name().to_string(),
            });
        }
        Ok(branch)
    }
}
