//! Command to load an existing remote branch.

use dialog_capability::Provider;
use dialog_effects::memory::Resolve;

use super::{OpenRemoteBranch, RemoteBranch};
use crate::repository::error::RepositoryError;

/// Command to load an existing remote branch.
///
/// Behaves like [`OpenRemoteBranch`] but errors if the persisted
/// snapshot cache has no revision (the branch has never been fetched
/// locally).
pub struct LoadRemoteBranch {
    open: OpenRemoteBranch,
}

impl<T> From<T> for LoadRemoteBranch
where
    T: Into<OpenRemoteBranch>,
{
    fn from(value: T) -> Self {
        Self { open: value.into() }
    }
}

impl LoadRemoteBranch {
    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteBranch, RepositoryError>
    where
        Env: Provider<Resolve>,
    {
        let branch = self.open.perform(env).await?;
        if branch.revision().is_none() {
            return Err(RepositoryError::BranchNotFound {
                name: branch.name().to_string(),
            });
        }
        Ok(branch)
    }
}
