//! Command to load an existing remote branch.

use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::RemoteBranch;
use super::reference::RemoteBranchReference;
use crate::repository::error::RepositoryError;

/// Command to load an existing remote branch.
///
/// Resolves the revision cell; errors if the branch has no revision.
pub struct LoadRemoteBranch(RemoteBranchReference);

impl LoadRemoteBranch {
    /// Create from a remote branch reference.
    pub fn new(reference: RemoteBranchReference) -> Self {
        Self(reference)
    }

    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteBranch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        self.0.revision.resolve(env).await?;
        if self.0.revision.get().is_none() {
            return Err(RepositoryError::BranchNotFound {
                name: self.0.name(),
            });
        }

        Ok(RemoteBranch::new(self.0))
    }
}
