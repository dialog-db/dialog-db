//! Command to load an existing remote branch.

use dialog_capability::Provider;
use dialog_effects::memory::Resolve;

use super::{RemoteBranch, RemoteBranchReference};
use crate::repository::error::RepositoryError;

/// Command to load an existing remote branch.
///
/// Resolves the persisted snapshot; errors if the branch has no revision.
pub struct LoadRemoteBranch(RemoteBranchReference);

impl LoadRemoteBranch {
    /// Create from a remote branch reference.
    pub fn new(reference: RemoteBranchReference) -> Self {
        Self(reference)
    }

    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteBranch, RepositoryError>
    where
        Env: Provider<Resolve>,
    {
        self.0.cache.resolve().perform(env).await?;
        let Some(edition) = self.0.cache.content() else {
            return Err(RepositoryError::BranchNotFound {
                name: self.0.name().to_string(),
            });
        };
        self.0.remote.reset(edition);
        Ok(RemoteBranch::new(self.0))
    }
}
