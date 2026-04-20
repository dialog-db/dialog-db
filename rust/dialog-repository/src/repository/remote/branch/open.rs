//! Command to open a remote branch.

use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::RemoteBranch;
use super::reference::RemoteBranchReference;
use crate::repository::error::RepositoryError;

/// Command to open a remote branch.
///
/// Resolves the persisted snapshot; does not error if the branch has no
/// revision (does not exist) yet.
pub struct OpenRemoteBranch(RemoteBranchReference);

impl OpenRemoteBranch {
    /// Create from a remote branch reference.
    pub fn new(reference: RemoteBranchReference) -> Self {
        Self(reference)
    }

    /// Execute the open operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteBranch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        self.0.cache.resolve().perform(env).await?;
        if let Some(edition) = self.0.cache.content() {
            self.0.remote.reset(edition);
        }
        Ok(RemoteBranch::new(self.0))
    }
}
