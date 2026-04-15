//! Command to open a remote branch.

use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::RemoteBranch;
use super::reference::RemoteBranchReference;
use crate::repository::error::RepositoryError;

/// Command to open a remote branch.
///
/// Resolves the revision cell; does not error if the branch has no revision
/// (does not exist) yet.
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
        self.0.local.resolve().perform(env).await?;
        Ok(RemoteBranch::new(self.0))
    }
}
