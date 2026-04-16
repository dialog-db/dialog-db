//! Publish command for remote branches.

use dialog_capability::Provider;
use dialog_capability::fork::Fork;
use dialog_common::ConditionalSync;
use dialog_effects::memory as memory_fx;

use super::RemoteBranch;
use crate::repository::error::RepositoryError;
use crate::repository::remote::address::RemoteSite;
use crate::repository::revision::Revision;

/// Command to publish a revision to the remote.
///
/// Publishes the revision to the remote memory via Fork and updates
/// the local cache cell.
pub struct Publish<'a> {
    branch: &'a RemoteBranch,
    revision: Revision,
}

impl<'a> Publish<'a> {
    /// Create a new publish command.
    pub fn new(branch: &'a RemoteBranch, revision: Revision) -> Self {
        Self { branch, revision }
    }

    /// Execute the publish.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<Fork<RemoteSite, memory_fx::Publish>>
            + Provider<memory_fx::Publish>
            + ConditionalSync,
    {
        let address = &self.branch.address().address;

        // Publish to remote via fork
        self.branch
            .remote
            .publish(self.revision.clone())
            .fork(address)
            .perform(env)
            .await?;

        // Update local cache
        self.branch
            .local
            .publish(self.revision)
            .perform(env)
            .await?;

        Ok(())
    }
}
