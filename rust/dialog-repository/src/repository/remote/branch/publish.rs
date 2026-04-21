//! Publish command for remote branches.

use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::memory::Publish;

use super::RemoteBranch;
use crate::PublishRemoteBranchError;
use crate::repository::remote::RemoteSite;
use crate::repository::revision::Revision;

/// Command to publish a revision to the remote.
///
/// Publishes the revision to the remote memory via Fork and persists the
/// new remote edition to the local snapshot cache.
pub struct PublishRemoteBranch<'a> {
    branch: &'a RemoteBranch,
    revision: Revision,
}

impl<'a> PublishRemoteBranch<'a> {
    /// Create a new publish command.
    pub fn new(branch: &'a RemoteBranch, revision: Revision) -> Self {
        Self { branch, revision }
    }

    /// Execute the publish.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), PublishRemoteBranchError>
    where
        Env: Provider<Fork<RemoteSite, Publish>> + Provider<Publish> + ConditionalSync,
    {
        let address = self.branch.address();

        // Publish to the upstream via fork. The in-memory upstream cell
        // picks up the new CAS edition internally; we then snapshot it
        // below.
        self.branch
            .upstream()
            .publish(self.revision)
            .fork(address.site())
            .perform(env)
            .await?;

        // Persist the upstream edition so that a future open/load can
        // hydrate its in-memory upstream cache without a round trip.
        let edition = self
            .branch
            .upstream()
            .edition()
            .ok_or(PublishRemoteBranchError::MissingEdition)?;
        self.branch.cache().publish(edition).perform(env).await?;

        Ok(())
    }
}
