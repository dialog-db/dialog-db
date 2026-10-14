//! Publish command for remote branches.

use crate::{ConnectedBranch, PublishRemoteBranchError, RemoteSite, Revision};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::memory::Publish;

/// Command to publish a revision to the remote.
///
/// Publishes the revision to the remote memory via Fork and persists the
/// new remote edition to the local snapshot cache.
///
/// The cache is written against the edition this handle last saw, not
/// read again first, unlike [`ObserveRemoteBranch`](super::ObserveRemoteBranch):
/// what is written is what the remote answered, not a head ordered against
/// the one recorded, so writing it over a newer head recorded through
/// another handle would move the record back. A pull or push opens the
/// handle for the one operation, so only a write to the same cache in
/// between is refused, as a version mismatch, and the next fetch records
/// where the remote stands.
pub struct PublishRemoteBranch<'a> {
    branch: &'a ConnectedBranch,
    revision: Revision,
}

impl<'a> PublishRemoteBranch<'a> {
    /// Create a new publish command.
    pub fn new(branch: &'a ConnectedBranch, revision: Revision) -> Self {
        Self { branch, revision }
    }

    /// Execute the publish.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), PublishRemoteBranchError>
    where
        Env: Provider<Fork<RemoteSite, Publish>> + Provider<Publish> + ConditionalSync,
    {
        // Publish to the upstream via fork. The in-memory upstream cell
        // picks up the new CAS edition internally; we then snapshot it
        // below.
        let remote = self.branch.repository().connection(env);
        self.branch
            .upstream()
            .publish(self.revision)
            .perform(&remote)
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
