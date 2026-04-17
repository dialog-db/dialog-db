//! Publish command for remote branches.

use dialog_capability::Provider;
use dialog_capability::fork::Fork;
use dialog_common::ConditionalSync;
use dialog_effects::memory as memory_fx;

use super::RemoteBranch;
use super::reference::RemoteSnapshot;
use crate::repository::error::RepositoryError;
use crate::repository::remote::address::RemoteSite;
use crate::repository::revision::Revision;

/// Command to publish a revision to the remote.
///
/// Publishes the revision to the remote memory via Fork and persists the
/// new remote edition to the local snapshot cache.
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
        let address = self.branch.repository.address();

        // Publish to remote via fork. The in-memory `remote` cell picks up
        // the new CAS edition internally; we then snapshot it below.
        self.branch
            .remote
            .publish(self.revision)
            .fork(address.site())
            .perform(env)
            .await?;

        // Persist the (revision, remote edition) pair so that a future
        // RemoteBranchReference can hydrate its in-memory `remote` cache
        // without a round trip.
        let (revision, edition) = self.branch.remote.snapshot().ok_or_else(|| {
            RepositoryError::StorageError("remote cell missing snapshot after publish".into())
        })?;
        let snapshot = RemoteSnapshot { revision, edition };
        self.branch.cache.publish(snapshot).perform(env).await?;

        Ok(())
    }
}
