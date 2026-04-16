//! Fetch command for remote branches.

use dialog_capability::Provider;
use dialog_capability::fork::Fork;
use dialog_common::ConditionalSync;
use dialog_effects::memory as memory_fx;

use super::RemoteBranch;
use crate::repository::error::RepositoryError;
use crate::repository::remote::address::RemoteSite;
use crate::repository::revision::Revision;

/// Command to fetch the latest revision from the remote.
///
/// Resolves the remote branch revision via Fork and updates
/// the local cache cell.
pub struct Fetch<'a> {
    branch: &'a RemoteBranch,
}

impl<'a> Fetch<'a> {
    /// Create a new fetch command.
    pub fn new(branch: &'a RemoteBranch) -> Self {
        Self { branch }
    }

    /// Execute the fetch.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, RepositoryError>
    where
        Env: Provider<Fork<RemoteSite, memory_fx::Resolve>>
            + Provider<memory_fx::Publish>
            + ConditionalSync,
    {
        self.branch
            .remote
            .resolve()
            .fork(&self.branch.address().address)
            .perform(env)
            .await?;

        let revision = self.branch.remote.get();

        // Update local cache
        if let Some(ref rev) = revision {
            self.branch.local.publish(rev.clone()).perform(env).await?;
        }

        Ok(revision)
    }
}
