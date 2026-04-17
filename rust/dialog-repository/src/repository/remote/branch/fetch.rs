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
/// Resolves the remote branch revision via Fork and persists the resulting
/// (revision, edition) pair to the local snapshot cache.
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
        let address = self.branch.repository.address();
        self.branch
            .remote
            .resolve()
            .fork(address.site())
            .perform(env)
            .await?;

        // Persist the new remote edition if the remote has one.
        let Some(edition) = self.branch.remote.edition() else {
            return Ok(None);
        };
        let revision = edition.content.clone();
        self.branch.cache.publish(edition).perform(env).await?;

        Ok(Some(revision))
    }
}
