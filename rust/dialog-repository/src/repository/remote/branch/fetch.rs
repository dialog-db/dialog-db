//! Fetch command for remote branches.

use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::memory::{Publish, Resolve};

use super::RemoteBranch;
use crate::FetchRemoteBranchError;
use crate::repository::remote::RemoteSite;
use crate::repository::revision::Revision;

/// Command to fetch the latest revision from the remote.
///
/// Resolves the remote branch revision via Fork and persists the resulting
/// (revision, edition) pair to the local snapshot cache.
pub struct FetchRemoteBranch<'a> {
    branch: &'a RemoteBranch,
}

impl<'a> FetchRemoteBranch<'a> {
    /// Create a new fetch command.
    pub fn new(branch: &'a RemoteBranch) -> Self {
        Self { branch }
    }

    /// Execute the fetch.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, FetchRemoteBranchError>
    where
        Env: Provider<Fork<RemoteSite, Resolve>> + Provider<Publish> + ConditionalSync,
    {
        let address = self.branch.address();
        self.branch
            .upstream()
            .resolve()
            .fork(address.site())
            .perform(env)
            .await?;

        // Persist the new remote edition if the remote has one.
        let Some(edition) = self.branch.upstream().edition() else {
            return Ok(None);
        };
        let revision = edition.content.clone();
        self.branch.cache().publish(edition).perform(env).await?;

        Ok(Some(revision))
    }
}
