//! Fetch command for remote branches.

use crate::{ConnectedBranch, FetchRemoteBranchError, RemoteSite, Revision};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::memory::{Publish, Resolve};

/// Command to fetch the latest revision from the remote.
///
/// Resolves the remote branch revision via Fork and persists the resulting
/// (revision, edition) pair to the local snapshot cache.
///
/// The cache is written against the edition this handle last saw, not
/// read again first, unlike [`ObserveRemoteBranch`](super::ObserveRemoteBranch):
/// what is written is what the remote answered, not a head ordered against
/// the one recorded, so writing it over a newer head recorded through
/// another handle would move the record back. A pull or push opens the
/// handle for the one operation, so only a write to the same cache in
/// between is refused, as a version mismatch, and the next fetch records
/// where the remote stands.
pub struct FetchRemoteBranch<'a> {
    branch: &'a ConnectedBranch,
}

impl<'a> FetchRemoteBranch<'a> {
    /// Create a new fetch command.
    pub fn new(branch: &'a ConnectedBranch) -> Self {
        Self { branch }
    }

    /// Execute the fetch.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, FetchRemoteBranchError>
    where
        Env: Provider<Fork<RemoteSite, Resolve>> + Provider<Publish> + ConditionalSync,
    {
        let remote = self.branch.repository().connection(env);
        self.branch.upstream().resolve().perform(&remote).await?;

        // Persist the new remote edition if the remote has one.
        let Some(edition) = self.branch.upstream().edition() else {
            return Ok(None);
        };
        let revision = edition.content.clone();
        self.branch.cache().publish(edition).perform(env).await?;

        Ok(Some(revision))
    }
}
