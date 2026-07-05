//! Command to open a remote branch.

use crate::{BranchReference, OpenRemoteBranchError, RemoteBranch, RemoteRepository};
use dialog_capability::Provider;
use dialog_effects::memory::Resolve;

/// Command to open a remote branch.
///
/// Resolves the persisted snapshot cache and primes the in-memory
/// upstream cell from the cache. Does not error when the cache is
/// empty (branch not yet fetched).
pub struct OpenRemoteBranch {
    repository: RemoteRepository,
    branch: BranchReference,
}

impl OpenRemoteBranch {
    /// Construct from an owned remote repository and a branch reference.
    pub(super) fn new(repository: RemoteRepository, branch: BranchReference) -> Self {
        Self { repository, branch }
    }

    /// Execute the open operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteBranch, OpenRemoteBranchError>
    where
        Env: Provider<Resolve>,
    {
        let reference = self.repository.branch(self.branch.name());
        let cache = reference.cache();
        cache.resolve().perform(env).await?;

        let upstream = reference.revision();
        if let Some(edition) = cache.content() {
            upstream.reset(edition);
        }

        Ok(RemoteBranch::new(
            self.repository,
            self.branch,
            cache,
            upstream,
        ))
    }
}
