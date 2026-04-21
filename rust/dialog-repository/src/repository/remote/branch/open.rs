//! Command to open a remote branch.

use dialog_capability::Provider;
use dialog_effects::memory::Resolve;

use super::{LoadedRemoteBranchReference, RemoteBranch, RemoteBranchReference};
use crate::OpenRemoteBranchError;

/// Command to open a remote branch.
///
/// Resolves the persisted snapshot cache and primes the in-memory
/// upstream cell from the cache if present. When the source reference
/// doesn't already carry a loaded address, it is resolved first. Does
/// not error when the cache is empty (branch not yet fetched).
pub struct OpenRemoteBranch {
    branch: Reference,
}

enum Reference {
    Unloaded(RemoteBranchReference),
    Loaded(LoadedRemoteBranchReference),
}

impl From<LoadedRemoteBranchReference> for OpenRemoteBranch {
    fn from(reference: LoadedRemoteBranchReference) -> Self {
        Self {
            branch: Reference::Loaded(reference),
        }
    }
}

impl From<RemoteBranchReference> for OpenRemoteBranch {
    fn from(reference: RemoteBranchReference) -> Self {
        Self {
            branch: Reference::Unloaded(reference),
        }
    }
}

impl OpenRemoteBranch {
    /// Execute the open operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteBranch, OpenRemoteBranchError>
    where
        Env: Provider<Resolve>,
    {
        let loaded = match self.branch {
            Reference::Loaded(loaded) => loaded,
            Reference::Unloaded(reference) => {
                let name = reference.name().to_string();
                reference.remote.load().perform(env).await?.branch(name)
            }
        };

        let cache = loaded.cache();
        cache.resolve().perform(env).await?;

        let upstream = loaded.revision();
        if let Some(edition) = cache.content() {
            upstream.reset(edition);
        }

        let LoadedRemoteBranchReference { repository, branch } = loaded;
        Ok(RemoteBranch::new(repository, branch, cache, upstream))
    }
}
