//! Reference for navigating to a remote branch.

use dialog_capability::Subject;
use dialog_effects::memory::Edition;

use super::load::LoadRemoteBranch;
use super::open::OpenRemoteBranch;
use crate::repository::branch::BranchName;
use crate::repository::memory::{Cell, MemoryExt};
use crate::repository::remote::repository::RemoteRepository;
use crate::repository::revision::Revision;

/// Cached snapshot of the remote branch's last known state: the remote
/// revision paired with the remote's CAS version, so that fresh
/// [`RemoteBranchReference`] instances can prime the in-memory remote cell
/// cache without hitting the network.
pub type RemoteEdition = Edition<Revision>;

/// A reference to a named branch in a remote repository.
///
/// Holds a persistent cache of the last known remote state (revision +
/// edition) plus an in-memory remote cell for fork-based resolve/publish.
#[derive(Debug, Clone)]
pub struct RemoteBranchReference {
    pub repository: RemoteRepository,
    /// Persistent cache of the last known remote state.
    /// Path: `remote/{name}/branch/{branch}/revision`.
    pub cache: Cell<RemoteEdition>,
    /// Remote subject's cell: `branch/{branch}/revision` at the remote subject.
    /// In-memory cache only; hydrate from `cache` on open/load.
    pub remote: Cell<Revision>,
}

impl RemoteBranchReference {
    /// The branch name, derived from the cache cell's path.
    pub fn name(&self) -> BranchName {
        let cell_name = self.cache.name();
        cell_name
            .strip_prefix("branch/")
            .and_then(|s| s.strip_suffix("/revision"))
            .unwrap_or(cell_name)
            .into()
    }

    /// The cached revision, if the snapshot has been resolved.
    pub fn revision(&self) -> Option<Revision> {
        self.cache.content().map(|e| e.content)
    }

    /// Open the remote branch (resolves local cache, no error if missing).
    pub fn open(self) -> OpenRemoteBranch {
        OpenRemoteBranch::new(self)
    }

    /// Load the remote branch (error if local cache has no revision).
    pub fn load(self) -> LoadRemoteBranch {
        LoadRemoteBranch::new(self)
    }
}

impl RemoteRepository {
    /// Get a branch reference at this remote repository.
    pub fn branch(&self, name: impl Into<BranchName>) -> RemoteBranchReference {
        let name = name.into();
        let cache = self
            .site()
            .cell(format!("branch/{}/revision", name.as_str()));
        let remote = Subject::from(self.did()).branch(name).cell("revision");
        RemoteBranchReference {
            repository: self.clone(),
            cache,
            remote,
        }
    }
}
