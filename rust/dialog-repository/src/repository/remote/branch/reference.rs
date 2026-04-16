//! Reference for navigating to a remote branch.

use dialog_capability::Subject;

use super::load::LoadRemoteBranch;
use super::open::OpenRemoteBranch;
use crate::repository::branch::BranchName;
use crate::repository::memory::{Cell, MemoryExt};
use crate::repository::remote::repository::RemoteRepository;
use crate::repository::revision::Revision;

/// A reference to a named branch in a remote repository.
///
/// Holds both a local cache cell (for persisting fetched revisions locally)
/// and a remote cell (for fork-based resolve/publish against the remote site).
#[derive(Debug, Clone)]
pub struct RemoteBranchReference {
    pub repository: RemoteRepository,
    /// Local cache: `remote/{name}/branch/{branch}/revision`
    pub local: Cell<Revision>,
    /// Remote subject's cell: `branch/{branch}/revision` at the remote subject
    pub remote: Cell<Revision>,
}

impl RemoteBranchReference {
    /// The branch name, derived from the local cell path.
    pub fn name(&self) -> BranchName {
        let cell_name = self.local.name();
        cell_name
            .strip_prefix("branch/")
            .and_then(|s| s.strip_suffix("/revision"))
            .unwrap_or(cell_name)
            .into()
    }

    /// The cached local revision, if resolved.
    pub fn revision(&self) -> Option<Revision> {
        self.local.get()
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
        let local = self
            .site()
            .cell(format!("branch/{}/revision", name.as_str()));
        let remote = Subject::from(self.did()).branch(name).cell("revision");
        RemoteBranchReference {
            repository: self.clone(),
            local,
            remote,
        }
    }
}
