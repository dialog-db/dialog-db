//! Reference for navigating to a remote branch.

use super::load::LoadRemoteBranch;
use super::open::OpenRemoteBranch;
use crate::RemoteAddress;
use crate::repository::branch::BranchName;
use crate::repository::memory::Cell;
use crate::repository::remote::repository::RemoteRepository;
use crate::repository::revision::Revision;

/// A reference to a named branch in a remote repository.
#[derive(Debug, Clone)]
pub struct RemoteBranchReference {
    pub repository: RemoteRepository,
    pub revision: Cell<Revision>,
}

impl RemoteBranchReference {
    /// The branch name, derived from the cell path.
    pub fn name(&self) -> BranchName {
        let cell_name = self.revision.name();
        cell_name
            .strip_prefix("branch/")
            .and_then(|s| s.strip_suffix("/revision"))
            .unwrap_or(cell_name)
            .into()
    }

    /// The cached remote revision, if resolved.
    pub fn revision(&self) -> Option<Revision> {
        self.revision.get()
    }

    /// The remote address.
    pub fn address(&self) -> RemoteAddress {
        self.repository.address()
    }

    /// Open the remote branch (resolves, no error if missing).
    pub fn open(self) -> OpenRemoteBranch {
        OpenRemoteBranch::new(self)
    }

    /// Load the remote branch (error if not found).
    pub fn load(self) -> LoadRemoteBranch {
        LoadRemoteBranch::new(self)
    }
}

impl RemoteRepository {
    /// Get a branch reference at this remote repository.
    pub fn branch(&self, name: impl Into<BranchName>) -> RemoteBranchReference {
        let name = name.into();
        let revision = self
            .site()
            .cell(format!("branch/{}/revision", name.as_str()));
        RemoteBranchReference {
            repository: self.clone(),
            revision,
        }
    }
}
