//! Reference for navigating to a named branch on a loaded remote.
//!
//! A [`RemoteBranchReference`] pairs a [`RemoteRepository`] with a
//! [`BranchReference`] rooted at the remote's subject. Produced by
//! [`RemoteRepository::branch`].

use crate::{
    BranchReference, Cell, LoadRemoteBranch, OpenRemoteBranch, RemoteRepository,
    RepositoryMemoryExt, Revision,
};
use dialog_capability::Subject;
use dialog_effects::memory::Edition;

/// Cached snapshot of the remote branch's last known state: the remote
/// revision paired with the remote's CAS version, so a fresh
/// [`RemoteBranch`] can prime its in-memory upstream cell cache without
/// hitting the network.
pub type RemoteEdition = Edition<Revision>;

/// A reference to a named branch on a loaded remote repository.
///
/// Carries the parent [`RemoteRepository`] (already-loaded address) and
/// names a branch on it. Produced by [`RemoteRepository::branch`].
#[derive(Debug, Clone)]
pub struct RemoteBranchReference {
    /// The loaded remote repository this branch lives on.
    pub repository: RemoteRepository,
    /// Names the branch at the remote repository's subject. Rooted at
    /// the remote repo's subject; path `memory/branch/{branch_name}`.
    pub branch: BranchReference,
}

impl RemoteRepository {
    /// A reference to a named branch at this remote repository.
    pub fn branch(&self, name: impl Into<String>) -> RemoteBranchReference {
        RemoteBranchReference {
            repository: self.clone(),
            branch: Subject::from(self.did()).branch(name),
        }
    }
}

impl RemoteBranchReference {
    /// The branch name.
    pub fn name(&self) -> &str {
        self.branch.name()
    }

    /// Cell holding the cached remote edition for this branch.
    ///
    /// Rooted at the enclosing repo's subject; path
    /// `memory/remote/{remote_name}/branch/{branch_name}/revision`.
    /// This is the local snapshot — it lives under the enclosing
    /// repository's subject, not the remote's.
    pub fn cache(&self) -> Cell<RemoteEdition> {
        self.repository
            .site()
            .cell(format!("branch/{}/revision", self.name()))
    }

    /// Cell representing the remote's own branch revision.
    ///
    /// Rooted at the remote repo's subject; path
    /// `memory/branch/{branch_name}/revision`. Used as the in-memory
    /// handle for fork-based resolve/publish — reads and writes through
    /// this cell cross the network.
    pub fn revision(&self) -> Cell<Revision> {
        self.branch.revision()
    }

    /// Open the remote branch (resolves local cache, no error if missing).
    pub fn open(self) -> OpenRemoteBranch {
        OpenRemoteBranch::new(self.repository, self.branch)
    }

    /// Load the remote branch (errors if the local cache has no revision).
    pub fn load(self) -> LoadRemoteBranch {
        LoadRemoteBranch::new(self.repository, self.branch)
    }
}
