//! Reference for navigating to a remote branch.
//!
//! A [`RemoteBranchReference`] pairs two capability references rooted at
//! *different* subjects:
//!
//! - [`RemoteReference`] — rooted at the **enclosing repository's**
//!   subject, path `memory/remote/{name}`. The snapshot cache cell
//!   hangs off this chain locally.
//! - [`BranchReference`] — rooted at the **remote repository's**
//!   subject, path `memory/branch/{name}`. This is what fork-based
//!   resolve/publish targets over the wire.
//!
//! These two chains do **not** nest: the branch capability is not a
//! sub-path under `remote/{name}`; it's a sibling chain on a different
//! subject. Both are needed — one names where to cache locally, the
//! other names what to read/write on the remote.
//!
//! The reference carries no loaded state; use
//! [`open`](RemoteBranchReference::open) or
//! [`load`](RemoteBranchReference::load) to resolve the address and
//! produce a [`RemoteBranch`].

use dialog_capability::Subject;
use dialog_effects::memory::Edition;

use super::{LoadRemoteBranch, OpenRemoteBranch};
use crate::repository::branch::BranchReference;
use crate::repository::memory::{Cell, RepositoryMemoryExt};
use crate::repository::remote::{RemoteAddress, RemoteReference, RemoteRepository};
use crate::repository::revision::Revision;

/// Cached snapshot of the remote branch's last known state: the remote
/// revision paired with the remote's CAS version, so a fresh
/// [`RemoteBranch`] can prime its in-memory upstream cell cache without
/// hitting the network.
pub type RemoteEdition = Edition<Revision>;

/// A reference to a named branch in a remote repository.
///
/// Holds the capability references for the remote and the branch, but
/// none of the loaded state — `open` / `load` produce a [`RemoteBranch`]
/// that carries the loaded address and cells.
///
/// See the module-level docs for why both references are needed and how
/// their capability chains relate.
#[derive(Debug, Clone)]
pub struct RemoteBranchReference {
    /// Names the remote under the enclosing repository's subject.
    ///
    /// Rooted at the enclosing repo's subject; path
    /// `memory/remote/{remote_name}`. The snapshot cache and retained
    /// address both live off this chain (see
    /// [`address`](Self::address) and [`cache`](Self::cache)).
    pub remote: RemoteReference,
    /// Names the branch at the *remote* repository's subject.
    ///
    /// Rooted at the remote repo's subject; path
    /// `memory/branch/{branch_name}`. Its `revision` cell (see
    /// [`revision`](Self::revision)) is what fork-based resolve/publish
    /// targets on the wire.
    pub branch: BranchReference,
}

impl RemoteBranchReference {
    /// Construct from a remote reference and a branch reference.
    pub fn new(remote: RemoteReference, branch: BranchReference) -> Self {
        Self { remote, branch }
    }

    /// The branch name.
    pub fn name(&self) -> &str {
        self.branch.name()
    }

    /// Cell holding the retained remote address for the enclosing remote.
    ///
    /// Rooted at the enclosing repo's subject; path
    /// `memory/remote/{remote_name}/address`.
    pub fn address(&self) -> Cell<RemoteAddress> {
        self.remote.address()
    }

    /// Cell holding the cached remote edition for this branch.
    ///
    /// Rooted at the enclosing repo's subject; path
    /// `memory/remote/{remote_name}/branch/{branch_name}/revision`.
    /// This is the local snapshot — it lives under the enclosing
    /// repository's subject, not the remote's.
    pub fn cache(&self) -> Cell<RemoteEdition> {
        self.remote.cell(format!("branch/{}/revision", self.name()))
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

    /// Open the remote branch (resolves address, then local cache; no
    /// error if the cache is empty).
    pub fn open(self) -> OpenRemoteBranch {
        self.into()
    }

    /// Load the remote branch (resolves address, then local cache;
    /// errors if the cache has no revision).
    pub fn load(self) -> LoadRemoteBranch {
        self.into()
    }
}

/// A [`BranchReference`] paired with the already-loaded
/// [`RemoteRepository`] it lives at.
///
/// Produced by [`RemoteRepository::branch`] to avoid re-resolving the
/// address cell that the parent [`RemoteRepository`] already loaded.
#[derive(Debug, Clone)]
pub struct LoadedRemoteBranchReference {
    /// The loaded remote repository (site + retained address).
    pub repository: RemoteRepository,
    /// Names the branch at the remote repository's subject.
    pub branch: BranchReference,
}

impl RemoteRepository {
    /// A reference to a named branch at this remote repository.
    ///
    /// The returned reference carries this already-loaded
    /// [`RemoteRepository`], so subsequent `open` / `load` calls skip
    /// re-resolving the address cell.
    pub fn branch(&self, name: impl Into<String>) -> LoadedRemoteBranchReference {
        let branch = Subject::from(self.did()).branch(name);
        LoadedRemoteBranchReference {
            repository: self.clone(),
            branch,
        }
    }
}

impl LoadedRemoteBranchReference {
    /// The branch name.
    pub fn name(&self) -> &str {
        self.branch.name()
    }

    /// Cell holding the cached remote edition for this branch.
    ///
    /// Rooted at the enclosing repo's subject; path
    /// `memory/remote/{remote_name}/branch/{branch_name}/revision`.
    pub fn cache(&self) -> Cell<RemoteEdition> {
        self.repository
            .site()
            .cell(format!("branch/{}/revision", self.name()))
    }

    /// Cell representing the remote's own branch revision.
    ///
    /// Rooted at the remote repo's subject; path
    /// `memory/branch/{branch_name}/revision`.
    pub fn revision(&self) -> Cell<Revision> {
        self.branch.revision()
    }

    /// Open the remote branch (resolves local cache, no error if missing).
    pub fn open(self) -> OpenRemoteBranch {
        self.into()
    }

    /// Load the remote branch (errors if the local cache has no revision).
    pub fn load(self) -> LoadRemoteBranch {
        self.into()
    }
}
