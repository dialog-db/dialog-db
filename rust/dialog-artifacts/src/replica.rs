//! Replica system for distributed database state management.
//!
//! This module provides the core abstractions for managing replica state,
//! including branches, revisions, and synchronization with remote replicas.

mod archive;
mod branch;
mod error;
mod issuer;
mod remote;
mod remote_types;
mod types;
mod upstream;

use dialog_storage::CborEncoder;

use crate::platform::{PlatformBackend, Storage as PlatformStorage};

// Re-export public types
pub use archive::Archive;
pub use branch::{Branch, Index, Upstream};
pub use error::{OperationContext, ReplicaError};
pub use issuer::Issuer;
pub use remote::{Remote, RemoteBranch};
pub use remote_types::{RemoteBackend, RemoteBranchState, RemoteState};
pub use types::{
    BranchId, BranchState, Edition, NodeReference, Occurence, Principal, Revision, Site,
    EMPT_TREE_HASH,
};
pub use upstream::UpstreamState;

/// Manages multiple branches within a replica.
#[derive(Debug)]
pub struct Branches<Backend: PlatformBackend> {
    issuer: Issuer,
    storage: PlatformStorage<Backend>,
}

impl<Backend: PlatformBackend + 'static> Branches<Backend> {
    /// Creates a new instance for the given backend.
    pub fn new(issuer: Issuer, backend: Backend) -> Self {
        let storage = PlatformStorage::new(backend, CborEncoder);
        Self { issuer, storage }
    }

    /// Loads a branch with given identifier, produces an error if it does not exist.
    pub async fn load(&self, id: &BranchId) -> Result<Branch<Backend>, ReplicaError> {
        Branch::load(id, self.issuer.clone(), self.storage.clone()).await
    }

    /// Loads a branch with the given identifier or creates a new one if
    /// it does not already exist.
    pub async fn open(&self, id: &BranchId) -> Result<Branch<Backend>, ReplicaError> {
        Branch::open(id, self.issuer.clone(), self.storage.clone()).await
    }
}

/// Manages remote repositories for synchronization.
#[derive(Debug)]
pub struct Remotes<Backend: PlatformBackend> {
    storage: PlatformStorage<Backend>,
}

impl<Backend: PlatformBackend> Remotes<Backend> {
    /// Creates a new remotes manager for the given backend.
    pub fn new(backend: Backend) -> Self {
        let storage = PlatformStorage::new(backend, CborEncoder);
        Self { storage }
    }

    /// Loads an existing remote repository by name.
    pub async fn load(&self, site: &Site) -> Result<Remote<Backend>, ReplicaError> {
        Remote::setup(site, self.storage.clone()).await
    }

    /// Adds a new remote repository with the given name and address.
    pub async fn add(&mut self, state: RemoteState) -> Result<Remote<Backend>, ReplicaError> {
        Remote::add(state, self.storage.clone()).await
    }
}

/// A replica represents a local instance of a distributed database.
#[derive(Debug)]
pub struct Replica<Backend: PlatformBackend> {
    issuer: Issuer,
    #[allow(dead_code)]
    storage: PlatformStorage<Backend>,
    /// Remote repositories for synchronization.
    pub remotes: Remotes<Backend>,
    /// Local branches in this replica.
    pub branches: Branches<Backend>,
}

impl<Backend: PlatformBackend + 'static> Replica<Backend> {
    /// Creates a new replica with the given issuer and storage backend.
    pub fn open(issuer: Issuer, backend: Backend) -> Result<Self, ReplicaError> {
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);

        let branches = Branches::new(issuer.clone(), backend.clone());
        let remotes = Remotes::new(backend.clone());
        Ok(Replica {
            issuer,
            storage,
            remotes,
            branches,
        })
    }

    /// Returns the principal (public key) of the issuer for this replica.
    pub fn principal(&self) -> &Principal {
        self.issuer.principal()
    }
}
