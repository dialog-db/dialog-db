//! Algebraic effects system.
//!
//! This module re-exports the core effect types from `dialog_common::fx`
//! and provides the replica effects for local/remote storage.
//!
//! # Design
//!
//! The effects are split into local and remote concerns:
//!
//! - **Local effects** (`LocalStore`, `LocalMemory`) - For local storage operations.
//!   These never touch the network.
//!
//! - **Remote effects** (`RemoteStore`, `RemoteMemory`) - For network operations.
//!   These require a `Site` with connection information.
//!
//! The "archive" pattern (try local, fallback remote, cache locally) is implemented
//! as explicit effectful code rather than hidden in providers.

pub use dialog_common::fx::{effect, effectful, provider};
use dialog_storage::{DialogStorageError, RestStorageConfig};
use thiserror::Error;

/// Identifies a remote site with connection information.
///
/// Contains the information needed to establish a connection to a remote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Site {
    /// REST API endpoint.
    Rest(RestStorageConfig),
}

impl Site {
    /// Create a new REST site.
    pub fn rest(config: RestStorageConfig) -> Self {
        Site::Rest(config)
    }

    /// Get a display name for this site (for logging/errors).
    pub fn name(&self) -> String {
        match self {
            Site::Rest(config) => config.endpoint.clone(),
        }
    }
}

/// Error type for local storage operations.
#[derive(Debug, Clone, Error)]
pub enum StorageError {
    /// Generic storage error.
    #[error("Storage error: {0}")]
    Storage(String),
    /// Key not found.
    #[error("Not found: {0}")]
    NotFound(String),
    /// Compare-and-swap conflict.
    #[error("Conflict: {0}")]
    Conflict(String),
}

impl From<DialogStorageError> for StorageError {
    fn from(e: DialogStorageError) -> Self {
        StorageError::Storage(format!("{:?}", e))
    }
}

/// Error type for network/remote operations.
#[derive(Debug, Clone, Error)]
pub enum NetworkError {
    /// Generic network error.
    #[error("Network error: {0}")]
    Network(String),
    /// Unknown site.
    #[error("Unknown site: {0}")]
    UnknownSite(String),
    /// Connection failed.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    /// Timeout.
    #[error("Timeout: {0}")]
    Timeout(String),
}

impl From<DialogStorageError> for NetworkError {
    fn from(e: DialogStorageError) -> Self {
        NetworkError::Network(format!("{:?}", e))
    }
}

/// Local blob storage (content-addressed).
///
/// Provides get/set operations for content-addressed data stored locally.
/// This effect never touches the network.
#[effect]
pub trait LocalStore {
    /// Get a value by key from local storage.
    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, StorageError>;
    /// Set a value by key in local storage.
    async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), StorageError>;
}

/// Local transactional memory (CAS-based state).
///
/// Provides resolve/replace operations for atomic state updates.
/// This effect is purely local - no network fallback.
#[effect]
pub trait LocalMemory {
    /// Resolve the current value and edition at the given address.
    /// Returns None if the address doesn't exist locally.
    async fn resolve(&self, address: Vec<u8>) -> Result<Option<(Vec<u8>, Vec<u8>)>, StorageError>;

    /// Replace the value at the given address with compare-and-swap semantics.
    /// Returns the new edition on success, or error on conflict.
    async fn replace(
        &mut self,
        address: Vec<u8>,
        edition: Option<Vec<u8>>,
        content: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, StorageError>;
}

/// Remote blob storage (network).
///
/// Provides get and import operations for content-addressed data on remote sites.
/// The provider is responsible for managing connections to the sites.
#[effect]
pub trait RemoteStore {
    /// Get a single block by key from a remote site.
    async fn get(&self, site: Site, key: Vec<u8>) -> Result<Option<Vec<u8>>, NetworkError>;
    /// Import multiple blocks to a remote site (used by push protocol).
    /// Provider can upload concurrently using TaskQueue.
    async fn import(
        &mut self,
        site: Site,
        blocks: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), NetworkError>;
}

/// Remote transactional memory (for branch state).
///
/// Provides resolve/replace operations for atomic state updates on remote sites.
/// The provider is responsible for managing connections to the sites.
#[effect]
pub trait RemoteMemory {
    /// Resolve the current value and edition at the given address on a remote site.
    async fn resolve(
        &self,
        site: Site,
        address: Vec<u8>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, NetworkError>;

    /// Replace the value at the given address on a remote site with CAS semantics.
    async fn replace(
        &mut self,
        site: Site,
        address: Vec<u8>,
        edition: Option<Vec<u8>>,
        content: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, NetworkError>;
}

/// Composite environment that provides all replica effects.
///
/// This trait combines LocalStore, LocalMemory, RemoteStore, and RemoteMemory
/// into a single environment that can handle all replica operations.
#[effect]
pub trait ReplicaEnv: LocalStore + LocalMemory + RemoteStore + RemoteMemory {}
