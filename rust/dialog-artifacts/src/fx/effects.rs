//! Effect trait definitions for local and remote storage operations.

use super::errors::{NetworkError, StorageError};
use super::local::Address as LocalAddress;
use super::remote::Address as RemoteAddress;
pub use dialog_common::fx::{effect, effectful};

/// Local blob storage (content-addressed).
///
/// Provides get/set operations for content-addressed data stored locally.
/// This effect never touches the network.
#[effect]
pub trait LocalStore {
    /// Get a value by key from local storage at the given DID.
    async fn get(&self, did: LocalAddress, key: Vec<u8>) -> Result<Option<Vec<u8>>, StorageError>;
    /// Set a value by key in local storage at the given DID.
    async fn set(&mut self, did: LocalAddress, key: Vec<u8>, value: Vec<u8>) -> Result<(), StorageError>;
}

/// Local transactional memory (CAS-based state).
///
/// Provides resolve/replace operations for atomic state updates.
/// This effect is purely local - no network fallback.
#[effect]
pub trait LocalMemory {
    /// Resolve the current value and edition at the given address.
    /// Returns None if the address doesn't exist locally.
    async fn resolve(&self, did: LocalAddress, address: Vec<u8>) -> Result<Option<(Vec<u8>, Vec<u8>)>, StorageError>;

    /// Replace the value at the given address with compare-and-swap semantics.
    /// Returns the new edition on success, or error on conflict.
    async fn replace(
        &mut self,
        did: LocalAddress,
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
    async fn get(&self, site: RemoteAddress, key: Vec<u8>) -> Result<Option<Vec<u8>>, NetworkError>;
    /// Import multiple blocks to a remote site (used by push protocol).
    /// Provider can upload concurrently using TaskQueue.
    async fn import(
        &mut self,
        site: RemoteAddress,
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
        site: RemoteAddress,
        address: Vec<u8>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, NetworkError>;

    /// Replace the value at the given address on a remote site with CAS semantics.
    async fn replace(
        &mut self,
        site: RemoteAddress,
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
pub trait Env: LocalStore + LocalMemory + RemoteStore + RemoteMemory {}
