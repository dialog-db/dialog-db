//! Effect trait definitions for storage operations.
//!
//! This module defines generic effect traits parameterized over address type,
//! enabling least-authority design where code only has access to the storage
//! locations it needs.
//!
//! # Design
//!
//! - `Memory<A>` - Transactional memory (CAS-based state) at address type `A`
//! - `Store<A>` - Blob storage (content-addressed) at address type `A`
//!
//! By parameterizing over address type:
//! - `Memory<LocalAddress>` only accesses local storage
//! - `Memory<RemoteAddress>` only accesses remote storage
//! - Code requiring `Memory<LocalAddress>` cannot touch remote storage
//!
//! For backwards compatibility, type aliases are provided:
//! - `LocalMemory` = `Memory<LocalAddress>`
//! - `RemoteMemory` = `Memory<RemoteAddress>`
//! - `LocalStore` = `Store<LocalAddress>`
//! - `RemoteStore` = `Store<RemoteAddress>`

use super::errors::MemoryError;
use super::local::Address as LocalAddress;
use super::remote::Address as RemoteAddress;
pub use dialog_common::fx::{effect, effectful};

// =============================================================================
// Generic Memory Effect
// =============================================================================

/// Transactional memory (CAS-based state) at a given address type.
///
/// Provides resolve/replace operations for atomic state updates.
/// The address type determines whether this is local or remote storage.
///
/// # Type Parameter
///
/// - `A`: The address type (e.g., `LocalAddress` or `RemoteAddress`)
///
/// # Least Authority
///
/// Code that only needs local memory should use `Memory<LocalAddress>`.
/// This prevents it from accidentally accessing remote storage.
#[effect]
pub trait Memory<A: Clone + Send + Sync + 'static> {
    /// Resolve the current value and edition at the given address.
    /// Returns None if the address doesn't exist.
    async fn resolve(
        &self,
        address: A,
        key: Vec<u8>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MemoryError>;

    /// Replace the value at the given address with compare-and-swap semantics.
    /// Returns the new edition on success, or error on conflict.
    async fn replace(
        &mut self,
        address: A,
        key: Vec<u8>,
        edition: Option<Vec<u8>>,
        content: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, MemoryError>;
}


// =============================================================================
// Generic Store Effect
// =============================================================================

/// Blob storage (content-addressed) at a given address type.
///
/// Provides get/set/import operations for content-addressed data.
/// The address type determines whether this is local or remote storage.
///
/// # Type Parameter
///
/// - `A`: The address type (e.g., `LocalAddress` or `RemoteAddress`)
#[effect]
pub trait Store<A: Clone + Send + Sync + 'static> {
    /// Get a value by key from storage at the given address.
    async fn get(&self, address: A, key: Vec<u8>) -> Result<Option<Vec<u8>>, MemoryError>;

    /// Set a value by key in storage at the given address.
    async fn set(&mut self, address: A, key: Vec<u8>, value: Vec<u8>) -> Result<(), MemoryError>;

    /// Import multiple key-value pairs in a batch.
    ///
    /// For remote stores, this can be implemented more efficiently than calling
    /// `set` for each block. For local stores, a default implementation can
    /// simply iterate and call `set`.
    async fn import(
        &mut self,
        address: A,
        blocks: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), MemoryError>;
}


// =============================================================================
// Composite Environment
// =============================================================================

/// Composite environment that provides all replica effects.
///
/// This trait combines local and remote Memory and Store capabilities
/// into a single environment that can handle all replica operations.

pub trait Env:
    Memory<LocalAddress> + Memory<RemoteAddress> + Store<LocalAddress> + Store<RemoteAddress>
{
}

// =============================================================================
// Temporary bridge effects for ContentAddressedStorage integration
// =============================================================================

use super::errors::{NetworkError, StorageError};
use dialog_storage::StorageBackend;

/// Acquire local storage backends for direct access.
///
/// **Temporary bridge effect**: This exists solely to support `Archive::acquire()`
/// which needs to construct an `ArchiveStore` implementing `ContentAddressedStorage`
/// for use with prolly trees. Once the tree is made effectful, this effect will
/// be removed and all storage access will go through `Store`.
#[effect]
pub trait LocalBackend {
    /// The storage backend type returned by this provider.
    type Backend: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + 'static;

    /// Acquire the underlying storage backend for a given DID.
    #[doc(hidden)]
    async fn backend(&self, did: LocalAddress) -> Result<Self::Backend, StorageError>;
}

/// Acquire remote storage backends for direct access.
///
/// **Temporary bridge effect**: This exists solely to support `Archive::acquire()`
/// which needs to construct an `ArchiveStore` implementing `ContentAddressedStorage`
/// for use with prolly trees. Once the tree is made effectful, this effect will
/// be removed and all storage access will go through `Store`.
#[effect]
pub trait RemoteBackend {
    /// The storage backend type returned by this provider.
    type Backend: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + 'static;

    /// Acquire the underlying storage backend for a given remote site.
    #[doc(hidden)]
    async fn backend(&self, site: RemoteAddress) -> Result<Self::Backend, NetworkError>;
}
