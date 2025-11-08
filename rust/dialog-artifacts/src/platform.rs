#![allow(missing_docs)]

use std::fmt::Debug;

use crate::replica::{BranchId, Site};
use async_trait::async_trait;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_storage::{
    CborEncoder, DialogStorageError, Encoder, StorageBackend, TransactionalMemory,
    TransactionalMemoryBackend,
};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

/// Errors that can occur when working with journal storage.
#[derive(Error, Debug)]
pub enum JournalError {
    /// Resolving a branch failed
    #[error("Failed to resolve branch {0}")]
    ResolveError(ResolveError),
    /// Encoding a revision failed
    #[error("Failed to encode revision: {0}")]
    EncodeError(String),
}

/// Errors that can occur when working with remote repositories.
#[derive(Error, Debug)]
pub enum RemoteError {
    #[error("Remote \"{remote}\" not found")]
    NotFound { remote: Site },
}

#[derive(Error, Debug, Clone)]
pub enum ResolveError {
    /// Error produced
    #[error("Branch not found")]
    BranchNotFound { branch: BranchId },
    #[error("Failed to resolve branch revision for {branch} from storage due to: {cause}")]
    StorageError { branch: BranchId, cause: String },
    #[error("Failed to decode branch revision for {branch}")]
    DecodeError { branch: BranchId },
}

#[derive(Error, Debug, Clone)]
pub enum FetchError {
    /// Error produced
    #[error("Branch not found")]
    BranchNotFound { branch: BranchId },
    #[error("Failed to read branch revision for {branch} due to: {cause}")]
    NetworkError { branch: BranchId, cause: String },
}

/// Address trait for keys that can be used with storage
/// Both Vec<u8> and Blake3Hash implement this, though they're used in different contexts
pub trait Address: ConditionalSync + Clone + AsRef<[u8]> + std::fmt::Debug {}

// Blanket implementation - any type satisfying the bounds automatically implements Address
impl<T: ConditionalSync + Clone + AsRef<[u8]> + std::fmt::Debug> Address for T {}

/// A storage backend with all the necessary bounds for platform operations.
///
/// This is a convenience trait for backends that use Vec<u8> keys (the common case
/// for platform storage like branches and remotes). Storage is flexible and works
/// with any key type implementing `From<Vec<u8>> + AsRef<[u8]>`.
pub trait PlatformBackend:
    StorageBackend<Key = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
    + TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
    + ConditionalSync
    + Clone
{
}

// Blanket implementation - any backend that satisfies the bounds is a PlatformBackend
impl<B> PlatformBackend for B where
    B: StorageBackend<Key = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + Clone
{
}

/// Adapter that maps a backend's error type to DialogStorageError
#[derive(Clone, Debug)]
pub struct ErrorMappingBackend<B> {
    inner: B,
}

impl<B> ErrorMappingBackend<B> {
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<B> StorageBackend for ErrorMappingBackend<B>
where
    B: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + ConditionalSync,
    B::Error: Into<DialogStorageError> + ConditionalSync,
{
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.inner.set(key, value).await.map_err(|e| e.into())
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.get(key).await.map_err(|e| e.into())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<B> TransactionalMemoryBackend for ErrorMappingBackend<B>
where
    B: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>> + ConditionalSync,
    B::Error: Into<DialogStorageError> + ConditionalSync,
    B::Edition: ConditionalSend + ConditionalSync + Clone,
{
    type Address = Vec<u8>;
    type Value = Vec<u8>;
    type Error = DialogStorageError;
    type Edition = B::Edition;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        self.inner.resolve(address).await.map_err(|e| e.into())
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        self.inner
            .replace(address, edition, content)
            .await
            .map_err(|e| e.into())
    }
}

/// A transactional storage wraps a backend and encoder, providing
/// a foundation for creating typed stores.
#[derive(Clone)]
pub struct Storage<Backend: StorageBackend, Codec: Encoder = CborEncoder>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Clone + Eq + std::hash::Hash,
{
    backend: Backend,
    codec: Codec,
}

impl<Backend: StorageBackend, Codec: Encoder> std::fmt::Debug for Storage<Backend, Codec>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Clone + Eq + std::hash::Hash,
    Codec: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field("backend", &"<Backend>")
            .field("codec", &self.codec)
            .finish()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend: StorageBackend, Codec: Encoder> Encoder for Storage<Backend, Codec>
where
    Backend: TransactionalMemoryBackend + ConditionalSync,
    Backend::Address: Clone + Eq + std::hash::Hash,
    <Backend as TransactionalMemoryBackend>::Value: Sync,
    Codec: ConditionalSync,
{
    type Bytes = Codec::Bytes;
    type Hash = Codec::Hash;
    type Error = Codec::Error;

    async fn encode<V>(&self, block: &V) -> Result<(Self::Hash, Self::Bytes), Self::Error>
    where
        V: serde::Serialize + ConditionalSync + std::fmt::Debug,
    {
        self.codec.encode(block).await
    }

    async fn decode<V>(&self, bytes: &[u8]) -> Result<V, Self::Error>
    where
        V: serde::de::DeserializeOwned + ConditionalSync,
    {
        self.codec.decode(bytes).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend: StorageBackend, Codec: Encoder> StorageBackend for Storage<Backend, Codec>
where
    Backend: TransactionalMemoryBackend + ConditionalSync,
    Backend::Key: From<Vec<u8>> + AsRef<[u8]> + ConditionalSync,
    <Backend as TransactionalMemoryBackend>::Address: Clone + Eq + std::hash::Hash,
    <Backend as TransactionalMemoryBackend>::Value: Sync,
    <Backend as StorageBackend>::Value: ConditionalSync,
    <Backend as StorageBackend>::Error: ConditionalSync,
    Codec: ConditionalSync,
{
    type Key = Backend::Key;
    type Value = <Backend as StorageBackend>::Value;
    type Error = <Backend as StorageBackend>::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.backend.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        self.backend.get(key).await
    }
}

impl<Backend: StorageBackend, Codec: Encoder> Storage<Backend, Codec>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Clone + Eq + std::hash::Hash,
{
    /// Creates a new transactional storage with the given backend and encoder
    pub fn new(backend: Backend, codec: Codec) -> Self {
        Self { backend, codec }
    }

    /// Opens a transactional memory at the given key.
    /// This provides encoding/decoding and caches the decoded value.
    pub async fn open<T>(
        &self,
        key: &Backend::Address,
    ) -> Result<TransactionalMemory<T, Self, Codec>, DialogStorageError>
    where
        T: Serialize + DeserializeOwned + ConditionalSync + std::fmt::Debug + Clone,
        Backend: TransactionalMemoryBackend<Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
        Backend::Address: Clone + AsRef<[u8]> + From<Vec<u8>>,
        Backend::Edition: Clone,
        Codec: ConditionalSync + Clone,
        Codec::Bytes: AsRef<[u8]>,
        Codec::Error: std::fmt::Display,
        Self: Clone,
    {
        TransactionalMemory::open(key.clone(), self, self.codec.clone()).await
    }
}

// Implement TransactionalMemoryBackend for Storage so it can be passed directly to replace/reload
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend: StorageBackend, Codec: Encoder> TransactionalMemoryBackend for Storage<Backend, Codec>
where
    Backend: TransactionalMemoryBackend + ConditionalSync,
    Backend::Address: Clone + Eq + std::hash::Hash + AsRef<[u8]> + From<Vec<u8>> + ConditionalSync,
    <Backend as TransactionalMemoryBackend>::Value: ConditionalSync + Sync,
    <Backend as TransactionalMemoryBackend>::Edition: ConditionalSync,
    <Backend as TransactionalMemoryBackend>::Error: ConditionalSync,
    Codec: ConditionalSync,
{
    type Address = Backend::Address;
    type Value = <Backend as TransactionalMemoryBackend>::Value;
    type Edition = <Backend as TransactionalMemoryBackend>::Edition;
    type Error = <Backend as TransactionalMemoryBackend>::Error;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        self.backend.resolve(address).await
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        self.backend.replace(address, edition, content).await
    }
}

/// Type alias for TransactionalMemory with default CborEncoder.
/// Type alias for backwards compatibility with old TypedStoreResource API.
/// Now uses TransactionalMemory from dialog_storage.
/// Both Storage and TransactionalMemory default to CborEncoder, so we don't need to specify it.
pub type TypedStoreResource<T, Backend> = TransactionalMemory<T, Storage<Backend>>;
