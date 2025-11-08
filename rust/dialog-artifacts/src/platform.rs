#![allow(missing_docs)]

use std::fmt::Debug;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::replica::{BranchId, Site};
use async_trait::async_trait;
use dialog_common::{ConditionalSend, ConditionalSync, SharedCell};
use dialog_storage::{
    CborEncoder, DialogStorageError, Encoder, StorageBackend, TransactionalMemory,
    TransactionalMemoryBackend, State,
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
///
/// The Edition type must be 'static to support caching with weak references.
pub trait PlatformBackend:
    StorageBackend<Key = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
    + TransactionalMemoryBackend<
        Address = Vec<u8>,
        Value = Vec<u8>,
        Error = DialogStorageError,
        Edition = Self::PlatformEdition,
    >
    + ConditionalSync
    + Clone
{
    /// The edition type for this backend, constrained to be 'static
    type PlatformEdition: Send + Sync + Clone + 'static;
}

// Blanket implementation - any backend that satisfies the bounds is a PlatformBackend
impl<B> PlatformBackend for B
where
    B: StorageBackend<Key = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + Clone,
    <B as TransactionalMemoryBackend>::Edition: Send + Sync + Clone + 'static,
{
    type PlatformEdition = <B as TransactionalMemoryBackend>::Edition;
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


/// Type-erased weak reference wrapper for cache storage
trait WeakStateRef: Send + Sync + std::any::Any {
    /// Try to upgrade the weak reference and check if it's still alive
    fn is_alive(&self) -> bool;

    /// Required for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Concrete implementation for a specific type
struct TypedWeakRef<T, Edition>(Weak<SharedCell<State<T, Edition>>>);

impl<T, Edition> WeakStateRef for TypedWeakRef<T, Edition>
where
    T: Send + Sync + 'static,
    Edition: Send + Sync + 'static,
{
    fn is_alive(&self) -> bool {
        self.0.strong_count() > 0
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Platform-specific storage with caching for TransactionalMemory instances.
///
/// This wrapper around Storage adds a cache of weak references to TransactionalMemory states.
/// When you call `open()` multiple times with the same address, you get back TransactionalMemory
/// instances that share the same underlying state, enabling optimistic concurrency control
/// across multiple accessors.
#[derive(Clone)]
pub struct PlatformStorage<Backend: PlatformBackend> {
    storage: Storage<Backend>,
    /// Cache of weak references to TransactionalMemory state, indexed by address
    /// Assumes each address is only used with one type T
    cache: Arc<SharedCell<HashMap<Vec<u8>, Arc<dyn WeakStateRef>>>>,
}

impl<Backend: PlatformBackend> PlatformStorage<Backend> {
    /// Creates a new platform storage with the given backend
    pub fn new(backend: Backend) -> Self {
        Self {
            storage: Storage::new(backend, CborEncoder),
            cache: Arc::new(SharedCell::new(HashMap::new())),
        }
    }

    /// Opens a transactional memory at the given key with caching.
    ///
    /// Multiple calls to `open()` with the same address and type will return
    /// TransactionalMemory instances that share the same underlying state.
    /// This sharing persists as long as at least one strong reference exists.
    pub async fn open<T>(
        &self,
        key: &Vec<u8>,
    ) -> Result<TransactionalMemory<T, Storage<Backend>>, DialogStorageError>
    where
        T: Serialize + DeserializeOwned + ConditionalSync + std::fmt::Debug + Clone + Send + Sync + 'static,
        <Backend as TransactionalMemoryBackend>::Edition: 'static,
    {
        // First, try to get from cache and upgrade weak reference
        {
            let cache = self.cache.read();
            if let Some(weak_ref) = cache.get(key) {
                // Downcast the Arc<dyn WeakStateRef> to access the concrete type
                if let Some(typed_weak) = weak_ref.as_any().downcast_ref::<TypedWeakRef<T, <Backend as TransactionalMemoryBackend>::Edition>>() {
                    // Try to upgrade the weak reference
                    if let Some(state) = typed_weak.0.upgrade() {
                        // We have a cached state! Create TransactionalMemory with it
                        return Ok(TransactionalMemory {
                            address: key.clone(),
                            state,
                            codec: CborEncoder,
                            policy: Default::default(),
                        });
                    }
                }
            }
        }

        // Not in cache or weak reference expired, create new instance
        let memory = self.storage.open(key).await?;

        // Store a weak reference in the cache
        {
            let weak = Arc::downgrade(&memory.state);
            let typed_weak = TypedWeakRef(weak);
            let arc_weak: Arc<dyn WeakStateRef> = Arc::new(typed_weak);
            let mut cache = self.cache.write();
            cache.insert(key.clone(), arc_weak);
        }

        Ok(memory)
    }

    /// Cleans up expired weak references from the cache
    pub fn cleanup_cache(&self) {
        let mut cache = self.cache.write();
        cache.retain(|_, weak_ref| {
            // Check if the weak reference is still alive
            weak_ref.is_alive()
        });
    }

    /// Gets the inner storage
    pub fn storage(&self) -> &Storage<Backend> {
        &self.storage
    }
}

impl<Backend: PlatformBackend> std::fmt::Debug for PlatformStorage<Backend> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlatformStorage")
            .field("storage", &self.storage)
            .field("cache", &format!("{} entries", self.cache.read().len()))
            .finish()
    }
}

/// Type alias for TransactionalMemory with default CborEncoder.
/// Type alias for backwards compatibility with old TypedStoreResource API.
/// Now uses TransactionalMemory from dialog_storage.
/// Both Storage and TransactionalMemory default to CborEncoder, so we don't need to specify it.
pub type TypedStoreResource<T, Backend> = TransactionalMemory<T, Storage<Backend>>;

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_storage::MemoryStorageBackend;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestBranchState {
        revision: String,
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_platform_storage_cache_shares_state() {
        // Create a PlatformStorage with an in-memory backend
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend);

        let key = b"branch/main".to_vec();

        // Open the same key twice
        let mut memory1 = storage.open::<TestBranchState>(&key).await.unwrap();
        let mut memory2 = storage.open::<TestBranchState>(&key).await.unwrap();

        // Initially both should be None
        assert_eq!(memory1.read(), None);
        assert_eq!(memory2.read(), None);

        // Update through memory1
        let state1 = TestBranchState {
            revision: "rev1".to_string(),
        };
        memory1.replace(Some(state1.clone()), storage.storage()).await.unwrap();

        // memory2 should see the update because they share the same state
        assert_eq!(memory2.read(), Some(state1.clone()));

        // Update through memory2
        let state2 = TestBranchState {
            revision: "rev2".to_string(),
        };
        memory2.replace(Some(state2.clone()), storage.storage()).await.unwrap();

        // memory1 should see the update
        assert_eq!(memory1.read(), Some(state2.clone()));

        // Open a third time - should still get shared state
        let memory3 = storage.open::<TestBranchState>(&key).await.unwrap();
        assert_eq!(memory3.read(), Some(state2));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_platform_storage_cache_cleanup() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend);

        let key1 = b"branch/main".to_vec();
        let key2 = b"branch/dev".to_vec();

        // Open two different keys
        {
            let _memory1 = storage.open::<TestBranchState>(&key1).await.unwrap();
            let _memory2 = storage.open::<TestBranchState>(&key2).await.unwrap();
            // Both are in cache now
        }
        // After dropping, weak references should be expired

        // Cleanup should remove expired entries
        storage.cleanup_cache();

        // Cache should be empty or have only living references
        let cache_size = storage.cache.read().len();
        assert_eq!(cache_size, 0, "Cache should be empty after cleanup");
    }
}

