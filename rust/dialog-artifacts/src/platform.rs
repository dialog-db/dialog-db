#![allow(missing_docs)]

use std::fmt::Debug;
use std::sync::Arc;

use crate::replica::{BranchId, Revision, Site};
use async_trait::async_trait;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_storage::{
    CborEncoder, DialogStorageError, Encoder, State, StorageBackend, TransactionalMemoryBackend,
    TypedState, TypedTransactionalMemory,
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
/// for platform storage like branches and remotes). TypedStore and Storage are more
/// flexible - they work with any key type implementing `From<Vec<u8>> + AsRef<[u8]>`.
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

/// A resource wrapper that maps storage errors to platform-specific errors.
#[derive(Debug, Clone)]
pub struct ErrorMappingResource<R> {
    inner: R,
}

/// Transparent wrapper for path strings used in storage namespacing
#[repr(transparent)]
pub struct Path<'a>(&'a str);

impl<'a> From<&'a str> for Path<'a> {
    fn from(s: &'a str) -> Self {
        Path(s)
    }
}

/// A transactional storage wraps a backend and encoder, providing
/// a foundation for creating typed stores with namespacing.
#[derive(Clone)]
pub struct Storage<Backend: StorageBackend, Codec: Encoder = CborEncoder>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Clone + Eq + std::hash::Hash,
{
    backend: Backend,
    codec: Codec,
    path: Option<String>,
    /// Cache of open TransactionalMemory instances, keyed by address.
    /// Uses Weak references so entries are automatically cleaned up when all strong refs are dropped.
    cache: Arc<
        dialog_common::SharedCell<
            std::collections::HashMap<
                Backend::Address,
                std::sync::Weak<dialog_common::SharedCell<dialog_storage::State<Backend>>>,
            >,
        >,
    >,
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
            .field("path", &self.path)
            .field("cache", &"<Cache>")
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
        let prefixed_key = self.prefix_key(key.as_ref()).into();
        self.backend.set(prefixed_key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let prefixed_key = self.prefix_key(key.as_ref()).into();
        self.backend.get(&prefixed_key).await
    }
}

impl<Backend: StorageBackend, Codec: Encoder> Storage<Backend, Codec>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Clone + Eq + std::hash::Hash,
{
    /// Creates a new transactional storage with the given backend and encoder
    pub fn new(backend: Backend, codec: Codec) -> Self {
        Self {
            backend,
            codec,
            path: None,
            cache: Arc::new(dialog_common::SharedCell::new(
                std::collections::HashMap::new(),
            )),
        }
    }

    /// Creates a namespaced storage at the given path
    pub fn at<'a>(&self, path: impl Into<Path<'a>>) -> Self {
        let new_path = match &self.path {
            Some(existing) => format!("{}/{}", existing, path.into().0),
            None => path.into().0.to_string(),
        };
        Self {
            backend: self.backend.clone(),
            codec: self.codec.clone(),
            path: Some(new_path),
            cache: self.cache.clone(),
        }
    }

    /// Creates a typed store that handles encoding/decoding transparently
    pub fn mount<T>(&self) -> TypedStore<T, Backend, Codec> {
        TypedStore {
            backend: self.backend.clone(),
            codec: self.codec.clone(),
            path: self.path.clone(),
            cache: Arc::new(dialog_common::SharedCell::new(
                std::collections::HashMap::new(),
            )),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Opens a typed transactional memory at the given key.
    /// This provides encoding/decoding and caches the decoded value.
    pub async fn open<T>(
        &self,
        key: &Backend::Address,
    ) -> Result<TypedTransactionalMemory<T, Self, Codec>, DialogStorageError>
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
        let prefixed_key = self.prefix_key(key.as_ref()).into();
        TypedTransactionalMemory::open(prefixed_key, self, self.codec.clone()).await
    }

    /// Prefixes the key bytes with the path (if any) and separator
    fn prefix_key(&self, key_bytes: &[u8]) -> Vec<u8> {
        match &self.path {
            Some(path) => {
                let mut result = path.as_bytes().to_vec();
                result.push(b'/');
                result.extend_from_slice(key_bytes);
                result
            }
            None => key_bytes.to_vec(),
        }
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
        let prefixed_address = self.prefix_key(address.as_ref()).into();
        self.backend.resolve(&prefixed_address).await
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        let prefixed_address = self.prefix_key(address.as_ref()).into();
        self.backend
            .replace(&prefixed_address, edition, content)
            .await
    }
}

/// A typed store that provides transparent encoding/decoding of values.
/// Values are stored as encoded bytes in the backend, but appear as type T to the user.
#[derive(Clone)]
pub struct TypedStore<T, Backend: StorageBackend, Codec: Encoder = CborEncoder>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Clone + Eq + std::hash::Hash,
{
    backend: Backend,
    codec: Codec,
    path: Option<String>,
    cache: Arc<
        dialog_common::SharedCell<
            std::collections::HashMap<
                Backend::Address,
                std::sync::Weak<dialog_common::SharedCell<TypedState<T, Backend::Edition>>>,
            >,
        >,
    >,
    _phantom: std::marker::PhantomData<T>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<T, Backend, Codec> Encoder for TypedStore<T, Backend, Codec>
where
    T: Clone + Send + Sync,
    Backend: StorageBackend + TransactionalMemoryBackend + ConditionalSync,
    Backend::Address: Clone + Eq + std::hash::Hash,
    Codec: Encoder + ConditionalSync,
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
impl<T, Backend, Codec> StorageBackend for TypedStore<T, Backend, Codec>
where
    T: Serialize + DeserializeOwned + ConditionalSync + std::fmt::Debug + Clone,
    Backend: StorageBackend<Key = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
    Codec: Encoder + ConditionalSync,
    Codec::Bytes: AsRef<[u8]>,
    Codec::Error: std::fmt::Display,
{
    type Key = Backend::Key;
    type Value = T;
    type Error = TypedStoreError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        // Encode the value
        let (_, bytes) = self
            .codec
            .encode(&value)
            .await
            .map_err(|e| DialogStorageError::EncodeFailed(e.to_string()))?;
        let encoded_value: <Backend as StorageBackend>::Value = bytes.as_ref().to_vec();

        // Prefix key and set
        let prefixed_key = self.prefix_key(key.as_ref()).into();
        self.backend.set(prefixed_key, encoded_value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        // Prefix key and get
        let prefixed_key = self.prefix_key(key.as_ref()).into();
        let bytes = self.backend.get(&prefixed_key).await?;

        // Decode if present
        match bytes {
            Some(b) => {
                let decoded = self
                    .codec
                    .decode(b.as_ref())
                    .await
                    .map_err(|e| DialogStorageError::EncodeFailed(e.to_string()))?;
                Ok(Some(decoded))
            }
            None => Ok(None),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<T, Backend, Codec> TransactionalMemoryBackend for TypedStore<T, Backend, Codec>
where
    T: Serialize + DeserializeOwned + ConditionalSync + std::fmt::Debug + Clone,
    Backend: StorageBackend<Key = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
    Backend::Edition: ConditionalSend + ConditionalSync + Clone,
    Codec: Encoder + ConditionalSync,
    Codec::Bytes: AsRef<[u8]>,
    Codec::Error: std::fmt::Display,
{
    type Address = Backend::Address;
    type Value = T;
    type Error = TypedStoreError;
    type Edition = Backend::Edition;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        // Prefix key and resolve from backend
        let prefixed_key = self.prefix_key(address.as_ref()).into();
        let result = self.backend.resolve(&prefixed_key).await?;

        // Decode if present
        match result {
            Some((bytes, edition)) => {
                let decoded = self
                    .codec
                    .decode(bytes.as_ref())
                    .await
                    .map_err(|e| DialogStorageError::EncodeFailed(e.to_string()))?;
                Ok(Some((decoded, edition)))
            }
            None => Ok(None),
        }
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        // Prefix key
        let prefixed_key = self.prefix_key(address.as_ref()).into();

        // Encode content if present
        let encoded_content = match content {
            Some(value) => {
                let (_, bytes) = self
                    .codec
                    .encode(&value)
                    .await
                    .map_err(|e| DialogStorageError::EncodeFailed(e.to_string()))?;
                Some(bytes.as_ref().to_vec())
            }
            None => None,
        };

        // Perform replace on backend
        self.backend
            .replace(&prefixed_key, edition, encoded_content)
            .await
    }
}

impl<T, Backend, Codec> TypedStore<T, Backend, Codec>
where
    T: Serialize + DeserializeOwned + ConditionalSync + std::fmt::Debug + Clone,
    Backend: StorageBackend<Key = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
    Backend::Edition: ConditionalSend + ConditionalSync + Clone,
    Backend::Address: Eq + std::hash::Hash + Clone,
    Codec: Encoder + ConditionalSync,
    Codec::Bytes: AsRef<[u8]>,
    Codec::Error: std::fmt::Display,
{
    /// Opens a TypedTransactionalMemory for the given key, using the cache to return existing instances.
    /// If multiple callers open the same key, they will share the same TypedTransactionalMemory instance.
    pub async fn open(
        &self,
        key: &Backend::Address,
    ) -> Result<TypedTransactionalMemory<T, Backend, Codec>, TypedStoreError> {
        let prefixed_key = self.prefix_key(key.as_ref()).into();

        // Clean up dead weak references and check if we have a live one
        let mut cache = self.cache.write();
        cache.retain(|_, weak| weak.strong_count() > 0);

        if let Some(weak) = cache.get(&prefixed_key) {
            if let Some(state) = weak.upgrade() {
                // Return a clone using the existing state
                return Ok(TypedTransactionalMemory {
                    address: prefixed_key,
                    state,
                    codec: self.codec.clone(),
                });
            }
        }

        // Create new TypedTransactionalMemory
        let memory =
            TypedTransactionalMemory::open(prefixed_key.clone(), &self.backend, self.codec.clone())
                .await?;

        // Store weak reference in cache
        cache.insert(prefixed_key, Arc::downgrade(&memory.state));

        Ok(memory)
    }

    /// Prefixes the key bytes with the path (if any) and separator
    fn prefix_key(&self, key_bytes: &[u8]) -> Vec<u8> {
        match &self.path {
            Some(path) => {
                let mut result = path.as_bytes().to_vec();
                result.push(b'/');
                result.extend_from_slice(key_bytes);
                result
            }
            None => key_bytes.to_vec(),
        }
    }
}

/// Error type for typed store operations
pub type TypedStoreError = DialogStorageError;

/// Type alias for TypedTransactionalMemory with CborEncoder (the common case).
/// Type alias for backwards compatibility with old TypedStoreResource API.
/// Now uses TypedTransactionalMemory from dialog_storage with a codec.
/// The Backend type parameter is Storage<Backend, CborEncoder> so that replace/reload
/// can accept &Storage directly.
pub type TypedStoreResource<T, Backend> =
    TypedTransactionalMemory<T, Storage<Backend, CborEncoder>, CborEncoder>;

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_storage::{CborEncoder, MemoryStorageBackend};
    use serde::{Deserialize, Serialize};

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestRecord {
        name: String,
        value: u32,
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_transactional_storage_creation() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = Storage::new(backend, CborEncoder);

        let _store = storage.at("test");
        // If we get here, storage and store were created successfully
    }

    // Tests below use the old resolve() and swap() API which has been removed.
    // These tests are commented out since Store now implements StorageBackend directly.
    // New tests should use the StorageBackend API (open() + replace()) or Memory API.

    /*
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_transactional_store_resolve_nonexistent() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = Storage::new(backend, CborEncoder);
        let store = storage.at("test");

        let key = b"key1".to_vec();
        let result: Option<TestRecord> = store.resolve::<_, _>(&key).await.unwrap();
        assert_eq!(result, None);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_transactional_store_swap_create() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = Storage::new(backend, CborEncoder);
        let mut store = storage.at("test");

        let key = b"key1".to_vec();
        let record = TestRecord {
            name: "key1".to_string(),
            value: 42,
        };

        // Create new record (when = None)
        store
            .swap::<_, _>(key.clone(), Some(record.clone()), None)
            .await
            .unwrap();

        // Verify it was stored
        let result: Option<TestRecord> = store.resolve::<_, _>(&key).await.unwrap();
        assert_eq!(result, Some(record));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_transactional_store_swap_update() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = Storage::new(backend, CborEncoder);
        let mut store = storage.at("test");

        let key = b"key1".to_vec();
        let record1 = TestRecord {
            name: "key1".to_string(),
            value: 42,
        };
        let record2 = TestRecord {
            name: "key1".to_string(),
            value: 100,
        };

        // Create
        store
            .swap::<_, _>(key.clone(), Some(record1.clone()), None)
            .await
            .unwrap();

        // Update with CAS
        store
            .swap::<_, _>(key.clone(), Some(record2.clone()), Some(record1))
            .await
            .unwrap();

        // Verify update
        let result: Option<TestRecord> = store.resolve::<_, _>(&key).await.unwrap();
        assert_eq!(result, Some(record2));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_transactional_store_swap_delete() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = Storage::new(backend, CborEncoder);
        let mut store = storage.at("test");

        let key = b"key1".to_vec();
        let record = TestRecord {
            name: "key1".to_string(),
            value: 42,
        };

        // Create
        store
            .swap::<_, _>(key.clone(), Some(record.clone()), None)
            .await
            .unwrap();

        // Delete with CAS
        store
            .swap::<Vec<u8>, TestRecord>(key.clone(), None, Some(record))
            .await
            .unwrap();

        // Verify deletion
        let result: Option<TestRecord> = store.resolve::<_, _>(&key).await.unwrap();
        assert_eq!(result, None);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_transactional_store_nested_namespaces() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = Storage::new(backend, CborEncoder);

        let mut store1 = storage.at("namespace1");
        let mut store2 = storage.at("namespace1").at("nested");

        let key = b"key1".to_vec();
        let record1 = TestRecord {
            name: "key1".to_string(),
            value: 1,
        };
        let record2 = TestRecord {
            name: "key1".to_string(),
            value: 2,
        };

        // Store same key in different namespaces
        store1
            .swap::<_, _>(key.clone(), Some(record1.clone()), None)
            .await
            .unwrap();
        store2
            .swap::<_, _>(key.clone(), Some(record2.clone()), None)
            .await
            .unwrap();

        // Verify they're isolated
        let result1: Option<TestRecord> = store1.resolve::<_, _>(&key).await.unwrap();
        let result2: Option<TestRecord> = store2.resolve::<_, _>(&key).await.unwrap();

        assert_eq!(result1, Some(record1));
        assert_eq!(result2, Some(record2));
    }
    */
}
