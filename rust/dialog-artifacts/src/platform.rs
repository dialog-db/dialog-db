#![allow(missing_docs)]

use crate::replica::{BranchId, Revision, Site};
use async_trait::async_trait;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_storage::{
    Blake3Hash, CborEncoder, DialogStorageError, Encoder, Resource, StorageBackend,
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

/// A journal manages revision history for a branch across canonical and cached storage.
#[allow(dead_code)]
pub struct Journal<Canonical: Resource<Value = Vec<u8>>, Cache: Resource<Value = Vec<u8>>> {
    branch: BranchId,
    canonical: Canonical,
    cache: Cache,
}

impl<
    Canonical: Resource<Value = Vec<u8>, Error = JournalError> + ConditionalSync,
    Cache: Resource<Value = Vec<u8>, Error = JournalError> + ConditionalSync,
> Journal<Canonical, Cache>
{
    /// resolves current known revision for this branch
    #[allow(dead_code)]
    async fn resolve(&self) -> Result<Option<Revision>, JournalError> {
        if let Some(content) = self.cache.content() {
            let revision = CborEncoder.decode(content).await.map_err(|_| {
                JournalError::ResolveError(ResolveError::DecodeError {
                    branch: self.branch.clone(),
                })
            })?;
            Ok(Some(revision))
        } else {
            Ok(None)
        }
    }

    #[allow(dead_code)]
    async fn fetch(&mut self) -> Result<Option<Revision>, JournalError> {
        self.canonical.reload().await?;
        let (revision, content) = if let Some(content) = self.canonical.content() {
            // we're making sure we can decode a revision
            let revision: Revision = CborEncoder.decode(content).await.map_err(|_| {
                JournalError::ResolveError(ResolveError::DecodeError {
                    branch: self.branch.clone(),
                })
            })?;

            (Some(revision), Some(content.clone()))
        } else {
            (None, None)
        };

        // update local cache so that next time we resolve we get the
        // latest revision we fetched.
        self.cache.replace_with(|_| content.clone()).await?;

        Ok(revision)
    }

    #[allow(dead_code)]
    async fn publish(&mut self, _branch: BranchId, revision: Revision) -> Result<(), JournalError> {
        let (_, after) = CborEncoder
            .encode(&revision)
            .await
            .map_err(|e| JournalError::EncodeError(e.to_string()))?;

        // Attempt to update canonical record
        self.canonical.replace(Some(after.clone())).await?;
        // If canonical was updated update local cache also
        self.cache.replace_with(|_| Some(after.clone())).await?;

        Ok(())
    }
}

/// A store for managing multiple journal instances.
#[allow(dead_code)]
pub struct JournalStore<
    Backend: StorageBackend<Key = String, Value = Vec<u8>>,
    Cache: StorageBackend<Key = String, Value = Vec<u8>>,
> {
    remote: Backend,
    cache: Cache,
}

impl<
    Backend: StorageBackend<Key = String, Value = Vec<u8>, Error = JournalError> + ConditionalSync,
    Cache: StorageBackend<Key = String, Value = Vec<u8>, Error = JournalError> + ConditionalSync,
> JournalStore<Backend, Cache>
{
    #[allow(dead_code)]
    async fn new(remote: Backend, cache: Cache) -> Self {
        Self { remote, cache }
    }

    #[allow(dead_code)]
    async fn mount(
        &mut self,
        branch: BranchId,
    ) -> Result<Journal<Backend::Resource, Cache::Resource>, JournalError> {
        let key = branch.to_string();
        let canonical = self.remote.open(&key).await?;
        let mut cache = self.cache.open(&key).await?;
        cache.replace(canonical.content().clone()).await?;

        Ok(Journal {
            branch,
            canonical,
            cache,
        })
    }
}

/// Manages remote repository connections (unused, for future use).
#[allow(dead_code)]
pub struct Remotes<Backend: StorageBackend> {
    backend: Backend,
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
    StorageBackend<Key = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError> + ConditionalSync + Clone
{
}

// Blanket implementation - any StorageBackend that satisfies the bounds is a PlatformBackend
impl<B> PlatformBackend for B where
    B: StorageBackend<Key = Vec<u8>, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + Clone
{
}

/// Adapter that maps a backend's error type to DialogStorageError
#[derive(Clone)]
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
    B::Resource: ConditionalSync + ConditionalSend,
{
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type Error = DialogStorageError;
    type Resource = ErrorMappingResource<B::Resource>;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.inner.set(key, value).await.map_err(|e| e.into())
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.get(key).await.map_err(|e| e.into())
    }

    async fn open(&self, key: &Self::Key) -> Result<Self::Resource, Self::Error> {
        let inner = self.inner.open(key).await.map_err(|e| e.into())?;
        Ok(ErrorMappingResource { inner })
    }
}

/// A resource wrapper that maps storage errors to platform-specific errors.
pub struct ErrorMappingResource<R> {
    inner: R,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<R> Resource for ErrorMappingResource<R>
where
    R: Resource<Value = Vec<u8>> + ConditionalSync + ConditionalSend,
    R::Error: Into<DialogStorageError>,
{
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    fn content(&self) -> &Option<Self::Value> {
        self.inner.content()
    }

    fn into_content(self) -> Option<Self::Value> {
        self.inner.into_content()
    }

    async fn reload(&mut self) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.reload().await.map_err(|e| e.into())
    }

    async fn replace(
        &mut self,
        value: Option<Self::Value>,
    ) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.replace(value).await.map_err(|e| e.into())
    }
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
#[derive(Debug, Clone)]
pub struct Storage<Backend: StorageBackend, Codec: Encoder = CborEncoder> {
    backend: Backend,
    codec: Codec,
    path: Option<String>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend: StorageBackend, Codec: Encoder> Encoder for Storage<Backend, Codec>
where
    Backend: ConditionalSync,
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

/// Resource wrapper for Storage that handles key prefixing
pub struct StorageResource<R: Resource> {
    inner: R,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<R> Resource for StorageResource<R>
where
    R: Resource + ConditionalSync,
{
    type Value = R::Value;
    type Error = R::Error;

    fn content(&self) -> &Option<Self::Value> {
        self.inner.content()
    }

    fn into_content(self) -> Option<Self::Value> {
        self.inner.into_content()
    }

    async fn reload(&mut self) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.reload().await
    }

    async fn replace(
        &mut self,
        value: Option<Self::Value>,
    ) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.replace(value).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend: StorageBackend, Codec: Encoder> StorageBackend for Storage<Backend, Codec>
where
    Backend: ConditionalSync,
    Backend::Key: From<Vec<u8>> + AsRef<[u8]> + ConditionalSync,
    Backend::Value: ConditionalSync,
    Backend::Error: ConditionalSync,
    Backend::Resource: ConditionalSync,
    Codec: ConditionalSync,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Resource = StorageResource<Backend::Resource>;
    type Error = Backend::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let prefixed_key = self.prefix_key(key.as_ref()).into();
        self.backend.set(prefixed_key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let prefixed_key = self.prefix_key(key.as_ref()).into();
        self.backend.get(&prefixed_key).await
    }

    async fn open(&self, key: &Self::Key) -> Result<Self::Resource, Self::Error> {
        let prefixed_key = self.prefix_key(key.as_ref()).into();
        let inner = self.backend.open(&prefixed_key).await?;
        Ok(StorageResource { inner })
    }
}

impl<Backend: StorageBackend, Codec: Encoder> Storage<Backend, Codec> {
    /// Creates a new transactional storage with the given backend and encoder
    pub fn new(backend: Backend, codec: Codec) -> Self {
        Self {
            backend,
            codec,
            path: None,
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
        }
    }

    /// Creates a typed store that handles encoding/decoding transparently
    pub fn mount<T>(&self) -> TypedStore<T, Backend, Codec> {
        TypedStore {
            backend: self.backend.clone(),
            codec: self.codec.clone(),
            path: self.path.clone(),
            _phantom: std::marker::PhantomData,
        }
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

/// A typed store that provides transparent encoding/decoding of values.
/// Values are stored as encoded bytes in the backend, but appear as type T to the user.
#[derive(Clone)]
pub struct TypedStore<T, Backend: StorageBackend, Codec: Encoder = CborEncoder> {
    backend: Backend,
    codec: Codec,
    path: Option<String>,
    _phantom: std::marker::PhantomData<T>,
}

/// Resource wrapper that transparently handles encoding/decoding for TypedStore
pub struct TypedStoreResource<T, Backend, Codec = CborEncoder>
where
    Backend: StorageBackend<Value = Vec<u8>, Error = DialogStorageError> + ConditionalSync,
    Backend::Key: From<Vec<u8>> + AsRef<[u8]> + ConditionalSync,
    Backend::Error: ConditionalSync,
    Codec: Encoder,
{
    inner: Backend::Resource,
    codec: Codec,
    decoded: Option<T>,
    _phantom: std::marker::PhantomData<(T, Backend)>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<T, Backend, Codec> Resource for TypedStoreResource<T, Backend, Codec>
where
    T: Serialize + DeserializeOwned + ConditionalSync + std::fmt::Debug + Clone,
    Backend: StorageBackend<Value = Vec<u8>, Error = DialogStorageError> + ConditionalSync,
    Backend::Key: From<Vec<u8>> + AsRef<[u8]> + ConditionalSync,
    Backend::Error: ConditionalSync,
    Backend::Resource: ConditionalSync + ConditionalSend,
    Codec: Encoder + ConditionalSync,
    Codec::Bytes: AsRef<[u8]>,
    Codec::Error: std::fmt::Display,
{
    type Value = T;
    type Error = TypedStoreError;

    fn content(&self) -> &Option<Self::Value> {
        &self.decoded
    }

    fn into_content(self) -> Option<Self::Value> {
        self.decoded
    }

    async fn reload(&mut self) -> Result<Option<Self::Value>, Self::Error> {
        let old_decoded = self.decoded.take();

        // Reload underlying resource
        self.inner.reload().await?;

        // Decode new content if present
        if let Some(bytes) = self.inner.content() {
            self.decoded = Some(
                self.codec
                    .decode(bytes.as_ref())
                    .await
                    .map_err(|e| DialogStorageError::EncodeFailed(e.to_string()))?,
            );
        }

        Ok(old_decoded)
    }

    async fn replace(
        &mut self,
        value: Option<Self::Value>,
    ) -> Result<Option<Self::Value>, Self::Error> {
        // Encode the new value if present
        let encoded_value = if let Some(ref v) = value {
            let (_, bytes) = self
                .codec
                .encode(v)
                .await
                .map_err(|e| DialogStorageError::EncodeFailed(e.to_string()))?;
            Some(bytes.as_ref().to_vec())
        } else {
            None
        };

        // Replace in underlying resource
        self.inner.replace(encoded_value).await?;

        // Update decoded cache and return old value
        let old_decoded = self.decoded.take();
        self.decoded = value;

        Ok(old_decoded)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<T, Backend, Codec> Encoder for TypedStore<T, Backend, Codec>
where
    T: Clone + Send + Sync,
    Backend: StorageBackend + ConditionalSync,
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
    Backend: StorageBackend<Value = Vec<u8>, Error = DialogStorageError> + ConditionalSync,
    Backend::Key: From<Vec<u8>> + AsRef<[u8]> + ConditionalSync,
    Backend::Error: ConditionalSync,
    Backend::Resource: ConditionalSync + ConditionalSend,
    Codec: Encoder + ConditionalSync,
    Codec::Bytes: AsRef<[u8]>,
    Codec::Error: std::fmt::Display,
{
    type Key = Backend::Key;
    type Value = T;
    type Resource = TypedStoreResource<T, Backend, Codec>;
    type Error = TypedStoreError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        // Encode the value
        let (_, bytes) = self
            .codec
            .encode(&value)
            .await
            .map_err(|e| DialogStorageError::EncodeFailed(e.to_string()))?;
        let encoded_value: Backend::Value = bytes.as_ref().to_vec();

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

    async fn open(&self, key: &Self::Key) -> Result<Self::Resource, Self::Error> {
        // Delegate to the inherent method
        self.open(key).await
    }
}

impl<T, Backend, Codec> TypedStore<T, Backend, Codec>
where
    T: Serialize + DeserializeOwned + ConditionalSync + std::fmt::Debug + Clone,
    Backend: StorageBackend<Value = Vec<u8>, Error = DialogStorageError> + ConditionalSync,
    Backend::Key: From<Vec<u8>> + AsRef<[u8]> + ConditionalSync,
    Backend::Error: ConditionalSync,
    Backend::Resource: ConditionalSync + ConditionalSend,
    Codec: Encoder + ConditionalSync,
    Codec::Bytes: AsRef<[u8]>,
    Codec::Error: std::fmt::Display,
{
    /// Opens a resource at the given key, providing CAS semantics
    pub async fn open(
        &self,
        key: &Backend::Key,
    ) -> Result<TypedStoreResource<T, Backend, Codec>, TypedStoreError> {
        // Prefix key and open backend resource
        let prefixed_key = self.prefix_key(key.as_ref()).into();
        let inner = self.backend.open(&prefixed_key).await?;

        // Decode current content if present
        let decoded = if let Some(bytes) = inner.content() {
            Some(
                self.codec
                    .decode(bytes.as_ref())
                    .await
                    .map_err(|e| DialogStorageError::EncodeFailed(e.to_string()))?,
            )
        } else {
            None
        };

        Ok(TypedStoreResource {
            inner,
            codec: self.codec.clone(),
            decoded,
            _phantom: std::marker::PhantomData,
        })
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

/// Adapter that converts Blake3Hash keys to/from Vec<u8> for the underlying backend.
/// This allows using a Vec<u8>-keyed backend as a Blake3Hash-keyed backend.
#[derive(Clone)]
pub struct Blake3KeyBackend<B> {
    inner: B,
}

impl<B> Blake3KeyBackend<B> {
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
}

/// Resource wrapper that handles Blake3Hash key conversion
pub struct Blake3KeyResource<R> {
    inner: R,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<B> StorageBackend for Blake3KeyBackend<B>
where
    B: StorageBackend<Value = Vec<u8>> + ConditionalSync,
    B::Key: Address + From<Vec<u8>>,
    B::Error: ConditionalSync,
    B::Resource: ConditionalSync,
{
    type Key = Blake3Hash;
    type Value = Vec<u8>;
    type Error = B::Error;
    type Resource = Blake3KeyResource<B::Resource>;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let backend_key = key.as_ref().to_vec().into();
        self.inner.set(backend_key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let backend_key: B::Key = key.as_ref().to_vec().into();
        self.inner.get(&backend_key).await
    }

    async fn open(&self, key: &Self::Key) -> Result<Self::Resource, Self::Error> {
        let backend_key: B::Key = key.as_ref().to_vec().into();
        let inner = self.inner.open(&backend_key).await?;
        Ok(Blake3KeyResource { inner })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<R> Resource for Blake3KeyResource<R>
where
    R: Resource<Value = Vec<u8>> + ConditionalSync + ConditionalSend,
{
    type Value = Vec<u8>;
    type Error = R::Error;

    fn content(&self) -> &Option<Self::Value> {
        self.inner.content()
    }

    fn into_content(self) -> Option<Self::Value> {
        self.inner.into_content()
    }

    async fn reload(&mut self) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.reload().await
    }

    async fn replace(
        &mut self,
        value: Option<Self::Value>,
    ) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.replace(value).await
    }
}

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
