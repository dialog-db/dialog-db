use crate::replica::{BranchId, Edition, Revision, Site};
use async_trait::async_trait;
use dialog_common::ConditionalSync;
use dialog_prolly_tree::KeyType;
use dialog_storage::{
    AtomicStorageBackend, CborEncoder, DialogStorageError, Encoder, RestStorageConfig,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum JournalError {
    /// Resolving a branch failed
    #[error("Failed to resolve branch {0}")]
    ResolveError(ResolveError),
    /// Encoding a revision failed
    #[error("Failed to encode revision: {0}")]
    EncodeError(String),
}

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

/// Represents a platform API for working with remotes and local cache
/// for those remotes.
trait JournalBackend<Address>: Sized {
    async fn open(address: Address) -> Result<Self, JournalError>;

    /// Resolves revision for the given branch for remote. It should resolve
    /// from the local cache as opposed to actual remote node. To fetch from
    /// the actual remote node, `fetch` should be called.
    async fn resolve(&self, branch: &BranchId) -> Result<Option<Revision>, JournalError>;

    /// Fetches revision for the given branch from the actual remote node.
    async fn fetch(&mut self, branch: &BranchId) -> Result<Option<Revision>, JournalError>;

    /// Publishes a new revision for the given branch.
    async fn publish(&mut self, branch: BranchId, revision: Revision) -> Result<(), JournalError>;
}

pub struct Journal<
    Backend: AtomicStorageBackend<Key = String, Value = Vec<u8>>,
    Cache: AtomicStorageBackend<Key = String, Value = Vec<u8>>,
> {
    remote: Backend,
    cache: Cache,
}

impl<
    Address,
    Backend: AtomicStorageBackend<Key = String, Value = Vec<u8>, Error = JournalError>,
    Cache: AtomicStorageBackend<Key = String, Value = Vec<u8>, Error = JournalError>,
> JournalBackend<Address> for Journal<Backend, Cache>
{
    async fn open(address: Address) -> Result<Self, JournalError> {
        unimplemented!()
    }

    async fn resolve(&self, branch: &BranchId) -> Result<Option<Revision>, JournalError> {
        if let Some(record) = self.cache.resolve(branch.id()).await? {
            let revision = CborEncoder.decode(&record).await.map_err(|_| {
                JournalError::ResolveError(ResolveError::DecodeError {
                    branch: branch.clone(),
                })
            })?;
            Ok(Some(revision))
        } else {
            Ok(None)
        }
    }

    async fn fetch(&mut self, branch: &BranchId) -> Result<Option<Revision>, JournalError> {
        let before = self.cache.resolve(branch.id()).await?;
        let (revision, record_bytes) =
            if let Some(record) = self.remote.resolve(branch.id()).await? {
                // we're making sure we can decode a revision
                let revision: Revision = CborEncoder.decode(&record).await.map_err(|_| {
                    JournalError::ResolveError(ResolveError::DecodeError {
                        branch: branch.clone(),
                    })
                })?;

                (Some(revision), Some(record))
            } else {
                (None, None)
            };

        // update local cache so that next time we resolve we get the
        // latest revision we fetched.
        self.cache
            .swap(branch.id().clone(), record_bytes, before)
            .await?;

        Ok(revision)
    }

    async fn publish(&mut self, branch: BranchId, revision: Revision) -> Result<(), JournalError> {
        let before = self.cache.resolve(branch.id()).await?;
        let (_, after) = CborEncoder
            .encode(&revision)
            .await
            .map_err(|e| JournalError::EncodeError(e.to_string()))?;
        // Attempt to update remote record.
        self.remote
            .swap(branch.id().clone(), Some(after.clone()), before.clone())
            .await?;
        self.cache
            .swap(branch.id().clone(), Some(after), before)
            .await?;

        Ok(())
    }
}

pub struct Remotes<Backend: AtomicStorageBackend> {
    backend: Backend,
}

pub trait RemoteStore: AtomicStorageBackend<Key = Site, Value = Vec<u8>> {}
impl<T: AtomicStorageBackend<Key = Site, Value = Vec<u8>>> RemoteStore for T {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteState {
    /// Name for this remote.
    id: Site,

    /// Address used to configure this remote
    address: RestStorageConfig,

    /// Causal reference to the previous state.
    cause: Edition<RemoteState>,
}
impl MemoryRecord for RemoteState {
    type Address = Site;
    type Edition = Edition<RemoteState>;

    fn address(&self) -> &Self::Address {
        &self.id
    }

    fn cause(&self) -> Option<&Self::Edition> {
        Some(&self.cause)
    }
}

pub struct Remote<'a, Memory: TransactionalMemory<Record = RemoteState>> {
    state: RemoteState,
    memory: &'a Memory,
}

impl<'a, Memory: TransactionalMemory<Record = RemoteState>> Remote<'a, Memory> {
    pub async fn load(site: &Site, memory: &'a Memory) -> Result<Self, RemoteError> {
        if let Some(state) = memory.read(site).await.map_err(|_| RemoteError::NotFound {
            remote: site.clone(),
        })? {
            Ok(Self { memory, state })
        } else {
            Err(RemoteError::NotFound {
                remote: site.clone(),
            })
        }
    }
}

/// A transactional storage wraps a backend and encoder, providing
/// a foundation for creating typed stores with namespacing.
pub struct TransactionalStorage<Backend, Encoder = CborEncoder> {
    backend: Backend,
    encoder: Encoder,
}

impl<B, E> TransactionalStorage<B, E> {
    /// Creates a new transactional storage with the given backend and encoder
    pub fn new(backend: B, encoder: E) -> Self {
        Self { backend, encoder }
    }

    /// Creates a namespaced store at the given path
    pub fn at<const HASH_SIZE: usize>(&self, path: &str) -> TransactionalStore<B, E>
    where
        B: Clone,
        E: Clone + dialog_storage::Encoder<HASH_SIZE>,
    {
        TransactionalStore {
            backend: self.backend.clone(),
            encoder: self.encoder.clone(),
            path: path.to_string(),
        }
    }
}

/// A namespaced transactional store that provides typed key-value operations
/// with transparent encoding/decoding.
pub struct TransactionalStore<Backend, Encoder = CborEncoder> {
    backend: Backend,
    encoder: Encoder,
    path: String,
}

impl<Backend, E> TransactionalStore<Backend, E>
where
    Backend: AtomicStorageBackend,
    Backend::Key: From<Vec<u8>>,
    Backend::Value: From<Vec<u8>> + AsRef<[u8]>,
    Backend::Error: std::fmt::Display,
{
    /// Creates a nested namespace under this store
    pub fn at<const HASH_SIZE: usize>(&self, path: &str) -> TransactionalStore<Backend, E>
    where
        Backend: Clone,
        E: Clone + dialog_storage::Encoder<HASH_SIZE>,
        <E as dialog_storage::Encoder<HASH_SIZE>>::Error: std::fmt::Display,
    {
        TransactionalStore {
            backend: self.backend.clone(),
            encoder: self.encoder.clone(),
            path: format!("{}/{}", self.path, path),
        }
    }

    /// Resolves a value for the given key from storage
    pub async fn resolve<const HASH_SIZE: usize, K, V>(
        &self,
        key: &K,
    ) -> Result<Option<V>, TransactionalStoreError>
    where
        K: KeyType,
        V: DeserializeOwned + ConditionalSync,
        E: dialog_storage::Encoder<HASH_SIZE>,
        <E as dialog_storage::Encoder<HASH_SIZE>>::Error: std::fmt::Display,
    {
        // Use KeyType::bytes() for key encoding
        let key_bytes = self.prefix_key(key.bytes());
        let storage_key: Backend::Key = key_bytes.into();

        if let Some(value_bytes) = self
            .backend
            .resolve(&storage_key)
            .await
            .map_err(|e| TransactionalStoreError::BackendError(e.to_string()))?
        {
            let value: V = self
                .encoder
                .decode(value_bytes.as_ref())
                .await
                .map_err(|e| TransactionalStoreError::EncoderError(e.to_string()))?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// Performs a compare-and-swap operation
    pub async fn swap<const HASH_SIZE: usize, K, V>(
        &mut self,
        key: K,
        value: Option<V>,
        when: Option<V>,
    ) -> Result<(), TransactionalStoreError>
    where
        K: KeyType,
        V: Serialize + DeserializeOwned + ConditionalSync + std::fmt::Debug,
        E: dialog_storage::Encoder<HASH_SIZE>,
        <E as dialog_storage::Encoder<HASH_SIZE>>::Error: std::fmt::Display,
    {
        // Encode key using KeyType::bytes()
        let key_bytes = self.prefix_key(key.bytes());
        let storage_key: Backend::Key = key_bytes.into();

        // Encode value if present
        let encoded_value = if let Some(v) = value {
            let (_, bytes) = self
                .encoder
                .encode(&v)
                .await
                .map_err(|e| TransactionalStoreError::EncoderError(e.to_string()))?;
            Some(bytes.as_ref().to_vec().into())
        } else {
            None
        };

        // Encode when condition if present
        let encoded_when = if let Some(w) = when {
            let (_, bytes) = self
                .encoder
                .encode(&w)
                .await
                .map_err(|e| TransactionalStoreError::EncoderError(e.to_string()))?;
            Some(bytes.as_ref().to_vec().into())
        } else {
            None
        };

        self.backend
            .swap(storage_key, encoded_value, encoded_when)
            .await
            .map_err(|e| TransactionalStoreError::BackendError(e.to_string()))?;

        Ok(())
    }

    /// Prefixes the key bytes with the path and separator
    fn prefix_key(&self, key_bytes: &[u8]) -> Vec<u8> {
        let mut result = self.path.as_bytes().to_vec();
        result.push(b'/');
        result.extend_from_slice(key_bytes);
        result
    }
}

// Implement Encoder for TransactionalStore to delegate to internal encoder
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<const HASH_SIZE: usize, Backend, E> dialog_storage::Encoder<HASH_SIZE>
    for TransactionalStore<Backend, E>
where
    Backend: AtomicStorageBackend + Clone + ConditionalSync,
    Backend::Key: From<Vec<u8>>,
    Backend::Value: From<Vec<u8>> + AsRef<[u8]>,
    E: dialog_storage::Encoder<HASH_SIZE> + Clone + ConditionalSync,
{
    type Bytes = E::Bytes;
    type Hash = E::Hash;
    type Error = E::Error;

    async fn encode<T>(&self, data: &T) -> Result<(Self::Hash, Self::Bytes), Self::Error>
    where
        T: Serialize + ConditionalSync + std::fmt::Debug,
    {
        self.encoder.encode(data).await
    }

    async fn decode<T>(&self, bytes: &[u8]) -> Result<T, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync,
    {
        self.encoder.decode(bytes).await
    }
}

// Implement Clone for TransactionalStore
impl<Backend, E> Clone for TransactionalStore<Backend, E>
where
    Backend: Clone,
    E: Clone,
{
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            encoder: self.encoder.clone(),
            path: self.path.clone(),
        }
    }
}

// Implement AtomicStorageBackend for TransactionalStore
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend, E> AtomicStorageBackend for TransactionalStore<Backend, E>
where
    Backend: AtomicStorageBackend + Clone + ConditionalSync,
    Backend::Key: From<Vec<u8>> + ConditionalSync,
    Backend::Value: From<Vec<u8>> + AsRef<[u8]> + ConditionalSync + PartialEq,
    Backend::Error: std::fmt::Display,
    E: Clone + ConditionalSync,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Error = DialogStorageError;

    async fn swap(
        &mut self,
        key: Self::Key,
        value: Option<Self::Value>,
        when: Option<Self::Value>,
    ) -> Result<(), Self::Error> {
        self.backend
            .swap(key, value, when)
            .await
            .map_err(|e| DialogStorageError::StorageBackend(e.to_string()))
    }

    async fn resolve(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        self.backend
            .resolve(key)
            .await
            .map_err(|e| DialogStorageError::StorageBackend(e.to_string()))
    }
}

/// Error type for transactional store operations
#[derive(Error, Debug, Clone)]
pub enum TransactionalStoreError {
    #[error("Backend error: {0}")]
    BackendError(String),
    #[error("Encoder error: {0}")]
    EncoderError(String),
}

/// A trait for records that can be stored with causal consistency.
/// Records have an address (used as storage key) and optional causal reference
/// to the previous record at the same address (for CAS semantics).
pub trait MemoryRecord: Serialize + DeserializeOwned {
    /// The address/key type for this record (must be serializable)
    type Address: Serialize;
    /// The edition/hash identifier for this record (must be serializable)
    type Edition: Serialize;

    /// Returns the address where this record should be stored
    fn address(&self) -> &Self::Address;

    /// Returns the causal reference to the prior record at this address.
    /// `None` if this is the first record at this address.
    fn cause(&self) -> Option<&Self::Edition>;
}

/// A transactional storage system that stores records with causal consistency.
/// Unlike [ContentAddressedStorage]:
/// 1. Records specify their own storage address (not content-addressed)
/// 2. Records maintain causal chains via compare-and-swap semantics
/// 3. Each instance is bound to a specific Record type
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait TransactionalMemory: ConditionalSync + 'static {
    /// The type of record this storage works with
    type Record: MemoryRecord;
    /// The type of hash/edition that is produced by this storage
    type Hash: ConditionalSync;
    /// The type of error that is produced by this storage
    type Error;

    /// Reads a record from the given address
    async fn read(
        &self,
        address: &<Self::Record as MemoryRecord>::Address,
    ) -> Result<Option<Self::Record>, Self::Error>;

    /// Stores a record and returns its hash/edition identifier.
    /// Fails if the current record doesn't match the cause (CAS semantics).
    async fn merge(&mut self, record: Self::Record) -> Result<Self::Hash, Self::Error>;
}

/// A transactional memory that wraps any [Encoder] + [AtomicStorageBackend]
/// to store records of type `R` with causal consistency.
///
/// # Example
/// ```ignore
/// let storage = Storage { encoder: CborEncoder, backend: atomic_backend };
/// let memory = Memory::<32, MyRecord, _>::new(storage);
///
/// let record = memory.read(&address).await?;
/// let hash = memory.merge(new_record).await?;
/// ```
pub struct Memory<const HASH_SIZE: usize, Record, Backend> {
    backend: Backend,
    _record: std::marker::PhantomData<Record>,
}

impl<const HASH_SIZE: usize, Backend, Record> Memory<HASH_SIZE, Record, Backend> {
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            _record: std::marker::PhantomData,
        }
    }

    /// Alias for `new` - opens a memory instance with the given backend/store
    pub fn open(backend: Backend) -> Self {
        Self::new(backend)
    }
}

impl<const HASH_SIZE: usize, Record, Backend> Clone for Memory<HASH_SIZE, Record, Backend>
where
    Backend: Clone,
{
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            _record: std::marker::PhantomData,
        }
    }
}

/// Error type for transactional memory operations
#[derive(Error, Debug, Clone)]
pub enum TransactionalMemoryError {
    #[error("Backend error: {0}")]
    BackendError(String),
    #[error("Encoder error: {0}")]
    EncoderError(String),
    #[error("Key conversion error: {0}")]
    KeyConversionError(String),
    #[error("Value conversion error: {0}")]
    ValueConversionError(String),
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<const HASH_SIZE: usize, Backend, Record> TransactionalMemory
    for Memory<HASH_SIZE, Record, Backend>
where
    Backend: Encoder<HASH_SIZE>
        + AtomicStorageBackend<Value = Backend::Bytes>
        + ConditionalSync
        + 'static,
    Backend::Hash: AsRef<[u8]> + ConditionalSync,
    Backend::Bytes: AsRef<[u8]> + ConditionalSync + 'static,
    Backend::Key: ConditionalSync + From<Backend::Bytes>,
    <Backend as Encoder<HASH_SIZE>>::Error: std::fmt::Display,
    <Backend as AtomicStorageBackend>::Error: std::fmt::Display,
    Record: MemoryRecord + ConditionalSync + std::fmt::Debug + 'static,
    Record::Address: std::fmt::Debug + ConditionalSync,
    Record::Edition: std::fmt::Debug + ConditionalSync,
{
    type Record = Record;
    type Hash = Backend::Hash;
    type Error = TransactionalMemoryError;

    async fn read(&self, address: &Record::Address) -> Result<Option<Record>, Self::Error> {
        // Encode address to bytes and use as storage key
        let (_address_hash, address_bytes) = self
            .backend
            .encode(address)
            .await
            .map_err(|e| TransactionalMemoryError::EncoderError(e.to_string()))?;
        let key: Backend::Key = address_bytes.into();

        // Try to resolve the record from storage
        let Some(encoded_bytes) = self
            .backend
            .resolve(&key)
            .await
            .map_err(|e| TransactionalMemoryError::BackendError(e.to_string()))?
        else {
            return Ok(None);
        };

        // Decode the record
        let record: Record = self
            .backend
            .decode(encoded_bytes.as_ref())
            .await
            .map_err(|e| TransactionalMemoryError::EncoderError(e.to_string()))?;

        Ok(Some(record))
    }

    async fn merge(&mut self, record: Record) -> Result<Self::Hash, Self::Error> {
        // Encode address to bytes and use as storage key
        let (_address_hash, address_bytes) = self
            .backend
            .encode(record.address())
            .await
            .map_err(|e| TransactionalMemoryError::EncoderError(e.to_string()))?;
        let address_key: Backend::Key = address_bytes.into();

        // Encode cause (edition) to bytes if present for CAS operation
        let cause_value: Option<Backend::Bytes> = match record.cause() {
            Some(edition) => {
                let (_edition_hash, edition_bytes) = self
                    .backend
                    .encode(edition)
                    .await
                    .map_err(|e| TransactionalMemoryError::EncoderError(e.to_string()))?;
                Some(edition_bytes)
            }
            None => None,
        };

        // Encode the record to get hash and bytes
        let (hash, encoded_bytes) = self
            .backend
            .encode(&record)
            .await
            .map_err(|e| TransactionalMemoryError::EncoderError(e.to_string()))?;

        // Perform the CAS operation
        self.backend
            .swap(address_key, Some(encoded_bytes), cause_value)
            .await
            .map_err(|e| TransactionalMemoryError::BackendError(e.to_string()))?;

        Ok(hash)
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

    impl MemoryRecord for TestRecord {
        type Address = String;
        type Edition = Edition<TestRecord>;

        fn address(&self) -> &Self::Address {
            &self.name
        }

        fn cause(&self) -> Option<&Self::Edition> {
            None // Simple test record without causality
        }
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_transactional_storage_creation() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = TransactionalStorage::new(backend, CborEncoder);

        let _store = storage.at::<32>("test");
        // If we get here, storage and store were created successfully
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_transactional_store_resolve_nonexistent() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = TransactionalStorage::new(backend, CborEncoder);
        let store = storage.at::<32>("test");

        let key = b"key1".to_vec();
        let result: Option<TestRecord> = store.resolve::<32, _, _>(&key).await.unwrap();
        assert_eq!(result, None);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_transactional_store_swap_create() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = TransactionalStorage::new(backend, CborEncoder);
        let mut store = storage.at::<32>("test");

        let key = b"key1".to_vec();
        let record = TestRecord {
            name: "key1".to_string(),
            value: 42,
        };

        // Create new record (when = None)
        store
            .swap::<32, _, _>(key.clone(), Some(record.clone()), None)
            .await
            .unwrap();

        // Verify it was stored
        let result: Option<TestRecord> = store.resolve::<32, _, _>(&key).await.unwrap();
        assert_eq!(result, Some(record));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_transactional_store_swap_update() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = TransactionalStorage::new(backend, CborEncoder);
        let mut store = storage.at::<32>("test");

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
            .swap::<32, _, _>(key.clone(), Some(record1.clone()), None)
            .await
            .unwrap();

        // Update with CAS
        store
            .swap::<32, _, _>(key.clone(), Some(record2.clone()), Some(record1))
            .await
            .unwrap();

        // Verify update
        let result: Option<TestRecord> = store.resolve::<32, _, _>(&key).await.unwrap();
        assert_eq!(result, Some(record2));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_transactional_store_swap_delete() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = TransactionalStorage::new(backend, CborEncoder);
        let mut store = storage.at::<32>("test");

        let key = b"key1".to_vec();
        let record = TestRecord {
            name: "key1".to_string(),
            value: 42,
        };

        // Create
        store
            .swap::<32, _, _>(key.clone(), Some(record.clone()), None)
            .await
            .unwrap();

        // Delete with CAS
        store
            .swap::<32, Vec<u8>, TestRecord>(key.clone(), None, Some(record))
            .await
            .unwrap();

        // Verify deletion
        let result: Option<TestRecord> = store.resolve::<32, _, _>(&key).await.unwrap();
        assert_eq!(result, None);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_transactional_store_nested_namespaces() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = TransactionalStorage::new(backend, CborEncoder);

        let mut store1 = storage.at::<32>("namespace1");
        let mut store2 = storage.at::<32>("namespace1").at::<32>("nested");

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
            .swap::<32, _, _>(key.clone(), Some(record1.clone()), None)
            .await
            .unwrap();
        store2
            .swap::<32, _, _>(key.clone(), Some(record2.clone()), None)
            .await
            .unwrap();

        // Verify they're isolated
        let result1: Option<TestRecord> = store1.resolve::<32, _, _>(&key).await.unwrap();
        let result2: Option<TestRecord> = store2.resolve::<32, _, _>(&key).await.unwrap();

        assert_eq!(result1, Some(record1));
        assert_eq!(result2, Some(record2));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_with_transactional_store() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = TransactionalStorage::new(backend, CborEncoder);
        let store = storage.at::<32>("records");

        let mut memory = Memory::<32, TestRecord, _>::open(store);

        let record = TestRecord {
            name: "test".to_string(),
            value: 123,
        };

        // Write via Memory
        memory.merge(record.clone()).await.unwrap();

        // Read via Memory - use String key since TestRecord::Address is String
        let result = memory.read(&"test".to_string()).await.unwrap();
        assert_eq!(result, Some(record));
    }
}
