//! Optimistic concurrency control for persistent storage.
//!
//! This module provides [`TransactionalMemory`] and [`TransactionalMemoryCell`] -
//! an [optimistic concurrency] primitive backed by durable storage (S3, filesystem,
//! etc.). Values survive process restarts and can be safely accessed by multiple
//! processes or replicas.
//!
//! # Architecture
//!
//! - [`TransactionalMemory<T>`]: Owns the backend and codec, provides cell deduplication -
//!   opening the same address twice returns cells that share state, so changes made
//!   through one cell are immediately visible to others.
//!
//! - [`TransactionalMemoryCell<T>`]: An individual cell for a specific address.
//!   Multiple cells for the same address share state via `Arc`. Cells are automatically
//!   cleaned up when all references are dropped.
//!
//! # Concurrency Model
//!
//! Cached state may become stale at any time because the underlying storage can be
//! updated by other processes. This is expected and handled gracefully:
//!
//! - Reads return the cached value, which may be outdated
//! - Writes use CAS semantics to detect conflicts
//! - `replace_with()` automatically reloads and retries on conflict
//!
//! Because staleness is inherent to the design, we also recover from `RwLock`
//! poisoning rather than propagating an error - a poisoned lock is just another
//! form of potentially stale data, handled the same way.
//!
//! [optimistic concurrency]: https://en.wikipedia.org/wiki/Software_transactional_memory

use super::backend::TransactionalMemoryBackend;
use crate::{CborEncoder, DialogStorageError, Encoder};
use dialog_common::{ConditionalSend, ConditionalSync};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock, Weak};

/// State for a transactional memory cell.
#[derive(Debug, Clone)]
pub struct CellState<T, Edition> {
    /// The current value
    pub value: Option<T>,
    /// The edition/version identifier for CAS operations
    pub edition: Option<Edition>,
}

impl<T, Edition> Default for CellState<T, Edition> {
    fn default() -> Self {
        Self {
            value: None,
            edition: None,
        }
    }
}

/// A map of open cells, keyed by address.
///
/// Tracks cells using weak references so they are automatically cleaned up
/// when all strong references are dropped. The Debug implementation shows
/// only live (non-dropped) cells.
struct Cells<Address, T, Edition>(HashMap<Address, Weak<RwLock<CellState<T, Edition>>>>);

impl<Address, T, Edition> Default for Cells<Address, T, Edition> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<Address, T, Edition> Cells<Address, T, Edition>
where
    Address: Hash + Eq,
{
    fn get(&self, address: &Address) -> Option<&Weak<RwLock<CellState<T, Edition>>>> {
        self.0.get(address)
    }

    fn insert(&mut self, address: Address, weak: Weak<RwLock<CellState<T, Edition>>>) {
        self.0.insert(address, weak);
    }

    fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&Address, &mut Weak<RwLock<CellState<T, Edition>>>) -> bool,
    {
        self.0.retain(f);
    }
}

impl<Address, T, Edition> std::fmt::Debug for Cells<Address, T, Edition>
where
    Address: std::fmt::Debug,
    T: std::fmt::Debug,
    Edition: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        for (addr, weak) in self.0.iter() {
            if let Some(state_arc) = weak.upgrade() {
                let state = state_arc.read().unwrap_or_else(|e| e.into_inner());
                map.entry(addr, &*state);
            }
        }
        map.finish()
    }
}

/// Policy controlling retry behavior for transactional operations.
///
/// Determines how many times to retry a failed Compare-And-Swap (CAS) operation
/// after reloading fresh state from storage.
#[derive(Debug, Clone)]
pub enum UpdatePolicy {
    /// Retry up to the specified number of times after the initial optimistic attempt.
    ///
    /// With `MaxRetries(n)`, the operation will:
    /// 1. Try once optimistically with the current cached state
    /// 2. On failure: reload from storage and retry up to `n` more times
    ///
    /// Example: `MaxRetries(1)` means 1 optimistic attempt + 1 reload/retry = 2 total attempts
    MaxRetries(usize),
}

impl Default for UpdatePolicy {
    fn default() -> Self {
        UpdatePolicy::MaxRetries(1)
    }
}

/// A transactional memory broker with cell deduplication.
///
/// `TransactionalMemory` brokers access to [`TransactionalMemoryCell`]s. When the
/// same address is opened multiple times, the returned cells share state, so
/// changes propagate immediately.
///
/// Cells are tracked using weak references, so when all cells for an address
/// are dropped, the entry is automatically cleaned up and the next `open()`
/// will fetch fresh data from the backend.
pub struct TransactionalMemory<T, Backend, const HASH_SIZE: usize = 32, Codec = CborEncoder>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Hash + Eq,
{
    codec: Codec,
    policy: UpdatePolicy,
    cells: RwLock<Cells<Backend::Address, T, Backend::Edition>>,
}

impl<T, Backend, const HASH_SIZE: usize> TransactionalMemory<T, Backend, HASH_SIZE, CborEncoder>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Hash + Eq,
{
    /// Creates a new transactional memory broker with the default CBOR codec.
    pub fn new() -> Self {
        Self {
            codec: CborEncoder,
            policy: UpdatePolicy::default(),
            cells: RwLock::new(Cells::default()),
        }
    }
}

impl<T, Backend, const HASH_SIZE: usize> Default
    for TransactionalMemory<T, Backend, HASH_SIZE, CborEncoder>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Hash + Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T, Backend, const HASH_SIZE: usize, Codec> TransactionalMemory<T, Backend, HASH_SIZE, Codec>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Hash + Eq,
{
    /// Creates a new transactional memory broker with a custom codec.
    pub fn with_codec(codec: Codec) -> Self {
        Self {
            codec,
            policy: UpdatePolicy::default(),
            cells: RwLock::new(Cells::default()),
        }
    }

    /// Sets the update policy for cells opened from this memory.
    pub fn with_policy(mut self, policy: UpdatePolicy) -> Self {
        self.policy = policy;
        self
    }
}

impl<T, Backend, const HASH_SIZE: usize, Codec> TransactionalMemory<T, Backend, HASH_SIZE, Codec>
where
    T: Serialize + DeserializeOwned + ConditionalSync + std::fmt::Debug + Clone,
    Backend: TransactionalMemoryBackend<Value = Vec<u8>>,
    Backend::Error: Into<DialogStorageError>,
    Backend::Address: Clone + std::fmt::Debug + Hash + Eq,
    Backend::Edition: Clone,
    Codec: Encoder + ConditionalSync + Clone,
    Codec::Bytes: AsRef<[u8]>,
    Codec::Error: std::fmt::Display,
{
    /// Opens a cell at the given address.
    ///
    /// If a cell for this address is already open (and not yet dropped), returns
    /// a new cell that shares the state. Otherwise, fetches the current value
    /// from the backend and creates a new cell.
    pub async fn open(
        &self,
        address: Backend::Address,
        backend: &Backend,
    ) -> Result<TransactionalMemoryCell<T, Backend, HASH_SIZE, Codec>, DialogStorageError> {
        // Check for existing cell
        {
            let cells = self.cells.read().unwrap_or_else(|e| e.into_inner());
            if let Some(weak) = cells.get(&address) {
                if let Some(state) = weak.upgrade() {
                    return Ok(TransactionalMemoryCell::new(
                        address,
                        state,
                        self.codec.clone(),
                        self.policy.clone(),
                    ));
                }
            }
        }

        // No existing cell - open a new one
        let cell = TransactionalMemoryCell::open(
            address.clone(),
            backend,
            self.codec.clone(),
            self.policy.clone(),
        )
        .await?;

        // Store weak reference
        {
            let mut cells = self.cells.write().unwrap_or_else(|e| e.into_inner());
            // Clean up any dead entries while we're here
            cells.retain(|_, weak| weak.strong_count() > 0);
            cells.insert(address, Arc::downgrade(&cell.state));
        }

        Ok(cell)
    }
}

impl<T, Backend, const HASH_SIZE: usize, Codec> std::fmt::Debug
    for TransactionalMemory<T, Backend, HASH_SIZE, Codec>
where
    T: std::fmt::Debug + Clone,
    Backend: TransactionalMemoryBackend,
    Backend::Address: Hash + Eq + std::fmt::Debug,
    Backend::Edition: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cells = self.cells.read().unwrap_or_else(|e| e.into_inner());
        f.debug_struct(&format!(
            "TransactionalMemory<{}>",
            std::any::type_name::<T>()
        ))
        .field("policy", &self.policy)
        .field("cells", &*cells)
        .finish()
    }
}

/// An individual transactional memory cell for a specific address.
///
/// Provides cached, typed access to a value in storage with CAS semantics.
/// Multiple cells for the same address (obtained via [`TransactionalMemory::open`])
/// share the same cached state.
///
/// # Key Features
///
/// - **Cached Decoded Values**: Decoding happens only during open() and reload()
/// - **Synchronous Reads**: read() is sync and returns the cached decoded value
/// - **CAS Semantics**: Maintains edition tracking and Compare-And-Swap guarantees
/// - **Shared State**: Multiple cells for the same address share cached state
pub struct TransactionalMemoryCell<T, Backend, const HASH_SIZE: usize = 32, Codec = CborEncoder>
where
    Backend: TransactionalMemoryBackend,
{
    address: Backend::Address,
    state: Arc<RwLock<CellState<T, Backend::Edition>>>,
    codec: Codec,
    policy: UpdatePolicy,
}

impl<T, Backend, const HASH_SIZE: usize, Codec>
    TransactionalMemoryCell<T, Backend, HASH_SIZE, Codec>
where
    T: Serialize + DeserializeOwned + ConditionalSync + std::fmt::Debug + Clone,
    Backend: TransactionalMemoryBackend<Value = Vec<u8>>,
    Backend::Error: Into<DialogStorageError>,
    Backend::Address: Clone + std::fmt::Debug,
    Backend::Edition: Clone,
    Codec: Encoder + ConditionalSync + Clone,
    Codec::Bytes: AsRef<[u8]>,
    Codec::Error: std::fmt::Display,
{
    /// Opens a new cell at the given address, fetching the current value from storage.
    pub async fn open(
        address: Backend::Address,
        backend: &Backend,
        codec: Codec,
        policy: UpdatePolicy,
    ) -> Result<Self, DialogStorageError> {
        let (value, edition) = if let Some((bytes, edition)) =
            backend.resolve(&address).await.map_err(|e| {
                DialogStorageError::StorageBackend(format!(
                    "Resolving memory at {:?} failed with error {}",
                    address,
                    e.into()
                ))
            })? {
            let decoded: T = codec
                .decode(bytes.as_ref())
                .await
                .map_err(|e| DialogStorageError::DecodeFailed(e.to_string()))?;
            (Some(decoded), Some(edition))
        } else {
            (None, None)
        };

        Ok(Self {
            address,
            state: Arc::new(RwLock::new(CellState { value, edition })),
            codec,
            policy,
        })
    }

    /// Creates a cell from an existing shared state.
    /// Used internally by `TransactionalMemory` for cell deduplication.
    pub fn new(
        address: Backend::Address,
        state: Arc<RwLock<CellState<T, Backend::Edition>>>,
        codec: Codec,
        policy: UpdatePolicy,
    ) -> Self {
        Self {
            address,
            state,
            codec,
            policy,
        }
    }

    /// Returns a reference to the address this cell is positioned at.
    pub fn address(&self) -> &Backend::Address {
        &self.address
    }

    /// Read the cached decoded value (synchronous).
    ///
    /// Returns the cached value without accessing the backend. The value may
    /// be stale - use `replace_with()` for atomic read-modify-write operations.
    pub fn read(&self) -> Option<T> {
        let entry = self.state.read().unwrap_or_else(|e| e.into_inner());
        entry.value.clone()
    }

    /// Read the cached decoded value with a callback.
    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Option<T>) -> R,
    {
        let entry = self.state.read().unwrap_or_else(|e| e.into_inner());
        f(&entry.value)
    }

    /// Reloads content from storage, replacing the cached state entirely.
    pub async fn reload(&self, backend: &Backend) -> Result<(), DialogStorageError> {
        let (value, edition) = if let Some((bytes, edition)) =
            backend.resolve(&self.address).await.map_err(|e| e.into())?
        {
            let decoded: T = self
                .codec
                .decode(bytes.as_ref())
                .await
                .map_err(|e| DialogStorageError::DecodeFailed(e.to_string()))?;
            (Some(decoded), Some(edition))
        } else {
            (None, None)
        };

        let mut guard = self.state.write().unwrap_or_else(|e| e.into_inner());
        *guard = CellState { value, edition };
        Ok(())
    }

    /// Replace the value with CAS semantics.
    ///
    /// Uses the cached edition to detect conflicts - if the backend was updated
    /// since our last read, the write is rejected. Use `replace_with()` for
    /// automatic retry on conflict.
    pub async fn replace(
        &self,
        value: Option<T>,
        backend: &Backend,
    ) -> Result<(), DialogStorageError> {
        let encoded = if let Some(ref v) = value {
            let (_, bytes) = self
                .codec
                .encode(v)
                .await
                .map_err(|e| DialogStorageError::EncodeFailed(e.to_string()))?;
            Some(bytes.as_ref().to_vec())
        } else {
            None
        };

        let current_edition = {
            let entry = self.state.read().unwrap_or_else(|e| e.into_inner());
            entry.edition.clone()
        };

        let new_edition = backend
            .replace(&self.address, current_edition.as_ref(), encoded)
            .await
            .map_err(|e| e.into())?;

        let mut guard = self.state.write().unwrap_or_else(|e| e.into_inner());
        *guard = CellState {
            value: value.clone(),
            edition: new_edition,
        };

        Ok(())
    }

    /// Atomic read-modify-write with automatic retry on conflict.
    ///
    /// Calls `f` with the current cached value to compute a new value, then
    /// attempts CAS. On conflict, reloads fresh data and retries according to
    /// the policy (`MaxRetries(n)` = 1 optimistic attempt + up to n retries).
    pub async fn replace_with<F>(&self, f: F, backend: &Backend) -> Result<(), DialogStorageError>
    where
        F: Fn(&Option<T>) -> Option<T> + ConditionalSend,
    {
        let UpdatePolicy::MaxRetries(mut n) = self.policy;

        loop {
            let current_value = {
                let entry = self.state.read().unwrap_or_else(|e| e.into_inner());
                entry.value.clone()
            };
            let new_value = f(&current_value);

            match self.replace(new_value, backend).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if n > 0 {
                        n -= 1;
                        self.reload(backend).await?;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }
}

impl<T, Backend, const HASH_SIZE: usize, Codec> Clone
    for TransactionalMemoryCell<T, Backend, HASH_SIZE, Codec>
where
    Backend: TransactionalMemoryBackend,
    Backend::Address: Clone,
    Codec: Clone,
{
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            state: Arc::clone(&self.state),
            codec: self.codec.clone(),
            policy: self.policy.clone(),
        }
    }
}

impl<T, Backend, const HASH_SIZE: usize, Codec> std::fmt::Debug
    for TransactionalMemoryCell<T, Backend, HASH_SIZE, Codec>
where
    T: std::fmt::Debug + Clone,
    Backend: TransactionalMemoryBackend,
    Backend::Address: std::fmt::Debug,
    Backend::Edition: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        f.debug_struct(&format!(
            "TransactionalMemoryCell<{}>",
            std::any::type_name::<T>()
        ))
        .field("address", &self.address)
        .field("value", &state.value)
        .field("edition", &state.edition)
        .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStorageBackend;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestData {
        name: String,
        value: u32,
    }

    #[dialog_common::test]
    async fn it_opens_non_existent_memory() -> anyhow::Result<()> {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

        let cell = memory.open(b"test-key".to_vec(), &backend).await?;

        assert!(cell.read().is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_writes_and_reads_value() -> anyhow::Result<()> {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

        let cell = memory.open(b"test-key".to_vec(), &backend).await?;

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        cell.replace(Some(data.clone()), &backend).await?;

        assert_eq!(cell.read(), Some(data.clone()));

        // Open again to verify persistence
        let cell2 = memory.open(b"test-key".to_vec(), &backend).await?;
        assert_eq!(cell2.read(), Some(data));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_shares_state_between_cells() -> anyhow::Result<()> {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

        let cell1 = memory.open(b"test-key".to_vec(), &backend).await?;
        let cell2 = memory.open(b"test-key".to_vec(), &backend).await?;

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        // Write through cell1
        cell1.replace(Some(data.clone()), &backend).await?;

        // cell2 should see the same data (shared state)
        assert_eq!(cell2.read(), Some(data));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_existing_value() -> anyhow::Result<()> {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

        let cell = memory.open(b"test-key".to_vec(), &backend).await?;

        let initial_data = TestData {
            name: "initial".to_string(),
            value: 1,
        };

        cell.replace(Some(initial_data), &backend).await?;

        let updated_data = TestData {
            name: "updated".to_string(),
            value: 2,
        };

        cell.replace(Some(updated_data.clone()), &backend).await?;

        assert_eq!(cell.read(), Some(updated_data.clone()));

        // Open again to verify the update persisted
        let cell2 = memory.open(b"test-key".to_vec(), &backend).await?;
        assert_eq!(cell2.read(), Some(updated_data));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_deletes_value() -> anyhow::Result<()> {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

        let cell = memory.open(b"test-key".to_vec(), &backend).await?;

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        cell.replace(Some(data), &backend).await?;
        cell.replace(None, &backend).await?;

        assert!(cell.read().is_none());

        // Verify deletion persisted
        let cell2 = memory.open(b"test-key".to_vec(), &backend).await?;
        assert!(cell2.read().is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_reloads_from_storage() -> anyhow::Result<()> {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let memory1: TransactionalMemory<TestData, _> = TransactionalMemory::new();
        let memory2: TransactionalMemory<TestData, _> = TransactionalMemory::new();

        let cell1 = memory1.open(b"test-key".to_vec(), &backend).await?;

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        cell1.replace(Some(data.clone()), &backend).await?;

        // Open from a different broker (simulates another process)
        let cell2 = memory2.open(b"test-key".to_vec(), &backend).await?;

        let updated_data = TestData {
            name: "updated".to_string(),
            value: 100,
        };

        cell2.replace(Some(updated_data.clone()), &backend).await?;

        // cell1 still has stale data
        assert_eq!(cell1.read(), Some(data));

        // After reload, cell1 should see the updated data
        cell1.reload(&backend).await?;
        assert_eq!(cell1.read(), Some(updated_data));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_uses_replace_with() -> anyhow::Result<()> {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

        let cell = memory.open(b"test-key".to_vec(), &backend).await?;

        // Initial write
        cell.replace_with(
            |_| {
                Some(TestData {
                    name: "initial".to_string(),
                    value: 1,
                })
            },
            &backend,
        )
        .await?;

        // Increment the value
        cell.replace_with(
            |current| {
                current.as_ref().map(|d| TestData {
                    name: d.name.clone(),
                    value: d.value + 1,
                })
            },
            &backend,
        )
        .await?;

        assert_eq!(
            cell.read(),
            Some(TestData {
                name: "initial".to_string(),
                value: 2,
            })
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_cleans_up_dropped_cells() -> anyhow::Result<()> {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        // Open and write
        {
            let cell = memory.open(b"test-key".to_vec(), &backend).await?;
            cell.replace(Some(data.clone()), &backend).await?;
        }
        // cell is dropped here

        // Modify the backend directly (simulates external change)
        let updated_data = TestData {
            name: "updated".to_string(),
            value: 100,
        };
        {
            // Use a separate memory to write
            let memory2: TransactionalMemory<TestData, _> = TransactionalMemory::new();
            let cell2 = memory2.open(b"test-key".to_vec(), &backend).await?;
            cell2.replace(Some(updated_data.clone()), &backend).await?;
        }

        // Open again - should fetch fresh from backend since old cell was dropped
        let cell3 = memory.open(b"test-key".to_vec(), &backend).await?;
        assert_eq!(cell3.read(), Some(updated_data));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_retries_on_concurrent_update() -> anyhow::Result<()> {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

        // Open cell and write initial value
        let cell = memory.open(b"test-key".to_vec(), &backend).await?;
        cell.replace(
            Some(TestData {
                name: "counter".to_string(),
                value: 10,
            }),
            &backend,
        )
        .await?;

        // Simulate concurrent update from another process by opening the cell
        // directly and modifying the backend. This makes our cell's edition stale.
        {
            let other_cell = TransactionalMemoryCell::<TestData, _, 32, CborEncoder>::open(
                b"test-key".to_vec(),
                &backend,
                CborEncoder,
                UpdatePolicy::default(),
            )
            .await?;

            other_cell
                .replace(
                    Some(TestData {
                        name: "counter".to_string(),
                        value: 50, // Changed by "another process"
                    }),
                    &backend,
                )
                .await?;
        }

        // Our cell still has stale data (value: 10)
        assert_eq!(
            cell.read(),
            Some(TestData {
                name: "counter".to_string(),
                value: 10,
            })
        );

        // But replace_with should detect the conflict, reload, and retry
        // It will increment whatever the current value is (50 -> 51)
        cell.replace_with(
            |current| {
                current.as_ref().map(|d| TestData {
                    name: d.name.clone(),
                    value: d.value + 1,
                })
            },
            &backend,
        )
        .await?;

        // After replace_with succeeds, our cell has the correct merged result
        assert_eq!(
            cell.read(),
            Some(TestData {
                name: "counter".to_string(),
                value: 51, // 50 (from other process) + 1
            })
        );

        Ok(())
    }
}
