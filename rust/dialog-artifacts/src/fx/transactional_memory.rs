//! Effectful transactional memory with cell deduplication.
//!
//! This module provides shared state cells that maintain synchronization when
//! the same address is opened multiple times. Changes made through one cell
//! are immediately visible to others with the same address.
//!
//! # Design
//!
//! - `TransactionalMemory<T, A>`: Manages cell deduplication - opening the same
//!   address twice returns cells that share state. Does not require a backend.
//!
//! - `Cell<T, A>`: A shared cell with cached decoded value. Provides effectful
//!   methods for storage operations:
//!   - `open()` - Load from storage (effectful, requires `Memory<A>`)
//!   - `reload()` - Refresh from storage (effectful)
//!   - `replace()` - CAS write to storage (effectful)
//!   - `replace_with()` - Atomic read-modify-write with retry (effectful)
//!   - `read()` - Synchronous read from cache
//!
//! # Least Authority
//!
//! By parameterizing over address type `A`:
//! - `Cell<T, LocalAddress>` only requires `Memory<LocalAddress>`
//! - `Cell<T, RemoteAddress>` only requires `Memory<RemoteAddress>`
//!
//! This prevents code with local-only access from touching remote storage.
//!
//! # Example
//!
//! ```ignore
//! use dialog_artifacts::fx::transactional_memory::{Cell, TransactionalMemory};
//! use dialog_artifacts::fx::local::Address as LocalAddress;
//!
//! // TransactionalMemory manages cell deduplication (no backend needed)
//! let memory: TransactionalMemory<BranchState, LocalAddress> = TransactionalMemory::new();
//!
//! // Open a cell (effectful - loads from storage)
//! let cell = memory.open(address, key)
//!     .perform(&mut env)
//!     .await?;
//!
//! // Read is synchronous from cache
//! let state = cell.read();
//!
//! // Write with CAS (effectful)
//! cell.replace(Some(new_state))
//!     .perform(&mut env)
//!     .await?;
//! ```

use super::effects::{effectful, Memory};
use super::errors::MemoryError;
use dialog_common::fx::Effect;
use dialog_common::ConditionalSync;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, RwLock, Weak};

/// State for a transactional memory cell.
#[derive(Debug, Clone)]
pub struct CellState<T, Edition = Vec<u8>> {
    /// The current cached value.
    pub value: Option<T>,
    /// The edition/version for CAS operations.
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

/// Policy controlling retry behavior for transactional operations.
#[derive(Debug, Clone)]
pub enum UpdatePolicy {
    /// Retry up to the specified number of times after the initial optimistic attempt.
    MaxRetries(usize),
}

impl Default for UpdatePolicy {
    fn default() -> Self {
        UpdatePolicy::MaxRetries(1)
    }
}

/// A shared transactional memory cell.
///
/// Multiple cells for the same address share state via `Arc`. Reads are
/// synchronous from the cached value. Writes use CAS semantics and are effectful.
///
/// # Type Parameters
///
/// - `T`: The type stored in the cell (must be serializable)
/// - `A`: The address type (e.g., `LocalAddress` or `RemoteAddress`)
/// - `Edition`: The edition/version type for CAS (defaults to `Vec<u8>`)
#[derive(Clone)]
pub struct Cell<T, A = super::local::Address, Edition = Vec<u8>> {
    /// The storage address.
    address: A,
    /// The key within the address's storage.
    key: Vec<u8>,
    /// Shared state.
    state: Arc<RwLock<CellState<T, Edition>>>,
    /// Update policy for retries.
    policy: UpdatePolicy,
}

impl<T, A, Edition> Cell<T, A, Edition>
where
    T: Clone,
    A: Clone,
    Edition: Clone,
{
    /// Create a new cell with the given address and initial state.
    pub fn new(address: A, key: Vec<u8>, state: CellState<T, Edition>) -> Self {
        Self {
            address,
            key,
            state: Arc::new(RwLock::new(state)),
            policy: UpdatePolicy::default(),
        }
    }

    /// Create a cell from an existing shared state.
    pub fn from_shared(
        address: A,
        key: Vec<u8>,
        state: Arc<RwLock<CellState<T, Edition>>>,
    ) -> Self {
        Self {
            address,
            key,
            state,
            policy: UpdatePolicy::default(),
        }
    }

    /// Set the update policy for this cell.
    pub fn with_policy(mut self, policy: UpdatePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Read the cached value (synchronous).
    pub fn read(&self) -> Option<T> {
        let guard = self.state.read().unwrap_or_else(|e| e.into_inner());
        guard.value.clone()
    }

    /// Read the cached value with a callback.
    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Option<T>) -> R,
    {
        let guard = self.state.read().unwrap_or_else(|e| e.into_inner());
        f(&guard.value)
    }

    /// Get the current edition.
    pub fn edition(&self) -> Option<Edition> {
        let guard = self.state.read().unwrap_or_else(|e| e.into_inner());
        guard.edition.clone()
    }

    /// Update the cached state directly (used after successful CAS).
    fn update_cache(&self, value: Option<T>, edition: Option<Edition>) {
        let mut guard = self.state.write().unwrap_or_else(|e| e.into_inner());
        *guard = CellState { value, edition };
    }

    /// Get the key for this cell.
    pub fn key(&self) -> &Vec<u8> {
        &self.key
    }

    /// Get the address for this cell.
    pub fn address(&self) -> &A {
        &self.address
    }
}

impl<T, A> Cell<T, A, Vec<u8>>
where
    T: Serialize + DeserializeOwned + Clone + Debug + ConditionalSync,
    A: Clone + Send + Sync + 'static,
{
    /// Opens a new cell at the given address, fetching the current value from storage.
    #[effectful(Memory<A>)]
    pub fn open(address: A, key: Vec<u8>) -> Result<Self, MemoryError> {
        let (value, edition) =
            if let Some((bytes, edition)) = perform!(Memory::<A>().resolve(address.clone(), key.clone()))? {
                
                
                
                fn open<A, P: Memory<A>>(address: A, key: Vec<u8>) -> impl Effect<Result<Self, MemoryError>> {
                    Task::spawn(|&mut provider| async move {
                        
                    })
                }

                let decoded: T = serde_ipld_dagcbor::from_slice(&bytes)
                    .map_err(|e| MemoryError::Storage(format!("Decode error: {}", e)))?;
                (Some(decoded), Some(edition))
            } else {
                (None, None)
            };

        Ok(Self {
            address,
            key,
            state: Arc::new(RwLock::new(CellState { value, edition })),
            policy: UpdatePolicy::default(),
        })
    }

    /// Reloads content from storage, replacing the cached state entirely.
    #[effectful(Memory<A>)]
    pub fn reload(&self) -> Result<(), MemoryError> {
        let (value, edition) =
            if let Some((bytes, edition)) = perform!(Memory::<A>().resolve(self.address.clone(), self.key.clone()))? {
                let decoded: T = serde_ipld_dagcbor::from_slice(&bytes)
                    .map_err(|e| MemoryError::Storage(format!("Decode error: {}", e)))?;
                (Some(decoded), Some(edition))
            } else {
                (None, None)
            };

        self.update_cache(value, edition);
        Ok(())
    }

    /// Replace the value with CAS semantics.
    ///
    /// Uses the cached edition to detect conflicts - if storage was updated
    /// since our last read, the write is rejected.
    #[effectful(Memory<A>)]
    pub fn replace(&self, value: Option<T>) -> Result<(), MemoryError> {
        let encoded = if let Some(ref v) = value {
            Some(
                serde_ipld_dagcbor::to_vec(v)
                    .map_err(|e| MemoryError::Storage(format!("Encode error: {}", e)))?,
            )
        } else {
            None
        };

        let current_edition = self.edition();

        let new_edition = perform!(Memory::<A>().replace(
            self.address.clone(),
            self.key.clone(),
            current_edition,
            encoded
        ))?;

        self.update_cache(value, new_edition);
        Ok(())
    }

    /// Atomic read-modify-write with automatic retry on conflict.
    ///
    /// Calls `f` with the current cached value to compute a new value, then
    /// attempts CAS. On conflict, reloads fresh data and retries according to
    /// the policy.
    #[effectful(Memory<A>)]
    pub fn replace_with<F>(&self, f: F) -> Result<(), MemoryError>
    where
        F: Fn(&Option<T>) -> Option<T> + Send + Sync + 'static,
    {
        let UpdatePolicy::MaxRetries(mut n) = self.policy.clone();

        loop {
            let current_value = self.read();
            let new_value = f(&current_value);

            let encoded = if let Some(ref v) = new_value {
                Some(
                    serde_ipld_dagcbor::to_vec(v)
                        .map_err(|e| MemoryError::Storage(format!("Encode error: {}", e)))?,
                )
            } else {
                None
            };

            let current_edition = self.edition();

            match perform!(Memory::<A>().replace(
                self.address.clone(),
                self.key.clone(),
                current_edition,
                encoded
            )) {
                Ok(new_edition) => {
                    self.update_cache(new_value, new_edition);
                    return Ok(());
                }
                Err(e) => {
                    if n > 0 {
                        n -= 1;
                        perform!(self.reload())?;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }
}

impl<T, A, Edition> Debug for Cell<T, A, Edition>
where
    T: Debug + Clone,
    A: Debug,
    Edition: Debug + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.state.read().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("Cell")
            .field("address", &self.address)
            .field("key", &self.key)
            .field("value", &guard.value)
            .field("edition", &guard.edition)
            .finish()
    }
}

/// Manages transactional memory cells with deduplication.
///
/// When the same address is opened multiple times, returns cells that share
/// state. Cells are tracked with weak references, so they are automatically
/// cleaned up when all strong references are dropped.
///
/// Storage operations are effectful - the broker itself does not hold a backend.
///
/// # Type Parameters
///
/// - `T`: The type stored in cells
/// - `A`: The address type (e.g., `LocalAddress` or `RemoteAddress`)
/// - `Edition`: The edition/version type for CAS (defaults to `Vec<u8>`)
pub struct TransactionalMemory<T, A = super::local::Address, Edition = Vec<u8>> {
    cells: RwLock<HashMap<(A, Vec<u8>), Weak<RwLock<CellState<T, Edition>>>>>,
    policy: UpdatePolicy,
}

impl<T, A, Edition> Default for TransactionalMemory<T, A, Edition>
where
    A: Hash + Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T, A, Edition> TransactionalMemory<T, A, Edition>
where
    A: Hash + Eq,
{
    /// Create a new transactional memory manager.
    pub fn new() -> Self {
        Self {
            cells: RwLock::new(HashMap::new()),
            policy: UpdatePolicy::default(),
        }
    }

    /// Set the default update policy for cells opened from this memory.
    pub fn with_policy(mut self, policy: UpdatePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Get an existing cell if one is open for this address.
    pub fn get(&self, address: &A, key: &[u8]) -> Option<Cell<T, A, Edition>>
    where
        A: Clone,
        T: Clone,
        Edition: Clone,
    {
        let cells = self.cells.read().unwrap_or_else(|e| e.into_inner());
        if let Some(weak) = cells.get(&(address.clone(), key.to_vec())) {
            if let Some(state) = weak.upgrade() {
                return Some(
                    Cell::from_shared(address.clone(), key.to_vec(), state)
                        .with_policy(self.policy.clone()),
                );
            }
        }
        None
    }

    /// Register a cell for deduplication.
    pub fn register(&self, cell: &Cell<T, A, Edition>)
    where
        A: Clone,
    {
        let mut cells = self.cells.write().unwrap_or_else(|e| e.into_inner());
        // Clean up dead entries
        cells.retain(|_, weak| weak.strong_count() > 0);
        cells.insert(
            (cell.address.clone(), cell.key.clone()),
            Arc::downgrade(&cell.state),
        );
    }
}

impl<T, A> TransactionalMemory<T, A, Vec<u8>>
where
    T: Serialize + DeserializeOwned + Clone + Debug + ConditionalSync,
    A: Clone + Hash + Eq + Send + Sync + 'static,
{
    /// Opens a cell at the given address.
    ///
    /// If a cell for this address is already open, returns a cell that shares
    /// state. Otherwise, fetches the current value from storage.
    #[effectful(Memory<A>)]
    pub fn open(&self, address: A, key: Vec<u8>) -> Result<Cell<T, A>, MemoryError> {
        // Check for existing cell
        if let Some(cell) = self.get(&address, &key) {
            return Ok(cell);
        }

        // Open new cell from storage
        let cell = perform!(Cell::<T, A>::open(address.clone(), key.clone()))?
            .with_policy(self.policy.clone());

        // Register for deduplication
        self.register(&cell);

        Ok(cell)
    }
}

impl<T, A, Edition> Debug for TransactionalMemory<T, A, Edition>
where
    T: Debug + Clone,
    A: Debug + Hash + Eq,
    Edition: Debug + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cells = self.cells.read().unwrap_or_else(|e| e.into_inner());
        let mut map = f.debug_map();
        for ((addr, key), weak) in cells.iter() {
            if let Some(state) = weak.upgrade() {
                let guard = state.read().unwrap_or_else(|e| e.into_inner());
                map.entry(&(addr, key), &*guard);
            }
        }
        map.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fx::local::Address as LocalAddress;

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestState {
        value: u32,
    }

    struct MockMemory {
        state: HashMap<(LocalAddress, Vec<u8>), (Vec<u8>, Vec<u8>)>,
        next_edition: u64,
    }

    impl MockMemory {
        fn new() -> Self {
            Self {
                state: HashMap::new(),
                next_edition: 1,
            }
        }
    }

    impl Memory<LocalAddress> for MockMemory {
        async fn resolve(
            &self,
            address: LocalAddress,
            key: Vec<u8>,
        ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MemoryError> {
            Ok(self.state.get(&(address, key)).cloned())
        }

        async fn replace(
            &mut self,
            address: LocalAddress,
            key: Vec<u8>,
            edition: Option<Vec<u8>>,
            content: Option<Vec<u8>>,
        ) -> Result<Option<Vec<u8>>, MemoryError> {
            let storage_key = (address, key);
            let current = self.state.get(&storage_key);

            match (current, edition.as_ref()) {
                (None, None) => {}
                (Some((_, current_edition)), Some(expected)) if current_edition == expected => {}
                _ => return Err(MemoryError::Conflict("Edition mismatch".to_string())),
            }

            match content {
                Some(value) => {
                    let new_edition = self.next_edition.to_be_bytes().to_vec();
                    self.next_edition += 1;
                    self.state.insert(storage_key, (value, new_edition.clone()));
                    Ok(Some(new_edition))
                }
                None => {
                    self.state.remove(&storage_key);
                    Ok(None)
                }
            }
        }
    }

    fn test_address() -> LocalAddress {
        LocalAddress::did("did:test:123")
    }

    #[tokio::test]
    async fn cell_open_and_read() {
        let mut memory = MockMemory::new();

        // Open empty cell
        let cell = Cell::<TestState, LocalAddress>::open(test_address(), b"key".to_vec())
            .perform(&mut memory)
            .await
            .unwrap();

        assert!(cell.read().is_none());

        // Write a value
        cell.replace(Some(TestState { value: 42 }))
            .perform(&mut memory)
            .await
            .unwrap();

        assert_eq!(cell.read(), Some(TestState { value: 42 }));
    }

    #[tokio::test]
    async fn cell_shares_state_through_arc() {
        let mut memory = MockMemory::new();

        let cell1 = Cell::<TestState, LocalAddress>::open(test_address(), b"key".to_vec())
            .perform(&mut memory)
            .await
            .unwrap();

        cell1
            .replace(Some(TestState { value: 42 }))
            .perform(&mut memory)
            .await
            .unwrap();

        // Create a second cell sharing the same state
        let cell2 = Cell::from_shared(test_address(), b"key".to_vec(), Arc::clone(&cell1.state));

        // Both cells see the same value
        assert_eq!(cell1.read(), Some(TestState { value: 42 }));
        assert_eq!(cell2.read(), Some(TestState { value: 42 }));

        // Update through cell1
        cell1
            .replace(Some(TestState { value: 100 }))
            .perform(&mut memory)
            .await
            .unwrap();

        // cell2 sees the update immediately
        assert_eq!(cell2.read(), Some(TestState { value: 100 }));
    }

    #[tokio::test]
    async fn transactional_memory_deduplicates_cells() {
        let mut backend = MockMemory::new();
        let memory: TransactionalMemory<TestState, LocalAddress> = TransactionalMemory::new();

        // First open creates a new cell
        let cell1 = memory
            .open(test_address(), b"key".to_vec())
            .perform(&mut backend)
            .await
            .unwrap();

        cell1
            .replace(Some(TestState { value: 1 }))
            .perform(&mut backend)
            .await
            .unwrap();

        // Second open returns a cell that shares state
        let cell2 = memory
            .open(test_address(), b"key".to_vec())
            .perform(&mut backend)
            .await
            .unwrap();

        // Both should have the same value
        assert_eq!(cell1.read(), Some(TestState { value: 1 }));
        assert_eq!(cell2.read(), Some(TestState { value: 1 }));

        // Update through cell1
        cell1
            .replace(Some(TestState { value: 50 }))
            .perform(&mut backend)
            .await
            .unwrap();

        // cell2 sees the update
        assert_eq!(cell2.read(), Some(TestState { value: 50 }));
    }

    #[tokio::test]
    async fn transactional_memory_cleans_up_dropped_cells() {
        let mut backend = MockMemory::new();
        let memory: TransactionalMemory<TestState, LocalAddress> = TransactionalMemory::new();

        // Create and drop a cell
        {
            let cell = memory
                .open(test_address(), b"key".to_vec())
                .perform(&mut backend)
                .await
                .unwrap();
            cell.replace(Some(TestState { value: 1 }))
                .perform(&mut backend)
                .await
                .unwrap();
        }

        // Modify storage directly (simulates external change)
        backend.state.insert(
            (test_address(), b"key".to_vec()),
            (
                serde_ipld_dagcbor::to_vec(&TestState { value: 999 }).unwrap(),
                vec![99],
            ),
        );

        // Next open should fetch fresh from storage since old cell was dropped
        let cell = memory
            .open(test_address(), b"key".to_vec())
            .perform(&mut backend)
            .await
            .unwrap();

        assert_eq!(cell.read(), Some(TestState { value: 999 }));
    }

    #[tokio::test]
    async fn cell_reload_updates_cache() {
        let mut memory = MockMemory::new();

        let cell = Cell::<TestState, LocalAddress>::open(test_address(), b"key".to_vec())
            .perform(&mut memory)
            .await
            .unwrap();

        cell.replace(Some(TestState { value: 1 }))
            .perform(&mut memory)
            .await
            .unwrap();

        // Modify storage directly
        memory.state.insert(
            (test_address(), b"key".to_vec()),
            (
                serde_ipld_dagcbor::to_vec(&TestState { value: 999 }).unwrap(),
                vec![99],
            ),
        );

        // Cell still has stale cache
        assert_eq!(cell.read(), Some(TestState { value: 1 }));

        // After reload, cell sees updated value
        cell.reload().perform(&mut memory).await.unwrap();
        assert_eq!(cell.read(), Some(TestState { value: 999 }));
    }

    #[tokio::test]
    async fn replace_with_retries_on_conflict() {
        let mut memory = MockMemory::new();

        let cell = Cell::<TestState, LocalAddress>::open(test_address(), b"key".to_vec())
            .perform(&mut memory)
            .await
            .unwrap();

        cell.replace(Some(TestState { value: 10 }))
            .perform(&mut memory)
            .await
            .unwrap();

        // Simulate concurrent update by modifying storage directly
        let new_edition = memory.next_edition.to_be_bytes().to_vec();
        memory.next_edition += 1;
        memory.state.insert(
            (test_address(), b"key".to_vec()),
            (
                serde_ipld_dagcbor::to_vec(&TestState { value: 50 }).unwrap(),
                new_edition,
            ),
        );

        // Cell still has stale cache (value: 10)
        assert_eq!(cell.read(), Some(TestState { value: 10 }));

        // replace_with should detect conflict, reload, and retry
        cell.replace_with(|current| current.as_ref().map(|s| TestState { value: s.value + 1 }))
            .perform(&mut memory)
            .await
            .unwrap();

        // After retry, should have 50 + 1 = 51
        assert_eq!(cell.read(), Some(TestState { value: 51 }));
    }
}
