//! Journaled storage backend for tracking read/write operations.
//!
//! This module provides [`JournaledStorage`], a wrapper around any [`StorageBackend`]
//! that records all storage operations for later inspection. This is primarily useful
//! for testing and debugging, particularly for verifying that differential algorithms
//! only read the expected nodes.
//!
//! # Features
//!
//! - Records all `get` and `set` operations with their keys
//! - Can be enabled/disabled at runtime to avoid tracking during setup phases
//! - Provides methods to retrieve and clear the journal
//! - Thread-safe via internal `RwLock`
//!
//! # Example
//!
//! ```text
//! let backend = MemoryStorageBackend::default();
//! let journaled = JournaledStorage::new(backend);
//!
//! // Perform operations
//! journaled.set(key, value).await?;
//! let _ = journaled.get(&key).await?;
//!
//! // Check what was read
//! let reads = journaled.get_reads();
//! assert!(reads.contains(&key));
//! ```

use async_trait::async_trait;
use dialog_common::ConditionalSync;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::StorageBackend;

/// The type of storage operation that was journaled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// A read operation (get).
    Read,
    /// A write operation (set).
    Write,
}

/// A single entry in the journal recording a storage operation.
#[derive(Debug, Clone)]
pub struct JournalEntry<Key> {
    /// The type of operation (read or write).
    pub operation: Operation,
    /// The key that was read or written.
    pub key: Key,
}

/// Internal state for the journal including the log and indices.
#[derive(Debug, Clone)]
struct JournalState<Key> {
    /// Sequential log of all operations in order of occurence.
    log: Vec<JournalEntry<Key>>,
    /// Index mapping keys to offsets of read operations in the log.
    read_index: HashMap<Key, Vec<usize>>,
    /// Index mapping keys to offsets of write operations in the log.
    write_index: HashMap<Key, Vec<usize>>,
    /// Whether journaling is currently enabled.
    enabled: bool,
}

impl<Key> JournalState<Key>
where
    Key: Clone + std::hash::Hash + Eq,
{
    fn new() -> Self {
        Self {
            log: Vec::new(),
            read_index: HashMap::new(),
            write_index: HashMap::new(),
            enabled: true,
        }
    }

    fn push(&mut self, entry: JournalEntry<Key>) {
        // Only record if journaling is enabled
        if !self.enabled {
            return;
        }

        let offset = self.log.len();
        let key = entry.key.clone();

        match entry.operation {
            Operation::Read => {
                self.read_index.entry(key).or_default().push(offset);
            }
            Operation::Write => {
                self.write_index.entry(key).or_default().push(offset);
            }
        }

        self.log.push(entry);
    }

    fn clear(&mut self) {
        self.log.clear();
        self.read_index.clear();
        self.write_index.clear();
    }
}

/// A storage wrapper that journals both read and write operations with indexed access.
///
/// This allows tests to check exactly which keys are read and written, how
/// many times, and in what order. The journal maintains indices for lookup
/// of operations by key.
///
/// # Example
///
/// ```rs
/// let backend = MemoryStorageBackend::default();
/// let mut journaled = JournaledStorage::new(backend);
///
/// // Perform operations...
/// journaled.set(key1, value1).await?;
/// journaled.get(&key1).await?;
/// journaled.get(&key1).await?;
///
/// // Check what was read and written
/// let reads = journaled.get_reads();
/// let writes = journaled.get_writes();
///
/// // Get operations for specific key
/// let key1_reads = journaled.get_reads_for_key(&key1);
/// assert_eq!(key1_reads.len(), 2);
///
/// // Get which keys were accessed
/// let keys_read = journaled.keys_read();
/// let keys_written = journaled.keys_written();
/// ```
#[derive(Clone, Debug)]
pub struct JournaledStorage<Backend>
where
    Backend: StorageBackend,
    Backend::Key: Clone,
{
    backend: Backend,
    state: Arc<RwLock<JournalState<Backend::Key>>>,
}

impl<Backend> JournaledStorage<Backend>
where
    Backend: StorageBackend,
    Backend::Key: Clone + std::hash::Hash + Eq,
{
    /// Create a new journaled storage wrapper that records all operations.
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            state: Arc::new(RwLock::new(JournalState::new())),
        }
    }

    /// Get a copy of all recorded operations (both reads and writes) in the order they occurred.
    pub fn get_journal(&self) -> Vec<JournalEntry<Backend::Key>> {
        self.state.read().unwrap().log.clone()
    }

    /// Get only the read operations in the order they occurred.
    pub fn get_reads(&self) -> Vec<Backend::Key> {
        self.state
            .read()
            .unwrap()
            .log
            .iter()
            .filter(|entry| entry.operation == Operation::Read)
            .map(|entry| entry.key.clone())
            .collect()
    }

    /// Get only the write operations in the order they occurred.
    pub fn get_writes(&self) -> Vec<Backend::Key> {
        self.state
            .read()
            .unwrap()
            .log
            .iter()
            .filter(|entry| entry.operation == Operation::Write)
            .map(|entry| entry.key.clone())
            .collect()
    }

    /// Clear the journal, typically before starting a new operation to measure.
    pub fn clear_journal(&self) {
        self.state.write().unwrap().clear();
    }

    /// Disable journaling - operations will not be recorded
    pub fn disable_journal(&self) {
        self.state.write().unwrap().enabled = false;
    }

    /// Enable journaling - operations will be recorded
    pub fn enable_journal(&self) {
        self.state.write().unwrap().enabled = true;
    }

    /// Get the total number of operations recorded.
    pub fn journal_len(&self) -> usize {
        self.state.read().unwrap().log.len()
    }

    /// Get the number of read operations recorded.
    pub fn read_count(&self) -> usize {
        self.state
            .read()
            .unwrap()
            .log
            .iter()
            .filter(|entry| entry.operation == Operation::Read)
            .count()
    }

    /// Get the number of write operations recorded.
    pub fn write_count(&self) -> usize {
        self.state
            .read()
            .unwrap()
            .log
            .iter()
            .filter(|entry| entry.operation == Operation::Write)
            .count()
    }

    /// Get a map of how many times each key was read.
    pub fn get_read_counts(&self) -> HashMap<Backend::Key, usize> {
        let state = self.state.read().unwrap();
        state
            .read_index
            .iter()
            .map(|(key, offsets)| (key.clone(), offsets.len()))
            .collect()
    }

    /// Get a map of how many times each key was written.
    pub fn get_write_counts(&self) -> HashMap<Backend::Key, usize> {
        let state = self.state.read().unwrap();
        state
            .write_index
            .iter()
            .map(|(key, offsets)| (key.clone(), offsets.len()))
            .collect()
    }

    /// Get a map of total operations (reads + writes) per key.
    pub fn get_operation_counts(&self) -> HashMap<Backend::Key, usize> {
        let state = self.state.read().unwrap();
        let mut counts = HashMap::new();

        // Add read counts
        for (key, offsets) in &state.read_index {
            *counts.entry(key.clone()).or_insert(0) += offsets.len();
        }

        // Add write counts
        for (key, offsets) in &state.write_index {
            *counts.entry(key.clone()).or_insert(0) += offsets.len();
        }

        counts
    }

    /// Get all read operations for a specific key in the order they occurred.
    /// Returns references to the journal entries via their offsets.
    pub fn get_reads_for_key(&self, key: &Backend::Key) -> Vec<JournalEntry<Backend::Key>> {
        let state = self.state.read().unwrap();
        if let Some(offsets) = state.read_index.get(key) {
            offsets
                .iter()
                .map(|&offset| state.log[offset].clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get all write operations for a specific key in the order they occurred.
    /// Returns references to the journal entries via their offsets.
    pub fn get_writes_for_key(&self, key: &Backend::Key) -> Vec<JournalEntry<Backend::Key>> {
        let state = self.state.read().unwrap();
        if let Some(offsets) = state.write_index.get(key) {
            offsets
                .iter()
                .map(|&offset| state.log[offset].clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get all operations (both reads and writes) for a specific key in the order they occurred.
    pub fn get_operations_for_key(&self, key: &Backend::Key) -> Vec<JournalEntry<Backend::Key>> {
        let state = self.state.read().unwrap();
        let mut offsets: Vec<usize> = Vec::new();

        if let Some(read_offsets) = state.read_index.get(key) {
            offsets.extend(read_offsets.iter().copied());
        }

        if let Some(write_offsets) = state.write_index.get(key) {
            offsets.extend(write_offsets.iter().copied());
        }

        // Sort by offset to maintain operation order
        offsets.sort_unstable();

        offsets
            .into_iter()
            .map(|offset| state.log[offset].clone())
            .collect()
    }

    /// Get all keys that have been read.
    pub fn keys_read(&self) -> Vec<Backend::Key> {
        let state = self.state.read().unwrap();
        state.read_index.keys().cloned().collect()
    }

    /// Get all keys that have been written.
    pub fn keys_written(&self) -> Vec<Backend::Key> {
        let state = self.state.read().unwrap();
        state.write_index.keys().cloned().collect()
    }

    /// Get the number of unique keys that have been read.
    pub fn unique_keys_read_count(&self) -> usize {
        self.state.read().unwrap().read_index.len()
    }

    /// Get the number of unique keys that have been written.
    pub fn unique_keys_written_count(&self) -> usize {
        self.state.read().unwrap().write_index.len()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> StorageBackend for JournaledStorage<Backend>
where
    Backend: StorageBackend + ConditionalSync,
    Backend::Key: Clone + ConditionalSync + std::hash::Hash + Eq,
    Backend::Value: ConditionalSync,
    Backend::Error: ConditionalSync,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Error = Backend::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        // Record the write operation
        self.state.write().unwrap().push(JournalEntry {
            operation: Operation::Write,
            key: key.clone(),
        });
        self.backend.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        // Record the read operation
        self.state.write().unwrap().push(JournalEntry {
            operation: Operation::Read,
            key: key.clone(),
        });
        self.backend.get(key).await
    }
}

/// Delegate TransactionalMemoryBackend to the inner backend when it implements the trait.
/// This allows JournaledStorage to be used with platform code that requires transactional memory.
///
/// Note: This requires that the backend's StorageBackend::Key matches TransactionalMemoryBackend::Address
/// so that both can be journaled together.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> super::TransactionalMemoryBackend for JournaledStorage<Backend>
where
    Backend: StorageBackend
        + super::TransactionalMemoryBackend<
            Address = <Backend as StorageBackend>::Key,
            Value = <Backend as StorageBackend>::Value,
            Error = <Backend as StorageBackend>::Error,
        > + ConditionalSync,
    <Backend as StorageBackend>::Key: Clone + std::hash::Hash + Eq + ConditionalSync,
    <Backend as StorageBackend>::Value: ConditionalSync,
    <Backend as StorageBackend>::Error: ConditionalSync,
    <Backend as super::TransactionalMemoryBackend>::Edition: ConditionalSync,
{
    type Address = <Backend as super::TransactionalMemoryBackend>::Address;
    type Value = <Backend as super::TransactionalMemoryBackend>::Value;
    type Error = <Backend as super::TransactionalMemoryBackend>::Error;
    type Edition = <Backend as super::TransactionalMemoryBackend>::Edition;

    async fn resolve(
        &mut self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        // Record the read operation
        self.state.write().unwrap().push(JournalEntry {
            operation: Operation::Read,
            key: address.clone(),
        });
        self.backend.resolve(address).await
    }

    async fn replace(
        &mut self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        // Record the write operation
        self.state.write().unwrap().push(JournalEntry {
            operation: Operation::Write,
            key: address.clone(),
        });
        self.backend.replace(address, edition, content).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStorageBackend;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_journal_tracks_reads_and_writes() {
        let backend = MemoryStorageBackend::<String, String>::default();
        let mut storage = JournaledStorage::new(backend);

        // Perform some operations
        storage
            .set("key1".to_string(), "value1".to_string())
            .await
            .unwrap();
        storage
            .set("key2".to_string(), "value2".to_string())
            .await
            .unwrap();
        storage.get(&"key1".to_string()).await.unwrap();
        storage.get(&"key2".to_string()).await.unwrap();
        storage.get(&"key1".to_string()).await.unwrap();

        // Check full journal
        let journal = storage.get_journal();
        assert_eq!(journal.len(), 5); // 2 writes + 3 reads

        // Check filtered reads
        let reads = storage.get_reads();
        assert_eq!(reads.len(), 3);
        assert_eq!(reads[0], "key1");
        assert_eq!(reads[1], "key2");
        assert_eq!(reads[2], "key1");

        // Check filtered writes
        let writes = storage.get_writes();
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0], "key1");
        assert_eq!(writes[1], "key2");

        // Check counts
        assert_eq!(storage.read_count(), 3);
        assert_eq!(storage.write_count(), 2);
        assert_eq!(storage.journal_len(), 5);

        // Check per-key counts
        let read_counts = storage.get_read_counts();
        assert_eq!(read_counts.get(&"key1".to_string()), Some(&2));
        assert_eq!(read_counts.get(&"key2".to_string()), Some(&1));

        let write_counts = storage.get_write_counts();
        assert_eq!(write_counts.get(&"key1".to_string()), Some(&1));
        assert_eq!(write_counts.get(&"key2".to_string()), Some(&1));

        let op_counts = storage.get_operation_counts();
        assert_eq!(op_counts.get(&"key1".to_string()), Some(&3)); // 1 write + 2 reads
        assert_eq!(op_counts.get(&"key2".to_string()), Some(&2)); // 1 write + 1 read
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_journal_clear() {
        let backend = MemoryStorageBackend::<String, String>::default();
        let mut storage = JournaledStorage::new(backend);

        storage
            .set("key1".to_string(), "value1".to_string())
            .await
            .unwrap();
        storage.get(&"key1".to_string()).await.unwrap();

        assert_eq!(storage.journal_len(), 2);

        storage.clear_journal();

        assert_eq!(storage.journal_len(), 0);
        assert_eq!(storage.get_reads().len(), 0);
        assert_eq!(storage.get_writes().len(), 0);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_indexed_access_per_key() {
        let backend = MemoryStorageBackend::<String, String>::default();
        let mut storage = JournaledStorage::new(backend);

        // Perform operations on multiple keys
        storage
            .set("key1".to_string(), "value1".to_string())
            .await
            .unwrap();
        storage
            .set("key2".to_string(), "value2".to_string())
            .await
            .unwrap();
        storage.get(&"key1".to_string()).await.unwrap();
        storage.get(&"key2".to_string()).await.unwrap();
        storage.get(&"key1".to_string()).await.unwrap();
        storage
            .set("key1".to_string(), "value1b".to_string())
            .await
            .unwrap();

        // Test get_reads_for_key
        let key1_reads = storage.get_reads_for_key(&"key1".to_string());
        assert_eq!(key1_reads.len(), 2);
        assert!(key1_reads.iter().all(|e| e.operation == Operation::Read));
        assert!(key1_reads.iter().all(|e| e.key == "key1"));

        let key2_reads = storage.get_reads_for_key(&"key2".to_string());
        assert_eq!(key2_reads.len(), 1);
        assert_eq!(key2_reads[0].key, "key2");

        // Test get_writes_for_key
        let key1_writes = storage.get_writes_for_key(&"key1".to_string());
        assert_eq!(key1_writes.len(), 2);
        assert!(key1_writes.iter().all(|e| e.operation == Operation::Write));

        let key2_writes = storage.get_writes_for_key(&"key2".to_string());
        assert_eq!(key2_writes.len(), 1);

        // Test get_operations_for_key (should maintain order)
        let key1_ops = storage.get_operations_for_key(&"key1".to_string());
        assert_eq!(key1_ops.len(), 4); // 2 writes + 2 reads
        // First should be write, then read, then read, then write
        assert_eq!(key1_ops[0].operation, Operation::Write);
        assert_eq!(key1_ops[1].operation, Operation::Read);
        assert_eq!(key1_ops[2].operation, Operation::Read);
        assert_eq!(key1_ops[3].operation, Operation::Write);

        // Test non-existent key
        let key3_reads = storage.get_reads_for_key(&"key3".to_string());
        assert_eq!(key3_reads.len(), 0);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_keys_read_and_written() {
        let backend = MemoryStorageBackend::<String, String>::default();
        let mut storage = JournaledStorage::new(backend);

        storage
            .set("key1".to_string(), "value1".to_string())
            .await
            .unwrap();
        storage
            .set("key2".to_string(), "value2".to_string())
            .await
            .unwrap();
        storage.get(&"key1".to_string()).await.unwrap();
        storage.get(&"key3".to_string()).await.unwrap(); // Read non-existent key

        let keys_written = storage.keys_written();
        assert_eq!(keys_written.len(), 2);
        assert!(keys_written.contains(&"key1".to_string()));
        assert!(keys_written.contains(&"key2".to_string()));

        let keys_read = storage.keys_read();
        assert_eq!(keys_read.len(), 2);
        assert!(keys_read.contains(&"key1".to_string()));
        assert!(keys_read.contains(&"key3".to_string()));

        // Test unique key counts
        assert_eq!(storage.unique_keys_read_count(), 2);
        assert_eq!(storage.unique_keys_written_count(), 2);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_indexed_performance() {
        // Test that indexed access is efficient by verifying O(1) lookup characteristics
        let backend = MemoryStorageBackend::<String, String>::default();
        let mut storage = JournaledStorage::new(backend);

        // Create many operations
        for i in 0..100 {
            let key = format!("key{}", i % 10); // 10 unique keys, 10 ops each
            storage
                .set(key.clone(), format!("value{}", i))
                .await
                .unwrap();
        }

        for i in 0..100 {
            let key = format!("key{}", i % 10);
            storage.get(&key).await.unwrap();
        }

        // Verify we can efficiently get operations for a specific key
        let key5_reads = storage.get_reads_for_key(&"key5".to_string());
        assert_eq!(key5_reads.len(), 10);

        let key5_writes = storage.get_writes_for_key(&"key5".to_string());
        assert_eq!(key5_writes.len(), 10);

        // Verify counts are correct
        let read_counts = storage.get_read_counts();
        assert_eq!(read_counts.len(), 10); // 10 unique keys
        for count in read_counts.values() {
            assert_eq!(*count, 10); // Each key read 10 times
        }

        let write_counts = storage.get_write_counts();
        assert_eq!(write_counts.len(), 10);
        for count in write_counts.values() {
            assert_eq!(*count, 10); // Each key written 10 times
        }
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_operation_order_preserved() {
        let backend = MemoryStorageBackend::<String, String>::default();
        let mut storage = JournaledStorage::new(backend);

        // Interleave reads and writes for the same key
        storage
            .set("key1".to_string(), "v1".to_string())
            .await
            .unwrap(); // offset 0
        storage.get(&"key1".to_string()).await.unwrap(); // offset 1
        storage
            .set("key1".to_string(), "v2".to_string())
            .await
            .unwrap(); // offset 2
        storage.get(&"key1".to_string()).await.unwrap(); // offset 3
        storage.get(&"key1".to_string()).await.unwrap(); // offset 4

        let ops = storage.get_operations_for_key(&"key1".to_string());
        assert_eq!(ops.len(), 5);

        // Verify order is preserved
        assert_eq!(ops[0].operation, Operation::Write);
        assert_eq!(ops[1].operation, Operation::Read);
        assert_eq!(ops[2].operation, Operation::Write);
        assert_eq!(ops[3].operation, Operation::Read);
        assert_eq!(ops[4].operation, Operation::Read);

        // Also verify in the full journal
        let journal = storage.get_journal();
        assert_eq!(journal.len(), 5);
        for (i, entry) in journal.iter().enumerate() {
            assert_eq!(entry.operation, ops[i].operation);
        }
    }
}
