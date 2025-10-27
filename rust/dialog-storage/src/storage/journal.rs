use async_trait::async_trait;
use dialog_common::ConditionalSync;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::StorageBackend;

/// A storage wrapper that journals which keys are read during operations.
///
/// This allows performance testing to verify that optimizations like hash-based
/// pruning work correctly and only read the necessary nodes. The journal records
/// the keys of all get() operations in the order they occurred.
///
/// # Example
///
/// ```ignore
/// let backend = MemoryStorageBackend::default();
/// let journaled = JournaledStorage::new(backend);
///
/// // Perform operations...
/// journaled.get(&key).await;
///
/// // Check what was read
/// let journal = journaled.get_journal().await;
/// assert_eq!(journal.len(), 1);
/// ```
#[derive(Clone)]
pub struct JournaledStorage<Backend>
where
    Backend: StorageBackend,
    Backend::Key: Clone,
{
    backend: Backend,
    journal: Arc<Mutex<Vec<Backend::Key>>>,
}

impl<Backend> JournaledStorage<Backend>
where
    Backend: StorageBackend,
    Backend::Key: Clone,
{
    /// Create a new journaled storage wrapper that records all read operations.
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            journal: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get a copy of all recorded read operations in the order they occurred.
    pub async fn get_journal(&self) -> Vec<Backend::Key> {
        self.journal.lock().await.clone()
    }

    /// Clear the journal, typically before starting a new operation to measure.
    pub async fn clear_journal(&self) {
        self.journal.lock().await.clear();
    }

    /// Get the number of read operations recorded.
    pub async fn journal_len(&self) -> usize {
        self.journal.lock().await.len()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> StorageBackend for JournaledStorage<Backend>
where
    Backend: StorageBackend + ConditionalSync,
    Backend::Key: Clone + ConditionalSync,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Error = Backend::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.backend.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        // Record the read operation
        self.journal.lock().await.push(key.clone());
        self.backend.get(key).await
    }
}
