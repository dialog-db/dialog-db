//! Algebraic effects system.
//!
//! This module re-exports the core effect types from `dialog_common::fx`
//! and provides tests demonstrating the `#[effect]` macro.

pub use dialog_common::fx::*;

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_macros::effect;
    use dialog_storage::{DialogStorageError, MemoryStorageBackend, StorageBackend};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // =========================================================================
    // BlockStore - Effect trait mirroring StorageBackend
    // Uses concrete Vec<u8> types for simplicity
    // =========================================================================

    #[effect]
    pub trait BlockStore {
        async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, DialogStorageError>;
        async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DialogStorageError>;
    }

    // Implement effect trait for any StorageBackend with Vec<u8> key/value
    impl<B> BlockStore::BlockStore for B
    where
        B: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + dialog_common::ConditionalSync,
        B::Error: Into<DialogStorageError>,
    {
        async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, DialogStorageError> {
            StorageBackend::get(self, &key).await.map_err(Into::into)
        }

        async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DialogStorageError> {
            StorageBackend::set(self, key, value)
                .await
                .map_err(Into::into)
        }
    }

    // =========================================================================
    // TransactionalMemory - Effect trait mirroring TransactionalMemoryBackend
    // Uses concrete Vec<u8> types for simplicity
    // =========================================================================

    #[effect]
    pub trait TransactionalMemory {
        async fn resolve(
            &self,
            address: Vec<u8>,
        ) -> Result<Option<(Vec<u8>, Vec<u8>)>, DialogStorageError>;

        async fn replace(
            &self,
            address: Vec<u8>,
            edition: Option<Vec<u8>>,
            content: Option<Vec<u8>>,
        ) -> Result<Option<Vec<u8>>, DialogStorageError>;
    }

    // Implement effect trait for MemoryStorageBackend<Vec<u8>, Vec<u8>>
    impl TransactionalMemory::TransactionalMemory for MemoryStorageBackend<Vec<u8>, Vec<u8>> {
        async fn resolve(
            &self,
            address: Vec<u8>,
        ) -> Result<Option<(Vec<u8>, Vec<u8>)>, DialogStorageError> {
            dialog_storage::TransactionalMemoryBackend::resolve(self, &address).await
        }

        async fn replace(
            &self,
            address: Vec<u8>,
            edition: Option<Vec<u8>>,
            content: Option<Vec<u8>>,
        ) -> Result<Option<Vec<u8>>, DialogStorageError> {
            dialog_storage::TransactionalMemoryBackend::replace(
                self,
                &address,
                edition.as_ref(),
                content,
            )
            .await
        }
    }

    // =========================================================================
    // Provider wrapper for Arc<Mutex<T>>
    // =========================================================================

    /// A provider wrapper for BlockStore
    struct BlockStoreProvider<T>(Arc<Mutex<T>>);

    impl<T> Provider for BlockStoreProvider<T>
    where
        T: BlockStore::BlockStore + Send,
    {
        type Capability = BlockStore::Capability;

        async fn provide(&self, capability: Self::Capability) -> BlockStore::Output {
            let mut backend = self.0.lock().await;
            BlockStore::dispatch(&mut *backend, capability).await
        }
    }

    /// A provider wrapper for TransactionalMemory
    struct TransactionalMemoryProvider<T>(Arc<Mutex<T>>);

    impl<T> Provider for TransactionalMemoryProvider<T>
    where
        T: TransactionalMemory::TransactionalMemory + Send,
    {
        type Capability = TransactionalMemory::Capability;

        async fn provide(&self, capability: Self::Capability) -> TransactionalMemory::Output {
            let mut backend = self.0.lock().await;
            TransactionalMemory::dispatch(&mut *backend, capability).await
        }
    }

    // =========================================================================
    // Tests
    // =========================================================================

    #[tokio::test]
    async fn test_block_store_with_memory_backend() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let provider = BlockStoreProvider(Arc::new(Mutex::new(backend)));

        // Set a value
        let set_task: Task<BlockStore::Capability, _> = Task::new(|co| async move {
            BlockStore::set(b"key".to_vec(), b"value".to_vec())
                .perform(&co)
                .await
        });
        set_task.perform(&provider).await.unwrap();

        // Get the value
        let get_task: Task<BlockStore::Capability, _> =
            Task::new(|co| async move { BlockStore::get(b"key".to_vec()).perform(&co).await });
        let result = get_task.perform(&provider).await.unwrap();

        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_direct_perform() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let provider = BlockStoreProvider(Arc::new(Mutex::new(backend)));

        // Direct perform without Task
        BlockStore::set(b"key".to_vec(), b"value".to_vec())
            .perform(&provider)
            .await
            .unwrap();

        let result = BlockStore::get(b"key".to_vec())
            .perform(&provider)
            .await
            .unwrap();
        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_copy_with_block_store() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let provider = BlockStoreProvider(Arc::new(Mutex::new(backend)));

        // Seed initial data
        BlockStore::set(b"source".to_vec(), b"hello".to_vec())
            .perform(&provider)
            .await
            .unwrap();

        // Copy task - demonstrates composed effects
        let copy_task: Task<BlockStore::Capability, _> = Task::new(|env| async move {
            let content = BlockStore::get(b"source".to_vec())
                .perform(&env)
                .await
                .unwrap();
            if let Some(value) = content {
                BlockStore::set(b"dest".to_vec(), value)
                    .perform(&env)
                    .await
                    .unwrap();
            }
        });

        copy_task.perform(&provider).await;

        // Verify
        let result = BlockStore::get(b"dest".to_vec())
            .perform(&provider)
            .await
            .unwrap();
        assert_eq!(result, Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn test_transactional_memory_resolve_replace() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let provider = TransactionalMemoryProvider(Arc::new(Mutex::new(backend)));

        let key = b"addr".to_vec();
        let value = b"data".to_vec();

        // Create new entry (edition = None means create)
        let edition = TransactionalMemory::replace(key.clone(), None, Some(value.clone()))
            .perform(&provider)
            .await
            .unwrap();
        assert!(edition.is_some());

        // Resolve it back
        let resolved = TransactionalMemory::resolve(key.clone())
            .perform(&provider)
            .await
            .unwrap();
        assert_eq!(resolved, Some((value.clone(), value.clone())));

        // Update with CAS (provide correct edition)
        let new_value = b"new_data".to_vec();
        let new_edition =
            TransactionalMemory::replace(key.clone(), Some(value), Some(new_value.clone()))
                .perform(&provider)
                .await
                .unwrap();
        assert!(new_edition.is_some());

        // Verify update
        let resolved = TransactionalMemory::resolve(key.clone())
            .perform(&provider)
            .await
            .unwrap();
        assert_eq!(resolved, Some((new_value.clone(), new_value.clone())));
    }

    #[tokio::test]
    async fn test_transactional_memory_cas_failure() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let provider = TransactionalMemoryProvider(Arc::new(Mutex::new(backend)));

        let key = b"addr".to_vec();
        let value = b"data".to_vec();

        // Create entry
        TransactionalMemory::replace(key.clone(), None, Some(value.clone()))
            .perform(&provider)
            .await
            .unwrap();

        // Try to update with wrong edition (CAS failure)
        let wrong_edition = b"wrong".to_vec();
        let result =
            TransactionalMemory::replace(key.clone(), Some(wrong_edition), Some(b"new".to_vec()))
                .perform(&provider)
                .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_transactional_memory_in_task() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let provider = TransactionalMemoryProvider(Arc::new(Mutex::new(backend)));

        // Helper to create a task that does read-modify-write with CAS
        fn make_increment_task() -> Task<
            TransactionalMemory::Capability,
            impl std::future::Future<Output = Result<Option<Vec<u8>>, DialogStorageError>>,
        > {
            Task::new(|env| async move {
                let key = b"counter".to_vec();

                // Resolve current value
                let current = TransactionalMemory::resolve(key.clone())
                    .perform(&env)
                    .await
                    .unwrap();

                let (new_value, edition) = match current {
                    Some((val, ed)) => {
                        // Increment the value (treating as u64)
                        let n = u64::from_le_bytes(val.try_into().unwrap_or([0u8; 8]));
                        ((n + 1).to_le_bytes().to_vec(), Some(ed))
                    }
                    None => (1u64.to_le_bytes().to_vec(), None),
                };

                TransactionalMemory::replace(key, edition, Some(new_value))
                    .perform(&env)
                    .await
            })
        }

        // Run the task multiple times
        for _ in 0..5 {
            make_increment_task().perform(&provider).await.unwrap();
        }

        // Verify final value
        let result = TransactionalMemory::resolve(b"counter".to_vec())
            .perform(&provider)
            .await
            .unwrap();
        let (val, _) = result.unwrap();
        let count = u64::from_le_bytes(val.try_into().unwrap());
        assert_eq!(count, 5);
    }

    // =========================================================================
    // Composition Tests - Using trait composition
    // =========================================================================

    #[effect]
    pub trait Env: BlockStore + TransactionalMemory {}

    /// A provider for the composite Env capability
    struct EnvProvider<T> {
        block_store: BlockStoreProvider<T>,
        transactional_memory: TransactionalMemoryProvider<T>,
    }

    impl<T> EnvProvider<T> {
        fn new(shared: Arc<Mutex<T>>) -> Self {
            Self {
                block_store: BlockStoreProvider(shared.clone()),
                transactional_memory: TransactionalMemoryProvider(shared),
            }
        }
    }

    impl<T> Provider for EnvProvider<T>
    where
        T: BlockStore::BlockStore + TransactionalMemory::TransactionalMemory + Send,
    {
        type Capability = Env::Capability;

        async fn provide(&self, capability: Env::Capability) -> Env::Output {
            Env::dispatch_composite(&(&self.block_store, &self.transactional_memory), capability).await
        }
    }

    #[tokio::test]
    async fn test_composite_env() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let shared = Arc::new(Mutex::new(backend));

        // Create a composite provider
        let provider = EnvProvider::new(shared);

        // A task that uses BOTH capabilities
        let task: Task<Env::Capability, _> = Task::new(|env| async move {
            // Use BlockStore capability
            BlockStore::set(b"key".to_vec(), b"value".to_vec())
                .perform(&env)
                .await
                .unwrap();

            // Use TransactionalMemory capability
            TransactionalMemory::replace(b"addr".to_vec(), None, Some(b"data".to_vec()))
                .perform(&env)
                .await
                .unwrap();

            // Read back from both
            let block_val = BlockStore::get(b"key".to_vec())
                .perform(&env)
                .await
                .unwrap();

            let tx_val = TransactionalMemory::resolve(b"addr".to_vec())
                .perform(&env)
                .await
                .unwrap();

            (block_val, tx_val)
        });

        let (block_val, tx_val) = task.perform(&provider).await;

        assert_eq!(block_val, Some(b"value".to_vec()));
        assert_eq!(tx_val, Some((b"data".to_vec(), b"data".to_vec())));
    }
}
