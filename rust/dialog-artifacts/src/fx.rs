//! Algebraic effects system.
//!
//! This module re-exports the core effect types from `dialog_common::fx`
//! and provides tests demonstrating the `#[effect]` macro.

pub use dialog_common::fx::*;

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_macros::{effect, provider};
    use dialog_storage::{DialogStorageError, MemoryStorageBackend, StorageBackend};

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
    // Provider wrapper for testing
    // =========================================================================

    /// A wrapper around MemoryStorageBackend that implements Provider.
    /// In real code, you'd use #[provider(BlockStore)] on your own types.
    #[provider(BlockStore)]
    struct BlockStoreProvider(MemoryStorageBackend<Vec<u8>, Vec<u8>>);

    impl BlockStore::BlockStore for BlockStoreProvider {
        async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, DialogStorageError> {
            StorageBackend::get(&self.0, &key).await.map_err(Into::into)
        }
        async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DialogStorageError> {
            StorageBackend::set(&mut self.0, key, value).await.map_err(Into::into)
        }
    }

    #[provider(TransactionalMemory)]
    struct TransactionalMemoryProvider(MemoryStorageBackend<Vec<u8>, Vec<u8>>);

    impl TransactionalMemory::TransactionalMemory for TransactionalMemoryProvider {
        async fn resolve(
            &self,
            address: Vec<u8>,
        ) -> Result<Option<(Vec<u8>, Vec<u8>)>, DialogStorageError> {
            dialog_storage::TransactionalMemoryBackend::resolve(&self.0, &address).await
        }

        async fn replace(
            &self,
            address: Vec<u8>,
            edition: Option<Vec<u8>>,
            content: Option<Vec<u8>>,
        ) -> Result<Option<Vec<u8>>, DialogStorageError> {
            dialog_storage::TransactionalMemoryBackend::replace(
                &self.0,
                &address,
                edition.as_ref(),
                content,
            )
            .await
        }
    }

    // =========================================================================
    // Tests
    // =========================================================================

    #[tokio::test]
    async fn test_block_store_with_memory_backend() {
        let mut provider = BlockStoreProvider(MemoryStorageBackend::default());

        // Set a value
        let set_task: Task<BlockStore::Capability, _> = Task::new(|co| async move {
            BlockStore::set(b"key".to_vec(), b"value".to_vec())
                .perform(&mut &co)
                .await
        });
        set_task.perform(&mut provider).await.unwrap();

        // Get the value
        let get_task: Task<BlockStore::Capability, _> =
            Task::new(|co| async move { BlockStore::get(b"key".to_vec()).perform(&mut &co).await });
        let result = get_task.perform(&mut provider).await.unwrap();

        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_direct_perform() {
        let mut provider = BlockStoreProvider(MemoryStorageBackend::default());

        // Direct perform without Task
        BlockStore::set(b"key".to_vec(), b"value".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();

        let result = BlockStore::get(b"key".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();
        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_copy_with_block_store() {
        let mut provider = BlockStoreProvider(MemoryStorageBackend::default());

        // Seed initial data
        BlockStore::set(b"source".to_vec(), b"hello".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();

        // Copy task - demonstrates composed effects
        let copy_task: Task<BlockStore::Capability, _> = Task::new(|co| async move {
            let content = BlockStore::get(b"source".to_vec())
                .perform(&mut &co)
                .await
                .unwrap();
            if let Some(value) = content {
                BlockStore::set(b"dest".to_vec(), value)
                    .perform(&mut &co)
                    .await
                    .unwrap();
            }
        });

        copy_task.perform(&mut provider).await;

        // Verify
        let result = BlockStore::get(b"dest".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();
        assert_eq!(result, Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn test_transactional_memory_resolve_replace() {
        let mut provider = TransactionalMemoryProvider(MemoryStorageBackend::default());

        let key = b"addr".to_vec();
        let value = b"data".to_vec();

        // Create new entry (edition = None means create)
        let edition = TransactionalMemory::replace(key.clone(), None, Some(value.clone()))
            .perform(&mut provider)
            .await
            .unwrap();
        assert!(edition.is_some());

        // Resolve it back
        let resolved = TransactionalMemory::resolve(key.clone())
            .perform(&mut provider)
            .await
            .unwrap();
        assert_eq!(resolved, Some((value.clone(), value.clone())));

        // Update with correct edition
        let new_value = b"new_data".to_vec();
        let new_edition =
            TransactionalMemory::replace(key.clone(), Some(value), Some(new_value.clone()))
                .perform(&mut provider)
                .await
                .unwrap();
        assert!(new_edition.is_some());

        // Verify update
        let resolved = TransactionalMemory::resolve(key.clone())
            .perform(&mut provider)
            .await
            .unwrap();
        assert_eq!(resolved, Some((new_value.clone(), new_value.clone())));
    }

    #[tokio::test]
    async fn test_transactional_memory_cas_failure() {
        let mut provider = TransactionalMemoryProvider(MemoryStorageBackend::default());

        let key = b"addr".to_vec();
        let value = b"data".to_vec();

        // Create entry
        TransactionalMemory::replace(key.clone(), None, Some(value.clone()))
            .perform(&mut provider)
            .await
            .unwrap();

        // Try to update with wrong edition - should fail
        let wrong_edition = b"wrong".to_vec();
        let result =
            TransactionalMemory::replace(key.clone(), Some(wrong_edition), Some(b"new".to_vec()))
                .perform(&mut provider)
                .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_transactional_memory_in_task() {
        let mut provider = TransactionalMemoryProvider(MemoryStorageBackend::default());

        // Helper to create a task that does read-modify-write with CAS
        fn make_increment_task() -> Task<
            TransactionalMemory::Capability,
            impl std::future::Future<Output = Result<Option<Vec<u8>>, DialogStorageError>>,
        > {
            Task::new(|co| async move {
                let key = b"counter".to_vec();

                // Resolve current value
                let current = TransactionalMemory::resolve(key.clone())
                    .perform(&mut &co)
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
                    .perform(&mut &co)
                    .await
            })
        }

        // Run the task multiple times
        for _ in 0..5 {
            make_increment_task().perform(&mut provider).await.unwrap();
        }

        // Verify final value
        let result = TransactionalMemory::resolve(b"counter".to_vec())
            .perform(&mut provider)
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
    pub trait CompositeEnv: BlockStore + TransactionalMemory {}

    // Provider that implements both BlockStore and TransactionalMemory
    #[provider(CompositeEnv)]
    struct CompositeProvider(MemoryStorageBackend<Vec<u8>, Vec<u8>>);

    impl CompositeEnv::CompositeEnv for CompositeProvider {}

    impl BlockStore::BlockStore for CompositeProvider {
        async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, DialogStorageError> {
            StorageBackend::get(&self.0, &key).await.map_err(Into::into)
        }
        async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DialogStorageError> {
            StorageBackend::set(&mut self.0, key, value).await.map_err(Into::into)
        }
    }

    impl TransactionalMemory::TransactionalMemory for CompositeProvider {
        async fn resolve(
            &self,
            address: Vec<u8>,
        ) -> Result<Option<(Vec<u8>, Vec<u8>)>, DialogStorageError> {
            dialog_storage::TransactionalMemoryBackend::resolve(&self.0, &address).await
        }

        async fn replace(
            &self,
            address: Vec<u8>,
            edition: Option<Vec<u8>>,
            content: Option<Vec<u8>>,
        ) -> Result<Option<Vec<u8>>, DialogStorageError> {
            dialog_storage::TransactionalMemoryBackend::replace(
                &self.0,
                &address,
                edition.as_ref(),
                content,
            )
            .await
        }
    }

    #[tokio::test]
    async fn test_composite_env() {
        let mut provider = CompositeProvider(MemoryStorageBackend::default());

        // A task that uses BOTH capabilities
        let task: Task<CompositeEnv::Capability, _> = Task::new(|co| async move {
            // Use BlockStore capability
            BlockStore::set(b"key".to_vec(), b"value".to_vec())
                .perform(&mut &co)
                .await
                .unwrap();

            // Use TransactionalMemory capability
            TransactionalMemory::replace(b"addr".to_vec(), None, Some(b"data".to_vec()))
                .perform(&mut &co)
                .await
                .unwrap();

            // Read back from both
            let block_val = BlockStore::get(b"key".to_vec())
                .perform(&mut &co)
                .await
                .unwrap();

            let tx_val = TransactionalMemory::resolve(b"addr".to_vec())
                .perform(&mut &co)
                .await
                .unwrap();

            (block_val, tx_val)
        });

        let (block_val, tx_val) = task.perform(&mut provider).await;

        assert_eq!(block_val, Some(b"value".to_vec()));
        assert_eq!(tx_val, Some((b"data".to_vec(), b"data".to_vec())));
    }

    // =========================================================================
    // Tests for #[effectful] macro
    // =========================================================================

    #[effectful(BlockStore)]
    fn copy_data(from: Vec<u8>, to: Vec<u8>) -> Result<(), DialogStorageError> {
        let content = perform!(BlockStore::get(from))?;
        if let Some(value) = content {
            perform!(BlockStore::set(to, value))?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_effectful_macro_basic() {
        let mut provider = BlockStoreProvider(MemoryStorageBackend::default());

        // Seed initial data
        BlockStore::set(b"source".to_vec(), b"hello".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();

        // Use the effectful function
        copy_data(b"source".to_vec(), b"dest".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();

        // Verify
        let result = BlockStore::get(b"dest".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();
        assert_eq!(result, Some(b"hello".to_vec()));
    }

    #[effectful(BlockStore, TransactionalMemory)]
    fn store_and_track(
        block_key: Vec<u8>,
        tx_key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, DialogStorageError> {
        // Store in block store
        perform!(BlockStore::set(block_key, value.clone()))?;
        // Track in transactional memory
        perform!(TransactionalMemory::replace(tx_key, None, Some(value)))
    }

    #[tokio::test]
    async fn test_effectful_macro_multi_capability() {
        let mut provider = CompositeProvider(MemoryStorageBackend::default());

        // Use the effectful function with composite capability
        // Use different keys for block store and transactional memory to avoid CAS conflicts
        let edition = store_and_track(b"block_key".to_vec(), b"tx_key".to_vec(), b"value".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();

        assert!(edition.is_some());

        // Verify block store has the value
        let block_val = BlockStore::get(b"block_key".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();
        assert_eq!(block_val, Some(b"value".to_vec()));

        // Verify transactional memory has it tracked
        let tx_val = TransactionalMemory::resolve(b"tx_key".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();
        assert!(tx_val.is_some());
    }

    // =========================================================================
    // Tests for #[effectful] on methods
    // =========================================================================

    struct Cache {
        prefix: Vec<u8>,
    }

    impl Cache {
        fn new(prefix: &[u8]) -> Self {
            Self {
                prefix: prefix.to_vec(),
            }
        }

        fn prefixed_key(&self, key: &[u8]) -> Vec<u8> {
            let mut full_key = self.prefix.clone();
            full_key.extend_from_slice(key);
            full_key
        }

        #[effectful(BlockStore)]
        fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, DialogStorageError> {
            let full_key = self.prefixed_key(&key);
            perform!(BlockStore::get(full_key))
        }

        #[effectful(BlockStore)]
        fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DialogStorageError> {
            let full_key = self.prefixed_key(&key);
            perform!(BlockStore::set(full_key, value))
        }

        #[effectful(BlockStore)]
        fn copy(&self, from: Vec<u8>, to: Vec<u8>) -> Result<(), DialogStorageError> {
            let content = perform!(self.get(from))?;
            if let Some(value) = content {
                perform!(self.set(to, value))?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_effectful_macro_on_methods() {
        let mut provider = BlockStoreProvider(MemoryStorageBackend::default());

        let cache = Cache::new(b"cache:");

        // Set a value using the effectful method
        cache
            .set(b"key1".to_vec(), b"value1".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();

        // Get the value back
        let result = cache.get(b"key1".to_vec()).perform(&mut provider).await.unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));

        // Copy to another key
        cache
            .copy(b"key1".to_vec(), b"key2".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();

        // Verify the copy worked
        let copied = cache.get(b"key2".to_vec()).perform(&mut provider).await.unwrap();
        assert_eq!(copied, Some(b"value1".to_vec()));

        // Verify the prefix was applied (check raw storage)
        let raw = BlockStore::get(b"cache:key1".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();
        assert_eq!(raw, Some(b"value1".to_vec()));
    }

    // =========================================================================
    // Tests for #[effectful] on traits
    // =========================================================================

    trait Storage {
        #[effectful(BlockStore)]
        fn save(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DialogStorageError>;

        #[effectful(BlockStore)]
        fn load(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, DialogStorageError>;
    }

    struct PrefixedStorage {
        prefix: Vec<u8>,
    }

    impl Storage for PrefixedStorage {
        #[effectful(BlockStore)]
        fn save(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DialogStorageError> {
            let mut full_key = self.prefix.clone();
            full_key.extend_from_slice(&key);
            perform!(BlockStore::set(full_key, value))
        }

        #[effectful(BlockStore)]
        fn load(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, DialogStorageError> {
            let mut full_key = self.prefix.clone();
            full_key.extend_from_slice(&key);
            perform!(BlockStore::get(full_key))
        }
    }

    #[tokio::test]
    async fn test_effectful_macro_on_trait() {
        let mut provider = BlockStoreProvider(MemoryStorageBackend::default());

        let storage = PrefixedStorage {
            prefix: b"storage:".to_vec(),
        };

        // Save a value
        storage
            .save(b"mykey".to_vec(), b"myvalue".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();

        // Load it back
        let result = storage
            .load(b"mykey".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();
        assert_eq!(result, Some(b"myvalue".to_vec()));

        // Verify the prefix was applied
        let raw = BlockStore::get(b"storage:mykey".to_vec())
            .perform(&mut provider)
            .await
            .unwrap();
        assert_eq!(raw, Some(b"myvalue".to_vec()));
    }
}
