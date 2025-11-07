use std::{collections::HashMap, ops::DerefMut, sync::Arc};

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::ConditionalSync;
use futures_util::Stream;
use tokio::sync::RwLock;

use crate::{DialogStorageError, StorageSource};

use super::{Resource, StorageBackend, TransactionalMemoryBackend};

/// A trivial implementation of [StorageBackend] - backed by a [HashMap] - where
/// all values are kept in memory and never persisted.
#[derive(Debug, Clone, Default)]
pub struct MemoryStorageBackend<Key, Value>
where
    Key: Eq + std::hash::Hash,
    Value: Clone,
{
    entries: Arc<RwLock<HashMap<Key, Value>>>,
}

/// A resource handle for a specific entry in [MemoryStorageBackend]
#[derive(Debug, Clone)]
pub struct MemoryResource<Key, Value>
where
    Key: Eq + std::hash::Hash + Clone,
    Value: Clone,
{
    entries: Arc<RwLock<HashMap<Key, Value>>>,
    key: Key,
    content: Option<Value>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> Resource for MemoryResource<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync + PartialEq,
{
    type Value = Value;
    type Error = DialogStorageError;

    fn content(&self) -> &Option<Self::Value> {
        &self.content
    }

    fn into_content(self) -> Option<Self::Value> {
        self.content
    }

    async fn reload(&mut self) -> Result<Option<Self::Value>, Self::Error> {
        let entries = self.entries.read().await;
        let prior = self.content.clone();
        if let Some(value) = entries.get(&self.key) {
            self.content = Some(value.clone());
        } else {
            self.content = None;
        }
        Ok(prior)
    }

    async fn replace(
        &mut self,
        value: Option<Self::Value>,
    ) -> Result<Option<Self::Value>, Self::Error> {
        let mut entries = self.entries.write().await;

        // Get current value from storage
        let current_value = entries.get(&self.key).cloned();

        // Check CAS condition - value must match what we loaded
        if current_value != self.content {
            return Err(DialogStorageError::StorageBackend(
                "CAS condition failed: value has changed".to_string(),
            ));
        }

        let prior = self.content.clone();

        // Perform the operation
        match value {
            Some(new_value) => {
                entries.insert(self.key.clone(), new_value.clone());
                self.content = Some(new_value);
            }
            None => {
                entries.remove(&self.key);
                self.content = None;
            }
        }

        Ok(prior)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> StorageBackend for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync + PartialEq,
{
    type Key = Key;
    type Value = Value;
    type Resource = MemoryResource<Key, Value>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let mut entries = self.entries.write().await;
        entries.insert(key, value);
        Ok(())
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let entries = self.entries.read().await;
        Ok(entries.get(key).cloned())
    }

    async fn open(&self, key: &Self::Key) -> Result<Self::Resource, Self::Error> {
        let entries = self.entries.read().await;
        let content = entries.get(key).cloned();
        Ok(MemoryResource {
            entries: self.entries.clone(),
            key: key.clone(),
            content,
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> TransactionalMemoryBackend for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync + PartialEq,
{
    type Address = Key;
    type Value = Value;
    type Error = DialogStorageError;
    type Edition = Value;

    async fn acquire(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        let entries = self.entries.read().await;
        Ok(entries.get(address).map(|value| (value.clone(), value.clone())))
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        let mut entries = self.entries.write().await;

        // Get current value from storage
        let current_value = entries.get(address);

        // Check CAS condition - value must match expected edition
        if current_value != edition {
            return Err(DialogStorageError::StorageBackend(
                "CAS condition failed: edition mismatch".to_string(),
            ));
        }

        // Perform the operation
        match content {
            Some(new_value) => {
                entries.insert(address.clone(), new_value.clone());
                Ok(Some(new_value))
            }
            None => {
                // Delete operation
                entries.remove(address);
                Ok(None)
            }
        }
    }
}

impl<Key, Value> StorageSource for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync + PartialEq,
{
    fn read(
        &self,
    ) -> impl Stream<
        Item = Result<
            (
                <Self as StorageBackend>::Key,
                <Self as StorageBackend>::Value,
            ),
            <Self as StorageBackend>::Error,
        >,
    > {
        try_stream! {
            let entries = self.entries.read().await;
            for (key, value) in entries.iter() {
                yield (key.clone(), value.clone());
            }
        }
    }

    fn drain(
        &mut self,
    ) -> impl Stream<
        Item = Result<
            (
                <Self as StorageBackend>::Key,
                <Self as StorageBackend>::Value,
            ),
            <Self as StorageBackend>::Error,
        >,
    > {
        try_stream! {
            let entries = std::mem::take(self.entries.write().await.deref_mut());

            for (key, value) in entries.into_iter() {
                yield (key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::transactional_memory::TransactionalMemory;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::*;

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_create() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        // Open TransactionalMemory for non-existent key
        let memory = TransactionalMemory::open(key.clone(), &backend).await.unwrap();
        assert_eq!(memory.read(), None, "Memory should start with None");

        // Create new entry
        let result = memory.replace(Some(value.clone()), &backend).await;
        assert!(result.is_ok(), "Should create new entry");

        // Verify it was stored
        let stored = TransactionalMemory::open(key, &backend).await.unwrap();
        assert_eq!(stored.read(), Some(value));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_update() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        // Create initial value
        backend.set(key.clone(), value1.clone()).await.unwrap();

        // Open TransactionalMemory with existing value
        let memory = TransactionalMemory::open(key.clone(), &backend).await.unwrap();
        assert_eq!(
            memory.read(),
            Some(value1.clone()),
            "Memory should have value1"
        );

        // Update with CAS condition (memory already has value1 loaded)
        let result = memory.replace(Some(value2.clone()), &backend).await;
        assert!(result.is_ok(), "Should update with correct CAS condition");

        // Verify updated value
        let stored = TransactionalMemory::open(key, &backend).await.unwrap();
        assert_eq!(stored.read(), Some(value2));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_cas_failure_value_mismatch() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        // Create initial value
        backend.set(key.clone(), value1.clone()).await.unwrap();

        // Open TransactionalMemory (captures value1)
        let memory = TransactionalMemory::open(key.clone(), &backend).await.unwrap();

        // Simulate concurrent modification: backend gets updated
        let wrong_value = b"wrong".to_vec();
        backend.set(key.clone(), wrong_value.clone()).await.unwrap();

        // Try to update based on stale value1 (should fail)
        let result = memory.replace(Some(value2.clone()), &backend).await;
        assert!(result.is_err(), "Should fail with wrong CAS condition");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("CAS condition failed"),
            "Error should mention CAS failure"
        );

        // Verify value is the concurrent modification
        let stored = TransactionalMemory::open(key, &backend).await.unwrap();
        assert_eq!(stored.read(), Some(wrong_value));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_cas_failure_key_not_exist() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value = b"new_value".to_vec();
        let expected_old = b"old_value".to_vec();

        // Create initial value
        backend
            .set(key.clone(), expected_old.clone())
            .await
            .unwrap();

        // Open TransactionalMemory (captures expected_old)
        let memory = TransactionalMemory::open(key.clone(), &backend).await.unwrap();

        // Simulate concurrent deletion
        backend
            .set(key.clone(), expected_old.clone())
            .await
            .unwrap(); // Reset to force entry removal on next line
        let mut entries = backend.entries.write().await;
        entries.remove(&key);
        drop(entries);

        // Try to update - should fail because key was deleted
        let result = memory.replace(Some(value), &backend).await;
        assert!(
            result.is_err(),
            "Should fail when key was concurrently deleted"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("CAS condition failed"),
            "Error should mention CAS failure"
        );
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_cas_failure_key_exists() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"existing_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        // Open TransactionalMemory for non-existent key (captures None)
        let memory = TransactionalMemory::open(key.clone(), &backend).await.unwrap();

        // Simulate concurrent creation: someone else creates the key
        backend.set(key.clone(), value1.clone()).await.unwrap();

        // Try to create with CAS condition "must not exist" (should fail because key now exists)
        let result = memory.replace(Some(value2), &backend).await;
        assert!(
            result.is_err(),
            "Should fail when key exists but CAS expects it not to"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("CAS condition failed"),
            "Error should mention CAS failure"
        );

        // Verify value unchanged
        let stored = TransactionalMemory::open(key, &backend).await.unwrap();
        assert_eq!(stored.read(), Some(value1));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_delete() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        // Create entry
        backend.set(key.clone(), value.clone()).await.unwrap();

        // Open TransactionalMemory and delete with CAS condition
        let memory = TransactionalMemory::open(key.clone(), &backend).await.unwrap();
        assert_eq!(
            memory.read(),
            Some(value),
            "Memory should have value"
        );

        let result = memory.replace(None, &backend).await;
        assert!(result.is_ok(), "Should delete with correct CAS condition");

        // Verify deleted
        let stored = TransactionalMemory::open(key, &backend).await.unwrap();
        assert_eq!(stored.read(), None);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_resolve_nonexistent() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"nonexistent".to_vec();
        let result = TransactionalMemory::open(key, &backend).await.unwrap();
        assert_eq!(
            result.read(),
            None,
            "Should return None for non-existent key"
        );
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_shared_state() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"shared_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        // Create initial value
        backend.set(key.clone(), value1.clone()).await.unwrap();

        // Open first TransactionalMemory
        let memory1 = TransactionalMemory::open(key.clone(), &backend).await.unwrap();
        assert_eq!(memory1.read(), Some(value1.clone()), "memory1 should have value1");

        // Clone to create memory2 - shares the same state
        let memory2 = memory1.clone();
        assert_eq!(memory2.read(), Some(value1.clone()), "memory2 should have value1");

        // Update through memory1
        memory1.replace(Some(value2.clone()), &backend).await.unwrap();

        // Verify both memory1 and memory2 see the update
        assert_eq!(memory1.read(), Some(value2.clone()), "memory1 should see value2");
        assert_eq!(memory2.read(), Some(value2.clone()), "memory2 should see value2 (shared state)");

        // Update through memory2
        let value3 = b"value3".to_vec();
        memory2.replace(Some(value3.clone()), &backend).await.unwrap();

        // Verify both see the update
        assert_eq!(memory1.read(), Some(value3.clone()), "memory1 should see value3 (shared state)");
        assert_eq!(memory2.read(), Some(value3.clone()), "memory2 should see value3");
    }
}
