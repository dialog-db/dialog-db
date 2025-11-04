use std::{collections::HashMap, ops::DerefMut, sync::Arc};

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::ConditionalSync;
use futures_util::Stream;
use tokio::sync::RwLock;

use crate::{DialogStorageError, StorageSource};

use super::{AtomicStorageBackend, Resource, StorageBackend};

/// A trivial implementation of [StorageBackend] - backed by a [HashMap] - where
/// all values are kept in memory and never persisted.
#[derive(Clone, Default)]
pub struct MemoryStorageBackend<Key, Value>
where
    Key: Eq + std::hash::Hash,
    Value: Clone,
{
    entries: Arc<RwLock<HashMap<Key, Value>>>,
}

/// A resource handle for a specific entry in [MemoryStorageBackend]
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
        self.content = entries.get(&self.key).cloned();
        Ok(prior)
    }

    async fn replace(
        &mut self,
        value: Option<Self::Value>,
    ) -> Result<Option<Self::Value>, Self::Error> {
        let mut entries = self.entries.write().await;

        // Get current value from storage
        let current = entries.get(&self.key).cloned();

        // Check CAS condition - current must match what we think it is
        if current != self.content {
            return Err(DialogStorageError::StorageBackend(
                "CAS condition failed: value has changed".to_string(),
            ));
        }

        let prior = self.content.clone();

        // Perform the operation
        match &value {
            Some(new_value) => {
                entries.insert(self.key.clone(), new_value.clone());
            }
            None => {
                entries.remove(&self.key);
            }
        }

        self.content = value;
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> AtomicStorageBackend for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync + PartialEq,
{
    type Key = Key;
    type Value = Value;
    type Error = DialogStorageError;

    async fn swap(
        &mut self,
        key: Self::Key,
        value: Option<Self::Value>,
        when: Option<Self::Value>,
    ) -> Result<(), Self::Error> {
        let mut entries = self.entries.write().await;

        // Get current value
        let current = entries.get(&key).cloned();

        // Check CAS condition
        match (when, current) {
            (Some(expected), Some(ref actual)) if expected != *actual => {
                // CAS failed - value doesn't match
                return Err(DialogStorageError::StorageBackend(
                    "CAS condition failed: value mismatch".to_string(),
                ));
            }
            (Some(_), None) => {
                // CAS failed - expected a value but key doesn't exist
                return Err(DialogStorageError::StorageBackend(
                    "CAS condition failed: key does not exist".to_string(),
                ));
            }
            (None, Some(_)) => {
                // CAS failed - expected no value but key exists
                return Err(DialogStorageError::StorageBackend(
                    "CAS condition failed: key already exists".to_string(),
                ));
            }
            _ => {
                // CAS condition satisfied
            }
        }

        // Perform the operation
        match value {
            Some(new_value) => {
                entries.insert(key, new_value);
            }
            None => {
                entries.remove(&key);
            }
        }

        Ok(())
    }

    async fn resolve(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let entries = self.entries.read().await;
        Ok(entries.get(key).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::*;

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_create() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        // Open resource for non-existent key
        let mut resource = backend.open(&key).await.unwrap();
        assert_eq!(resource.content(), &None, "Resource should start with None");

        // Create new entry
        let result = resource.replace(Some(value.clone())).await;
        assert!(result.is_ok(), "Should create new entry");

        // Verify it was stored
        let stored = backend.open(&key).await.unwrap();
        assert_eq!(stored.content(), &Some(value));
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

        // Open resource with existing value
        let mut resource = backend.open(&key).await.unwrap();
        assert_eq!(resource.content(), &Some(value1.clone()), "Resource should have value1");

        // Update with CAS condition (resource already has value1 loaded)
        let result = resource.replace(Some(value2.clone())).await;
        assert!(result.is_ok(), "Should update with correct CAS condition");

        // Verify updated value
        let stored = backend.open(&key).await.unwrap();
        assert_eq!(stored.content(), &Some(value2));
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

        // Open resource (captures value1)
        let mut resource = backend.open(&key).await.unwrap();

        // Simulate concurrent modification: backend gets updated
        let wrong_value = b"wrong".to_vec();
        backend.set(key.clone(), wrong_value.clone()).await.unwrap();

        // Try to update based on stale value1 (should fail)
        let result = resource.replace(Some(value2.clone())).await;
        assert!(result.is_err(), "Should fail with wrong CAS condition");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("CAS condition failed"),
            "Error should mention CAS failure"
        );

        // Verify value is the concurrent modification
        let stored = backend.open(&key).await.unwrap();
        assert_eq!(stored.content(), &Some(wrong_value));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_cas_failure_key_not_exist() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value = b"new_value".to_vec();
        let expected_old = b"old_value".to_vec();

        // Create initial value
        backend.set(key.clone(), expected_old.clone()).await.unwrap();

        // Open resource (captures expected_old)
        let mut resource = backend.open(&key).await.unwrap();

        // Simulate concurrent deletion
        backend.set(key.clone(), expected_old.clone()).await.unwrap(); // Reset to force entry removal on next line
        let mut entries = backend.entries.write().await;
        entries.remove(&key);
        drop(entries);

        // Try to update - should fail because key was deleted
        let result = resource.replace(Some(value)).await;
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

        // Open resource for non-existent key (captures None)
        let mut resource = backend.open(&key).await.unwrap();

        // Simulate concurrent creation: someone else creates the key
        backend.set(key.clone(), value1.clone()).await.unwrap();

        // Try to create with CAS condition "must not exist" (should fail because key now exists)
        let result = resource.replace(Some(value2)).await;
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
        let stored = backend.open(&key).await.unwrap();
        assert_eq!(stored.content(), &Some(value1));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_delete() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        // Create entry
        backend.set(key.clone(), value.clone()).await.unwrap();

        // Open resource and delete with CAS condition
        let mut resource = backend.open(&key).await.unwrap();
        assert_eq!(resource.content(), &Some(value), "Resource should have value");

        let result = resource.replace(None).await;
        assert!(result.is_ok(), "Should delete with correct CAS condition");

        // Verify deleted
        let stored = backend.open(&key).await.unwrap();
        assert_eq!(stored.content(), &None);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_resolve_nonexistent() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"nonexistent".to_vec();
        let result = backend.open(&key).await.unwrap();
        assert_eq!(result.content(), &None, "Should return None for non-existent key");
    }
}
