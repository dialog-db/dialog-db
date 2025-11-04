use std::{collections::HashMap, ops::DerefMut, sync::Arc};

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::ConditionalSync;
use futures_util::Stream;
use tokio::sync::RwLock;

use crate::{DialogStorageError, StorageSource};

use super::{AtomicStorageBackend, StorageBackend};

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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> StorageBackend for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync,
{
    type Key = Key;
    type Value = Value;
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
}

impl<Key, Value> StorageSource for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync,
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
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        // Create new entry (when = None means "must not exist")
        let result = backend.swap(key.clone(), Some(value.clone()), None).await;
        assert!(result.is_ok(), "Should create new entry");

        // Verify it was stored
        let stored = backend.resolve(&key).await.unwrap();
        assert_eq!(stored, Some(value));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_update() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        // Create initial value
        backend
            .swap(key.clone(), Some(value1.clone()), None)
            .await
            .unwrap();

        // Update with CAS condition
        let result = backend
            .swap(key.clone(), Some(value2.clone()), Some(value1.clone()))
            .await;
        assert!(result.is_ok(), "Should update with correct CAS condition");

        // Verify updated value
        let stored = backend.resolve(&key).await.unwrap();
        assert_eq!(stored, Some(value2));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_cas_failure_value_mismatch() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();
        let wrong_value = b"wrong".to_vec();

        // Create initial value
        backend
            .swap(key.clone(), Some(value1.clone()), None)
            .await
            .unwrap();

        // Try to update with wrong CAS condition
        let result = backend
            .swap(key.clone(), Some(value2.clone()), Some(wrong_value))
            .await;
        assert!(result.is_err(), "Should fail with wrong CAS condition");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("CAS condition failed"),
            "Error should mention CAS failure"
        );

        // Verify value unchanged
        let stored = backend.resolve(&key).await.unwrap();
        assert_eq!(stored, Some(value1));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_cas_failure_key_not_exist() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"nonexistent".to_vec();
        let value = b"new_value".to_vec();
        let expected_old = b"old_value".to_vec();

        // Try to update a non-existent key with CAS condition
        let result = backend
            .swap(key.clone(), Some(value), Some(expected_old))
            .await;
        assert!(
            result.is_err(),
            "Should fail when key doesn't exist but CAS expects a value"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("key does not exist"),
            "Error should mention key doesn't exist"
        );
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_cas_failure_key_exists() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"existing_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        // Create initial value
        backend
            .swap(key.clone(), Some(value1.clone()), None)
            .await
            .unwrap();

        // Try to create again with CAS condition "must not exist" (when = None)
        let result = backend.swap(key.clone(), Some(value2), None).await;
        assert!(
            result.is_err(),
            "Should fail when key exists but CAS expects it not to"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("key already exists"),
            "Error should mention key already exists"
        );

        // Verify value unchanged
        let stored = backend.resolve(&key).await.unwrap();
        assert_eq!(stored, Some(value1));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_delete() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        // Create entry
        backend
            .swap(key.clone(), Some(value.clone()), None)
            .await
            .unwrap();

        // Delete with CAS condition
        let result = backend.swap(key.clone(), None, Some(value)).await;
        assert!(result.is_ok(), "Should delete with correct CAS condition");

        // Verify deleted
        let stored = backend.resolve(&key).await.unwrap();
        assert_eq!(stored, None);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_resolve_nonexistent() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"nonexistent".to_vec();
        let result = backend.resolve(&key).await.unwrap();
        assert_eq!(result, None, "Should return None for non-existent key");
    }
}
