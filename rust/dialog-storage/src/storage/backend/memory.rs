use std::{collections::HashMap, ops::DerefMut, sync::Arc};

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::ConditionalSync;
use futures_util::Stream;
use tokio::sync::RwLock;

use crate::{DialogStorageError, StorageSource};

use super::{StorageBackend, TransactionalMemoryBackend};

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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> StorageBackend for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync + PartialEq,
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

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        let entries = self.entries.read().await;
        Ok(entries
            .get(address)
            .map(|value| (value.clone(), value.clone())))
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
    use crate::CborEncoder;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
    struct TestValue {
        data: String,
    }

    impl TestValue {
        fn new(data: impl Into<String>) -> Self {
            Self { data: data.into() }
        }
    }
    use crate::storage::transactional_memory::TransactionalMemory;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::*;

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_create() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value = TestValue::new("test_value");

        // Open TransactionalMemory for non-existent key
        let mut memory =
            TransactionalMemory::<TestValue, _, _>::open(key.clone(), &backend, CborEncoder)
                .await
                .unwrap();
        assert_eq!(memory.read(), None, "Memory should start with None");

        // Create new entry
        let result = memory.replace(Some(value.clone()), &backend).await;
        assert!(result.is_ok(), "Should create new entry");

        // Verify it was stored
        let stored = TransactionalMemory::<TestValue, _, _>::open(key, &backend, CborEncoder)
            .await
            .unwrap();
        assert_eq!(stored.read(), Some(value));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_update() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value1 = TestValue::new("value1");
        let value2 = TestValue::new("value2");

        // Create initial value
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&value1).await.unwrap().1;
            backend.set(key.clone(), encoded).await.unwrap();
        }

        // Open TransactionalMemory with existing value
        let mut memory =
            TransactionalMemory::<TestValue, _, _>::open(key.clone(), &backend, CborEncoder)
                .await
                .unwrap();
        assert_eq!(
            memory.read(),
            Some(value1.clone()),
            "Memory should have value1"
        );

        // Update with CAS condition (memory already has value1 loaded)
        let result = memory.replace(Some(value2.clone()), &backend).await;
        assert!(result.is_ok(), "Should update with correct CAS condition");

        // Verify updated value
        let stored = TransactionalMemory::<TestValue, _, _>::open(key, &backend, CborEncoder)
            .await
            .unwrap();
        assert_eq!(stored.read(), Some(value2));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_cas_failure_value_mismatch() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value1 = TestValue::new("value1");
        let value2 = TestValue::new("value2");

        // Create initial value
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&value1).await.unwrap().1;
            backend.set(key.clone(), encoded).await.unwrap();
        }

        // Open TransactionalMemory (captures value1)
        let mut memory =
            TransactionalMemory::<TestValue, _, _>::open(key.clone(), &backend, CborEncoder)
                .await
                .unwrap();

        // Simulate concurrent modification: backend gets updated
        let wrong_value = TestValue::new("wrong");
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&wrong_value).await.unwrap().1;
            backend.set(key.clone(), encoded).await.unwrap();
        }

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
        let stored = TransactionalMemory::<TestValue, _, _>::open(key, &backend, CborEncoder)
            .await
            .unwrap();
        assert_eq!(stored.read(), Some(wrong_value));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_cas_failure_key_not_exist() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value = TestValue::new("new_value");
        let expected_old = TestValue::new("old_value");

        // Create initial value
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&expected_old).await.unwrap().1;
            backend.set(key.clone(), encoded).await.unwrap();
        }

        // Open TransactionalMemory (captures expected_old)
        let mut memory =
            TransactionalMemory::<TestValue, _, _>::open(key.clone(), &backend, CborEncoder)
                .await
                .unwrap();

        // Simulate concurrent deletion
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&expected_old).await.unwrap().1;
            backend.set(key.clone(), encoded).await.unwrap(); // Reset to force entry removal on next line
        }
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
        let value1 = TestValue::new("value1");
        let value2 = TestValue::new("value2");

        // Open TransactionalMemory for non-existent key (captures None)
        let mut memory =
            TransactionalMemory::<TestValue, _, _>::open(key.clone(), &backend, CborEncoder)
                .await
                .unwrap();

        // Simulate concurrent creation: someone else creates the key
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&value1).await.unwrap().1;
            backend.set(key.clone(), encoded).await.unwrap();
        }

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
        let stored = TransactionalMemory::<TestValue, _, _>::open(key, &backend, CborEncoder)
            .await
            .unwrap();
        assert_eq!(stored.read(), Some(value1));
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_swap_delete() {
        let mut backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"test_key".to_vec();
        let value = TestValue::new("test_value");

        // Create entry
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&value).await.unwrap().1;
            backend.set(key.clone(), encoded).await.unwrap();
        }

        // Open TransactionalMemory and delete with CAS condition
        let mut memory =
            TransactionalMemory::<TestValue, _, _>::open(key.clone(), &backend, CborEncoder)
                .await
                .unwrap();
        assert_eq!(memory.read(), Some(value), "Memory should have value");

        let result = memory.replace(None, &backend).await;
        assert!(result.is_ok(), "Should delete with correct CAS condition");

        // Verify deleted
        let stored = TransactionalMemory::<TestValue, _, _>::open(key, &backend, CborEncoder)
            .await
            .unwrap();
        assert_eq!(stored.read(), None);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_memory_resolve_nonexistent() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let key = b"nonexistent".to_vec();
        let result = TransactionalMemory::<TestValue, _, _>::open(key, &backend, CborEncoder)
            .await
            .unwrap();
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
        let value1 = TestValue::new("value1");
        let value2 = TestValue::new("value2");

        // Create initial value
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&value1).await.unwrap().1;
            backend.set(key.clone(), encoded).await.unwrap();
        }

        // Open first TransactionalMemory
        let mut memory1 =
            TransactionalMemory::<TestValue, _, _>::open(key.clone(), &backend, CborEncoder)
                .await
                .unwrap();
        assert_eq!(
            memory1.read(),
            Some(value1.clone()),
            "memory1 should have value1"
        );

        // Clone to create memory2 - shares the same state
        let mut memory2 = memory1.clone();
        assert_eq!(
            memory2.read(),
            Some(value1.clone()),
            "memory2 should have value1"
        );

        // Update through memory1
        memory1
            .replace(Some(value2.clone()), &backend)
            .await
            .unwrap();

        // Verify both memory1 and memory2 see the update
        assert_eq!(
            memory1.read(),
            Some(value2.clone()),
            "memory1 should see value2"
        );
        assert_eq!(
            memory2.read(),
            Some(value2.clone()),
            "memory2 should see value2 (shared state)"
        );

        // Update through memory2
        let value3 = TestValue::new("value3");
        memory2
            .replace(Some(value3.clone()), &backend)
            .await
            .unwrap();

        // Verify both see the update
        assert_eq!(
            memory1.read(),
            Some(value3.clone()),
            "memory1 should see value3 (shared state)"
        );
        assert_eq!(
            memory2.read(),
            Some(value3.clone()),
            "memory2 should see value3"
        );
    }
}
