use async_trait::async_trait;
use futures_util::{Stream, TryStreamExt};
use js_sys::{Object, Reflect, Uint8Array};
use rexie::{ObjectStore, Rexie, RexieBuilder, TransactionMode};
use std::{marker::PhantomData, rc::Rc};
use wasm_bindgen::{JsCast, JsValue};

use crate::{DialogStorageError, StorageSink};

use super::{StorageBackend, TransactionalMemoryBackend};

const INDEXEDDB_STORAGE_VERSION: u32 = 1;

/// An IndexedDB-based [`StorageBackend`] implementation.
#[derive(Clone)]
pub struct IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]>,
    Value: AsRef<[u8]> + From<Vec<u8>>,
{
    db: Rc<Rexie>,
    store_name: String,
    key_type: PhantomData<Key>,
    value_type: PhantomData<Value>,
}

unsafe impl<Key, Value> Send for IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]>,
    Value: AsRef<[u8]> + From<Vec<u8>>,
{
}
unsafe impl<Key, Value> Sync for IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]>,
    Value: AsRef<[u8]> + From<Vec<u8>>,
{
}

impl<Key, Value> IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]>,
    Value: AsRef<[u8]> + From<Vec<u8>>,
{
    /// Creates a new [`IndexedDbStorageBackend`].
    pub async fn new(db_name: &str, store_name: &str) -> Result<Self, DialogStorageError> {
        let db = RexieBuilder::new(db_name)
            .version(INDEXEDDB_STORAGE_VERSION)
            .add_object_store(ObjectStore::new(store_name).auto_increment(false))
            .build()
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        Ok(IndexedDbStorageBackend {
            db: Rc::new(db),
            store_name: store_name.to_owned(),
            key_type: PhantomData,
            value_type: PhantomData,
        })
    }
}

#[async_trait(?Send)]
impl<Key, Value> StorageBackend for IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + PartialEq,
{
    type Key = Key;
    type Value = Value;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let tx = self
            .db
            .transaction(&[&self.store_name], TransactionMode::ReadWrite)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(&self.store_name)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let key_array = bytes_to_typed_array(key.as_ref());

        // Read current version if it exists, otherwise start at 1
        let current_version = store
            .get(key_array.clone())
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?
            .and_then(|js_val| {
                Reflect::get(&js_val, &JsValue::from_str("version"))
                    .ok()
                    .and_then(|v| v.as_f64())
                    .map(|v| v as u64)
            })
            .unwrap_or(0);

        let new_version = current_version + 1;
        let versioned_value = create_versioned_value(value.as_ref(), new_version)?;

        store
            .put(&versioned_value, Some(&key_array))
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        tx.done()
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        Ok(())
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let tx = self
            .db
            .transaction(&[&self.store_name], TransactionMode::ReadOnly)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(&self.store_name)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let key = bytes_to_typed_array(key.as_ref());

        let Some(js_val) = store
            .get(key)
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?
        else {
            return Ok(None);
        };

        let value = parse_value_only(js_val)?;

        tx.done()
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        Ok(Some(value))
    }
}

#[async_trait(?Send)]
impl<Key, Value> TransactionalMemoryBackend for IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + PartialEq,
{
    type Address = Key;
    type Value = Value;
    type Error = DialogStorageError;
    type Edition = u64;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        let tx = self
            .db
            .transaction(&[&self.store_name], TransactionMode::ReadOnly)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(&self.store_name)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let key_array = bytes_to_typed_array(address.as_ref());
        let result = store
            .get(key_array)
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        match result {
            Some(js_val) => {
                let (value, version) = parse_versioned_value(js_val)?;
                Ok(Some((value, version)))
            }
            None => Ok(None),
        }
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        let tx = self
            .db
            .transaction(&[&self.store_name], TransactionMode::ReadWrite)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(&self.store_name)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let key_array = bytes_to_typed_array(address.as_ref());

        // Check CAS condition - get current version
        let current_js = store
            .get(key_array.clone())
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let current_version = if let Some(js_val) = current_js {
            let (_, version) = parse_versioned_value::<Value>(js_val)?;
            Some(version)
        } else {
            None
        };

        // Verify edition matches
        if current_version.as_ref() != edition {
            return Err(DialogStorageError::StorageBackend(
                "CAS condition failed: edition mismatch".to_string(),
            ));
        }

        // Perform the operation
        match content {
            Some(new_value) => {
                let new_version = current_version.unwrap_or(0) + 1;
                let versioned_value = create_versioned_value(new_value.as_ref(), new_version)?;
                store
                    .put(&versioned_value, Some(&key_array))
                    .await
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

                tx.done()
                    .await
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

                Ok(Some(new_version))
            }
            None => {
                // Delete operation
                store
                    .delete(key_array)
                    .await
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

                tx.done()
                    .await
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

                Ok(None)
            }
        }
    }
}

#[async_trait(?Send)]
impl<Key, Value> StorageSink for IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + PartialEq,
{
    async fn write<EntryStream>(
        &mut self,
        stream: EntryStream,
    ) -> Result<(), <Self as StorageBackend>::Error>
    where
        EntryStream: Stream<
            Item = Result<
                (
                    <Self as StorageBackend>::Key,
                    <Self as StorageBackend>::Value,
                ),
                <Self as StorageBackend>::Error,
            >,
        >,
    {
        let tx = self
            .db
            .transaction(&[&self.store_name], TransactionMode::ReadWrite)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(&self.store_name)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        tokio::pin!(stream);

        let mut entries = Vec::<(JsValue, Option<JsValue>)>::new();

        while let Some((key, value)) = stream.try_next().await? {
            let key_array = bytes_to_typed_array(key.as_ref());

            // Read current version if exists
            let current_version = store
                .get(key_array.clone())
                .await
                .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?
                .and_then(|js_val| {
                    Reflect::get(&js_val, &JsValue::from_str("version"))
                        .ok()
                        .and_then(|v| v.as_f64())
                        .map(|v| v as u64)
                })
                .unwrap_or(0);

            let new_version = current_version + 1;
            let versioned_value = create_versioned_value(value.as_ref(), new_version)?;

            entries.push((versioned_value, Some(key_array)));
        }

        store.put_all(entries.into_iter()).await.map_err(|error| {
            DialogStorageError::StorageBackend(format!(
                "Failed while writing bulk entries to IndexedDB: {error}"
            ))
        })?;

        tx.done()
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        Ok(())
    }
}

fn bytes_to_typed_array(bytes: &[u8]) -> JsValue {
    let array = Uint8Array::new_with_length(bytes.len() as u32);
    array.copy_from(bytes);
    JsValue::from(array)
}

/// Creates a versioned value object: { value: Uint8Array, version: number }
fn create_versioned_value(value: &[u8], version: u64) -> Result<JsValue, DialogStorageError> {
    let obj = Object::new();
    let value_array = bytes_to_typed_array(value);

    Reflect::set(&obj, &JsValue::from_str("value"), &value_array).map_err(|_| {
        DialogStorageError::StorageBackend("Failed to set value property".to_string())
    })?;

    Reflect::set(
        &obj,
        &JsValue::from_str("version"),
        &JsValue::from_f64(version as f64),
    )
    .map_err(|_| {
        DialogStorageError::StorageBackend("Failed to set version property".to_string())
    })?;

    Ok(JsValue::from(obj))
}

/// Extracts value and version from stored object
fn parse_versioned_value<Value>(js_val: JsValue) -> Result<(Value, u64), DialogStorageError>
where
    Value: From<Vec<u8>>,
{
    // Get the value bytes
    let value_js = Reflect::get(&js_val, &JsValue::from_str("value")).map_err(|_| {
        DialogStorageError::StorageBackend("Failed to get value property".to_string())
    })?;

    let value_bytes = value_js
        .dyn_into::<Uint8Array>()
        .map_err(|_| DialogStorageError::StorageBackend("Value is not Uint8Array".to_string()))?
        .to_vec();

    // Get the version number
    let version_js = Reflect::get(&js_val, &JsValue::from_str("version")).map_err(|_| {
        DialogStorageError::StorageBackend("Failed to get version property".to_string())
    })?;

    let version = version_js
        .as_f64()
        .ok_or_else(|| DialogStorageError::StorageBackend("Version is not a number".to_string()))?
        as u64;

    Ok((Value::from(value_bytes), version))
}

/// Extracts just the value from stored object (for StorageBackend get/set operations)
fn parse_value_only<Value>(js_val: JsValue) -> Result<Value, DialogStorageError>
where
    Value: From<Vec<u8>>,
{
    let value_js = Reflect::get(&js_val, &JsValue::from_str("value")).map_err(|_| {
        DialogStorageError::StorageBackend("Failed to get value property".to_string())
    })?;

    let value_bytes = value_js
        .dyn_into::<Uint8Array>()
        .map_err(|_| DialogStorageError::StorageBackend("Value is not Uint8Array".to_string()))?
        .to_vec();

    Ok(Value::from(value_bytes))
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use crate::CborEncoder;
    use crate::storage::transactional_memory::TransactionalMemory;
    use serde::{Deserialize, Serialize};
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    /// Test struct for exercising serialization/deserialization
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestValue {
        data: String,
    }

    impl TestValue {
        fn new(data: impl Into<String>) -> Self {
            Self { data: data.into() }
        }
    }

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_create() {
        let backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_create", "test_store")
                .await
                .unwrap();

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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_update() {
        let mut backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_update", "test_store")
                .await
                .unwrap();

        let key = b"test_key".to_vec();
        let value1 = TestValue::new("value1");
        let value2 = TestValue::new("value2");

        // Create initial value
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&value1).await.unwrap().1;
            backend.set(key.clone(), encoded.to_vec()).await.unwrap();
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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_cas_failure_value_mismatch() {
        let mut backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_cas_fail", "test_store")
                .await
                .unwrap();

        let key = b"test_key".to_vec();
        let value1 = TestValue::new("value1");
        let value2 = TestValue::new("value2");

        // Create initial value
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&value1).await.unwrap().1;
            backend.set(key.clone(), encoded.to_vec()).await.unwrap();
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
            backend.set(key.clone(), encoded.to_vec()).await.unwrap();
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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_cas_failure_key_not_exist() {
        let mut backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_no_key", "test_store")
                .await
                .unwrap();

        let key = b"test_key".to_vec();
        let value = TestValue::new("new_value");
        let expected_old = TestValue::new("old_value");

        // Create initial value
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&expected_old).await.unwrap().1;
            backend.set(key.clone(), encoded.to_vec()).await.unwrap();
        }

        // Open TransactionalMemory (captures expected_old)
        let mut memory =
            TransactionalMemory::<TestValue, _, _>::open(key.clone(), &backend, CborEncoder)
                .await
                .unwrap();

        // Simulate concurrent deletion by directly accessing the database
        let tx = backend
            .db
            .transaction(&[&backend.store_name], TransactionMode::ReadWrite)
            .unwrap();
        let store = tx.store(&backend.store_name).unwrap();
        let key_array = bytes_to_typed_array(key.as_ref());
        store.delete(key_array).await.unwrap();
        tx.commit().await.unwrap();

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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_cas_failure_key_exists() {
        let mut backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_exists", "test_store")
                .await
                .unwrap();

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
            backend.set(key.clone(), encoded.to_vec()).await.unwrap();
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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_delete() {
        let mut backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_delete", "test_store")
                .await
                .unwrap();

        let key = b"test_key".to_vec();
        let value = TestValue::new("test_value");

        // Create entry
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&value).await.unwrap().1;
            backend.set(key.clone(), encoded.to_vec()).await.unwrap();
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

    #[wasm_bindgen_test]
    async fn test_indexeddb_resolve_nonexistent() {
        let backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_resolve", "test_store")
                .await
                .unwrap();

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

    #[wasm_bindgen_test]
    async fn test_indexeddb_shared_state() {
        let mut backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_shared", "test_store")
                .await
                .unwrap();

        let key = b"shared_key".to_vec();
        let value1 = TestValue::new("value1");
        let value2 = TestValue::new("value2");

        // Create initial value - encode it first
        {
            use crate::Encoder;
            let encoded1 = CborEncoder.encode(&value1).await.unwrap().1;
            backend.set(key.clone(), encoded1.to_vec()).await.unwrap();
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
