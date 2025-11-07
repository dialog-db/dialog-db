use async_trait::async_trait;
use futures_util::{Stream, TryStreamExt};
use js_sys::{Object, Reflect, Uint8Array};
use rexie::{ObjectStore, Rexie, RexieBuilder, TransactionMode};
use std::{marker::PhantomData, rc::Rc};
use wasm_bindgen::{JsCast, JsValue};

use crate::{DialogStorageError, StorageSink};

use super::{Resource, StorageBackend};

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

/// A resource handle for a specific entry in [IndexedDbStorageBackend]
#[derive(Clone)]
pub struct IndexedDbResource<Key, Value>
where
    Key: AsRef<[u8]> + Clone,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone,
{
    backend: IndexedDbStorageBackend<Key, Value>,
    key: Key,
    content: Option<Value>,
}

#[async_trait(?Send)]
impl<Key, Value> Resource for IndexedDbResource<Key, Value>
where
    Key: AsRef<[u8]> + Clone,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + PartialEq,
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
        let prior = self.content.clone();

        let tx = self
            .backend
            .db
            .transaction(&[&self.backend.store_name], TransactionMode::ReadOnly)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(&self.backend.store_name)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let key_array = bytes_to_typed_array(self.key.as_ref());
        let result = store
            .get(key_array)
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        self.content = result.map(|js_val| parse_value_only(js_val)).transpose()?;

        Ok(prior)
    }

    async fn replace(
        &mut self,
        value: Option<Self::Value>,
    ) -> Result<Option<Self::Value>, Self::Error> {
        // Use a transaction for atomic read-check-write
        let tx = self
            .backend
            .db
            .transaction(&[&self.backend.store_name], TransactionMode::ReadWrite)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(&self.backend.store_name)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let key_array = bytes_to_typed_array(self.key.as_ref());

        // Read current value and version in transaction
        let current_js = store
            .get(key_array.clone())
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let (current_value, current_version) = if let Some(js_val) = current_js {
            let (val, ver) = parse_versioned_value(js_val)?;
            (Some(val), ver)
        } else {
            (None, 0)
        };

        // Check CAS condition - current must match what we expect
        if current_value != self.content {
            return Err(DialogStorageError::StorageBackend(
                "CAS condition failed: value has changed".to_string(),
            ));
        }

        let prior = self.content.clone();

        // Perform the write or delete
        match &value {
            Some(new_value) => {
                let new_version = current_version + 1;
                let versioned_value = create_versioned_value(new_value.as_ref(), new_version)?;
                store
                    .put(&versioned_value, Some(&key_array))
                    .await
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
            }
            None => {
                store
                    .delete(key_array)
                    .await
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
            }
        }

        // Commit transaction
        tx.done()
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        self.content = value;
        Ok(prior)
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
    type Resource = IndexedDbResource<Key, Value>;
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

    async fn open(&self, key: &Self::Key) -> Result<Self::Resource, Self::Error> {
        let tx = self
            .db
            .transaction(&[&self.store_name], TransactionMode::ReadOnly)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(&self.store_name)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let key_array = bytes_to_typed_array(key.as_ref());

        let result = store
            .get(key_array)
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let content = result.map(|js_val| parse_value_only(js_val)).transpose()?;

        Ok(IndexedDbResource {
            backend: self.clone(),
            key: key.clone(),
            content,
        })
    }
}

#[async_trait(?Send)]
impl<Key, Value> super::TransactionalMemoryBackend for IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + PartialEq,
{
    type Address = Key;
    type Value = Value;
    type Error = DialogStorageError;
    type Edition = u64;

    async fn acquire(
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
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_create() {
        let backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_create", "test_store")
                .await
                .unwrap();

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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_update() {
        let mut backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_update", "test_store")
                .await
                .unwrap();

        let key = b"test_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        // Create initial value
        backend.set(key.clone(), value1.clone()).await.unwrap();

        // Open resource with existing value
        let mut resource = backend.open(&key).await.unwrap();
        assert_eq!(
            resource.content(),
            &Some(value1.clone()),
            "Resource should have value1"
        );

        // Update with CAS condition (resource already has value1 loaded)
        let result = resource.replace(Some(value2.clone())).await;
        assert!(result.is_ok(), "Should update with correct CAS condition");

        // Verify updated value
        let stored = backend.open(&key).await.unwrap();
        assert_eq!(stored.content(), &Some(value2));
    }

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_cas_failure_value_mismatch() {
        let mut backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_cas_fail", "test_store")
                .await
                .unwrap();

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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_cas_failure_key_not_exist() {
        let mut backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_no_key", "test_store")
                .await
                .unwrap();

        let key = b"test_key".to_vec();
        let value = b"new_value".to_vec();
        let expected_old = b"old_value".to_vec();

        // Create initial value
        backend
            .set(key.clone(), expected_old.clone())
            .await
            .unwrap();

        // Open resource (captures expected_old)
        let mut resource = backend.open(&key).await.unwrap();

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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_cas_failure_key_exists() {
        let mut backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_exists", "test_store")
                .await
                .unwrap();

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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_delete() {
        let mut backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_delete", "test_store")
                .await
                .unwrap();

        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        // Create entry
        backend.set(key.clone(), value.clone()).await.unwrap();

        // Open resource and delete with CAS condition
        let mut resource = backend.open(&key).await.unwrap();
        assert_eq!(
            resource.content(),
            &Some(value),
            "Resource should have value"
        );

        let result = resource.replace(None).await;
        assert!(result.is_ok(), "Should delete with correct CAS condition");

        // Verify deleted
        let stored = backend.open(&key).await.unwrap();
        assert_eq!(stored.content(), &None);
    }

    #[wasm_bindgen_test]
    async fn test_indexeddb_resolve_nonexistent() {
        let backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_resolve", "test_store")
                .await
                .unwrap();

        let key = b"nonexistent".to_vec();
        let result = backend.open(&key).await.unwrap();
        assert_eq!(
            result.content(),
            &None,
            "Should return None for non-existent key"
        );
    }
}
