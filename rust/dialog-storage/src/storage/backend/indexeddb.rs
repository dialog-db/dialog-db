use async_trait::async_trait;
use futures_util::{Stream, TryStreamExt};
use js_sys::Uint8Array;
use rexie::{ObjectStore, Rexie, RexieBuilder, TransactionMode};
use std::{marker::PhantomData, rc::Rc};
use wasm_bindgen::{JsCast, JsValue};

use crate::{DialogStorageError, StorageSink};

use super::{AtomicStorageBackend, Resource, StorageBackend};

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

        self.content = result
            .map(|value| {
                value
                    .dyn_into::<Uint8Array>()
                    .map(|arr| Value::from(arr.to_vec()))
                    .map_err(|_| {
                        DialogStorageError::StorageBackend("Failed to downcast value".to_string())
                    })
            })
            .transpose()?;

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

        // Read current value in transaction
        let current = store
            .get(key_array.clone())
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let current_value = current
            .map(|value| {
                value
                    .dyn_into::<Uint8Array>()
                    .map(|arr| Value::from(arr.to_vec()))
                    .map_err(|_| {
                        DialogStorageError::StorageBackend("Failed to downcast value".to_string())
                    })
            })
            .transpose()?;

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
                let value_array = bytes_to_typed_array(new_value.as_ref());
                store
                    .put(&value_array, Some(&key_array))
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

        let key = bytes_to_typed_array(key.as_ref());
        let value = bytes_to_typed_array(value.as_ref());

        store
            .put(&value, Some(&key))
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

        let Some(value) = store
            .get(key)
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?
        else {
            return Ok(None);
        };

        let out = value
            .dyn_into::<Uint8Array>()
            .map_err(|value| {
                DialogStorageError::StorageBackend(format!(
                    "Failed to downcast value to bytes: {:?}",
                    value
                ))
            })?
            .to_vec();
        tx.done()
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        Ok(Some(Value::from(out)))
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

        let content = result
            .map(|value| {
                value
                    .dyn_into::<Uint8Array>()
                    .map(|arr| Value::from(arr.to_vec()))
                    .map_err(|_| {
                        DialogStorageError::StorageBackend("Failed to downcast value".to_string())
                    })
            })
            .transpose()?;

        Ok(IndexedDbResource {
            backend: self.clone(),
            key: key.clone(),
            content,
        })
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
            let key = bytes_to_typed_array(key.as_ref());
            let value = bytes_to_typed_array(value.as_ref());
            entries.push((value, Some(key)));
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

#[async_trait(?Send)]
impl<Key, Value> AtomicStorageBackend for IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + PartialEq,
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
        let tx = self
            .db
            .transaction(&[&self.store_name], TransactionMode::ReadWrite)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(&self.store_name)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let key_array = bytes_to_typed_array(key.as_ref());

        // Read current value
        let current = store
            .get(key_array.clone())
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let current_value = current
            .map(|value| {
                value
                    .dyn_into::<Uint8Array>()
                    .map(|arr| Value::from(arr.to_vec()))
                    .map_err(|_| {
                        DialogStorageError::StorageBackend("Failed to downcast value".to_string())
                    })
            })
            .transpose()?;

        // Check CAS condition
        match (when, current_value) {
            (Some(expected), Some(ref actual)) if expected != *actual => {
                // CAS failed - current value doesn't match expected
                return Err(DialogStorageError::StorageBackend(
                    "CAS condition failed: value mismatch".to_string(),
                ));
            }
            (Some(_expected), None) => {
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
                // CAS condition satisfied, proceed with write
            }
        }

        // Perform the write or delete
        match value {
            Some(new_value) => {
                let value_array = bytes_to_typed_array(new_value.as_ref());
                store
                    .put(&value_array, Some(&key_array))
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

        tx.done()
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        Ok(())
    }

    async fn resolve(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        // resolve is the same as get for IndexedDB
        self.get(key).await
    }
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

        let mut backend = backend;
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        // Create new entry (when = None means "must not exist")
        let result = backend.swap(key.clone(), Some(value.clone()), None).await;
        assert!(result.is_ok(), "Should create new entry");

        // Verify it was stored
        let stored = backend.resolve(&key).await.unwrap();
        assert_eq!(stored, Some(value));
    }

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_update() {
        let backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_update", "test_store")
                .await
                .unwrap();

        let mut backend = backend;
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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_cas_failure_value_mismatch() {
        let backend = IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new(
            "test_db_cas_fail",
            "test_store",
        )
        .await
        .unwrap();

        let mut backend = backend;
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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_cas_failure_key_not_exist() {
        let backend = IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new(
            "test_db_no_key",
            "test_store",
        )
        .await
        .unwrap();

        let mut backend = backend;
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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_cas_failure_key_exists() {
        let backend = IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new(
            "test_db_exists",
            "test_store",
        )
        .await
        .unwrap();

        let mut backend = backend;
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

    #[wasm_bindgen_test]
    async fn test_indexeddb_swap_delete() {
        let backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_delete", "test_store")
                .await
                .unwrap();

        let mut backend = backend;
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

    #[wasm_bindgen_test]
    async fn test_indexeddb_resolve_nonexistent() {
        let backend =
            IndexedDbStorageBackend::<Vec<u8>, Vec<u8>>::new("test_db_resolve", "test_store")
                .await
                .unwrap();

        let key = b"nonexistent".to_vec();
        let result = backend.resolve(&key).await.unwrap();
        assert_eq!(result, None, "Should return None for non-existent key");
    }
}
