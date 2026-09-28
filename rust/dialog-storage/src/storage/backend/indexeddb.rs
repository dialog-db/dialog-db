use crate::storage::idb::{Database, TransactionMode};
use async_trait::async_trait;
use base58::ToBase58;
use js_sys::Uint8Array;
use std::{marker::PhantomData, rc::Rc};
use wasm_bindgen::{JsCast, JsValue};

use crate::DialogStorageError;

use super::StorageBackend;

const INDEXEDDB_STORAGE_VERSION: u32 = 3;

/// Object store name for key-value storage (StorageBackend).
const INDEX_STORE: &str = "index";
/// Object store name the retired transactional memory used. Still created so
/// the database schema stays at its current version.
const MEMORY_STORE: &str = "memory";

/// An IndexedDB-based storage implementation.
///
/// This struct provides a [`StorageBackend`] over the `"index"` object store,
/// holding raw `Uint8Array` values under base58-encoded keys.
#[derive(Clone)]
pub struct IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]>,
    Value: AsRef<[u8]> + From<Vec<u8>>,
{
    db: Rc<Database>,
    key_type: PhantomData<Key>,
    value_type: PhantomData<Value>,
}

impl<Key, Value> IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]>,
    Value: AsRef<[u8]> + From<Vec<u8>>,
{
    /// Creates a new [`IndexedDbStorageBackend`].
    ///
    /// This opens (or creates) an IndexedDB database with the `"index"` object
    /// store (and the vestigial `"memory"` store the schema still declares).
    pub async fn new(db_name: &str) -> Result<Self, DialogStorageError> {
        let db = Database::open(
            db_name,
            Some(INDEXEDDB_STORAGE_VERSION),
            &[INDEX_STORE, MEMORY_STORE],
        )
        .await
        .map_err(|error| DialogStorageError::Storage(format!("{error}")))?;

        Ok(IndexedDbStorageBackend {
            db: Rc::new(db),
            key_type: PhantomData,
            value_type: PhantomData,
        })
    }
}

#[async_trait(?Send)]
impl<Key, Value> StorageBackend for IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone,
{
    type Key = Key;
    type Value = Value;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let tx = self
            .db
            .transaction(&[INDEX_STORE], TransactionMode::ReadWrite)
            .map_err(|error| DialogStorageError::Storage(format!("{error}")))?;
        let store = tx
            .store(INDEX_STORE)
            .map_err(|error| DialogStorageError::Storage(format!("{error}")))?;

        // Base58 encode key for better DevTools readability
        let key = JsValue::from_str(&key.as_ref().to_base58());
        let value = bytes_to_typed_array(value.as_ref());

        store
            .put(&value, Some(&key))
            .await
            .map_err(|error| DialogStorageError::Storage(format!("{error}")))?;

        tx.settle()
            .await
            .map_err(|error| DialogStorageError::Storage(format!("{error}")))?;

        Ok(())
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let tx = self
            .db
            .transaction(&[INDEX_STORE], TransactionMode::ReadOnly)
            .map_err(|error| DialogStorageError::Storage(format!("{error}")))?;
        let store = tx
            .store(INDEX_STORE)
            .map_err(|error| DialogStorageError::Storage(format!("{error}")))?;

        // Base58 encode key for lookup
        let key = JsValue::from_str(&key.as_ref().to_base58());

        let Some(value) = store
            .get(key)
            .await
            .map_err(|error| DialogStorageError::Storage(format!("{error}")))?
        else {
            return Ok(None);
        };

        let out = value
            .dyn_into::<Uint8Array>()
            .map_err(|value| {
                DialogStorageError::Storage(format!(
                    "Failed to downcast value to bytes: {:?}",
                    value
                ))
            })?
            .to_vec();
        tx.settle()
            .await
            .map_err(|error| DialogStorageError::Storage(format!("{error}")))?;

        Ok(Some(Value::from(out)))
    }
}

fn bytes_to_typed_array(bytes: &[u8]) -> JsValue {
    let array = Uint8Array::new_with_length(bytes.len() as u32);
    array.copy_from(bytes);
    JsValue::from(array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    /// Generate a unique database name to avoid conflicts between tests
    fn unique_db_name(prefix: &str) -> String {
        format!("{}-{}", prefix, js_sys::Date::now() as u64)
    }

    // StorageBackend tests

    #[dialog_common::test]
    async fn it_returns_none_for_non_existent_key() -> Result<()> {
        let db_name = unique_db_name("test-get-none");
        let backend: IndexedDbStorageBackend<Vec<u8>, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        let result = backend.get(&b"missing".to_vec()).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_sets_and_gets_value() -> Result<()> {
        let db_name = unique_db_name("test-set-get");
        let mut backend: IndexedDbStorageBackend<Vec<u8>, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        let key = b"test-key".to_vec();
        let value = b"test-value".to_vec();

        backend.set(key.clone(), value.clone()).await?;

        let result = backend.get(&key).await?;
        assert_eq!(result, Some(value));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_overwrites_existing_value() -> Result<()> {
        let db_name = unique_db_name("test-overwrite");
        let mut backend: IndexedDbStorageBackend<Vec<u8>, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        let key = b"test-key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        backend.set(key.clone(), value1).await?;
        backend.set(key.clone(), value2.clone()).await?;

        let result = backend.get(&key).await?;
        assert_eq!(result, Some(value2));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_binary_keys() -> Result<()> {
        let db_name = unique_db_name("test-binary-keys");
        let mut backend: IndexedDbStorageBackend<Vec<u8>, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        // Binary key with non-UTF8 bytes
        let key = vec![0x00, 0xff, 0xfe, 0x01];
        let value = b"binary key value".to_vec();

        backend.set(key.clone(), value.clone()).await?;

        let result = backend.get(&key).await?;
        assert_eq!(result, Some(value));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_empty_value() -> Result<()> {
        let db_name = unique_db_name("test-empty-value");
        let mut backend: IndexedDbStorageBackend<Vec<u8>, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        let key = b"empty-key".to_vec();
        let value = vec![];

        backend.set(key.clone(), value.clone()).await?;

        let result = backend.get(&key).await?;
        assert_eq!(result, Some(value));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_multiple_keys() -> Result<()> {
        let db_name = unique_db_name("test-multiple-keys");
        let mut backend: IndexedDbStorageBackend<Vec<u8>, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        let key1 = b"key1".to_vec();
        let key2 = b"key2".to_vec();
        let key3 = b"key3".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();
        let value3 = b"value3".to_vec();

        backend.set(key1.clone(), value1.clone()).await?;
        backend.set(key2.clone(), value2.clone()).await?;
        backend.set(key3.clone(), value3.clone()).await?;

        assert_eq!(backend.get(&key1).await?, Some(value1));
        assert_eq!(backend.get(&key2).await?, Some(value2));
        assert_eq!(backend.get(&key3).await?, Some(value3));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_value() -> Result<()> {
        let db_name = unique_db_name("test-large-value");
        let mut backend: IndexedDbStorageBackend<Vec<u8>, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        let key = b"large-key".to_vec();
        // 1MB value
        let value: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

        backend.set(key.clone(), value.clone()).await?;

        let result = backend.get(&key).await?;
        assert_eq!(result, Some(value));
        Ok(())
    }
}
