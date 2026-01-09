use async_trait::async_trait;
use base58::ToBase58;
use dialog_common::Blake3Hash;
use futures_util::{Stream, TryStreamExt};
use js_sys::{Object, Reflect, Uint8Array};
use rexie::{ObjectStore, Rexie, RexieBuilder, TransactionMode};
use std::{marker::PhantomData, rc::Rc};
use wasm_bindgen::{JsCast, JsValue};

use crate::{DialogStorageError, StorageSink};

use super::{StorageBackend, TransactionalMemoryBackend};

const INDEXEDDB_STORAGE_VERSION: u32 = 3;

/// Object store name for key-value storage (StorageBackend).
const INDEX_STORE: &str = "index";
/// Object store name for transactional memory (TransactionalMemoryBackend).
const MEMORY_STORE: &str = "memory";

/// An IndexedDB-based storage implementation.
///
/// This struct provides both [`StorageBackend`] and [`TransactionalMemoryBackend`]
/// implementations using separate IndexedDB object stores:
///
/// - `"index"` store: Used by `StorageBackend` for key-value storage with base58-encoded keys
/// - `"memory"` store: Used by `TransactionalMemoryBackend` with UTF-8 string keys
///
/// Both stores hold raw `Uint8Array` values. The transactional backend computes
/// BLAKE3 hashes on read to use as editions for CAS semantics.
#[derive(Clone)]
pub struct IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]>,
    Value: AsRef<[u8]> + From<Vec<u8>>,
{
    db: Rc<Rexie>,
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
    ///
    /// This opens (or creates) an IndexedDB database with two object stores:
    /// - `"index"`: for `StorageBackend` operations (base58-encoded keys)
    /// - `"memory"`: for `TransactionalMemoryBackend` operations (UTF-8 string keys)
    pub async fn new(db_name: &str) -> Result<Self, DialogStorageError> {
        let db = RexieBuilder::new(db_name)
            .version(INDEXEDDB_STORAGE_VERSION)
            .add_object_store(ObjectStore::new(INDEX_STORE).auto_increment(false))
            .add_object_store(ObjectStore::new(MEMORY_STORE).auto_increment(false))
            .build()
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

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
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + PartialEq,
{
    type Key = Key;
    type Value = Value;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let tx = self
            .db
            .transaction(&[INDEX_STORE], TransactionMode::ReadWrite)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(INDEX_STORE)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        // Base58 encode key for better DevTools readability
        let key = JsValue::from_str(&key.as_ref().to_base58());
        let value = bytes_to_typed_array(value.as_ref());

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
            .transaction(&[INDEX_STORE], TransactionMode::ReadOnly)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(INDEX_STORE)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        // Base58 encode key for lookup
        let key = JsValue::from_str(&key.as_ref().to_base58());

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

/// Transactional memory backend using BLAKE3 content hashes for CAS.
///
/// This implementation computes a BLAKE3 hash of each value to use as its edition.
/// Values are stored as raw bytes in the `"memory"` object store - the hash is
/// computed on read, not stored. Keys are treated as UTF-8 strings.
///
/// # Edition Strategy
///
/// Uses a 32-byte BLAKE3 hash as the edition. CAS succeeds when the hash of
/// the current stored value matches the expected edition. This provides
/// content-addressable semantics - identical values have identical editions.
///
/// # Cross-Tab Safety
///
/// IndexedDB transactions are atomic and serialized across tabs. The `replace`
/// operation performs read and write within a single `ReadWrite` transaction,
/// so concurrent writes from multiple tabs are safely serialized by IndexedDB.
#[async_trait(?Send)]
impl<Key, Value> TransactionalMemoryBackend for IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone,
{
    type Address = Key;
    type Value = Value;
    type Error = DialogStorageError;
    type Edition = Blake3Hash;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        let tx = self
            .db
            .transaction(&[MEMORY_STORE], TransactionMode::ReadOnly)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(MEMORY_STORE)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        // Treat address as UTF-8 string for DevTools readability
        let key = address_to_string(address)?;
        let entry = store
            .get(key)
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let Some(entry) = entry else {
            return Ok(None);
        };

        let bytes = entry
            .dyn_into::<Uint8Array>()
            .map_err(|_| DialogStorageError::StorageBackend("Value is not Uint8Array".to_string()))?
            .to_vec();

        let hash = Blake3Hash::hash(&bytes);
        Ok(Some((Value::from(bytes), hash)))
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        let tx = self
            .db
            .transaction(&[MEMORY_STORE], TransactionMode::ReadWrite)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(MEMORY_STORE)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        // Treat address as UTF-8 string for DevTools readability
        let key = address_to_string(address)?;

        // Read current value and compute its hash
        let current = store
            .get(key.clone())
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let current_hash = if let Some(entry) = &current {
            let bytes = entry
                .clone()
                .dyn_into::<Uint8Array>()
                .map_err(|_| {
                    DialogStorageError::StorageBackend("Value is not Uint8Array".to_string())
                })?
                .to_vec();
            Some(Blake3Hash::hash(&bytes))
        } else {
            None
        };

        // Perform the operation
        match content {
            Some(value) => {
                let bytes = value.as_ref();
                let hash = Blake3Hash::hash(bytes);

                // If current value already matches desired value, succeed without writing
                if current_hash.as_ref() == Some(&hash) {
                    return Ok(Some(hash));
                }

                // Check edition only if we need to write
                if current_hash.as_ref() != edition {
                    return Err(DialogStorageError::StorageBackend(
                        "CAS condition failed: edition mismatch".to_string(),
                    ));
                }

                let entry = bytes_to_typed_array(bytes);
                store
                    .put(&entry, Some(&key))
                    .await
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

                tx.done()
                    .await
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

                Ok(Some(hash))
            }
            None => {
                // Delete operation - if already deleted, succeed
                if current.is_none() {
                    return Ok(None);
                }

                // Check edition only if we need to delete
                if current_hash.as_ref() != edition {
                    return Err(DialogStorageError::StorageBackend(
                        "CAS condition failed: edition mismatch".to_string(),
                    ));
                }

                store
                    .delete(key)
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
            .transaction(&[INDEX_STORE], TransactionMode::ReadWrite)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        let store = tx
            .store(INDEX_STORE)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        tokio::pin!(stream);

        let mut entries = Vec::<(JsValue, Option<JsValue>)>::new();

        while let Some((key, value)) = stream.try_next().await? {
            // Base58 encode key for better DevTools readability
            let key = JsValue::from_str(&key.as_ref().to_base58());
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

/// Convert address bytes to a JS string key for IndexedDB.
/// Addresses are expected to be valid UTF-8.
fn address_to_string<Key: AsRef<[u8]>>(address: &Key) -> Result<JsValue, DialogStorageError> {
    let s = std::str::from_utf8(address.as_ref())
        .map_err(|e| DialogStorageError::StorageBackend(format!("Invalid UTF-8 address: {e}")))?;
    Ok(JsValue::from_str(s))
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

    // TransactionalMemoryBackend tests

    #[dialog_common::test]
    async fn it_resolves_non_existent_address() -> Result<()> {
        let db_name = unique_db_name("test-stm-resolve-none");
        let backend: IndexedDbStorageBackend<String, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        let result = backend.resolve(&"missing".to_string()).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_creates_new_value() -> Result<()> {
        let db_name = unique_db_name("test-stm-create");
        let backend: IndexedDbStorageBackend<String, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        let content = b"hello world".to_vec();
        let edition = backend
            .replace(&"test-key".to_string(), None, Some(content.clone()))
            .await?;

        assert!(edition.is_some());

        // Verify it can be resolved
        let resolved = backend.resolve(&"test-key".to_string()).await?;
        assert!(resolved.is_some());
        let (value, resolved_edition) = resolved.unwrap();
        assert_eq!(value, content);
        assert_eq!(Some(resolved_edition), edition);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_existing_value() -> Result<()> {
        let db_name = unique_db_name("test-stm-update");
        let backend: IndexedDbStorageBackend<String, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        // Create initial value
        let initial = b"initial".to_vec();
        let edition1 = backend
            .replace(&"test-key".to_string(), None, Some(initial))
            .await?
            .unwrap();

        // Update with correct edition
        let updated = b"updated".to_vec();
        let edition2 = backend
            .replace(
                &"test-key".to_string(),
                Some(&edition1),
                Some(updated.clone()),
            )
            .await?;

        assert!(edition2.is_some());
        assert_ne!(edition1, edition2.unwrap());

        // Verify update
        let (value, _) = backend.resolve(&"test-key".to_string()).await?.unwrap();
        assert_eq!(value, updated);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_on_edition_mismatch() -> Result<()> {
        let db_name = unique_db_name("test-stm-mismatch");
        let backend: IndexedDbStorageBackend<String, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        // Create initial value
        let initial = b"initial".to_vec();
        let _edition = backend
            .replace(&"test-key".to_string(), None, Some(initial))
            .await?;

        // Try to update with wrong edition
        let wrong_edition = Blake3Hash::hash(b"wrong");
        let result = backend
            .replace(
                &"test-key".to_string(),
                Some(&wrong_edition),
                Some(b"new value".to_vec()),
            )
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("edition mismatch"));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_creating_when_exists() -> Result<()> {
        let db_name = unique_db_name("test-stm-create-exists");
        let backend: IndexedDbStorageBackend<String, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        // Create initial value
        let initial = b"initial".to_vec();
        backend
            .replace(&"test-key".to_string(), None, Some(initial))
            .await?;

        // Try to create again (edition = None means "expect not to exist")
        let result = backend
            .replace(&"test-key".to_string(), None, Some(b"new value".to_vec()))
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("edition mismatch"));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_deletes_value() -> Result<()> {
        let db_name = unique_db_name("test-stm-delete");
        let backend: IndexedDbStorageBackend<String, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        // Create value
        let content = b"to be deleted".to_vec();
        let edition = backend
            .replace(&"test-key".to_string(), None, Some(content))
            .await?
            .unwrap();

        // Delete with correct edition
        let result = backend
            .replace(&"test-key".to_string(), Some(&edition), None)
            .await?;

        assert!(result.is_none()); // Delete returns None

        // Verify it's gone
        let resolved = backend.resolve(&"test-key".to_string()).await?;
        assert!(resolved.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_invalid_utf8_address() -> Result<()> {
        let db_name = unique_db_name("test-stm-invalid-utf8");
        let backend: IndexedDbStorageBackend<Vec<u8>, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        // Invalid UTF-8 sequence
        let invalid_address = vec![0xff, 0xfe];
        let result = backend.resolve(&invalid_address).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid UTF-8"));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_produces_deterministic_content_hash() -> Result<()> {
        let db_name = unique_db_name("test-stm-hash-deterministic");
        let backend: IndexedDbStorageBackend<String, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        let content = b"same content".to_vec();

        // Create value
        let edition1 = backend
            .replace(&"key1".to_string(), None, Some(content.clone()))
            .await?
            .unwrap();

        // Create same value at different key
        let edition2 = backend
            .replace(&"key2".to_string(), None, Some(content))
            .await?
            .unwrap();

        // Same content should produce same edition (content hash)
        assert_eq!(edition1, edition2);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_subdirectory_addresses() -> Result<()> {
        let db_name = unique_db_name("test-stm-subdir");
        let backend: IndexedDbStorageBackend<String, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        let address = "path/to/nested/key".to_string();
        let content = b"nested value".to_vec();

        let edition = backend
            .replace(&address, None, Some(content.clone()))
            .await?;

        assert!(edition.is_some());

        let (value, _) = backend.resolve(&address).await?.unwrap();
        assert_eq!(value, content);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_with_stale_edition_when_value_matches() -> Result<()> {
        let db_name = unique_db_name("test-stm-stale-edition");
        let backend: IndexedDbStorageBackend<String, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        // Create initial value
        let content = b"desired value".to_vec();
        let _edition = backend
            .replace(&"test-key".to_string(), None, Some(content.clone()))
            .await?;

        // Try to replace with wrong edition but same value - should succeed
        let wrong_edition = Blake3Hash::hash(b"wrong");
        let result = backend
            .replace(
                &"test-key".to_string(),
                Some(&wrong_edition),
                Some(content.clone()),
            )
            .await;

        assert!(result.is_ok());
        // Should return the hash of the content
        assert_eq!(result.unwrap(), Some(Blake3Hash::hash(&content)));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_deleting_already_deleted() -> Result<()> {
        let db_name = unique_db_name("test-stm-delete-already-deleted");
        let backend: IndexedDbStorageBackend<String, Vec<u8>> =
            IndexedDbStorageBackend::new(&db_name).await?;

        // Try to delete non-existent key with wrong edition - should succeed
        let wrong_edition = Blake3Hash::hash(b"wrong");
        let result = backend
            .replace(&"test-key".to_string(), Some(&wrong_edition), None)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
        Ok(())
    }
}
