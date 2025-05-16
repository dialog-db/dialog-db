use async_trait::async_trait;
use futures_util::{Stream, TryStreamExt};
use js_sys::Uint8Array;
use rexie::{ObjectStore, Rexie, RexieBuilder, TransactionMode};
use std::{marker::PhantomData, rc::Rc};
use wasm_bindgen::{JsCast, JsValue};

use crate::{DialogStorageError, StorageSink};

use super::StorageBackend;

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
}

#[async_trait(?Send)]
impl<Key, Value> StorageSink for IndexedDbStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone,
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
