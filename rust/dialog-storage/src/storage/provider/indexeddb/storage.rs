//! Storage capability provider for IndexedDB.
//!
//! This is a lower-level API where the store name maps directly to an IndexedDB
//! object store. You can use any store path, including `archive/index` or `memory`.

use super::{IndexedDb, to_uint8array};
use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_effects::storage::{
    Delete, DeleteCapability, Get, GetCapability, List, ListCapability, ListResult, Set,
    SetCapability, StorageError,
};
use js_sys::Uint8Array;
use wasm_bindgen::{JsCast, JsValue};

fn storage_error(e: impl std::fmt::Display) -> StorageError {
    StorageError::Storage(e.to_string())
}

impl From<super::IndexedDbError> for StorageError {
    fn from(e: super::IndexedDbError) -> Self {
        StorageError::Storage(e.to_string())
    }
}

#[async_trait(?Send)]
impl Provider<Get> for IndexedDb {
    async fn execute(&mut self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, StorageError> {
        let subject = effect.subject().into();
        let store_name = effect.store();
        let js_key = JsValue::from_str(effect.key());

        let store = self.open(&subject).await?.store(store_name).await?;

        store
            .query(|object_store| async move {
                let value = object_store.get(js_key).await.map_err(storage_error)?;

                let Some(value) = value else {
                    return Ok(None);
                };

                let bytes = value
                    .dyn_into::<Uint8Array>()
                    .map_err(|_| StorageError::Storage("Value is not Uint8Array".to_string()))?
                    .to_vec();

                Ok(Some(bytes))
            })
            .await
    }
}

#[async_trait(?Send)]
impl Provider<Set> for IndexedDb {
    async fn execute(&mut self, effect: Capability<Set>) -> Result<(), StorageError> {
        let subject = effect.subject().into();
        let store_name = effect.store();
        let js_key = JsValue::from_str(effect.key());
        let js_value: JsValue = to_uint8array(effect.value()).into();

        let store = self.open(&subject).await?.store(store_name).await?;

        store
            .transact(|object_store| async move {
                object_store
                    .put(&js_value, Some(&js_key))
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
            .await
    }
}

#[async_trait(?Send)]
impl Provider<Delete> for IndexedDb {
    async fn execute(&mut self, effect: Capability<Delete>) -> Result<(), StorageError> {
        let subject = effect.subject().into();
        let store_name = effect.store();
        let js_key = JsValue::from_str(effect.key());

        let store = self.open(&subject).await?.store(store_name).await?;

        store
            .transact(|object_store| async move {
                object_store.delete(js_key).await.map_err(storage_error)?;
                Ok(())
            })
            .await
    }
}

#[async_trait(?Send)]
impl Provider<List> for IndexedDb {
    async fn execute(&mut self, effect: Capability<List>) -> Result<ListResult, StorageError> {
        let subject = effect.subject().into();
        let store_name = effect.store();
        let continuation_token = effect.continuation_token().map(|s| s.to_string());

        let store = self.open(&subject).await?.store(store_name).await?;

        store
            .query(|object_store| async move {
                let all_keys = object_store
                    .get_all_keys(None, None)
                    .await
                    .map_err(storage_error)?;

                // Convert JS keys to strings
                let mut keys: Vec<String> =
                    all_keys.into_iter().filter_map(|k| k.as_string()).collect();

                // Sort for consistent ordering
                keys.sort();

                // Apply pagination based on continuation token
                let page_size = 1000;
                let start_index = if let Some(ref token) = continuation_token {
                    keys.iter()
                        .position(|k| k.as_str() > token.as_str())
                        .unwrap_or(keys.len())
                } else {
                    0
                };

                let end_index = (start_index + page_size).min(keys.len());
                let is_truncated = end_index < keys.len();
                let next_token = if is_truncated {
                    keys.get(end_index - 1).cloned()
                } else {
                    None
                };

                let page_keys = keys[start_index..end_index].to_vec();

                Ok(ListResult {
                    keys: page_keys,
                    is_truncated,
                    next_continuation_token: next_token,
                })
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::Subject;
    use dialog_effects::storage::{Storage, Store};

    fn unique_subject(prefix: &str) -> Subject {
        Subject::from(format!(
            "did:test:{}-{}",
            prefix,
            js_sys::Date::now() as u64
        ))
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_key() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("storage-get-none");

        let effect = subject
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Get::new("missing-key"));

        let result = effect.perform(&mut provider).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_sets_and_gets_value() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("storage-set-get");
        let key = "test-key";
        let value = b"test-value".to_vec();

        // Set value
        subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Set::new(key, value.clone()))
            .perform(&mut provider)
            .await?;

        // Get value
        let result = subject
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Get::new(key))
            .perform(&mut provider)
            .await?;

        assert_eq!(result, Some(value));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_overwrites_existing_value() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("storage-overwrite");
        let key = "test-key";

        // Set initial value
        subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Set::new(key, b"value1".to_vec()))
            .perform(&mut provider)
            .await?;

        // Overwrite
        subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Set::new(key, b"value2".to_vec()))
            .perform(&mut provider)
            .await?;

        // Get value
        let result = subject
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Get::new(key))
            .perform(&mut provider)
            .await?;

        assert_eq!(result, Some(b"value2".to_vec()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_deletes_value() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("storage-delete");
        let key = "test-key";

        // Set value
        subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Set::new(key, b"test-value".to_vec()))
            .perform(&mut provider)
            .await?;

        // Delete value
        subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Delete::new(key))
            .perform(&mut provider)
            .await?;

        // Verify deleted
        let result = subject
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Get::new(key))
            .perform(&mut provider)
            .await?;

        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_lists_keys() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("storage-list");

        // Set multiple values
        for i in 0..5 {
            subject
                .clone()
                .attenuate(Storage)
                .attenuate(Store::new("index"))
                .invoke(Set::new(format!("key-{}", i), b"value".to_vec()))
                .perform(&mut provider)
                .await?;
        }

        // List keys
        let result = subject
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(List::new(None))
            .perform(&mut provider)
            .await?;

        assert_eq!(result.keys.len(), 5);
        assert!(!result.is_truncated);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_stores() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("storage-stores");

        // Set in different stores
        subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("store1"))
            .invoke(Set::new("key", b"value1".to_vec()))
            .perform(&mut provider)
            .await?;

        subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("store2"))
            .invoke(Set::new("key", b"value2".to_vec()))
            .perform(&mut provider)
            .await?;

        // Get from store1
        let result1 = subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("store1"))
            .invoke(Get::new("key"))
            .perform(&mut provider)
            .await?;
        assert_eq!(result1, Some(b"value1".to_vec()));

        // Get from store2
        let result2 = subject
            .attenuate(Storage)
            .attenuate(Store::new("store2"))
            .invoke(Get::new("key"))
            .perform(&mut provider)
            .await?;
        assert_eq!(result2, Some(b"value2".to_vec()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_empty_value() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("storage-empty-value");

        let key = "empty-key";
        let value = vec![];

        subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Set::new(key, value.clone()))
            .perform(&mut provider)
            .await?;

        let result = subject
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Get::new(key))
            .perform(&mut provider)
            .await?;

        assert_eq!(result, Some(value));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_multiple_keys() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("storage-multiple-keys");

        let key1 = "key1";
        let key2 = "key2";
        let key3 = "key3";
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();
        let value3 = b"value3".to_vec();

        subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Set::new(key1, value1.clone()))
            .perform(&mut provider)
            .await?;

        subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Set::new(key2, value2.clone()))
            .perform(&mut provider)
            .await?;

        subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Set::new(key3, value3.clone()))
            .perform(&mut provider)
            .await?;

        let result1 = subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Get::new(key1))
            .perform(&mut provider)
            .await?;
        assert_eq!(result1, Some(value1));

        let result2 = subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Get::new(key2))
            .perform(&mut provider)
            .await?;
        assert_eq!(result2, Some(value2));

        let result3 = subject
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Get::new(key3))
            .perform(&mut provider)
            .await?;
        assert_eq!(result3, Some(value3));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_value() -> anyhow::Result<()> {
        let mut provider = IndexedDb::new();
        let subject = unique_subject("storage-large-value");

        let key = "large-key";
        // 1MB value
        let value: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

        subject
            .clone()
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Set::new(key, value.clone()))
            .perform(&mut provider)
            .await?;

        let result = subject
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Get::new(key))
            .perform(&mut provider)
            .await?;

        assert_eq!(result, Some(value));

        Ok(())
    }
}
