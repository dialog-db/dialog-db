//! Storage capability provider for IndexedDB.
//!
//! Implements key-value storage effects using IndexedDB object stores.
//! Each `Store` maps to an IndexedDB object store named `storage/{store_name}`.
//! Keys are stored as strings.

use super::{IndexedDb, IndexedDbError, to_uint8array};
use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_effects::storage::{
    Delete, DeleteCapability, Get, GetCapability, List, ListCapability, ListResult, Set,
    SetCapability, StorageError,
};
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;

const STORAGE_PREFIX: &str = "storage/";

fn store_name(store: &str) -> String {
    format!("{STORAGE_PREFIX}{store}")
}

fn to_err(e: impl std::fmt::Display) -> StorageError {
    StorageError::Storage(e.to_string())
}

struct Err(StorageError);

impl From<IndexedDbError> for Err {
    fn from(e: IndexedDbError) -> Self {
        Self(StorageError::Storage(e.to_string()))
    }
}

impl From<StorageError> for Err {
    fn from(e: StorageError) -> Self {
        Self(e)
    }
}

#[async_trait(?Send)]
impl Provider<Get> for IndexedDb {
    async fn execute(&self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, StorageError> {
        let subject = effect.subject().to_string();
        let store = store_name(effect.store());
        let key = String::from_utf8_lossy(effect.key()).into_owned();

        self.open(&subject).await.map_err(to_err)?;
        let mut session = self.take_session(&subject).map_err(to_err)?;

        let result: Result<_, Err> = async {
            let idb_store = session.store(&store).await?;
            let js_key = JsValue::from_str(&key);

            let value = idb_store
                .query(|object_store| async move {
                    object_store
                        .get(js_key)
                        .await
                        .map_err(|e| Err(StorageError::Storage(e.to_string())))
                })
                .await?;

            match value {
                Some(js_val) => {
                    let array: js_sys::Uint8Array = js_val
                        .dyn_into()
                        .map_err(|_| StorageError::Storage("expected Uint8Array".into()))?;
                    Ok(Some(array.to_vec()))
                }
                None => Ok(None),
            }
        }
        .await;

        self.return_session(&subject, session);
        result.map_err(|e| e.0)
    }
}

#[async_trait(?Send)]
impl Provider<Set> for IndexedDb {
    async fn execute(&self, effect: Capability<Set>) -> Result<(), StorageError> {
        let subject = effect.subject().to_string();
        let store = store_name(effect.store());
        let key = String::from_utf8_lossy(effect.key()).into_owned();
        let value = effect.value().to_vec();

        self.open(&subject).await.map_err(to_err)?;
        let mut session = self.take_session(&subject).map_err(to_err)?;

        let result: Result<_, Err> = async {
            let idb_store = session.store(&store).await?;
            let js_key = JsValue::from_str(&key);
            let js_val = to_uint8array(&value);

            idb_store
                .transact(|object_store| async move {
                    object_store
                        .put(&js_val, Some(&js_key))
                        .await
                        .map_err(|e| Err(StorageError::Storage(e.to_string())))?;
                    Ok::<(), Err>(())
                })
                .await?;
            Ok(())
        }
        .await;

        self.return_session(&subject, session);
        result.map_err(|e| e.0)
    }
}

#[async_trait(?Send)]
impl Provider<Delete> for IndexedDb {
    async fn execute(&self, effect: Capability<Delete>) -> Result<(), StorageError> {
        let subject = effect.subject().to_string();
        let store = store_name(effect.store());
        let key = String::from_utf8_lossy(effect.key()).into_owned();

        self.open(&subject).await.map_err(to_err)?;
        let mut session = self.take_session(&subject).map_err(to_err)?;

        let result: Result<_, Err> = async {
            let idb_store = session.store(&store).await?;
            let js_key = JsValue::from_str(&key);

            idb_store
                .transact(|object_store| async move {
                    object_store
                        .delete(js_key)
                        .await
                        .map_err(|e| Err(StorageError::Storage(e.to_string())))?;
                    Ok::<(), Err>(())
                })
                .await?;
            Ok(())
        }
        .await;

        self.return_session(&subject, session);
        result.map_err(|e| e.0)
    }
}

#[async_trait(?Send)]
impl Provider<List> for IndexedDb {
    async fn execute(&self, effect: Capability<List>) -> Result<ListResult, StorageError> {
        let subject = effect.subject().to_string();
        let store = store_name(effect.store());
        let prefix = String::from_utf8_lossy(effect.prefix()).into_owned();

        self.open(&subject).await.map_err(to_err)?;
        let mut session = self.take_session(&subject).map_err(to_err)?;

        let result: Result<_, Err> = async {
            let idb_store = session.store(&store).await?;

            let all_keys: Vec<String> = idb_store
                .query(|object_store| async move {
                    let js_keys = object_store
                        .get_all_keys(None, None)
                        .await
                        .map_err(|e| Err(StorageError::Storage(e.to_string())))?;

                    let mut keys = Vec::new();
                    for js_key in js_keys {
                        if let Some(s) = js_key.as_string() {
                            keys.push(s);
                        }
                    }
                    Ok::<_, Err>(keys)
                })
                .await?;

            let filtered: Vec<String> = if prefix.is_empty() {
                all_keys
            } else {
                all_keys
                    .into_iter()
                    .filter(|k| k.starts_with(&prefix))
                    .collect()
            };

            Ok(ListResult {
                keys: filtered,
                is_truncated: false,
                next_continuation_token: None,
            })
        }
        .await;

        self.return_session(&subject, session);
        result.map_err(|e| e.0)
    }
}

#[cfg(test)]
mod tests {
    use super::IndexedDb;
    use dialog_capability::Subject;
    use dialog_effects::storage::{Get, List, Set, Storage, Store};

    fn unique_subject(prefix: &str) -> Subject {
        let did_str = format!(
            "did:test:idb-storage-{}-{}",
            prefix,
            js_sys::Date::now() as u64
        );
        let did: dialog_capability::Did = did_str.parse().unwrap();
        Subject::from(did)
    }

    fn store_cap(subject: Subject, store_name: &str) -> dialog_capability::Capability<Store> {
        subject.attenuate(Storage).attenuate(Store::new(store_name))
    }

    #[dialog_common::test]
    async fn set_and_get_roundtrip() {
        let provider = IndexedDb::new();
        let subject = unique_subject("set-get");

        store_cap(subject.clone(), "data")
            .invoke(Set::new(b"hello".to_vec(), b"world".to_vec()))
            .perform(&provider)
            .await
            .unwrap();

        let result = store_cap(subject, "data")
            .invoke(Get::new(b"hello"))
            .perform(&provider)
            .await
            .unwrap();

        assert_eq!(result, Some(b"world".to_vec()));
    }

    #[dialog_common::test]
    async fn get_missing_returns_none() {
        let provider = IndexedDb::new();
        let subject = unique_subject("get-none");

        let result = store_cap(subject, "data")
            .invoke(Get::new(b"missing"))
            .perform(&provider)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[dialog_common::test]
    async fn keys_with_slashes_roundtrip() {
        let provider = IndexedDb::new();
        let subject = unique_subject("slash-keys");

        let key = b"a/b/c";
        store_cap(subject.clone(), "data")
            .invoke(Set::new(key.to_vec(), b"nested".to_vec()))
            .perform(&provider)
            .await
            .unwrap();

        let result = store_cap(subject, "data")
            .invoke(Get::new(key.to_vec()))
            .perform(&provider)
            .await
            .unwrap();

        assert_eq!(result, Some(b"nested".to_vec()));
    }

    #[dialog_common::test]
    async fn list_returns_all_keys() {
        let provider = IndexedDb::new();
        let subject = unique_subject("list-all");

        let key1 = "aud1/sub1/iss1.cid1";
        let key2 = "aud1/_/iss2.cid2";
        let key3 = "aud2/sub2/iss3.cid3";

        for (k, v) in [(key1, "d1"), (key2, "d2"), (key3, "d3")] {
            store_cap(subject.clone(), "ucan")
                .invoke(Set::new(k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                .perform(&provider)
                .await
                .unwrap();
        }

        let result = store_cap(subject, "ucan")
            .invoke(List::new(None))
            .perform(&provider)
            .await
            .unwrap();

        assert_eq!(result.keys.len(), 3);
        assert!(result.keys.contains(&key1.to_string()));
        assert!(result.keys.contains(&key2.to_string()));
        assert!(result.keys.contains(&key3.to_string()));
    }

    #[dialog_common::test]
    async fn list_with_prefix_filters() {
        let provider = IndexedDb::new();
        let subject = unique_subject("list-prefix");

        let key1 = "aud1/sub1/iss1.cid1";
        let key2 = "aud1/_/iss2.cid2";
        let key3 = "aud2/sub2/iss3.cid3";

        for (k, v) in [(key1, "d1"), (key2, "d2"), (key3, "d3")] {
            store_cap(subject.clone(), "ucan")
                .invoke(Set::new(k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                .perform(&provider)
                .await
                .unwrap();
        }

        let result = store_cap(subject, "ucan")
            .invoke(List::with_prefix("aud1/"))
            .perform(&provider)
            .await
            .unwrap();

        assert_eq!(result.keys.len(), 2);
        assert!(result.keys.contains(&key1.to_string()));
        assert!(result.keys.contains(&key2.to_string()));
    }
}
