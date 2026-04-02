//! Storage capability provider for volatile storage.
//!
//! Implements key-value storage effects by storing data in the session's
//! storage HashMap, keyed by (store_name, key_bytes).

use super::{StorageKey, Volatile};
use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_effects::storage::{Delete, Get, List, ListResult, Set, StorageError};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Get> for Volatile {
    async fn execute(&self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, StorageError> {
        let subject = effect.subject().clone();
        let store = effect.store().to_string();
        let key = effect.key().to_vec();

        let storage_key: StorageKey = (store, key);

        let sessions = self.sessions.read();
        Ok(sessions
            .get(&subject)
            .and_then(|session| session.storage.get(&storage_key).cloned()))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Set> for Volatile {
    async fn execute(&self, effect: Capability<Set>) -> Result<(), StorageError> {
        let subject = effect.subject().clone();
        let store = effect.store().to_string();
        let key = effect.key().to_vec();
        let value = effect.value().to_vec();

        let storage_key: StorageKey = (store, key);

        let mut sessions = self.sessions.write();
        let session = sessions.entry(subject).or_default();
        session.storage.insert(storage_key, value);

        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Delete> for Volatile {
    async fn execute(&self, effect: Capability<Delete>) -> Result<(), StorageError> {
        let subject = effect.subject().clone();
        let store = effect.store().to_string();
        let key = effect.key().to_vec();

        let storage_key: StorageKey = (store, key);

        let mut sessions = self.sessions.write();
        if let Some(session) = sessions.get_mut(&subject) {
            session.storage.remove(&storage_key);
        }

        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<List> for Volatile {
    async fn execute(&self, effect: Capability<List>) -> Result<ListResult, StorageError> {
        let subject = effect.subject().clone();
        let store = effect.store().to_string();
        let prefix = String::from_utf8_lossy(effect.prefix()).into_owned();

        let sessions = self.sessions.read();
        let keys: Vec<String> = sessions
            .get(&subject)
            .map(|session| {
                session
                    .storage
                    .keys()
                    .filter(|(s, _)| s == &store)
                    .filter_map(|(_, k)| String::from_utf8(k.clone()).ok())
                    .filter(|k| k.starts_with(&prefix))
                    .collect()
            })
            .unwrap_or_default();

        Ok(ListResult {
            keys,
            is_truncated: false,
            next_continuation_token: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Did, Subject};
    use dialog_effects::storage::{Storage, Store};

    fn unique_subject(prefix: &str) -> Subject {
        let did: Did = format!(
            "did:test:{}-{}",
            prefix,
            dialog_common::time::now()
                .duration_since(dialog_common::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
        .parse()
        .unwrap();
        Subject::from(did)
    }

    fn store_cap(subject: Subject, store_name: &str) -> Capability<Store> {
        subject.attenuate(Storage).attenuate(Store::new(store_name))
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_key() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("storage-get-none");

        let effect = store_cap(subject, "index").invoke(Get::new(b"missing"));
        let result = effect.perform(&provider).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_value() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("storage-set-get");

        store_cap(subject.clone(), "index")
            .invoke(Set::new(b"key1".to_vec(), b"value1".to_vec()))
            .perform(&provider)
            .await?;

        let result = store_cap(subject, "index")
            .invoke(Get::new(b"key1"))
            .perform(&provider)
            .await?;
        assert_eq!(result, Some(b"value1".to_vec()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_by_store_name() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("storage-isolation");

        store_cap(subject.clone(), "store-a")
            .invoke(Set::new(b"key".to_vec(), b"value-a".to_vec()))
            .perform(&provider)
            .await?;

        store_cap(subject.clone(), "store-b")
            .invoke(Set::new(b"key".to_vec(), b"value-b".to_vec()))
            .perform(&provider)
            .await?;

        let result_a = store_cap(subject.clone(), "store-a")
            .invoke(Get::new(b"key"))
            .perform(&provider)
            .await?;
        assert_eq!(result_a, Some(b"value-a".to_vec()));

        let result_b = store_cap(subject, "store-b")
            .invoke(Get::new(b"key"))
            .perform(&provider)
            .await?;
        assert_eq!(result_b, Some(b"value-b".to_vec()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_deletes_key() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("storage-delete");

        store_cap(subject.clone(), "index")
            .invoke(Set::new(b"key".to_vec(), b"value".to_vec()))
            .perform(&provider)
            .await?;

        store_cap(subject.clone(), "index")
            .invoke(Delete::new(b"key"))
            .perform(&provider)
            .await?;

        let result = store_cap(subject, "index")
            .invoke(Get::new(b"key"))
            .perform(&provider)
            .await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_lists_keys_in_store() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("storage-list");

        store_cap(subject.clone(), "index")
            .invoke(Set::new(b"alpha".to_vec(), b"1".to_vec()))
            .perform(&provider)
            .await?;

        store_cap(subject.clone(), "index")
            .invoke(Set::new(b"beta".to_vec(), b"2".to_vec()))
            .perform(&provider)
            .await?;

        store_cap(subject.clone(), "other")
            .invoke(Set::new(b"gamma".to_vec(), b"3".to_vec()))
            .perform(&provider)
            .await?;

        let result = store_cap(subject, "index")
            .invoke(List::new(None))
            .perform(&provider)
            .await?;

        assert_eq!(result.keys.len(), 2);
        assert!(result.keys.contains(&"alpha".to_string()));
        assert!(result.keys.contains(&"beta".to_string()));
        assert!(!result.is_truncated);

        Ok(())
    }
}
