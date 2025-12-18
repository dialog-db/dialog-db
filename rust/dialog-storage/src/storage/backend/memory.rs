use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::HashMap, ops::DerefMut, sync::Arc};

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::ConditionalSync;
use futures_util::Stream;
use tokio::sync::RwLock;

use crate::{DialogStorageError, StorageSource};

use super::{StorageBackend, TransactionalMemoryBackend};

/// An entry with versioning for CAS operations.
#[derive(Clone)]
struct VersionedEntry<Value> {
    value: Value,
    version: u64,
}

/// A trivial implementation of [StorageBackend] - backed by a [HashMap] - where
/// all values are kept in memory and never persisted.
#[derive(Clone, Default)]
pub struct MemoryStorageBackend<Key, Value>
where
    Key: Eq + std::hash::Hash,
    Value: Clone,
{
    entries: Arc<RwLock<HashMap<Key, VersionedEntry<Value>>>>,
    next_version: Arc<AtomicU64>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> StorageBackend for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync,
{
    type Key = Key;
    type Value = Value;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let version = self.next_version.fetch_add(1, Ordering::SeqCst);
        let mut entries = self.entries.write().await;
        entries.insert(key, VersionedEntry { value, version });
        Ok(())
    }
    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let entries = self.entries.read().await;
        Ok(entries.get(key).map(|entry| entry.value.clone()))
    }
}

impl<Key, Value> StorageSource for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync,
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
            for (key, entry) in entries.iter() {
                yield (key.clone(), entry.value.clone());
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

            for (key, entry) in entries.into_iter() {
                yield (key, entry.value);
            }
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> TransactionalMemoryBackend for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync,
{
    type Address = Key;
    type Value = Value;
    type Error = DialogStorageError;
    type Edition = u64;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        let entries = self.entries.read().await;
        Ok(entries
            .get(address)
            .map(|entry| (entry.value.clone(), entry.version)))
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        let mut entries = self.entries.write().await;

        // Check CAS precondition
        match (edition, entries.get(address)) {
            // Creating new: require key doesn't exist
            (None, Some(_)) => {
                return Err(DialogStorageError::StorageBackend(
                    "CAS conflict: key already exists".to_string(),
                ));
            }
            // Updating existing: require versions match
            (Some(expected_version), Some(entry)) if entry.version != *expected_version => {
                return Err(DialogStorageError::StorageBackend(
                    "CAS conflict: version mismatch".to_string(),
                ));
            }
            // Updating non-existent: fail
            (Some(_), None) => {
                return Err(DialogStorageError::StorageBackend(
                    "CAS conflict: key does not exist".to_string(),
                ));
            }
            // All other cases are valid
            _ => {}
        }

        match content {
            Some(value) => {
                let new_version = self.next_version.fetch_add(1, Ordering::SeqCst);
                entries.insert(
                    address.clone(),
                    VersionedEntry {
                        value,
                        version: new_version,
                    },
                );
                Ok(Some(new_version))
            }
            None => {
                entries.remove(address);
                Ok(None)
            }
        }
    }
}
