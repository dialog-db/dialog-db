use std::{collections::HashMap, ops::DerefMut, sync::Arc};

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::{Blake3Hash, ConditionalSync};
use futures_util::Stream;
use tokio::sync::RwLock;

use crate::{DialogStorageError, StorageSource};

use super::{StorageBackend, TransactionalMemoryBackend};

/// A trivial implementation of [StorageBackend] - backed by a [HashMap] - where
/// all values are kept in memory and never persisted.
#[derive(Debug, Clone, Default)]
pub struct MemoryStorageBackend<Key, Value>
where
    Key: Eq + std::hash::Hash,
    Value: Clone,
{
    entries: Arc<RwLock<HashMap<Key, Value>>>,
}

/// A resource handle for a specific entry in [MemoryStorageBackend]

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> StorageBackend for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync + PartialEq,
{
    type Key = Key;
    type Value = Value;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let mut entries = self.entries.write().await;
        entries.insert(key, value);
        Ok(())
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let entries = self.entries.read().await;
        Ok(entries.get(key).cloned())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> TransactionalMemoryBackend for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync + PartialEq,
{
    type Address = Key;
    type Value = Value;
    type Error = DialogStorageError;
    type Edition = Value;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        let entries = self.entries.read().await;
        Ok(entries
            .get(address)
            .map(|value| (value.clone(), value.clone())))
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        let mut entries = self.entries.write().await;

        // Get current value from storage
        let current_value = entries.get(address);

        // Check CAS condition - value must match expected edition
        if current_value != edition {
            return Err(DialogStorageError::StorageBackend(
                "CAS condition failed: edition mismatch".to_string(),
            ));
        }

        // Perform the operation
        match content {
            Some(new_value) => {
                entries.insert(address.clone(), new_value.clone());
                Ok(Some(new_value))
            }
            None => {
                // Delete operation
                entries.remove(address);
                Ok(None)
            }
        }
    }
}

impl<Key, Value> StorageSource for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: Clone + ConditionalSync + PartialEq,
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
            for (key, value) in entries.iter() {
                yield (key.clone(), value.clone());
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

            for (key, value) in entries.into_iter() {
                yield (key, value);
            }
        }
    }
}

/// Transactional memory backend using BLAKE3 content hashes for CAS.
///
/// This implementation computes a BLAKE3 hash of each value to use as its edition.
/// The hash is computed on read, not stored separately. This provides content-addressable
/// semantics - identical values have identical editions.
///
/// # Edition Strategy
///
/// Uses a 32-byte BLAKE3 hash as the edition. CAS succeeds when the hash of
/// the current stored value matches the expected edition. If the stored value
/// has the same content (and thus the same hash) as expected, the update proceeds
/// even if a concurrent write occurred - this is safe because the content is identical.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> TransactionalMemoryBackend for MemoryStorageBackend<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash + ConditionalSync,
    Value: AsRef<[u8]> + Clone + ConditionalSync,
{
    type Address = Key;
    type Value = Value;
    type Error = DialogStorageError;
    type Edition = Blake3Hash;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        let entries = self.entries.read().await;
        Ok(entries.get(address).map(|value| {
            let hash = Blake3Hash::hash(value.as_ref());
            (value.clone(), hash)
        }))
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        let mut entries = self.entries.write().await;

        // Check CAS precondition by comparing content hashes
        match (edition, entries.get(address)) {
            // Creating new: require key doesn't exist
            (None, Some(_)) => {
                return Err(DialogStorageError::StorageBackend(
                    "CAS conflict: key already exists".to_string(),
                ));
            }
            // Updating existing: require content hash matches
            (Some(expected_hash), Some(existing_value)) => {
                let current_hash = Blake3Hash::hash(existing_value.as_ref());
                if &current_hash != expected_hash {
                    return Err(DialogStorageError::StorageBackend(
                        "CAS conflict: content hash mismatch".to_string(),
                    ));
                }
            }
            // Updating non-existent: fail
            (Some(_), None) => {
                return Err(DialogStorageError::StorageBackend(
                    "CAS conflict: key does not exist".to_string(),
                ));
            }
            // Creating new when key doesn't exist: valid
            (None, None) => {}
        }

        match content {
            Some(value) => {
                let new_hash = Blake3Hash::hash(value.as_ref());
                entries.insert(address.clone(), value);
                Ok(Some(new_hash))
            }
            None => {
                entries.remove(address);
                Ok(None)
            }
        }
    }
}
