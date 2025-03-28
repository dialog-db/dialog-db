use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;
use x_common::{ConditionalSend, ConditionalSync};

use crate::XStorageError;

use super::StorageBackend;

/// A trivial implementation of [StorageBackend] - backed by a [HashMap] - where
/// all values are kept in memory and never persisted.
#[derive(Default)]
pub struct MemoryStorageBackend<K, V>
where
    K: Eq + std::hash::Hash,
    V: Clone,
{
    entries: Arc<Mutex<HashMap<K, V>>>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<K, V> StorageBackend for MemoryStorageBackend<K, V>
where
    K: Eq + std::hash::Hash + ConditionalSync,
    V: Clone + ConditionalSend,
{
    type Key = K;
    type Value = V;
    type Error = XStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let mut entries = self.entries.lock().await;
        entries.insert(key, value);
        Ok(())
    }
    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let entries = self.entries.lock().await;
        Ok(entries.get(key).map(|value| value.clone()))
    }
}
