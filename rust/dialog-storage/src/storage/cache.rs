use crate::DialogStorageError;

use super::StorageBackend;
use async_trait::async_trait;
use dialog_common::ConditionalSync;
use tokio::sync::Mutex;

use sieve_cache::SieveCache;
use std::{hash::Hash, sync::Arc};

/// A [StorageCache] acts as a transparent proxy to an inner
/// [StorageBackend] implementation. Writes to the cache are passed
/// through to the inner storage. Reads are cached in a [SieveCache],
/// and may be retrieved from there on future reads.
///
/// TODO: Should we also proactively cache writes?
#[derive(Clone)]
pub struct StorageCache<Backend>
where
    Backend: StorageBackend,
    Backend::Key: Eq + Clone + Hash,
    Backend::Value: Clone,
{
    backend: Backend,
    cache: Arc<Mutex<SieveCache<Backend::Key, Backend::Value>>>,
}

impl<Backend> StorageCache<Backend>
where
    Backend: StorageBackend,
    Backend::Key: Eq + Clone + Hash,
    Backend::Value: Clone,
{
    /// Wrap the provided [StorageBackend] so that it is fronted by a cache with
    /// capacity equal to `cache_size`
    pub fn new(backend: Backend, cache_size: usize) -> Result<Self, DialogStorageError> {
        Ok(Self {
            backend,
            cache: Arc::new(Mutex::new(SieveCache::new(cache_size).map_err(
                |error| {
                    DialogStorageError::StorageBackend(format!(
                        "Could not initialize cache: {error}"
                    ))
                },
            )?)),
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> StorageBackend for StorageCache<Backend>
where
    Backend: StorageBackend + ConditionalSync,
    Backend::Key: Eq + Clone + Hash + ConditionalSync,
    Backend::Value: Clone + ConditionalSync,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Error = Backend::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.cache.lock().await.insert(key.clone(), value.clone());
        self.backend.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let mut cache = self.cache.lock().await;
        if let Some(value) = cache.get(key) {
            return Ok(Some(value.clone()));
        }
        if let Some(value) = self.backend.get(key).await? {
            cache.insert(key.clone(), value.clone());
            return Ok(Some(value));
        }

        Ok(None)
    }
}
