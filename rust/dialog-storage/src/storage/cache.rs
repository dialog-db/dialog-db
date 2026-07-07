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
    Backend::Key: Eq + Clone + Hash,
    Backend::Value: Clone,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Error = Backend::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.cache.lock().await.insert(key.clone(), value.clone());
        self.backend.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        // Check the cache under a BRIEF lock, released before any backend I/O.
        // Holding the lock across `backend.get(...).await` (below) serializes
        // every block read in the process behind that single round-trip: the
        // cache is one shared mutex, so on a single-threaded executor a miss's
        // network fetch stalls all other reads — every other branch's queries
        // and syncs included — for its full duration.
        {
            let mut cache = self.cache.lock().await;
            if let Some(value) = cache.get(key) {
                return Ok(Some(value.clone()));
            }
        }

        // Fetch from the backend WITHOUT the cache lock held. Two concurrent
        // misses for the same key may both fetch; that's harmless (blocks are
        // content-addressed, so the value is identical) and far cheaper than
        // serializing every reader behind one in-flight fetch. Resolve the
        // result fully before re-acquiring the lock so the (non-`Send`) error
        // type never straddles the `lock().await`.
        let fetched = self.backend.get(key).await?;
        if let Some(value) = fetched {
            self.cache.lock().await.insert(key.clone(), value.clone());
            return Ok(Some(value));
        }

        Ok(None)
    }
}
