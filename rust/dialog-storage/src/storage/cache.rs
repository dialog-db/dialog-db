use crate::DialogStorageError;

use super::{Resource, StorageBackend};
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

/// A cached resource that wraps a backend Resource and uses the cache for reads/writes
#[derive(Clone)]
pub struct CachedResource<Key, Value, R>
where
    Key: Eq + Clone + Hash + ConditionalSync,
    Value: Clone + ConditionalSync,
    R: Resource<Value = Value> + ConditionalSync,
{
    key: Key,
    resource: R,
    cache: Arc<Mutex<SieveCache<Key, Value>>>,
}

impl<Key, Value, R> std::fmt::Debug for CachedResource<Key, Value, R>
where
    Key: Eq + Clone + Hash + ConditionalSync + std::fmt::Debug,
    Value: Clone + ConditionalSync,
    R: Resource<Value = Value> + ConditionalSync + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedResource")
            .field("key", &self.key)
            .field("resource", &self.resource)
            .field("cache", &"<SieveCache>")
            .finish()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value, R> Resource for CachedResource<Key, Value, R>
where
    Key: Eq + Clone + Hash + ConditionalSync,
    Value: Clone + ConditionalSync,
    R: Resource<Value = Value> + ConditionalSync,
{
    type Value = Value;
    type Error = R::Error;

    fn content(&self) -> &Option<Self::Value> {
        // Delegate to underlying resource for content
        self.resource.content()
    }

    fn into_content(self) -> Option<Self::Value> {
        self.resource.into_content()
    }

    async fn reload(&mut self) -> Result<Option<Self::Value>, Self::Error> {
        // Reload from backend through the wrapped resource
        let prior = self.resource.reload().await?;

        // Update cache with new content
        if let Some(value) = self.resource.content() {
            self.cache
                .lock()
                .await
                .insert(self.key.clone(), value.clone());
        }

        Ok(prior)
    }

    async fn replace(
        &mut self,
        value: Option<Self::Value>,
    ) -> Result<Option<Self::Value>, Self::Error> {
        // Perform the replace on the underlying resource (includes CAS check)
        let prior = self.resource.replace(value.clone()).await?;

        // Update cache with new value
        match &value {
            Some(v) => {
                self.cache.lock().await.insert(self.key.clone(), v.clone());
            }
            None => {
                // Value was deleted, could remove from cache but SieveCache will handle eviction
            }
        }

        Ok(prior)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> StorageBackend for StorageCache<Backend>
where
    Backend: StorageBackend + ConditionalSync,
    Backend::Key: Eq + Clone + Hash + ConditionalSync,
    Backend::Value: Clone + ConditionalSync,
    Backend::Resource: ConditionalSync,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Resource = CachedResource<Backend::Key, Backend::Value, Backend::Resource>;
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

    async fn open(&self, key: &Self::Key) -> Result<Self::Resource, Self::Error> {
        // Open the backend resource and wrap it with caching
        let resource = self.backend.open(key).await?;

        // Populate cache with current content if available
        if let Some(value) = resource.content() {
            self.cache.lock().await.insert(key.clone(), value.clone());
        }

        Ok(CachedResource {
            key: key.clone(),
            resource,
            cache: self.cache.clone(),
        })
    }
}
