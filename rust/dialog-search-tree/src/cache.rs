use dialog_common::{ConditionalSend, ConditionalSync};

#[cfg(not(target_arch = "wasm32"))]
use sieve_cache::ShardedSieveCache as SieveCache;
#[cfg(target_arch = "wasm32")]
use sieve_cache::SieveCache;

use std::hash::Hash;
#[cfg(target_arch = "wasm32")]
use std::{cell::RefCell, rc::Rc};

const CACHE_CAPACITY: usize = 2048;

/// A thread-safe cache for storing frequently accessed values.
#[derive(Clone)]
pub struct Cache<K, V>
where
    K: Eq + Hash + Clone + ConditionalSend + ConditionalSync,
    V: Clone + ConditionalSend + ConditionalSync,
{
    #[cfg(not(target_arch = "wasm32"))]
    cache: SieveCache<K, V>,

    #[cfg(target_arch = "wasm32")]
    cache: Rc<RefCell<SieveCache<K, V>>>,
}

impl<K, V> std::fmt::Debug for Cache<K, V>
where
    K: Eq + Hash + Clone + ConditionalSend + ConditionalSync,
    V: Clone + ConditionalSend + ConditionalSync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("Cache");
        #[cfg(not(target_arch = "wasm32"))]
        let debug_struct = debug_struct.field("cache", &self.cache.len());

        #[cfg(target_arch = "wasm32")]
        let debug_struct = debug_struct.field("cache", &self.cache.borrow().len());

        debug_struct.finish()
    }
}

impl<K, V> Default for Cache<K, V>
where
    K: Eq + Hash + Clone + ConditionalSend + ConditionalSync,
    V: Clone + ConditionalSend + ConditionalSync,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Clone + ConditionalSend + ConditionalSync,
    V: Clone + ConditionalSend + ConditionalSync,
{
    /// Creates a new cache with a fixed capacity.
    pub fn new() -> Self {
        // SAFETY: `SieveCache` only returns an error if the cache capacity is 0.
        let cache = SieveCache::new(CACHE_CAPACITY).unwrap();

        Self {
            #[cfg(not(target_arch = "wasm32"))]
            cache,
            #[cfg(target_arch = "wasm32")]
            cache: Rc::new(RefCell::new(cache)),
        }
    }

    /// Retrieves a value from the cache, or fetches it using the provided
    /// function.
    pub async fn get_or_fetch<F, E>(&self, key: &K, fetcher: F) -> Result<Option<V>, E>
    where
        F: AsyncFnOnce(&K) -> Result<Option<V>, E>,
    {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(value) = self.cache.get(key) {
            return Ok(Some(value));
        }

        #[cfg(target_arch = "wasm32")]
        if let Some(value) = self.cache.borrow_mut().get(key) {
            let value = value.clone();
            return Ok(Some(value));
        }

        Ok(if let Some(value) = fetcher(key).await? {
            #[cfg(not(target_arch = "wasm32"))]
            self.cache.insert(key.clone(), value.clone());
            #[cfg(target_arch = "wasm32")]
            self.cache.borrow_mut().insert(key.clone(), value.clone());

            Some(value)
        } else {
            None
        })
    }

    /// Inserts a key-value pair into the cache.
    pub fn insert(&self, key: K, value: V) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        let cache = &self.cache;
        #[cfg(target_arch = "wasm32")]
        let mut cache = self.cache.borrow_mut();

        cache.insert(key, value)
    }
}
