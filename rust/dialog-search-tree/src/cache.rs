mod fetches;
use fetches::{Claim, Fetches};

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
///
/// Misses are single-flighted: concurrent misses for one key perform a single
/// fetch and share its outcome. Everything holding a clone of the same cache
/// shares that arbitration, so readers that arrive at the same cold value from
/// different queries pay for it once between them.
#[derive(Clone)]
pub struct Cache<K, V>
where
    K: Eq + Hash + Clone + ConditionalSync,
    V: Clone + ConditionalSync,
{
    #[cfg(not(target_arch = "wasm32"))]
    cache: SieveCache<K, V>,

    // NOTE: On "native" we use `SharedSieveCache` which internally wraps
    // the cached values in an `Arc`. On web we use `SieveCache`, which is
    // `Clone` but would deep-clone the cache without the wrapping `Rc`.
    #[cfg(target_arch = "wasm32")]
    cache: Rc<RefCell<SieveCache<K, V>>>,

    fetches: Fetches<K, V>,
}

impl<K, V> std::fmt::Debug for Cache<K, V>
where
    K: Eq + Hash + Clone + ConditionalSync,
    V: Clone + ConditionalSync,
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
    K: Eq + Hash + Clone + ConditionalSync,
    V: Clone + ConditionalSync,
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

            fetches: Fetches::new(),
        }
    }

    /// Retrieves a value from the cache, or fetches it using the provided
    /// function.
    ///
    /// Only one fetch per key is ever in flight through a given cache: a
    /// caller that misses while another caller is already fetching the same
    /// key waits for that fetch instead of issuing its own. A fetch that fails
    /// or is dropped before it publishes leaves nothing behind, so the callers
    /// waiting on it start over and fetch for themselves.
    pub async fn get_or_fetch<F, E>(&self, key: &K, fetcher: F) -> Result<Option<V>, E>
    where
        F: AsyncFnOnce(&K) -> Result<Option<V>, E>,
    {
        let fetch = loop {
            if let Some(value) = self.get(key) {
                return Ok(Some(value));
            }

            match self.fetches.claim(key) {
                Claim::Ours(fetch) => break fetch,
                Claim::InFlight(mut outcome) => match outcome.recv().await {
                    Ok(value) => return Ok(value),
                    // The fetch we were waiting on failed or was dropped. Its
                    // claim is already withdrawn, so start over: either the
                    // value landed in the cache anyway, or we fetch it.
                    Err(_) => continue,
                },
            }
        };

        // A failure returns here, dropping the claim and releasing the key.
        let value = fetcher(key).await?;

        if let Some(value) = &value {
            self.insert(key.clone(), value.clone());
        }
        fetch.publish(&value);

        Ok(value)
    }

    /// Inserts a key-value pair into the cache.
    pub fn insert(&self, key: K, value: V) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        let cache = &self.cache;
        #[cfg(target_arch = "wasm32")]
        let mut cache = self.cache.borrow_mut();

        cache.insert(key, value)
    }

    /// Retrieves a value from the cache, if it is cached.
    fn get(&self, key: &K) -> Option<V> {
        #[cfg(not(target_arch = "wasm32"))]
        let value = self.cache.get(key);
        #[cfg(target_arch = "wasm32")]
        let value = self.cache.borrow_mut().get(key).cloned();

        value
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::Result;

    use super::Cache;
    use crate::helpers::yield_once;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[dialog_common::test]
    async fn it_clears_the_inflight_entry_when_a_fetch_fails() -> Result<()> {
        let cache = Cache::<u8, u8>::new();
        let attempts = Arc::new(AtomicUsize::new(0));

        let failed = cache
            .get_or_fetch(&1, async |_| {
                attempts.fetch_add(1, Ordering::SeqCst);
                yield_once().await;
                Err::<Option<u8>, &str>("no")
            })
            .await;

        assert!(failed.is_err());
        assert_eq!(cache.fetches.len(), 0);

        let retried = cache
            .get_or_fetch(&1, async |_| {
                attempts.fetch_add(1, Ordering::SeqCst);
                yield_once().await;
                Ok::<Option<u8>, &str>(Some(7))
            })
            .await;

        assert_eq!(retried, Ok(Some(7)));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(cache.fetches.len(), 0);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_lets_waiters_retry_a_failed_fetch() -> Result<()> {
        let cache = Cache::<u8, u8>::new();
        let attempts = Arc::new(AtomicUsize::new(0));

        let fetch = async |_: &u8| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            yield_once().await;
            if attempt == 0 { Err("no") } else { Ok(Some(7)) }
        };

        let (first, second) = futures_util::future::join(
            cache.get_or_fetch(&1, fetch),
            cache.get_or_fetch(&1, fetch),
        )
        .await;

        // Whichever of the two claimed the fetch failed; the other found the
        // claim withdrawn and fetched successfully for itself.
        assert!(first.is_err() || second.is_err());
        assert_eq!(first.or(second), Ok(Some(7)));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(cache.fetches.len(), 0);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_opens_an_outcome_channel_only_when_a_waiter_arrives() -> Result<()> {
        let cache = Cache::<u8, u8>::new();

        let lone = cache.get_or_fetch(&1, async |_| {
            assert!(
                !cache.fetches.awaited(&1),
                "a fetch nobody waits on should hold a bare claim"
            );
            yield_once().await;
            Ok::<Option<u8>, &str>(Some(7))
        });

        assert_eq!(lone.await, Ok(Some(7)));

        let contended = cache.get_or_fetch(&2, async |_| {
            yield_once().await;
            assert!(
                cache.fetches.awaited(&2),
                "a waiter's arrival should open the outcome channel"
            );
            Ok::<Option<u8>, &str>(Some(9))
        });

        let (first, second) = futures_util::future::join(
            contended,
            cache.get_or_fetch(&2, async |_| -> Result<Option<u8>, &str> {
                unreachable!("the waiter must share the claimed fetch, not issue its own")
            }),
        )
        .await;

        assert_eq!(first, Ok(Some(9)));
        assert_eq!(second, Ok(Some(9)));
        assert_eq!(cache.fetches.len(), 0);

        Ok(())
    }
}
