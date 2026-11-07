use dialog_common::{ConditionalSend, ConditionalSync};

mod nodes;
pub use nodes::{NODE_CACHE_BUDGET, NodeCache, Scope};

#[cfg(not(target_arch = "wasm32"))]
use sieve_cache::ShardedSieveCache as SieveCache;
#[cfg(target_arch = "wasm32")]
use sieve_cache::SieveCache;

use std::hash::Hash;
#[cfg(target_arch = "wasm32")]
use std::{cell::RefCell, rc::Rc};

/// How many entries a [`Cache`] holds when no capacity is named.
pub const CACHE_CAPACITY: usize = 2048;

/// The fewest entries a shard of a native [`Cache`] is given.
///
/// A sharded cache bounds each shard on its own, at least one entry each,
/// so a capacity below the shard count would hold one entry per shard, more
/// than it was asked to. The shard count therefore shrinks with the
/// capacity, down to one; [`CACHE_CAPACITY`] and anything larger keep all
/// sixteen.
#[cfg(not(target_arch = "wasm32"))]
const ENTRIES_PER_SHARD: usize = 128;

/// The most shards a native [`Cache`] splits into.
#[cfg(not(target_arch = "wasm32"))]
const MAX_SHARDS: usize = 16;

/// A thread-safe cache for storing frequently accessed values.
///
/// A miss fetches for itself, always. Nothing here waits on a fetch that
/// another caller is performing, because nothing here can drive that fetch:
/// a future advances only while its owner polls it, and an owner that is
/// parked (a range scan suspended between two yields, holding its
/// read-aheads; a fetch whose proof reads the tree it is fetching) leaves
/// anyone waiting on its fetch parked with it. The cost of that rule is that
/// two callers missing one key at the same moment each read it from the
/// backend; the value lands once, and every later reader is served from the
/// cache.
#[derive(Clone)]
pub struct Cache<K, V>
where
    K: Eq + Hash + Clone + ConditionalSend + ConditionalSync,
    V: Clone + ConditionalSend + ConditionalSync,
{
    #[cfg(not(target_arch = "wasm32"))]
    cache: SieveCache<K, V>,

    // NOTE: On "native" we use `SharedSieveCache` which internally wraps
    // the cached values in an `Arc`. On web we use `SieveCache`, which is
    // `Clone` but would deep-clone the cache without the wrapping `Rc`.
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
    /// Creates a new cache with the default capacity ([`CACHE_CAPACITY`]).
    pub fn new() -> Self {
        Self::with_capacity(CACHE_CAPACITY)
    }

    /// Creates a new cache holding at most `capacity` entries.
    ///
    /// Prefer a small capacity when the cached values are large (e.g. spilled
    /// value blocks), since the cache bounds entry COUNT, not total bytes: a
    /// large count cap over large values can hold a lot of memory.
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        // SAFETY: `SieveCache` only returns an error if the capacity or the
        // shard count is 0; both are at least 1 here, so this never panics.
        #[cfg(not(target_arch = "wasm32"))]
        let cache = SieveCache::with_shards(
            capacity,
            (capacity / ENTRIES_PER_SHARD).clamp(1, MAX_SHARDS),
        )
        .unwrap();
        #[cfg(target_arch = "wasm32")]
        let cache = SieveCache::new(capacity).unwrap();

        Self {
            #[cfg(not(target_arch = "wasm32"))]
            cache,
            #[cfg(target_arch = "wasm32")]
            cache: Rc::new(RefCell::new(cache)),
        }
    }

    /// Retrieves a value from the cache, or fetches it using the provided
    /// function.
    ///
    /// The fetch is the caller's own: it runs inside this future and
    /// advances exactly when the caller polls, so the caller never depends
    /// on anyone else's progress. A fetch that fails leaves nothing behind.
    pub async fn get_or_fetch<F, E>(&self, key: &K, fetcher: F) -> Result<Option<V>, E>
    where
        F: AsyncFnOnce(&K) -> Result<Option<V>, E>,
    {
        if let Some(value) = self.get(key) {
            return Ok(Some(value));
        }

        let value = fetcher(key).await?;
        if let Some(value) = &value {
            self.insert(key.clone(), value.clone());
        }

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

    /// The cached value for `key`, if the cache holds it, with no IO.
    ///
    /// Lets a caller follow a read path as far as memory already covers it,
    /// so a read-ahead can name the node a descent would actually stop at
    /// instead of guessing.
    pub fn get_cached(&self, key: &K) -> Option<V> {
        self.get(key)
    }

    /// How many entries the cache may hold.
    pub fn capacity(&self) -> usize {
        #[cfg(not(target_arch = "wasm32"))]
        let capacity = self.cache.capacity();
        #[cfg(target_arch = "wasm32")]
        let capacity = self.cache.borrow().capacity();

        capacity
    }

    /// How many entries the cache holds now.
    pub fn len(&self) -> usize {
        #[cfg(not(target_arch = "wasm32"))]
        let len = self.cache.len();
        #[cfg(target_arch = "wasm32")]
        let len = self.cache.borrow().len();

        len
    }

    /// Whether the cache holds nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use anyhow::Result;

    use super::Cache;
    use crate::helpers::yield_once;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A reader must never wait on a fetch it cannot drive. The first fetch
    /// here is polled exactly once and then never again, the shape of a
    /// range scan's read-ahead once the scan is parked between two yields:
    /// still in flight, never dropped, and advancing only if its owner is
    /// polled. A second read of the same key must complete regardless.
    // Native only: the bound is tokio's timer, which has no wasm runtime.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_never_makes_a_reader_wait_on_a_fetch_nobody_drives() -> Result<()> {
        use std::task::{Context, Poll};

        let cache = Cache::<u32, u32>::new();
        let started = AtomicBool::new(false);

        let mut parked = Box::pin(cache.get_or_fetch(&7, async |_| {
            started.store(true, Ordering::SeqCst);
            std::future::pending::<Result<Option<u32>, ()>>().await
        }));
        let waker = std::task::Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(parked.as_mut().poll(&mut context), Poll::Pending));
        assert!(
            started.load(Ordering::SeqCst),
            "the parked fetch is in flight"
        );

        let read = cache.get_or_fetch(&7, async |_| Ok::<_, ()>(Some(70)));
        let value = tokio::time::timeout(std::time::Duration::from_secs(2), read)
            .await
            .expect("a reader must not wait on a fetch nobody drives")
            .unwrap();
        assert_eq!(value, Some(70));

        drop(parked);
        assert_eq!(cache.get(&7), Some(70));
        Ok(())
    }

    /// A cache holds no more entries than it was asked to, however small
    /// the capacity: a third entry into a cache of two evicts one.
    #[dialog_common::test]
    async fn it_holds_no_more_entries_than_its_capacity() -> Result<()> {
        let cache = Cache::<u32, u32>::with_capacity(2);
        assert_eq!(cache.capacity(), 2);
        for key in 0..3 {
            cache.insert(key, key);
        }
        assert_eq!(cache.len(), 2);
        assert_eq!(
            Cache::<u32, u32>::new().capacity(),
            super::CACHE_CAPACITY,
            "a cache with no named capacity keeps the default"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_leaves_nothing_behind_when_a_fetch_fails() -> Result<()> {
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
        assert_eq!(cache.get(&1), None);

        let retried = cache
            .get_or_fetch(&1, async |_| {
                attempts.fetch_add(1, Ordering::SeqCst);
                yield_once().await;
                Ok::<Option<u8>, &str>(Some(7))
            })
            .await;

        assert_eq!(retried, Ok(Some(7)));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        Ok(())
    }

    /// The price of never waiting: two callers missing one key at the same
    /// moment each fetch it. Both see the value, and it lands once.
    #[dialog_common::test]
    async fn it_lets_concurrent_misses_each_fetch() -> Result<()> {
        let cache = Cache::<u8, u8>::new();
        let attempts = Arc::new(AtomicUsize::new(0));

        let fetch = async |_: &u8| {
            attempts.fetch_add(1, Ordering::SeqCst);
            yield_once().await;
            Ok::<Option<u8>, &str>(Some(7))
        };

        let (first, second) = futures_util::future::join(
            cache.get_or_fetch(&1, fetch),
            cache.get_or_fetch(&1, fetch),
        )
        .await;

        assert_eq!(first, Ok(Some(7)));
        assert_eq!(second, Ok(Some(7)));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(cache.get(&1), Some(7));

        Ok(())
    }
}
