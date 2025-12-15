use dialog_common::{ConditionalSend, ConditionalSync};
use sieve_cache::ShardedSieveCache;
use std::hash::Hash;

#[derive(Clone)]
pub struct Cache<K, V>
where
    K: Eq + Hash + Clone + ConditionalSend + ConditionalSync,
    V: Clone + ConditionalSend + ConditionalSync,
{
    // TODO: This needs to be the non-sharded version on Wasm
    cache: ShardedSieveCache<K, V>,
}

impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Clone + ConditionalSend + ConditionalSync,
    V: Clone + ConditionalSend + ConditionalSync,
{
    pub fn new() -> Self {
        Self {
            cache: ShardedSieveCache::new(2048).unwrap(),
        }
    }

    pub async fn get_or_fetch<F, E>(&self, key: &K, fetcher: F) -> Result<Option<V>, E>
    where
        F: AsyncFnOnce(&K) -> Result<Option<V>, E>,
    {
        if let Some(value) = self.cache.get(key) {
            return Ok(Some(value));
        }

        Ok(if let Some(value) = fetcher(key).await? {
            self.cache.insert(key.clone(), value.clone());
            Some(value)
        } else {
            None
        })
    }

    pub fn insert(&self, key: K, value: V) -> bool {
        self.cache.insert(key, value)
    }
}

// use dashmap::DashMap;
// use std::sync::Weak;

// pub struct Cache<K, V> {
//     cache: Arc<DashMap<K, Weak<V>>>,
// }

// impl<K, V> Cache<K, V>
// where
//     K: Hash + Ord + Clone,
// {
//     pub fn new() -> Self {
//         Self {
//             cache: Default::default(),
//         }
//     }

//     pub async fn get_or_fetch<F, E>(&self, key: &K, fetcher: F) -> Result<Option<Arc<V>>, E>
//     where
//         F: AsyncFnOnce(&K) -> Result<Option<V>, E>,
//     {
//         if let Some(value) = self.cache.get(key) {
//             let weak_pointer = value.value();
//             if let Some(pointer) = weak_pointer.upgrade() {
//                 return Ok(Some(pointer));
//             }
//         }

//         Ok(if let Some(fetched) = fetcher(key).await? {
//             let value = Arc::new(fetched);
//             self.cache.insert(key.clone(), Arc::downgrade(&value));
//             Some(value)
//         } else {
//             None
//         })
//     }

//     pub fn collect_garbage(&self) {
//         self.cache.retain(|_, value| value.strong_count() > 0);
//     }
// }
