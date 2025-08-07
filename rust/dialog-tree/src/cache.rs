use std::{fmt::Debug, sync::Arc};

use elsa::FrozenBTreeMap;

use crate::DialogTreeError;

#[derive(Clone)]
pub struct AppendCache<K, V>
where
    K: Ord + Clone + Debug,
{
    cache: FrozenBTreeMap<K, Arc<V>>,
}

impl<K, V> AppendCache<K, V>
where
    K: Ord + Clone + Debug,
{
    pub fn new() -> Self {
        Self {
            cache: Default::default(),
        }
    }

    pub async fn retrieve_or_append<'a, F, E>(
        &'a self,
        key: K,
        append: F,
    ) -> Result<&'a V, DialogTreeError>
    where
        F: AsyncFnOnce() -> Result<V, E>,
        DialogTreeError: From<E>,
    {
        Ok(if let Some(value) = self.cache.get(&key) {
            value
        } else {
            let owned_value = append().await?;
            self.cache.insert(key.clone(), Arc::new(owned_value));
            self.cache
                .get(&key)
                .ok_or_else(|| DialogTreeError::Cache(format!("Expected item was missing")))?
        })
    }
}

impl<K, V> std::fmt::Debug for AppendCache<K, V>
where
    K: Ord + Clone + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendCache")
            .field("size", &self.cache.len())
            .finish()
    }
}
