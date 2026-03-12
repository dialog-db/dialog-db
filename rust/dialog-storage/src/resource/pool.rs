//! Caching pool for address-keyed resources.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::RwLock;

/// A pool that caches resources by address.
///
/// Callers are responsible for opening resources externally and
/// inserting them via [`Pool::insert`]. The pool provides thread-safe
/// lookup and insertion through an internal `RwLock`.
///
/// Uses `RwLock` for interior mutability so callers can check and
/// insert through `&self`. All lock guards are short-lived and never
/// held across `.await` points.
///
/// If the lock is poisoned (a thread panicked while holding it),
/// methods recover by replacing the contents with an empty map since
/// the pool is only a cache.
pub struct Pool<Address, R> {
    resources: RwLock<HashMap<Address, R>>,
}

impl<Address, R> Pool<Address, R> {
    /// Create a new empty pool.
    pub fn new() -> Self {
        Self {
            resources: RwLock::new(HashMap::new()),
        }
    }

    /// Get the number of cached resources.
    pub fn len(&self) -> usize {
        self.resources
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// Check if the pool has no cached resources.
    pub fn is_empty(&self) -> bool {
        self.resources
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty()
    }
}

impl<Address, R> Default for Pool<Address, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Address: Eq + Hash, R> Pool<Address, R> {
    /// Check whether the pool already contains a resource for the given address.
    pub fn contains(&self, address: &Address) -> bool {
        self.resources
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(address)
    }

    /// Get a clone of the cached resource for the given address.
    ///
    /// Requires `R: Clone` so the value can be extracted without holding
    /// the lock. When `R` is `Arc<T>`, this is a cheap reference-count bump.
    pub fn get(&self, address: &Address) -> Option<R>
    where
        R: Clone,
    {
        self.resources
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(address)
            .cloned()
    }

    /// Insert a resource for the given address, returning the old one if present.
    pub fn insert(&self, address: Address, resource: R) -> Option<R> {
        self.resources
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(address, resource)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestAddress(String);

    #[test]
    fn it_starts_empty() {
        let pool: Pool<TestAddress, String> = Pool::new();
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn it_inserts_and_checks_contains() {
        let pool: Pool<TestAddress, String> = Pool::new();

        pool.insert(TestAddress("x".into()), "hello".into());

        assert!(pool.contains(&TestAddress("x".into())));
        assert!(!pool.contains(&TestAddress("y".into())));
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn it_replaces_existing_entry() {
        let pool: Pool<TestAddress, String> = Pool::new();

        let old = pool.insert(TestAddress("x".into()), "first".into());
        assert!(old.is_none());

        let old = pool.insert(TestAddress("x".into()), "second".into());
        assert_eq!(old, Some("first".into()));
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn it_isolates_by_address() {
        let pool: Pool<TestAddress, String> = Pool::new();

        pool.insert(TestAddress("a".into()), "alpha".into());
        pool.insert(TestAddress("b".into()), "beta".into());

        assert_eq!(pool.len(), 2);
        assert!(pool.contains(&TestAddress("a".into())));
        assert!(pool.contains(&TestAddress("b".into())));
    }
}
