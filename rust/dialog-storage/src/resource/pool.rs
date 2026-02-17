//! Caching pool for address-keyed resources.

use std::collections::HashMap;
use std::hash::Hash;

use super::Resource;

/// A pool that caches resources by address.
///
/// On first access to an address, the resource is opened via
/// [`Resource::open`] and cached. Subsequent accesses return the
/// cached instance.
pub struct Pool<Address, R> {
    resources: HashMap<Address, R>,
}

impl<Address, R> Pool<Address, R> {
    /// Create a new empty pool.
    pub fn new() -> Self {
        Self {
            resources: HashMap::new(),
        }
    }

    /// Get the number of cached resources.
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    /// Check if the pool has no cached resources.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

impl<Address, R> Default for Pool<Address, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Address, R> Pool<Address, R>
where
    Address: Eq + Hash + Clone,
    R: Resource<Address>,
{
    /// Get a mutable reference to a resource for the given address,
    /// opening it if not already cached.
    pub async fn open(&mut self, address: &Address) -> Result<&mut R, R::Error> {
        if !self.resources.contains_key(address) {
            let resource = R::open(address).await?;
            self.resources.insert(address.clone(), resource);
        }
        Ok(self.resources.get_mut(address).unwrap())
    }
}

impl<Address: Eq + Hash, R> Pool<Address, R> {
    /// Get a mutable reference to a cached resource, if it exists.
    pub fn get_mut(&mut self, address: &Address) -> Option<&mut R> {
        self.resources.get_mut(address)
    }

    /// Insert a resource for the given address, returning the old one if present.
    pub fn insert(&mut self, address: Address, resource: R) -> Option<R> {
        self.resources.insert(address, resource)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::Resource;
    use std::collections::HashMap;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestAddress(String);

    struct MockConnection {
        data: HashMap<String, Vec<u8>>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Resource<TestAddress> for MockConnection {
        type Error = std::io::Error;

        async fn open(_address: &TestAddress) -> Result<Self, Self::Error> {
            Ok(MockConnection {
                data: HashMap::new(),
            })
        }
    }

    #[dialog_common::test]
    async fn it_opens_and_caches_resources() {
        let mut pool: Pool<TestAddress, MockConnection> = Pool::new();
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);

        let conn = pool.open(&TestAddress("a".into())).await.unwrap();
        conn.data.insert("key".into(), b"value".to_vec());

        assert!(!pool.is_empty());
        assert_eq!(pool.len(), 1);

        // Second open returns the cached connection with data intact
        let conn = pool.open(&TestAddress("a".into())).await.unwrap();
        assert_eq!(conn.data.get("key"), Some(&b"value".to_vec()));
        assert_eq!(pool.len(), 1);
    }

    #[dialog_common::test]
    async fn it_isolates_resources_by_address() {
        let mut pool: Pool<TestAddress, MockConnection> = Pool::new();

        pool.open(&TestAddress("a".into())).await.unwrap();
        pool.open(&TestAddress("b".into())).await.unwrap();

        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn it_inserts_and_retrieves_manually() {
        let mut pool: Pool<TestAddress, MockConnection> = Pool::new();

        pool.insert(
            TestAddress("x".into()),
            MockConnection {
                data: HashMap::new(),
            },
        );

        assert!(pool.get_mut(&TestAddress("x".into())).is_some());
        assert!(pool.get_mut(&TestAddress("y".into())).is_none());
    }
}
