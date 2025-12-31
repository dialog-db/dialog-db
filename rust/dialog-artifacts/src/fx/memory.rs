//! In-memory environment for testing.
//!
//! Provides a fully in-memory implementation of the fx environment
//! using `MemoryStorageBackend` for both storage and memory backends.

use super::connection::Connection;
use super::environment::Environment;
use super::local::Address as LocalAddress;
use super::remote::Address as RemoteAddress;
use super::site::Site;
use dialog_storage::MemoryStorageBackend;
use std::collections::HashMap;
use std::convert::Infallible;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Memory storage backend type alias.
pub type MemoryBackend = MemoryStorageBackend<Vec<u8>, Vec<u8>>;

/// A connector that creates/retrieves memory backends from a shared pool.
///
/// This connector maintains a cache of backends keyed by address,
/// ensuring that repeated opens to the same address return backends
/// that share the same underlying storage.
#[derive(Clone)]
pub struct MemoryConnector<A>
where
    A: Hash + Eq + Clone,
{
    backends: Arc<RwLock<HashMap<A, MemoryBackend>>>,
}

impl<A> Default for MemoryConnector<A>
where
    A: Hash + Eq + Clone,
{
    fn default() -> Self {
        Self {
            backends: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<A> MemoryConnector<A>
where
    A: Hash + Eq + Clone,
{
    /// Create a new memory connector with an empty backend pool.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<A> Connection<MemoryBackend> for MemoryConnector<A>
where
    A: Hash + Eq + Clone + Send + Sync,
{
    type Address = A;
    type Error = Infallible;

    async fn open(&self, address: &Self::Address) -> Result<MemoryBackend, Self::Error> {
        let mut backends = self.backends.write().await;
        let backend = backends
            .entry(address.clone())
            .or_insert_with(MemoryBackend::default)
            .clone();
        Ok(backend)
    }
}

/// Memory-backed site for local addresses.
pub type LocalMemorySite = Site<
    MemoryBackend,
    MemoryBackend,
    MemoryConnector<LocalAddress>,
    MemoryConnector<LocalAddress>,
    LocalAddress,
>;

/// Memory-backed site for remote addresses.
pub type RemoteMemorySite = Site<
    MemoryBackend,
    MemoryBackend,
    MemoryConnector<RemoteAddress>,
    MemoryConnector<RemoteAddress>,
    RemoteAddress,
>;

/// Test environment using memory backends for everything.
///
/// This environment is useful for testing without real storage infrastructure.
/// Both local and remote sites are backed by in-memory storage.
///
/// # Example
///
/// ```ignore
/// use dialog_artifacts::fx::memory::TestEnv;
/// use dialog_artifacts::fx::{LocalStore, local};
/// use dialog_common::fx::Effect;
///
/// let mut env = TestEnv::default();
/// let did = local::Address::did("did:test:alice");
///
/// // Use the environment as an effect provider
/// LocalStore.set(did, b"key".to_vec(), b"value".to_vec())
///     .perform(&mut env)
///     .await
///     .unwrap();
/// ```
pub type TestEnv = Environment<LocalMemorySite, RemoteMemorySite>;

impl Default for TestEnv {
    fn default() -> Self {
        let local_store_connector = MemoryConnector::new();
        let local_memory_connector = MemoryConnector::new();
        let remote_store_connector = MemoryConnector::new();
        let remote_memory_connector = MemoryConnector::new();

        let local = LocalMemorySite::new(local_store_connector, local_memory_connector);
        let remote = RemoteMemorySite::new(remote_store_connector, remote_memory_connector);

        Environment::new(local, remote)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fx::{
        local, remote, Env, LocalMemory, LocalStore, RemoteMemory, RemoteStore,
    };
    use dialog_common::fx::Effect;
    use dialog_storage::{AuthMethod, RestStorageConfig};

    fn test_did() -> local::Address {
        local::Address::did("did:test:alice")
    }

    fn test_remote() -> remote::Address {
        remote::Address::rest(RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: None,
            headers: vec![],
            timeout_seconds: None,
        })
    }

    #[tokio::test]
    async fn test_local_store_operations() {
        let mut env = TestEnv::default();
        let did = test_did();

        // Set a value
        LocalStore
            .set(did.clone(), b"key".to_vec(), b"value".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        // Get it back
        let result = LocalStore
            .get(did, b"key".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_local_memory_operations() {
        let mut env = TestEnv::default();
        let did = test_did();

        // Create a new entry
        let edition = LocalMemory
            .replace(did.clone(), b"addr".to_vec(), None, Some(b"value1".to_vec()))
            .perform(&mut env)
            .await
            .unwrap();

        assert!(edition.is_some());

        // Resolve it
        let resolved = LocalMemory
            .resolve(did, b"addr".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        assert!(resolved.is_some());
        let (value, _) = resolved.unwrap();
        assert_eq!(value, b"value1".to_vec());
    }

    #[tokio::test]
    async fn test_remote_store_operations() {
        let mut env = TestEnv::default();
        let site = test_remote();

        // Import some blocks
        RemoteStore
            .import(
                site.clone(),
                vec![(b"key".to_vec(), b"remote_value".to_vec())],
            )
            .perform(&mut env)
            .await
            .unwrap();

        // Get it back
        let result = RemoteStore
            .get(site, b"key".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        assert_eq!(result, Some(b"remote_value".to_vec()));
    }

    #[tokio::test]
    async fn test_remote_memory_operations() {
        let mut env = TestEnv::default();
        let site = test_remote();

        // Create a new entry
        let edition = RemoteMemory
            .replace(site.clone(), b"addr".to_vec(), None, Some(b"value1".to_vec()))
            .perform(&mut env)
            .await
            .unwrap();

        assert!(edition.is_some());

        // Resolve it
        let resolved = RemoteMemory
            .resolve(site, b"addr".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        assert!(resolved.is_some());
        let (value, _) = resolved.unwrap();
        assert_eq!(value, b"value1".to_vec());
    }

    #[tokio::test]
    async fn test_local_and_remote_are_isolated() {
        let mut env = TestEnv::default();
        let did = test_did();
        let site = test_remote();

        // Set local value
        LocalStore
            .set(did.clone(), b"key".to_vec(), b"local".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        // Import remote value with same key
        RemoteStore
            .import(site.clone(), vec![(b"key".to_vec(), b"remote".to_vec())])
            .perform(&mut env)
            .await
            .unwrap();

        // Both should be independent
        let local_val = LocalStore
            .get(did, b"key".to_vec())
            .perform(&mut env)
            .await
            .unwrap();
        let remote_val = RemoteStore
            .get(site, b"key".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        assert_eq!(local_val, Some(b"local".to_vec()));
        assert_eq!(remote_val, Some(b"remote".to_vec()));
    }

    #[tokio::test]
    async fn test_different_dids_are_isolated() {
        let mut env = TestEnv::default();
        let alice = local::Address::did("did:test:alice");
        let bob = local::Address::did("did:test:bob");

        // Set value for alice
        LocalStore
            .set(alice.clone(), b"key".to_vec(), b"alice-value".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        // Bob doesn't see it
        let bob_val = LocalStore
            .get(bob, b"key".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        assert_eq!(bob_val, None);
    }

    #[tokio::test]
    async fn test_env_can_be_cloned_and_shares_state() {
        let mut env1 = TestEnv::default();
        let did = test_did();

        // Set value in env1
        LocalStore
            .set(did.clone(), b"key".to_vec(), b"shared".to_vec())
            .perform(&mut env1)
            .await
            .unwrap();

        // Clone and read from env2
        let mut env2 = env1.clone();
        let result = LocalStore
            .get(did, b"key".to_vec())
            .perform(&mut env2)
            .await
            .unwrap();

        assert_eq!(result, Some(b"shared".to_vec()));
    }

    #[tokio::test]
    async fn test_implements_env_trait() {
        fn assert_env<T: Env>() {}
        assert_env::<TestEnv>();
    }
}
