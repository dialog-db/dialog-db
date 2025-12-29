//! In-memory site implementation for testing.
//!
//! This module provides a memory-backed site that's useful for testing
//! without requiring real storage infrastructure.

use super::{Capability, Local, Remote, TheSite};
use dialog_storage::MemoryStorageBackend;
use std::convert::Infallible;

/// In-memory site backed by `MemoryStorageBackend`.
pub type MemorySite = TheSite<MemoryStorageBackend<Vec<u8>, Vec<u8>>>;

// =============================================================================
// Capability implementations for MemorySite
// =============================================================================

impl Capability<Local> for MemorySite {
    type Error = Infallible;

    fn acquire(_address: &Local) -> Result<Self, Self::Error> {
        Ok(MemorySite::default())
    }
}

impl Capability<Remote> for MemorySite {
    type Error = Infallible;

    fn acquire(_address: &Remote) -> Result<Self, Self::Error> {
        // For testing, remote addresses produce memory sites
        Ok(MemorySite::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::site::{CapabilityProvider, Provider, Site, TestEnv};
    use dialog_storage::StorageBackend;

    #[tokio::test]
    async fn test_local_site() {
        let mut env: TestEnv = Default::default();

        let site = env.acquire(&Local::repository("alice")).unwrap();

        // Write and read
        let mut store = site.store();
        store.set(b"key".to_vec(), b"value".to_vec()).await.unwrap();

        let value = store.get(&b"key".to_vec()).await.unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_remote_site() {
        use crate::site::rest::RestAddress;
        use dialog_storage::{AuthMethod, RestStorageConfig};

        let mut env: TestEnv = Default::default();

        let config = RestStorageConfig {
            endpoint: "http://example.com".into(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: None,
            headers: vec![],
            timeout_seconds: None,
        };
        let site = env.acquire(&Remote::rest(RestAddress::new(config))).unwrap();

        let mut store = site.store();
        store.set(b"key".to_vec(), b"value".to_vec()).await.unwrap();

        let value = store.get(&b"key".to_vec()).await.unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_environment_caches_sites() {
        let mut env: TestEnv = Default::default();

        let address = Local::repository("alice");

        // First access
        let site1 = env.acquire(&address).unwrap();
        let mut store1 = site1.store();
        store1
            .set(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();

        // Second access returns cached site with same data
        let site2 = env.acquire(&address).unwrap();
        let store2 = site2.store();
        let value = store2.get(&b"key".to_vec()).await.unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_different_addresses_are_isolated() {
        let mut env: TestEnv = Default::default();

        let alice = env.acquire(&Local::repository("alice")).unwrap();
        let bob = env.acquire(&Local::repository("bob")).unwrap();

        // Write to alice
        let mut alice_store = alice.store();
        alice_store
            .set(b"key".to_vec(), b"alice-value".to_vec())
            .await
            .unwrap();

        // Bob doesn't see it
        let bob_store = bob.store();
        let value = bob_store.get(&b"key".to_vec()).await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_shared_environment_shares_sites() {
        use crate::site::rest::RestAddress;
        use dialog_storage::{AuthMethod, RestStorageConfig};

        let mut env: TestEnv = Default::default();

        // First replica opens remote
        let config = RestStorageConfig {
            endpoint: "http://origin.example.com".into(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: None,
            headers: vec![],
            timeout_seconds: None,
        };
        let remote_addr = Remote::rest(RestAddress::new(config));
        let site1 = env.acquire(&remote_addr).unwrap();
        let mut store1 = site1.store();
        store1
            .set(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();

        // Clone environment for second replica
        let mut env2 = env.clone();

        // Second replica sees the same data (same cached site)
        let site2 = env2.acquire(&remote_addr).unwrap();
        let store2 = site2.store();
        let value = store2.get(&b"key".to_vec()).await.unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_provider_directly() {
        let mut provider: CapabilityProvider<Local, MemorySite> = CapabilityProvider::new();

        let site = provider.acquire(&Local::repository("alice")).unwrap();
        let mut store = site.store();
        store
            .set(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();

        // Same address returns cached site
        let site2 = provider.acquire(&Local::repository("alice")).unwrap();
        let store2 = site2.store();
        let value = store2.get(&b"key".to_vec()).await.unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    }
}
