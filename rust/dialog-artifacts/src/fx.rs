//! Algebraic effects system.
//!
//! This module re-exports the core effect types from `dialog_common::fx`
//! and provides the replica effects for local/remote storage.
//!
//! # Design
//!
//! The effects are split into local and remote concerns:
//!
//! - **Local effects** (`LocalStore`, `LocalMemory`) - For local storage operations.
//!   These never touch the network.
//!
//! - **Remote effects** (`RemoteStore`, `RemoteMemory`) - For network operations.
//!   These require a remote address with connection information.
//!
//! The "archive" pattern (try local, fallback remote, cache locally) is implemented
//! as explicit effectful code rather than hidden in providers.
//!
//! # Architecture
//!
//! - `Connection<Resource>` - Trait for opening connections to resources
//! - `Site<S, M, SC, MC, A>` - Pool of storage/memory backends keyed by address
//! - `Environment<L, R>` - Composes local and remote sites
//! - `local::Address` - Address type for local storage (DID, path)
//! - `remote::Address` - Address type for remote storage (REST config)

mod archive;
mod archive_store;
mod branch;
mod connection;
pub mod connectors;
mod effects;
mod environment;
mod errors;
pub mod local;
pub mod memory;
pub mod remote;
pub mod replica;
mod site;
pub mod transactional_memory;

// Re-export everything for convenience
pub use archive::*;
pub use archive_store::*;
pub use branch::*;
pub use connection::*;
pub use connectors::*;
pub use effects::*;
pub use environment::*;
pub use errors::*;
pub use site::*;

#[cfg(test)]
mod tests {
    use super::{
        effectful, local, remote, Env, Memory, Store, MemoryError,
    };
    use dialog_common::fx::Effect;
    use dialog_storage::{AuthMethod, RestStorageConfig};
    use std::collections::HashMap;

    struct MemoryStore {
        data: HashMap<Vec<u8>, Vec<u8>>,
    }

    // In the new design, just implementing the trait is enough - the `#[effect]`
    // macro generates blanket `Effect` implementations automatically.
    impl Store<local::Address> for MemoryStore {
        async fn get(&self, _did: local::Address, key: Vec<u8>) -> Result<Option<Vec<u8>>, MemoryError> {
            Ok(self.data.get(&key).cloned())
        }
        async fn set(&mut self, _did: local::Address, key: Vec<u8>, value: Vec<u8>) -> Result<(), MemoryError> {
            self.data.insert(key, value);
            Ok(())
        }
        async fn import(&mut self, _did: local::Address, blocks: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), MemoryError> {
            for (key, value) in blocks {
                self.data.insert(key, value);
            }
            Ok(())
        }
    }

    fn test_did() -> local::Address {
        local::Address::did("did:test:123")
    }

    #[tokio::test]
    async fn it_performs_local_store_effects() {
        let mut store = MemoryStore {
            data: HashMap::new(),
        };

        Store::<local::Address>()
            .set(test_did(), b"key".to_vec(), b"value".to_vec())
            .perform(&mut store)
            .await
            .unwrap();

        let result = Store::<local::Address>()
            .get(test_did(), b"key".to_vec())
            .perform(&mut store)
            .await
            .unwrap();

        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn it_returns_none_for_missing_key() {
        let mut store = MemoryStore {
            data: HashMap::new(),
        };

        let result = Store::<local::Address>()
            .get(test_did(), b"missing".to_vec())
            .perform(&mut store)
            .await
            .unwrap();

        assert_eq!(result, None);
    }

    struct MemoryState {
        state: HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)>,
        next_edition: u64,
    }

    impl Memory<local::Address> for MemoryState {
        async fn resolve(
            &self,
            _did: local::Address,
            address: Vec<u8>,
        ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MemoryError> {
            Ok(self.state.get(&address).cloned())
        }

        async fn replace(
            &mut self,
            _did: local::Address,
            address: Vec<u8>,
            edition: Option<Vec<u8>>,
            content: Option<Vec<u8>>,
        ) -> Result<Option<Vec<u8>>, MemoryError> {
            let current = self.state.get(&address);

            match (current, edition.as_ref()) {
                (None, None) => {}
                (Some((_, current_edition)), Some(expected)) if current_edition == expected => {}
                _ => {
                    return Err(MemoryError::Conflict("Edition mismatch".to_string()))
                }
            }

            match content {
                Some(value) => {
                    let new_edition = self.next_edition.to_be_bytes().to_vec();
                    self.next_edition += 1;
                    self.state.insert(address, (value, new_edition.clone()));
                    Ok(Some(new_edition))
                }
                None => {
                    self.state.remove(&address);
                    Ok(None)
                }
            }
        }
    }

    #[tokio::test]
    async fn it_performs_local_memory_cas_operations() {
        let mut state = MemoryState {
            state: HashMap::new(),
            next_edition: 1,
        };

        let edition = Memory::<local::Address>()
            .replace(test_did(), b"addr".to_vec(), None, Some(b"value1".to_vec()))
            .perform(&mut state)
            .await
            .unwrap();

        assert!(edition.is_some());
        let edition = edition.unwrap();

        let resolved = Memory::<local::Address>()
            .resolve(test_did(), b"addr".to_vec())
            .perform(&mut state)
            .await
            .unwrap();

        assert_eq!(resolved, Some((b"value1".to_vec(), edition.clone())));

        let new_edition = Memory::<local::Address>()
            .replace(test_did(), b"addr".to_vec(), Some(edition), Some(b"value2".to_vec()))
            .perform(&mut state)
            .await
            .unwrap();

        assert!(new_edition.is_some());

        let resolved = Memory::<local::Address>()
            .resolve(test_did(), b"addr".to_vec())
            .perform(&mut state)
            .await
            .unwrap();

        assert_eq!(resolved.map(|(v, _)| v), Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn it_rejects_cas_on_edition_mismatch() {
        let mut state = MemoryState {
            state: HashMap::new(),
            next_edition: 1,
        };

        Memory::<local::Address>()
            .replace(test_did(), b"addr".to_vec(), None, Some(b"value1".to_vec()))
            .perform(&mut state)
            .await
            .unwrap();

        let result = Memory::<local::Address>()
            .replace(
                test_did(),
                b"addr".to_vec(),
                Some(b"wrong_edition".to_vec()),
                Some(b"value2".to_vec()),
            )
            .perform(&mut state)
            .await;

        assert!(matches!(result, Err(MemoryError::Conflict(_))));
    }

    struct MockRemote {
        data: HashMap<(String, Vec<u8>), Vec<u8>>,
    }

    impl Store<remote::Address> for MockRemote {
        async fn get(
            &self,
            site: remote::Address,
            key: Vec<u8>,
        ) -> Result<Option<Vec<u8>>, MemoryError> {
            Ok(self.data.get(&(site.name(), key)).cloned())
        }

        async fn set(
            &mut self,
            site: remote::Address,
            key: Vec<u8>,
            value: Vec<u8>,
        ) -> Result<(), MemoryError> {
            self.data.insert((site.name(), key), value);
            Ok(())
        }

        async fn import(
            &mut self,
            site: remote::Address,
            blocks: Vec<(Vec<u8>, Vec<u8>)>,
        ) -> Result<(), MemoryError> {
            for (key, value) in blocks {
                self.data.insert((site.name(), key), value);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn it_performs_remote_store_effects() {
        let mut remote = MockRemote {
            data: HashMap::new(),
        };

        let site = remote::Address::rest(RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: None,
            headers: vec![],
            timeout_seconds: None,
        });

        Store::<remote::Address>()
            .import(site.clone(), vec![(b"key".to_vec(), b"value".to_vec())])
            .perform(&mut remote)
            .await
            .unwrap();

        let result = Store::<remote::Address>()
            .get(site, b"key".to_vec())
            .perform(&mut remote)
            .await
            .unwrap();

        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[effectful(Store<local::Address>)]
    fn copy_block(did: local::Address, from: Vec<u8>, to: Vec<u8>) -> Result<(), MemoryError>
    where
        Capability: Store<local::Address>,
    {
        if let Some(value) = perform!(Store::<local::Address>().get(did.clone(), from))? {
            perform!(Store::<local::Address>().set(did, to, value))?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn it_supports_effectful_functions_with_local_store() {
        let mut store = MemoryStore {
            data: HashMap::new(),
        };

        Store::<local::Address>()
            .set(test_did(), b"source".to_vec(), b"data".to_vec())
            .perform(&mut store)
            .await
            .unwrap();

        copy_block(test_did(), b"source".to_vec(), b"dest".to_vec())
            .perform(&mut store)
            .await
            .unwrap();

        let result = Store::<local::Address>()
            .get(test_did(), b"dest".to_vec())
            .perform(&mut store)
            .await
            .unwrap();

        assert_eq!(result, Some(b"data".to_vec()));
    }

    struct MockEnv {
        local_store: HashMap<Vec<u8>, Vec<u8>>,
        local_memory: HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)>,
        remote_store: HashMap<(String, Vec<u8>), Vec<u8>>,
        remote_memory: HashMap<(String, Vec<u8>), (Vec<u8>, Vec<u8>)>,
        next_edition: u64,
    }

    impl Env for MockEnv {}

    impl Store<local::Address> for MockEnv {
        async fn get(&self, _did: local::Address, key: Vec<u8>) -> Result<Option<Vec<u8>>, MemoryError> {
            Ok(self.local_store.get(&key).cloned())
        }
        async fn set(&mut self, _did: local::Address, key: Vec<u8>, value: Vec<u8>) -> Result<(), MemoryError> {
            self.local_store.insert(key, value);
            Ok(())
        }
        async fn import(&mut self, _did: local::Address, blocks: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), MemoryError> {
            for (key, value) in blocks {
                self.local_store.insert(key, value);
            }
            Ok(())
        }
    }

    impl Memory<local::Address> for MockEnv {
        async fn resolve(
            &self,
            _did: local::Address,
            address: Vec<u8>,
        ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MemoryError> {
            Ok(self.local_memory.get(&address).cloned())
        }

        async fn replace(
            &mut self,
            _did: local::Address,
            address: Vec<u8>,
            edition: Option<Vec<u8>>,
            content: Option<Vec<u8>>,
        ) -> Result<Option<Vec<u8>>, MemoryError> {
            let current = self.local_memory.get(&address);
            match (current, edition.as_ref()) {
                (None, None) => {}
                (Some((_, e)), Some(expected)) if e == expected => {}
                _ => return Err(MemoryError::Conflict("Edition mismatch".to_string())),
            }
            match content {
                Some(value) => {
                    let new_edition = self.next_edition.to_be_bytes().to_vec();
                    self.next_edition += 1;
                    self.local_memory
                        .insert(address, (value, new_edition.clone()));
                    Ok(Some(new_edition))
                }
                None => {
                    self.local_memory.remove(&address);
                    Ok(None)
                }
            }
        }
    }

    impl Store<remote::Address> for MockEnv {
        async fn get(
            &self,
            site: remote::Address,
            key: Vec<u8>,
        ) -> Result<Option<Vec<u8>>, MemoryError> {
            Ok(self.remote_store.get(&(site.name(), key)).cloned())
        }

        async fn set(
            &mut self,
            site: remote::Address,
            key: Vec<u8>,
            value: Vec<u8>,
        ) -> Result<(), MemoryError> {
            self.remote_store.insert((site.name(), key), value);
            Ok(())
        }

        async fn import(
            &mut self,
            site: remote::Address,
            blocks: Vec<(Vec<u8>, Vec<u8>)>,
        ) -> Result<(), MemoryError> {
            for (key, value) in blocks {
                self.remote_store.insert((site.name(), key), value);
            }
            Ok(())
        }
    }

    impl Memory<remote::Address> for MockEnv {
        async fn resolve(
            &self,
            site: remote::Address,
            address: Vec<u8>,
        ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MemoryError> {
            Ok(self.remote_memory.get(&(site.name(), address)).cloned())
        }

        async fn replace(
            &mut self,
            site: remote::Address,
            address: Vec<u8>,
            edition: Option<Vec<u8>>,
            content: Option<Vec<u8>>,
        ) -> Result<Option<Vec<u8>>, MemoryError> {
            let key = (site.name(), address);
            let current = self.remote_memory.get(&key);
            match (current, edition.as_ref()) {
                (None, None) => {}
                (Some((_, e)), Some(expected)) if e == expected => {}
                _ => {
                    return Err(MemoryError::Network("Edition mismatch".to_string()))
                }
            }
            match content {
                Some(value) => {
                    let new_edition = self.next_edition.to_be_bytes().to_vec();
                    self.next_edition += 1;
                    self.remote_memory.insert(key, (value, new_edition.clone()));
                    Ok(Some(new_edition))
                }
                None => {
                    self.remote_memory.remove(&key);
                    Ok(None)
                }
            }
        }
    }

    #[effectful(Store<local::Address> + Store<remote::Address>)]
    fn fetch_and_cache(
        did: local::Address,
        site: remote::Address,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>
    where
        Capability: Store<local::Address> + Store<remote::Address>,
    {
        if let Some(value) = perform!(Store::<local::Address>().get(did.clone(), key.clone()))? {
            return Ok(Some(value));
        }

        if let Some(value) = perform!(Store::<remote::Address>().get(site, key.clone()))? {
            perform!(Store::<local::Address>().set(did, key, value.clone()))?;
            return Ok(Some(value));
        }

        Ok(None)
    }

    #[tokio::test]
    async fn it_composes_local_and_remote_effects() {
        let mut env = MockEnv {
            local_store: HashMap::new(),
            local_memory: HashMap::new(),
            remote_store: HashMap::new(),
            remote_memory: HashMap::new(),
            next_edition: 1,
        };

        let site = remote::Address::rest(RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: None,
            headers: vec![],
            timeout_seconds: None,
        });

        Store::<remote::Address>()
            .import(
                site.clone(),
                vec![(b"key".to_vec(), b"remote_value".to_vec())],
            )
            .perform(&mut env)
            .await
            .unwrap();

        let result = fetch_and_cache::<MockEnv>(test_did(), site.clone(), b"key".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        assert_eq!(result, Some(b"remote_value".to_vec()));

        let cached = Store::<local::Address>()
            .get(test_did(), b"key".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        assert_eq!(cached, Some(b"remote_value".to_vec()));
    }

    #[tokio::test]
    async fn it_returns_cached_value_without_remote_fetch() {
        let mut env = MockEnv {
            local_store: HashMap::new(),
            local_memory: HashMap::new(),
            remote_store: HashMap::new(),
            remote_memory: HashMap::new(),
            next_edition: 1,
        };

        let site = remote::Address::rest(RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: None,
            headers: vec![],
            timeout_seconds: None,
        });

        Store::<local::Address>()
            .set(test_did(), b"key".to_vec(), b"local_value".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        let result = fetch_and_cache::<MockEnv>(test_did(), site, b"key".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        assert_eq!(result, Some(b"local_value".to_vec()));
    }
}
