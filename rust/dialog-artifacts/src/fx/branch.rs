//! Effectful branch operations.
//!
//! This module provides effectful versions of branch operations
//! that work with the algebraic effects system.

use super::effects::{Memory, Store, effectful};
use super::errors::MemoryError;
use super::local::Address as LocalAddress;
use super::remote::Address as RemoteAddress;
use dialog_common::fx::Effect;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

/// Error type for branch operations.
#[derive(Debug, Clone, Error)]
pub enum BranchError {
    /// Storage or network error.
    #[error("{0}")]
    Memory(#[from] MemoryError),
    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),
    /// Branch not found.
    #[error("Branch '{0}' not found")]
    NotFound(String),
    /// No upstream configured.
    #[error("Branch '{0}' has no upstream")]
    NoUpstream(String),
}

/// A branch identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BranchId(pub String);

impl BranchId {
    /// Create a new branch ID.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the storage key for this branch's state.
    pub fn state_key(&self) -> Vec<u8> {
        format!("local/{}", self.0).into_bytes()
    }

    /// Get the storage key for a remote branch's cached state.
    pub fn remote_cache_key(&self, site: &str) -> Vec<u8> {
        format!("remote/{}/{}", site, self.0).into_bytes()
    }
}

/// Effectful branch state operations.
///
/// These functions operate on branch state using the effect system
/// rather than requiring a specific storage backend.
pub struct BranchOps;

impl BranchOps {
    /// Resolve the current state of a branch.
    ///
    /// Returns None if the branch doesn't exist.
    #[effectful(Memory<LocalAddress>)]
    pub fn resolve<T: DeserializeOwned>(
        did: LocalAddress,
        branch: BranchId,
    ) -> Result<Option<T>, BranchError> {
        let key = branch.state_key();

        match perform!(Memory::<LocalAddress>().resolve(did, key))? {
            Some((content, _edition)) => {
                let state: T = serde_ipld_dagcbor::from_slice(&content)
                    .map_err(|e| BranchError::Serialization(e.to_string()))?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    /// Update the state of a branch atomically.
    ///
    /// Uses compare-and-swap semantics to ensure atomic updates.
    #[effectful(Memory<LocalAddress>)]
    pub fn update<T: Serialize + DeserializeOwned>(
        did: LocalAddress,
        branch: BranchId,
        f: impl FnOnce(Option<T>) -> Option<T>,
    ) -> Result<Option<T>, BranchError> {
        let key = branch.state_key();

        // Resolve current state
        let (current, edition) = match perform!(Memory::<LocalAddress>().resolve(did.clone(), key.clone()))? {
            Some((content, edition)) => {
                let state: T = serde_ipld_dagcbor::from_slice(&content)
                    .map_err(|e| BranchError::Serialization(e.to_string()))?;
                (Some(state), Some(edition))
            }
            None => (None, None),
        };

        // Apply the update function
        let new_state = f(current);

        // Serialize and write
        let new_content = match &new_state {
            Some(state) => Some(
                serde_ipld_dagcbor::to_vec(state)
                    .map_err(|e| BranchError::Serialization(e.to_string()))?,
            ),
            None => None,
        };

        perform!(Memory::<LocalAddress>().replace(did, key, edition, new_content))?;

        Ok(new_state)
    }

}

/// Effectful remote branch operations.
pub struct RemoteBranchOps;

impl RemoteBranchOps {
    /// Resolve the cached state of a remote branch.
    #[effectful(Memory<LocalAddress>)]
    pub fn resolve_cache<T: DeserializeOwned>(
        did: LocalAddress,
        site: String,
        branch: BranchId,
    ) -> Result<Option<T>, BranchError> {
        let key = branch.remote_cache_key(&site);

        match perform!(Memory::<LocalAddress>().resolve(did, key))? {
            Some((content, _edition)) => {
                let state: T = serde_ipld_dagcbor::from_slice(&content)
                    .map_err(|e| BranchError::Serialization(e.to_string()))?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    /// Fetch the current state from a remote branch.
    ///
    /// Updates the local cache with the remote state.
    #[effectful(Memory<LocalAddress> + Memory<RemoteAddress>)]
    pub fn fetch<T: Serialize + DeserializeOwned + Clone>(
        did: LocalAddress,
        remote: RemoteAddress,
        branch: BranchId,
    ) -> Result<Option<T>, BranchError> {
        let remote_key = branch.state_key();
        let cache_key = branch.remote_cache_key(&remote.name());

        // Fetch from remote
        let remote_state = match perform!(Memory::<RemoteAddress>().resolve(remote, remote_key))? {
            Some((content, _edition)) => {
                let state: T = serde_ipld_dagcbor::from_slice(&content)
                    .map_err(|e| BranchError::Serialization(e.to_string()))?;
                Some(state)
            }
            None => None,
        };

        // Update local cache
        let (_, cache_edition) = perform!(Memory::<LocalAddress>().resolve(did.clone(), cache_key.clone()))?
            .map(|(c, e)| (Some(c), Some(e)))
            .unwrap_or((None, None));

        let new_content = match &remote_state {
            Some(state) => Some(
                serde_ipld_dagcbor::to_vec(state)
                    .map_err(|e| BranchError::Serialization(e.to_string()))?,
            ),
            None => None,
        };

        perform!(Memory::<LocalAddress>().replace(did, cache_key, cache_edition, new_content))?;

        Ok(remote_state)
    }

    /// Publish state to a remote branch.
    #[effectful(Memory<RemoteAddress>)]
    pub fn publish<T: Serialize>(
        remote: RemoteAddress,
        branch: BranchId,
        state: T,
    ) -> Result<(), BranchError> {
        let key = branch.state_key();

        // Resolve current remote edition
        let edition = perform!(Memory::<RemoteAddress>().resolve(remote.clone(), key.clone()))?
            .map(|(_, edition)| edition);

        // Serialize and publish
        let content = serde_ipld_dagcbor::to_vec(&state)
            .map_err(|e| BranchError::Serialization(e.to_string()))?;

        perform!(Memory::<RemoteAddress>().replace(remote, key, edition, Some(content)))?;

        Ok(())
    }

    /// Import blocks to remote storage.
    #[effectful(Store<RemoteAddress>)]
    pub fn import_blocks(
        remote: RemoteAddress,
        blocks: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), BranchError> {
        Ok(perform!(Store::<RemoteAddress>().import(remote, blocks))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fx::memory::TestEnv;
    use dialog_common::fx::Effect;
    use dialog_storage::{AuthMethod, RestStorageConfig};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestState {
        value: String,
        version: u32,
    }

    fn test_did() -> LocalAddress {
        LocalAddress::did("did:test:branch")
    }

    fn test_remote() -> RemoteAddress {
        RemoteAddress::rest(RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: None,
            headers: vec![],
            timeout_seconds: None,
        })
    }

    #[tokio::test]
    async fn it_updates_branch_state() {
        let mut env = TestEnv::default();
        let branch = BranchId::new("main");

        // Initial update creates the branch
        let result = BranchOps::update(test_did(), branch.clone(), |_: Option<TestState>| {
            Some(TestState {
                value: "hello".to_string(),
                version: 1,
            })
        })
        .perform(&mut env)
        .await
        .unwrap();

        assert_eq!(
            result,
            Some(TestState {
                value: "hello".to_string(),
                version: 1,
            })
        );

        // Resolve returns the state
        let resolved: Option<TestState> = BranchOps::resolve(test_did(), branch.clone())
            .perform(&mut env)
            .await
            .unwrap();

        assert_eq!(
            resolved,
            Some(TestState {
                value: "hello".to_string(),
                version: 1,
            })
        );

        // Update modifies existing state
        let result = BranchOps::update(test_did(), branch.clone(), |state: Option<TestState>| {
            state.map(|mut s| {
                s.version += 1;
                s
            })
        })
        .perform(&mut env)
        .await
        .unwrap();

        assert_eq!(
            result,
            Some(TestState {
                value: "hello".to_string(),
                version: 2,
            })
        );
    }

    #[tokio::test]
    async fn it_fetches_remote_state() {
        use crate::fx::effects::Memory;

        let mut env = TestEnv::default();
        let branch = BranchId::new("main");
        let remote = test_remote();

        // Put state on remote
        let state = TestState {
            value: "remote".to_string(),
            version: 5,
        };
        let content = serde_ipld_dagcbor::to_vec(&state).unwrap();
        Memory::<RemoteAddress>()
            .replace(remote.clone(), branch.state_key(), None, Some(content))
            .perform(&mut env)
            .await
            .unwrap();

        // Fetch from remote
        let fetched: Option<TestState> =
            RemoteBranchOps::fetch(test_did(), remote.clone(), branch.clone())
                .perform(&mut env)
                .await
                .unwrap();

        assert_eq!(fetched, Some(state.clone()));

        // Check cache was updated
        let cached: Option<TestState> =
            RemoteBranchOps::resolve_cache(test_did(), remote.name(), branch)
                .perform(&mut env)
                .await
                .unwrap();

        assert_eq!(cached, Some(state));
    }
}
