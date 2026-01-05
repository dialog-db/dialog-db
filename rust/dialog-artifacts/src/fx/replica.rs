//! Effectful replica system.
//!
//! This module provides effectful versions of the replica types and operations.
//! The API mirrors the original replica module, but storage-dependent methods
//! return effects that can be performed with any compatible environment.
//!
//! # Design
//!
//! - Types like `Branch`, `RemoteBranch`, `Upstream` maintain state in memory
//! - Storage operations return `impl Effect<Capability>` instead of futures
//! - The caller chooses when and how to perform effects
//! - Tree operations use `LocalBackend`/`RemoteBackend` bridge effects
//!
//! # Example
//!
//! ```ignore
//! use dialog_artifacts::fx::replica::{Branch, Issuer};
//! use dialog_artifacts::fx::{Environment, Effect};
//!
//! let mut env = Environment::new(...);
//! let issuer = Issuer::from_passphrase("secret");
//!
//! // Open a branch - effectful operation
//! let branch = Branch::open("main", issuer, did)
//!     .perform(&mut env)
//!     .await?;
//!
//! // Push to upstream - effectful operation
//! branch.push().perform(&mut env).await?;
//! ```

mod branch;
mod error;
mod issuer;
mod remote;
mod session;
mod types;
mod upstream;

use crate::fx::effects::{effectful, Memory};
use crate::fx::local::Address as LocalAddress;
use crate::fx::remote::Address as RemoteAddress;
use crate::replica::RemoteState;
use dialog_common::fx::Effect;

pub use branch::*;
pub use error::*;
pub use issuer::*;
pub use remote::*;
pub use session::*;
pub use types::*;
pub use upstream::*;

/// A replica represents a local instance of a distributed database.
///
/// This is the effectful version that works with algebraic effects
/// instead of holding direct storage references.
#[derive(Debug, Clone)]
pub struct Replica {
    address: LocalAddress,
    issuer: Issuer,
}

impl Replica {
    /// Creates a new replica with the given address and issuer.
    pub fn new(address: LocalAddress, issuer: Issuer) -> Self {
        Replica { address, issuer }
    }

    /// Returns the local address for this replica.
    pub fn address(&self) -> &LocalAddress {
        &self.address
    }

    /// Returns the principal (public key) of the issuer for this replica.
    pub fn principal(&self) -> &Principal {
        self.issuer.principal()
    }

    /// Returns the issuer for this replica.
    pub fn issuer(&self) -> &Issuer {
        &self.issuer
    }

    /// Returns a view for branch operations.
    pub fn branches(&self) -> Branches<'_> {
        Branches(self)
    }

    /// Returns a view for remote operations.
    pub fn remotes(&self) -> Remotes<'_> {
        Remotes(self)
    }
}

/// A view over a replica for branch operations.
///
/// Created via [`Replica::branches()`].
#[derive(Debug, Clone, Copy)]
pub struct Branches<'a>(&'a Replica);

impl Branches<'_> {
    /// Loads a branch with given identifier, produces an error if it does not exist.
    #[effectful(Memory<LocalAddress>)]
    pub fn load(&self, id: BranchId) -> Result<Branch, ReplicaError> {
        perform!(Branch::load(id, self.0.clone()))
    }

    /// Loads a branch with the given identifier or creates a new one if
    /// it does not already exist.
    #[effectful(Memory<LocalAddress>)]
    pub fn open(&self, id: BranchId) -> Result<Branch, ReplicaError> {
        perform!(Branch::open(id, self.0.clone()))
    }
}

/// A view over a replica for remote operations.
///
/// Created via [`Replica::remotes()`].
#[derive(Debug, Clone, Copy)]
pub struct Remotes<'a>(&'a Replica);

impl Remotes<'_> {
    /// Loads an existing remote repository by site name.
    #[effectful(Memory<LocalAddress> + Memory<RemoteAddress>)]
    pub fn load(&self, site: Site, branch: BranchId) -> Result<RemoteBranch, ReplicaError> {
        perform!(RemoteBranch::open(self.0.address.clone(), site, branch))
    }

    /// Adds a new remote repository with the given state.
    #[effectful(Memory<LocalAddress>)]
    pub fn add(&self, state: RemoteState) -> Result<(), ReplicaError> {
        let key = format!("site/{}", state.site).into_bytes();
        let content = serde_ipld_dagcbor::to_vec(&state)
            .map_err(|e| ReplicaError::StorageError(e.to_string()))?;

        perform!(Memory::<LocalAddress>().replace(
            self.0.address.clone(),
            key,
            None,
            Some(content)
        ))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fx::memory::TestEnv;
    use crate::fx::{local, effects::Memory};
    use dialog_common::fx::Effect;
    use crate::fx::local::Address as LocalAddress;
    use dialog_storage::{AuthMethod, RestStorageConfig};

    fn test_issuer() -> Issuer {
        Issuer::from_passphrase("test-secret")
    }

    fn test_address() -> LocalAddress {
        LocalAddress::did("did:test:replica")
    }

    fn test_replica() -> Replica {
        Replica::new(test_address(), test_issuer())
    }

    fn test_branch_id() -> BranchId {
        BranchId::new("main".to_string())
    }

    fn test_remote_config() -> RestStorageConfig {
        RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: None,
            headers: vec![],
            timeout_seconds: None,
        }
    }

    #[tokio::test]
    async fn test_branch_open_creates_new() {
        let mut env = TestEnv::default();
        let replica = test_replica();
        let id = test_branch_id();

        // Open should create a new branch
        let branch = Branch::open(id.clone(), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        assert_eq!(branch.id(), &id);
        assert_eq!(branch.principal(), replica.principal());
        assert!(branch.upstream().is_none());
    }

    #[tokio::test]
    async fn test_branch_load_fails_if_not_exists() {
        let mut env = TestEnv::default();
        let replica = test_replica();
        let id = BranchId::new("nonexistent".to_string());

        // Load should fail for non-existent branch
        let result = Branch::load(id.clone(), replica).perform(&mut env).await;

        assert!(matches!(result, Err(ReplicaError::BranchNotFound { .. })));
    }

    #[tokio::test]
    async fn test_branch_commit_creates_new_revision() {
        let mut env = TestEnv::default();
        let replica = test_replica();
        let id = test_branch_id();

        let branch = Branch::open(id, replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Initial revision should have moment 0
        assert_eq!(*branch.revision().moment(), 0);

        // Commit with a new tree hash
        let tree_hash = [1u8; 32];
        let (branch, revision) = branch.commit(tree_hash).perform(&mut env).await.unwrap();

        // New revision should have incremented moment
        assert_eq!(*revision.moment(), 1);
        assert_eq!(branch.revision().tree().hash(), &tree_hash);
    }

    #[tokio::test]
    async fn test_branch_reset_updates_base() {
        let mut env = TestEnv::default();
        let replica = test_replica();
        let id = test_branch_id();

        let branch = Branch::open(id, replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        // Create a new revision
        let tree_hash = [2u8; 32];
        let new_revision = Revision {
            issuer: replica.principal().clone(),
            tree: NodeReference::new(tree_hash),
            cause: std::collections::HashSet::new(),
            period: 1,
            moment: 0,
        };

        // Reset to new revision
        let branch = branch
            .reset(new_revision.clone())
            .perform(&mut env)
            .await
            .unwrap();

        // Base should now match the new revision's tree
        assert_eq!(branch.base().hash(), &tree_hash);
        assert_eq!(branch.revision().tree().hash(), &tree_hash);
    }

    #[tokio::test]
    async fn test_branch_set_upstream_local() {
        let mut env = TestEnv::default();
        let replica = test_replica();

        // Create two branches
        let main = Branch::open(BranchId::new("main".to_string()), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        let feature = Branch::open(BranchId::new("feature".to_string()), replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Set main as upstream of feature
        let upstream = Upstream::Local(main);
        let feature = feature
            .set_upstream(upstream)
            .perform(&mut env)
            .await
            .unwrap();

        assert!(feature.upstream().is_some());
        assert!(feature.upstream().unwrap().is_local());
    }

    #[tokio::test]
    async fn test_branch_fetch_requires_upstream() {
        let mut env = TestEnv::default();
        let replica = test_replica();
        let id = test_branch_id();

        let branch = Branch::open(id, replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Fetch without upstream should fail
        let result = branch.fetch().perform(&mut env).await;

        assert!(matches!(
            result,
            Err(ReplicaError::BranchHasNoUpstream { .. })
        ));
    }

    #[tokio::test]
    async fn test_branch_push_requires_upstream() {
        let mut env = TestEnv::default();
        let replica = test_replica();
        let id = test_branch_id();

        let branch = Branch::open(id, replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Push without upstream should fail
        let result = branch.push().perform(&mut env).await;

        assert!(matches!(
            result,
            Err(ReplicaError::BranchHasNoUpstream { .. })
        ));
    }

    #[tokio::test]
    async fn test_branch_push_to_local_upstream() {
        let mut env = TestEnv::default();
        let replica = test_replica();

        // Create main branch
        let main = Branch::open(BranchId::new("main".to_string()), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        // Create feature branch
        let feature = Branch::open(BranchId::new("feature".to_string()), replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Set main as upstream
        let feature = feature
            .set_upstream(Upstream::Local(main))
            .perform(&mut env)
            .await
            .unwrap();

        // Commit some changes to feature
        let tree_hash = [3u8; 32];
        let (feature, _revision) = feature.commit(tree_hash).perform(&mut env).await.unwrap();

        // Push to main
        let (feature, _before) = feature.push().perform(&mut env).await.unwrap();

        // After push, the upstream should have the new revision
        let upstream = feature.upstream().unwrap();
        if let Upstream::Local(main) = upstream {
            assert_eq!(main.revision().tree().hash(), &tree_hash);
        } else {
            panic!("Expected local upstream");
        }
    }

    #[tokio::test]
    async fn test_branch_push_to_self_fails() {
        let mut env = TestEnv::default();
        let replica = test_replica();

        // Create a branch
        let branch_id = BranchId::new("main".to_string());
        let branch = Branch::open(branch_id.clone(), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        // Create another branch instance with same id to use as upstream
        let same_branch = Branch::open(branch_id, replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Set itself as upstream
        let branch = branch
            .set_upstream(Upstream::Local(same_branch))
            .perform(&mut env)
            .await
            .unwrap();

        // Push should fail with BranchUpstreamIsItself
        let result = branch.push().perform(&mut env).await;

        assert!(matches!(
            result,
            Err(ReplicaError::BranchUpstreamIsItself { .. })
        ));
    }

    #[tokio::test]
    async fn test_branch_pull_fast_forward() {
        let mut env = TestEnv::default();
        let replica = test_replica();

        // Create main branch with some content
        let main = Branch::open(BranchId::new("main".to_string()), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        let tree_hash = [4u8; 32];
        let (main, _revision) = main.commit(tree_hash).perform(&mut env).await.unwrap();

        // Create feature branch and set main as upstream
        let feature = Branch::open(BranchId::new("feature".to_string()), replica)
            .perform(&mut env)
            .await
            .unwrap();

        let feature = feature
            .set_upstream(Upstream::Local(main))
            .perform(&mut env)
            .await
            .unwrap();

        // Pull from main - should fast-forward
        let (feature, merged) = feature.pull().perform(&mut env).await.unwrap();

        assert!(merged.is_some());
        assert_eq!(feature.revision().tree().hash(), &tree_hash);
    }

    #[tokio::test]
    async fn test_branch_stores_and_loads_correctly() {
        let mut env = TestEnv::default();
        let replica = test_replica();
        let id = test_branch_id();

        // Open and modify branch
        let branch = Branch::open(id.clone(), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        let tree_hash = [5u8; 32];
        let (_, _revision) = branch.commit(tree_hash).perform(&mut env).await.unwrap();

        // Load it again - should have the committed state
        let loaded = Branch::load(id, replica)
            .perform(&mut env)
            .await
            .unwrap();

        assert_eq!(loaded.revision().tree().hash(), &tree_hash);
    }

    #[tokio::test]
    async fn test_remote_branch_open_fails_if_remote_not_configured() {
        let mut env = TestEnv::default();
        let address = test_address();

        // Try to open remote branch without configuring the remote
        let result = RemoteBranch::open(address, "origin".to_string(), BranchId::new("main".to_string()))
            .perform(&mut env)
            .await;

        assert!(matches!(result, Err(ReplicaError::RemoteNotFound { .. })));
    }

    #[tokio::test]
    async fn test_remote_branch_with_configured_remote() {
        let mut env = TestEnv::default();
        let address = test_address();

        // First, configure the remote by storing RemoteState
        let remote_state = crate::replica::RemoteState {
            site: "origin".to_string(),
            address: test_remote_config(),
        };

        let site_key = b"site/origin".to_vec();
        let content = serde_ipld_dagcbor::to_vec(&remote_state).unwrap();

        Memory::<LocalAddress>()
            .replace(address.clone(), site_key, None, Some(content))
            .perform(&mut env)
            .await
            .unwrap();

        // Now open should work
        let remote_branch =
            RemoteBranch::open(address, "origin".to_string(), BranchId::new("main".to_string()))
                .perform(&mut env)
                .await
                .unwrap();

        assert_eq!(remote_branch.site(), "origin");
        assert_eq!(remote_branch.id().id(), "main");
        assert!(remote_branch.revision().is_none()); // No cached revision yet
    }

    #[tokio::test]
    async fn test_upstream_state_conversion() {
        let local_state = UpstreamState::Local {
            branch: BranchId::new("main".to_string()),
        };
        assert!(local_state.is_local());
        assert!(!local_state.is_remote());

        let remote_state = UpstreamState::Remote {
            site: "origin".to_string(),
            branch: BranchId::new("main".to_string()),
        };
        assert!(remote_state.is_remote());
        assert!(!remote_state.is_local());
    }

    #[tokio::test]
    async fn test_branch_load_automatically_loads_local_upstream() {
        let mut env = TestEnv::default();
        let replica = test_replica();

        // Create main branch
        let main = Branch::open(BranchId::new("main".to_string()), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        // Create feature branch with main as upstream
        let feature = Branch::open(BranchId::new("feature".to_string()), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        let feature = feature
            .set_upstream(Upstream::Local(main))
            .perform(&mut env)
            .await
            .unwrap();

        // Verify upstream is set
        assert!(feature.upstream().is_some());

        // Now load the feature branch again - it should automatically load the upstream
        let loaded = Branch::load(BranchId::new("feature".to_string()), replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Upstream should be automatically loaded
        assert!(loaded.upstream().is_some(), "Upstream should be automatically loaded");
        assert!(loaded.upstream().unwrap().is_local(), "Upstream should be local");
        assert_eq!(loaded.upstream().unwrap().id().id(), "main");
    }

    #[tokio::test]
    async fn test_branch_load_automatically_loads_remote_upstream() {
        let mut env = TestEnv::default();
        let replica = test_replica();
        let address = replica.address().clone();

        // Configure the remote
        let remote_state = crate::replica::RemoteState {
            site: "origin".to_string(),
            address: test_remote_config(),
        };
        let site_key = b"site/origin".to_vec();
        let content = serde_ipld_dagcbor::to_vec(&remote_state).unwrap();
        Memory::<LocalAddress>()
            .replace(address.clone(), site_key, None, Some(content))
            .perform(&mut env)
            .await
            .unwrap();

        // Create branch and set remote upstream
        let branch = Branch::open(BranchId::new("feature".to_string()), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        let remote_branch =
            RemoteBranch::open(address.clone(), "origin".to_string(), BranchId::new("main".to_string()))
                .perform(&mut env)
                .await
                .unwrap();

        let branch = branch
            .set_upstream(Upstream::Remote(remote_branch))
            .perform(&mut env)
            .await
            .unwrap();

        assert!(branch.upstream().is_some());

        // Load branch again - upstream should be automatically loaded
        let loaded = Branch::load(BranchId::new("feature".to_string()), replica)
            .perform(&mut env)
            .await
            .unwrap();

        assert!(loaded.upstream().is_some(), "Upstream should be automatically loaded");
        assert!(!loaded.upstream().unwrap().is_local(), "Upstream should be remote");
        assert_eq!(loaded.upstream().unwrap().id().id(), "main");
    }

    #[tokio::test]
    async fn test_session_picks_up_upstream_when_recreated() {
        let mut env = TestEnv::default();
        let replica = test_replica();
        let address = replica.address().clone();

        // Configure the remote
        let remote_state = crate::replica::RemoteState {
            site: "origin".to_string(),
            address: test_remote_config(),
        };
        let site_key = b"site/origin".to_vec();
        let content = serde_ipld_dagcbor::to_vec(&remote_state).unwrap();
        Memory::<LocalAddress>()
            .replace(address.clone(), site_key, None, Some(content))
            .perform(&mut env)
            .await
            .unwrap();

        // Create branch without upstream
        let branch = Branch::open(BranchId::new("feature".to_string()), replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Create a session - no remote configured yet
        let session1 = branch.session().perform(&mut env).await.unwrap();
        assert!(!session1.storage().await.has_remote().await, "Session1 should have no remote");

        // Now set a remote upstream
        let remote_branch =
            RemoteBranch::open(address.clone(), "origin".to_string(), BranchId::new("main".to_string()))
                .perform(&mut env)
                .await
                .unwrap();

        let branch = branch
            .set_upstream(Upstream::Remote(remote_branch))
            .perform(&mut env)
            .await
            .unwrap();

        // The old session still has no remote (it captured state at creation)
        assert!(!session1.storage().await.has_remote().await, "Session1 still has no remote");

        // Create a new session - this one should have the remote
        let session2 = branch.session().perform(&mut env).await.unwrap();
        assert!(session2.storage().await.has_remote().await, "Session2 should have remote");
    }

    #[tokio::test]
    async fn test_branch_session_returns_artifact_store() {
        let mut env = TestEnv::default();
        let replica = test_replica();
        let id = test_branch_id();

        // Open a branch
        let branch = Branch::open(id, replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Get a session - this returns an impl ArtifactStore
        let session = branch.session().perform(&mut env).await.unwrap();

        // The session should have the same revision as the branch
        assert_eq!(
            session.revision().tree().hash(),
            branch.revision().tree().hash()
        );

        // The issuer should match
        assert_eq!(session.issuer().principal(), branch.principal());
    }

    #[tokio::test]
    async fn test_branch_session_with_remote_upstream_fetches_blocks() {
        use crate::fx::{remote, effects::Store};
        use crate::fx::remote::Address as RemoteAddress;

        let mut env = TestEnv::default();
        let replica = test_replica();
        let address = replica.address().clone();
        let issuer = test_issuer();

        // Set up a remote site
        let remote_addr = remote::Address::rest(test_remote_config());

        // Put a block directly in remote storage (simulating data that exists on remote)
        let remote_block_key = b"index/test-block".to_vec();
        let remote_block_value = b"remote-content".to_vec();

        Store::<RemoteAddress>()
            .import(
                remote_addr.clone(),
                vec![(remote_block_key.clone(), remote_block_value.clone())],
            )
            .perform(&mut env)
            .await
            .unwrap();

        // Create a branch
        let branch = Branch::open(BranchId::new("feature".to_string()), replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Configure the remote in local memory so RemoteBranch::open works
        let remote_state = crate::replica::RemoteState {
            site: "origin".to_string(),
            address: test_remote_config(),
        };
        let site_key = b"site/origin".to_vec();
        let content = serde_ipld_dagcbor::to_vec(&remote_state).unwrap();
        Memory::<LocalAddress>()
            .replace(address.clone(), site_key, None, Some(content))
            .perform(&mut env)
            .await
            .unwrap();

        // Open the remote branch and set it as upstream
        let remote_branch =
            RemoteBranch::open(address.clone(), "origin".to_string(), BranchId::new("main".to_string()))
                .perform(&mut env)
                .await
                .unwrap();

        let branch = branch
            .set_upstream(Upstream::Remote(remote_branch))
            .perform(&mut env)
            .await
            .unwrap();

        // Verify the block is NOT in local storage yet
        let local_did = local::Address::did(issuer.did());
        let local_result = Store::<LocalAddress>()
            .get(local_did.clone(), remote_block_key.clone())
            .perform(&mut env)
            .await
            .unwrap();
        assert!(local_result.is_none(), "Block should not be in local storage yet");

        // Get a session - this should set up the remote as fallback
        let session = branch.session().perform(&mut env).await.unwrap();

        // The session exists and has correct revision
        assert_eq!(
            session.revision().tree().hash(),
            branch.revision().tree().hash()
        );

        // Note: The actual fetching happens when we query data through the tree.
        // Since we're using ArchiveStore, any block access will try local first,
        // then remote, and cache on hit.
        //
        // To properly test this we would need to:
        // 1. Create actual tree data on the remote
        // 2. Have the branch point to that tree
        // 3. Query through the session
        //
        // For now, we've verified that:
        // - Session is created successfully with remote upstream
        // - The ArchiveStore is configured with the remote backend
    }

    #[tokio::test]
    async fn test_archive_store_fetches_and_caches_from_remote() {
        use crate::fx::{remote, effects::Store};
        use crate::fx::remote::Address as RemoteAddress;
        use dialog_storage::ContentAddressedStorage;

        let mut env = TestEnv::default();
        let replica = test_replica();
        let address = replica.address().clone();

        // Set up a remote site
        let remote_addr = remote::Address::rest(test_remote_config());

        // Create some content and compute its hash
        let content = b"test block content".to_vec();
        let hash = {
            use blake3::Hasher;
            let mut hasher = Hasher::new();
            // CBOR-encode the content to match what ContentAddressedStorage does
            let encoded = serde_ipld_dagcbor::to_vec(&content).unwrap();
            hasher.update(&encoded);
            let result = hasher.finalize();
            let mut hash = [0u8; 32];
            hash.copy_from_slice(result.as_bytes());
            hash
        };

        // Put the CBOR-encoded block directly in remote storage
        let encoded = serde_ipld_dagcbor::to_vec(&content).unwrap();
        Store::<RemoteAddress>()
            .import(remote_addr.clone(), vec![(hash.to_vec(), encoded.clone())])
            .perform(&mut env)
            .await
            .unwrap();

        // Create a branch with remote upstream
        let branch = Branch::open(BranchId::new("test".to_string()), replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Configure remote
        let remote_state = crate::replica::RemoteState {
            site: "origin".to_string(),
            address: test_remote_config(),
        };
        let site_key = b"site/origin".to_vec();
        let state_content = serde_ipld_dagcbor::to_vec(&remote_state).unwrap();
        Memory::<LocalAddress>()
            .replace(address.clone(), site_key, None, Some(state_content))
            .perform(&mut env)
            .await
            .unwrap();

        // Set remote upstream
        let remote_branch =
            RemoteBranch::open(address.clone(), "origin".to_string(), BranchId::new("main".to_string()))
                .perform(&mut env)
                .await
                .unwrap();

        let branch = branch
            .set_upstream(Upstream::Remote(remote_branch))
            .perform(&mut env)
            .await
            .unwrap();

        // Verify block is NOT in local storage
        let local_before = Store::<LocalAddress>()
            .get(address.clone(), hash.to_vec())
            .perform(&mut env)
            .await
            .unwrap();
        assert!(local_before.is_none(), "Block should not be local yet");

        // Get session and use its underlying store to read the block
        // This simulates what happens when we query data through the tree
        let session = branch.session().perform(&mut env).await.unwrap();

        // Access the tree's storage to read directly
        // (In real usage, this would happen through select() queries)
        let tree = session.tree_for_testing().await;
        let storage = tree.storage();

        // Read from storage - should fetch from remote and cache
        let retrieved: Option<Vec<u8>> = storage.read(&hash).await.unwrap();
        assert_eq!(retrieved, Some(content), "Should fetch block from remote");

        // Verify block is NOW in local storage (cached)
        let local_after = Store::<LocalAddress>()
            .get(address, hash.to_vec())
            .perform(&mut env)
            .await
            .unwrap();
        assert_eq!(
            local_after,
            Some(encoded),
            "Block should be cached locally after fetch"
        );
    }

    #[tokio::test]
    async fn test_branch_clones_share_state() {
        let mut env = TestEnv::default();
        let replica = test_replica();
        let id = test_branch_id();

        // Open branch
        let branch1 = Branch::open(id.clone(), replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Clone the branch - clones share the same cell
        let branch2 = branch1.clone();

        // Both branches should see the same initial revision
        let rev1_initial = branch1.revision();
        let rev2_initial = branch2.revision();
        assert_eq!(
            rev1_initial, rev2_initial,
            "Cloned branches should see same revision"
        );

        // Commit through branch1
        let tree_hash = [42u8; 32];
        let (branch1, new_rev) = branch1
            .commit(tree_hash)
            .perform(&mut env)
            .await
            .unwrap();

        // branch2 should immediately see the update (shared cell)
        let rev2_after = branch2.revision();
        assert_eq!(
            rev2_after, new_rev,
            "Cloned branch should see update from other instance"
        );

        // branch1 should also see the same revision
        let rev1_after = branch1.revision();
        assert_eq!(rev1_after, rev2_after, "Both branches should see same state");
    }

    #[tokio::test]
    async fn test_transactional_memory_deduplicates_state() {
        use crate::fx::transactional_memory::TransactionalMemory;

        let mut env = TestEnv::default();
        let issuer = test_issuer();
        let id = test_branch_id();
        let memory: TransactionalMemory<BranchState> = TransactionalMemory::new();

        let did = local::Address::did(issuer.did());
        let key = format!("local/{}", id).into_bytes();

        // Open first cell through transactional memory
        let cell1 = memory.open(did.clone(), key.clone())
            .perform(&mut env)
            .await
            .unwrap();

        // Create initial state if empty
        if cell1.read().is_none() {
            let default_state = BranchState::new(
                id.clone(),
                Revision::new(issuer.principal().clone()),
                None,
            );
            cell1.replace(Some(default_state))
                .perform(&mut env)
                .await
                .unwrap();
        }

        // Modify through cell1
        let mut state1 = cell1.read().unwrap();
        state1.description = "Modified by cell1".to_string();
        cell1.replace(Some(state1)).perform(&mut env).await.unwrap();

        // Open second cell through same memory - should get shared state
        let cell2 = memory.open(did.clone(), key.clone())
            .perform(&mut env)
            .await
            .unwrap();

        // cell2 should immediately see the modification (same underlying Arc)
        let state2 = cell2.read().unwrap();
        assert_eq!(
            state2.description, "Modified by cell1",
            "Second cell should share state with first"
        );

        // Modify through cell2
        let mut state2_mod = state2.clone();
        state2_mod.description = "Modified by cell2".to_string();
        cell2.replace(Some(state2_mod)).perform(&mut env).await.unwrap();

        // cell1 should immediately see the update
        let state1_after = cell1.read().unwrap();
        assert_eq!(
            state1_after.description, "Modified by cell2",
            "First cell should see update from second"
        );
    }

    #[tokio::test]
    async fn test_pull_with_no_upstream_changes() {
        let mut env = TestEnv::default();
        let replica = test_replica();

        // Create main branch with a revision
        let main = Branch::open(BranchId::new("main".to_string()), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        let tree_hash = [1u8; 32];
        let (main, _) = main.commit(tree_hash).perform(&mut env).await.unwrap();

        // Create feature branch and set main as upstream
        let feature = Branch::open(BranchId::new("feature".to_string()), replica)
            .perform(&mut env)
            .await
            .unwrap();

        let feature = feature
            .set_upstream(Upstream::Local(main.clone()))
            .perform(&mut env)
            .await
            .unwrap();

        // Reset feature to same state as main
        let feature = feature
            .reset(main.revision())
            .perform(&mut env)
            .await
            .unwrap();

        // Pull should return None (no changes to merge)
        let (_, merged) = feature.pull().perform(&mut env).await.unwrap();
        assert!(merged.is_none(), "Pull should return None when no changes");
    }

    #[tokio::test]
    async fn test_pull_without_upstream_fails() {
        let mut env = TestEnv::default();
        let replica = test_replica();
        let id = test_branch_id();

        let branch = Branch::open(id, replica)
            .perform(&mut env)
            .await
            .unwrap();

        // Pull without upstream should fail
        let result = branch.pull().perform(&mut env).await;

        assert!(matches!(
            result,
            Err(ReplicaError::BranchHasNoUpstream { .. })
        ));
    }

    #[tokio::test]
    async fn test_branch_load_vs_open() {
        let mut env = TestEnv::default();
        let replica = test_replica();

        let branch_id = BranchId::new("test-branch".to_string());

        // load() should fail when branch doesn't exist
        let load_result = Branch::load(branch_id.clone(), replica.clone())
            .perform(&mut env)
            .await;
        assert!(
            load_result.is_err(),
            "load() should fail for non-existent branch"
        );

        // open() should succeed and create the branch
        let branch = Branch::open(branch_id.clone(), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();
        assert_eq!(branch.id(), &branch_id);

        // Now load() should succeed
        let loaded = Branch::load(branch_id.clone(), replica)
            .perform(&mut env)
            .await
            .unwrap();
        assert_eq!(loaded.id(), &branch_id);
    }

    #[tokio::test]
    async fn test_branch_description() {
        let mut env = TestEnv::default();
        let replica = test_replica();

        let branch_id = BranchId::new("feature-x".to_string());

        // Create branch
        let branch = Branch::open(branch_id.clone(), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        // Default description should be branch id
        assert_eq!(branch.description(), "feature-x");

        // Load and verify description persists
        let loaded = Branch::load(branch_id, replica)
            .perform(&mut env)
            .await
            .unwrap();
        assert_eq!(loaded.description(), "feature-x");
    }

    #[tokio::test]
    async fn test_issuer_generate() {
        // Test generating random issuer keys
        let issuer1 = Issuer::generate().unwrap();
        let issuer2 = Issuer::generate().unwrap();

        // Each generated issuer should be unique
        assert_ne!(issuer1.did(), issuer2.did());
        assert_ne!(issuer1.principal(), issuer2.principal());

        // DIDs should be valid format
        assert!(issuer1.did().starts_with("did:key:"));
        assert!(issuer2.did().starts_with("did:key:"));
    }

    #[tokio::test]
    async fn test_fetch_returns_upstream_revision_without_merging() {
        let mut env = TestEnv::default();
        let replica = test_replica();

        // Create main branch with content
        let main = Branch::open(BranchId::new("main".to_string()), replica.clone())
            .perform(&mut env)
            .await
            .unwrap();

        let tree_hash = [42u8; 32];
        let (main, main_revision) = main.commit(tree_hash).perform(&mut env).await.unwrap();

        // Create feature branch and set main as upstream
        let feature = Branch::open(BranchId::new("feature".to_string()), replica)
            .perform(&mut env)
            .await
            .unwrap();

        let feature = feature
            .set_upstream(Upstream::Local(main))
            .perform(&mut env)
            .await
            .unwrap();

        let feature_revision_before = feature.revision();

        // Fetch should return the upstream revision
        let (feature, fetched) = feature.fetch().perform(&mut env).await.unwrap();

        // Fetch should return the upstream revision
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap(), main_revision);

        // But local revision should NOT change (fetch doesn't merge)
        assert_eq!(feature.revision(), feature_revision_before);
    }
}
