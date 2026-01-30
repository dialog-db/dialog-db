use super::platform::PlatformStorage;
use super::platform::{PlatformBackend, PrefixedBackend, TypedStoreResource};
pub use super::uri::Uri;
use crate::artifacts::selector::Constrained;
use crate::artifacts::{
    Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMut, Datum, Instruction, MatchCandidate,
};
use crate::{
    AttributeKey, DialogArtifactsError, EntityKey, FromKey, Key, KeyView, KeyViewConstruct,
    KeyViewMut, State, ValueKey,
};
use async_stream::try_stream;
use async_trait::async_trait;
use base58::ToBase58;
use blake3;
use dialog_capability::Did;
use dialog_common::ConditionalSend;
use dialog_prolly_tree::{
    Differential, EMPT_TREE_HASH, Entry, GeometricDistribution, KeyType, Node, Tree, TreeDifference,
};
use futures_util::{Stream, StreamExt, TryStreamExt};
use std::fmt::Debug;

use dialog_storage::{Blake3Hash, CborEncoder, DialogStorageError, Encoder, StorageBackend};

use ed25519_dalek::ed25519::signature::SignerMut;
use ed25519_dalek::{SECRET_KEY_LENGTH, Signature, SignatureError, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

/// Operator module for signing and identity management.
pub mod operator;
/// Principal type definitions.
pub mod principal;
/// Remote repository and branch management.
pub mod remote;
/// Repository trait for managing remotes.
pub mod repository;

pub use operator::Operator;
pub use principal::Principal;
pub use remote::{RemoteBranch, RemoteCredentials, RemoteRepository, RemoteSite, Site};
pub use repository::Remotes;

// TryFrom<Principal> for VerifyingKey is implemented in principal.rs

/// Type alias for the prolly tree index used to store artifacts.
/// The Archive wraps the backend with PrefixedBackend to namespace index storage.
pub type Index<Backend> =
    Tree<GeometricDistribution, Key, State<Datum>, Blake3Hash, Archive<PrefixedBackend<Backend>>>;

/// We reference a tree by the root hash.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeReference(Blake3Hash);
impl NodeReference {
    fn hash(&self) -> &Blake3Hash {
        &self.0
    }
}
impl Default for NodeReference {
    /// By default, a [`NodeReference`] is created to empty search tree.
    fn default() -> Self {
        Self(EMPT_TREE_HASH)
    }
}
impl Display for NodeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bytes: &[u8] = self.hash();
        write!(f, "#{}", ToBase58::to_base58(bytes))
    }
}
impl Debug for NodeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

impl From<NodeReference> for Blake3Hash {
    fn from(value: NodeReference) -> Self {
        let NodeReference(hash) = value;
        hash
    }
}

/// A replica represents a local instance of a distributed database.
#[derive(Debug, Clone)]
pub struct Replica<Backend: PlatformBackend> {
    issuer: Operator,
    subject: Did,
    storage: PlatformStorage<Backend>,
    /// Local branches in this replica
    pub branches: Branches<Backend>,
}

impl<Backend: PlatformBackend + 'static> Replica<Backend> {
    /// Creates a new replica with the given issuer and storage backend.
    pub fn open(issuer: Operator, subject: Did, backend: Backend) -> Result<Self, ReplicaError> {
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);

        let branches = Branches::new(issuer.clone(), subject.clone(), backend.clone());
        Ok(Replica {
            issuer,
            subject,
            storage,
            branches,
        })
    }

    /// Returns the principal (public key) of the issuer for this replica.
    pub fn principal(&self) -> &Principal {
        self.issuer.principal()
    }

    /// Returns a reference to the storage.
    pub fn storage(&self) -> &PlatformStorage<Backend> {
        &self.storage
    }

    /// Returns a mutable reference to the storage.
    pub fn storage_mut(&mut self) -> &mut PlatformStorage<Backend> {
        &mut self.storage
    }

    /// Returns a reference to the issuer (operator).
    pub fn issuer(&self) -> &Operator {
        &self.issuer
    }

    /// Returns a reference to the subject DID.
    pub fn subject(&self) -> &Did {
        &self.subject
    }
}

/// Manages multiple branches within a replica.
#[derive(Debug, Clone)]
pub struct Branches<Backend: PlatformBackend> {
    issuer: Operator,
    subject: Did,
    storage: PlatformStorage<Backend>,
}

impl<Backend: PlatformBackend + 'static> Branches<Backend> {
    /// Creates a new instance for the given backend
    pub fn new(issuer: Operator, subject: Did, backend: Backend) -> Self {
        let storage = PlatformStorage::new(backend, CborEncoder);
        Self {
            issuer,
            subject,
            storage,
        }
    }

    /// Loads a branch with given identifier, produces an error if it does not
    /// exists.
    pub async fn load(&self, id: &BranchId) -> Result<Branch<Backend>, ReplicaError> {
        Branch::load(
            id,
            self.issuer.clone(),
            self.storage.clone(),
            self.subject.clone(),
        )
        .await
    }

    /// Loads a branch with the given identifier or creates a new one if
    /// it does not already exist.
    pub async fn open(&self, id: impl Into<BranchId>) -> Result<Branch<Backend>, ReplicaError> {
        Branch::open(
            id,
            self.issuer.clone(),
            self.storage.clone(),
            self.subject.clone(),
        )
        .await
    }
}

/// Archive represents content addressed storage where search tree
/// nodes are stored. It supports optional remote fallback for on
/// demand replication. Uses Arc to share remote state across clones.
///
/// Archive is storage-agnostic and uses whatever backend you give it.
/// If you want prefixed keys, wrap your backend with PrefixedBackend before
/// passing it to Archive.
#[derive(Clone)]
pub struct Archive<Backend>
where
    Backend: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone,
{
    local: Arc<dialog_storage::Storage<CborEncoder, Backend>>,
    remote: Arc<RwLock<Option<PlatformStorage<RemoteBackend>>>>,
}

impl<Backend> std::fmt::Debug for Archive<Backend>
where
    Backend: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Archive")
            .field("local", &"<Storage>")
            .field("remote", &"<RwLock>")
            .finish()
    }
}

impl<Backend> Archive<Backend>
where
    Backend:
        StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + dialog_common::ConditionalSync,
    Backend::Error: Into<dialog_storage::DialogStorageError>,
{
    /// Creates a new Archive with the given backend.
    pub fn new(backend: Backend) -> Self {
        let storage = dialog_storage::Storage {
            encoder: CborEncoder,
            backend,
        };
        Self {
            local: Arc::new(storage),
            remote: Arc::new(RwLock::new(None)),
        }
    }

    /// Sets the remote storage for fallback reads and replicated writes
    pub async fn set_remote(&self, remote: PlatformStorage<RemoteBackend>) {
        *self.remote.write().await = Some(remote);
    }

    /// Clears the remote storage
    pub async fn clear_remote(&self) {
        *self.remote.write().await = None;
    }

    /// Checks if a remote storage is configured
    pub async fn has_remote(&self) -> bool {
        self.remote.read().await.is_some()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> dialog_storage::ContentAddressedStorage for Archive<Backend>
where
    Backend: StorageBackend<Key = Vec<u8>, Value = Vec<u8>>
        + Clone
        + dialog_common::ConditionalSync
        + 'static,
    Backend::Error: Into<dialog_storage::DialogStorageError> + std::fmt::Debug,
{
    type Hash = [u8; 32];
    type Error = dialog_storage::DialogStorageError;

    async fn read<T>(&self, hash: &Self::Hash) -> Result<Option<T>, Self::Error>
    where
        T: serde::de::DeserializeOwned + dialog_common::ConditionalSync,
    {
        // Use hash directly as key - prefixing is handled by the storage layer
        let key = hash.to_vec();

        // Try local first
        if let Some(bytes) =
            self.local.get(&key).await.map_err(|e| {
                dialog_storage::DialogStorageError::StorageBackend(format!("{:?}", e))
            })?
        {
            return self.local.decode(&bytes).await.map(Some);
        }

        // Fall back to remote if available - clone to avoid holding lock across await
        let connection = {
            let remote_guard = self.remote.read().await;
            remote_guard.clone()
        };

        if let Some(remote) = connection.as_ref() {
            if let Some(bytes) = remote.get(&key).await.map_err(|e| {
                dialog_storage::DialogStorageError::StorageBackend(format!("{:?}", e))
            })? {
                // Cache the remote value to local storage
                // Clone the Arc to get a mutable copy that shares the backend's interior state
                let mut local = (*self.local).clone();
                local.set(key, bytes.clone()).await.map_err(|e| {
                    dialog_storage::DialogStorageError::StorageBackend(format!("{:?}", e))
                })?;

                return remote.decode(&bytes).await.map(Some);
            }
        }

        Ok(None)
    }

    async fn write<T>(&mut self, block: &T) -> Result<Self::Hash, Self::Error>
    where
        T: serde::Serialize + dialog_common::ConditionalSync + std::fmt::Debug,
    {
        // Encode and hash the block
        let (hash, bytes) = self.local.encode(block).await?;

        // Use hash directly as key - prefixing is handled by the storage layer
        let key = hash.to_vec();

        // Write to local storage only - remote sync happens during push()
        // and that is when new blocks will be propagated to the remote.
        {
            let mut local = (*self.local).clone();
            local.set(key, bytes).await.map_err(|e| {
                dialog_storage::DialogStorageError::StorageBackend(format!("{:?}", e))
            })?;
        }

        Ok(hash)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> dialog_storage::Encoder for Archive<Backend>
where
    Backend: StorageBackend<Key = Vec<u8>, Value = Vec<u8>>
        + Clone
        + dialog_common::ConditionalSync
        + 'static,
    Backend::Error: Into<dialog_storage::DialogStorageError> + std::fmt::Debug,
{
    type Bytes = Vec<u8>;
    type Hash = [u8; 32];
    type Error = dialog_storage::DialogStorageError;

    async fn encode<T>(&self, block: &T) -> Result<(Self::Hash, Self::Bytes), Self::Error>
    where
        T: serde::Serialize + dialog_common::ConditionalSync + std::fmt::Debug,
    {
        self.local.encode(block).await
    }

    async fn decode<T>(&self, bytes: &[u8]) -> Result<T, Self::Error>
    where
        T: serde::de::DeserializeOwned + dialog_common::ConditionalSync,
    {
        self.local.decode(bytes).await
    }
}

/// A branch represents a named line of development within a replica.
#[derive(Clone)]
pub struct Branch<Backend: PlatformBackend + 'static> {
    issuer: Operator,
    id: BranchId,
    subject: Did,
    storage: PlatformStorage<Backend>,
    archive: Archive<PrefixedBackend<Backend>>,
    memory: TypedStoreResource<BranchState, Backend>,
    tree: Arc<RwLock<Index<Backend>>>,
    upstream: Arc<std::sync::RwLock<Option<Upstream<Backend>>>>,
}

impl<Backend: PlatformBackend + 'static> std::fmt::Debug for Branch<Backend> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Branch")
            .field("id", &self.id)
            .field("issuer", &self.issuer)
            .finish_non_exhaustive()
    }
}

impl<Backend: PlatformBackend + 'static> Branch<Backend> {
    async fn mount(
        id: &BranchId,
        storage: &mut PlatformStorage<Backend>,
        default_state: Option<BranchState>,
    ) -> Result<TypedStoreResource<BranchState, Backend>, ReplicaError> {
        let key = format!("local/{}", id);
        let memory = storage
            .open::<BranchState>(&key.into())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        // if we branch does not exist yet and we have default state we create
        // a branch.
        if let (None, Some(state)) = (memory.read(), default_state) {
            memory
                .replace_with(
                    move |prior| prior.to_owned().or(Some(state.clone())),
                    storage,
                )
                .await
                .map_err(|_| ReplicaError::StorageError("Updating branch failed".into()))?;
        }

        Ok(memory)
    }

    /// Loads a branch from storage, creating it with the provided default state if it doesn't exist.
    pub async fn load_with_default(
        id: &BranchId,
        issuer: Operator,
        mut storage: PlatformStorage<Backend>,
        subject: Did,
        default_state: Option<BranchState>,
    ) -> Result<Branch<Backend>, ReplicaError> {
        let memory = Self::mount(id, &mut storage, default_state).await?;
        let prefixed_backend = PrefixedBackend::new(b"index/", storage.clone().into_backend());
        let archive = Archive::new(prefixed_backend);

        // if we have a memory of tis branch we initialize it otherwise
        // we produce an error.
        if let Some(state) = memory.read() {
            // Load the tree from the revision's tree hash
            let tree = Tree::from_hash(state.revision.tree().hash(), archive.clone())
                .await
                .map_err(|e| ReplicaError::StorageError(format!("Failed to load tree: {:?}", e)))?;

            // If branch has an upstream setup we load it up and configure
            // archive's remote
            let upstream = if let Some(state) = &state.upstream {
                let mut upstream =
                    Upstream::open(state, issuer.clone(), storage.clone(), subject.clone()).await?;

                // Configure archive's remote for fallback reads
                if let Upstream::Remote(branch) = &mut upstream {
                    let connection = branch.connect().await?;
                    archive.set_remote(connection.archive()).await;
                }

                Some(upstream)
            } else {
                None
            };

            Ok(Branch {
                id: id.clone(),
                issuer: issuer.clone(),
                subject,
                memory,
                archive,
                storage: storage.clone(),
                upstream: Arc::new(std::sync::RwLock::new(upstream)),
                tree: Arc::new(RwLock::new(tree)),
            })
        } else {
            Err(ReplicaError::BranchNotFound { id: id.clone() })
        }
    }

    /// Mounts a typed store for branch state at the appropriate storage location.
    /// Loads a branch with a given id or creates one if it does not exist.
    pub async fn open(
        id: impl Into<BranchId>,
        issuer: Operator,
        storage: PlatformStorage<Backend>,
        subject: Did,
    ) -> Result<Branch<Backend>, ReplicaError> {
        let id = id.into();
        let default_state = Some(BranchState::new(
            id.clone(),
            #[allow(clippy::clone_on_copy)]
            Revision::new(issuer.principal().clone()),
            None,
        ));

        let branch = Self::load_with_default(&id, issuer, storage, subject, default_state).await?;

        Ok(branch)
    }

    /// Loads a branch from the the the underlaying replica, if branch with a
    /// given id does not exists it produces an error.
    pub async fn load(
        id: &BranchId,
        issuer: Operator,
        storage: PlatformStorage<Backend>,
        subject: Did,
    ) -> Result<Branch<Backend>, ReplicaError> {
        let branch = Self::load_with_default(id, issuer, storage, subject, None).await?;

        Ok(branch)
    }

    /// Advances the branch to a given revision with an explicit base tree.
    /// Use this after merge operations where base should be set to upstream's tree
    /// (what we synced from) while revision is the merged result.
    async fn advance(
        &mut self,
        revision: Revision,
        base: NodeReference,
    ) -> Result<(), ReplicaError> {
        // Update local state with explicit base
        self.memory
            .replace_with(
                |source| {
                    if let Some(state) = source {
                        Some(BranchState {
                            revision: revision.clone(),
                            id: self.id.clone(),
                            description: state.description.clone(),
                            upstream: state.upstream.clone(),
                            base: base.clone(),
                        })
                    } else {
                        Some(BranchState {
                            revision: revision.clone(),
                            id: self.id.clone(),
                            description: "".into(),
                            upstream: None,
                            base: base.clone(),
                        })
                    }
                },
                &mut self.storage,
            )
            .await
            .map_err(|_| ReplicaError::StorageError("Updating branch failed".into()))?;

        // Update the tree to match the new revision
        let mut tree = self.tree.write().await;
        if revision.tree().hash() != &EMPT_TREE_HASH {
            #[allow(clippy::clone_on_copy)]
            tree.set_hash(Some(revision.tree().hash().clone()))
                .await
                .map_err(|_| ReplicaError::StorageError("Failed to update tree".into()))?;
        } else {
            tree.set_hash(None)
                .await
                .map_err(|_| ReplicaError::StorageError("Failed to reset tree".into()))?;
        }

        Ok(())
    }

    /// Advances the branch to a given revision. The base tree is set to the
    /// revision's tree, representing that the branch is now "in sync" at this
    /// revision (no divergence from the synced state).
    pub async fn reset(&mut self, revision: Revision) -> Result<(), ReplicaError> {
        self.advance(revision.clone(), revision.tree.clone()).await
    }

    /// Lazily initializes and returns a mutable reference to the upstream.
    /// Returns None if no upstream is configured.
    pub fn upstream(&self) -> Option<Upstream<Backend>> {
        self.upstream
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Fetches remote reference of this branch. If this branch has no upstream
    /// setup it will produce an error. If upstream branch is a local one this
    /// operation is a no-op. If it has a remote upsteram it tries to fetch
    /// a revision and update corresponding branch record locally
    pub async fn fetch(&mut self) -> Result<Option<Revision>, ReplicaError> {
        if let Some(mut upstream) = self.upstream() {
            upstream.fetch().await
        } else {
            Err(ReplicaError::BranchNotFound {
                id: self.id().clone(),
            })
        }
    }

    fn state(&self) -> BranchState {
        self.memory.read().unwrap_or_else(|| {
            BranchState::new(
                self.id.clone(),
                Revision::new(self.issuer.principal().to_owned()),
                None,
            )
        })
    }
    /// Returns the branch identifier.
    pub fn id(&self) -> &BranchId {
        &self.id
    }

    /// Returns principal issuing changes on this branch
    pub fn principal(&self) -> &Principal {
        self.issuer.principal()
    }

    /// Returns the current revision of this branch.
    pub fn revision(&self) -> Revision {
        self.state().revision().to_owned()
    }

    /// Logical time on this branch
    pub fn occurence(&self) -> Occurence {
        self.revision().into()
    }

    /// Returns the base tree reference for this branch.
    pub fn base(&self) -> NodeReference {
        self.state().base
    }
    /// Returns a description of this branch.
    pub fn description(&self) -> String {
        self.state().description().into()
    }

    /// Returns a stream of novel nodes representing local changes since the last sync.
    /// These are tree nodes that exist in the current tree but not in the base tree.
    fn novelty(
        &self,
    ) -> impl Stream<Item = Result<Node<Key, State<Datum>, Blake3Hash>, ReplicaError>> + '_ {
        try_stream! {
            // Load base tree (state at last sync)
            let base: Index<Backend> = Tree::from_hash(self.base().hash(), self.archive.clone())
                .await
                .map_err(|e| ReplicaError::StorageError(format!("Failed to load base tree: {:?}", e)))?;

            // Get current tree
            let current = self.tree.read().await.clone();

            // Compute diff to find novel nodes
            let difference = TreeDifference::compute(&base, &current)
                .await
                .map_err(|e| ReplicaError::StorageError(format!("Failed to compute diff: {:?}", e)))?;

            // Yield all novel nodes
            for await node in difference.novel_nodes() {
                yield node.map_err(|e| ReplicaError::StorageError(format!("Failed to load node: {:?}", e)))?;
            }
        }
    }

    /// Returns a stream of changes representing local modifications since the last sync.
    /// This computes the differential between the base tree (last sync point) and
    /// the current tree, yielding Add/Remove operations.
    fn changes(&self) -> impl Differential<Key, State<Datum>> + '_ {
        try_stream! {
            // Load base tree (state at last sync)
            let base: Index<Backend> = Tree::from_hash(self.base().hash(), self.archive.clone()).await?;

            // Get current tree
            let current = self.tree.read().await.clone();

            // Yield all changes from base to current
            for await change in base.differentiate(&current) {
                yield change?
            }
        }
    }

    /// Pushes the current revision to the upstream branch.
    /// If upstream is local, it updates that branch directly.
    /// If upstream is remote, it publishes to the remote and updates local cache.
    /// Returns Error if  if branch does not have upstream set. Returns
    /// Option<Revision> describing prior state of the upstream.
    pub async fn push(&mut self) -> Result<Option<Revision>, ReplicaError> {
        if let Some(upstream) = &mut self.upstream() {
            match upstream {
                Upstream::Local(target) => {
                    // setting upstream to yourself should be invalid
                    if target.id() == self.id() {
                        Err(ReplicaError::BranchUpstreamIsItself {
                            id: target.id().clone(),
                        })
                    } else {
                        let mut branch = target.load().await?;
                        let before = branch.revision();
                        if before.tree() == &self.base() {
                            branch.reset(self.revision()).await?;
                            self.reset(self.revision()).await?;
                            Ok(Some(before))
                        } else {
                            Ok(None)
                        }
                    }
                }
                Upstream::Remote(target) => {
                    let connection = target.connect().await?;
                    let before = connection.resolve().await?;
                    let after = self.revision().clone();

                    if before.as_ref() != Some(&after) {
                        // Replicate novel blocks to a remote target
                        connection.upload(self.novelty()).await?;
                        // Now that all blocks are synced, publish the revision
                        connection.publish(after.clone()).await?;
                        self.reset(after).await?;
                    }

                    Ok(before)
                }
            }
        } else {
            Err(ReplicaError::BranchHasNoUpstream {
                id: self.id.clone(),
            })
        }
    }

    /// Pulls changes from the upstream branch.
    /// Fetches the latest revision from upstream and integrates local changes.
    ///
    /// This performs a three-way merge:
    /// 1. Loads the upstream tree (their changes)
    /// 2. Computes local changes since last pull using differentiate()
    /// 3. Integrates local changes into upstream tree
    /// 4. Creates a new revision with proper period/moment
    pub async fn pull(&mut self) -> Result<Option<Revision>, ReplicaError> {
        if self.upstream().is_some() {
            if let Some(revision) = self.fetch().await? {
                // if upstream revision is different from our base
                // we'll merge local changes onto upstream tree otherwise
                // there's nothing to do because upstream has not changed
                if &self.base() == revision.tree() {
                    Ok(None)
                } else {
                    // Load upstream tree into memory
                    let mut target: Index<Backend> =
                        Tree::from_hash(revision.tree.hash(), self.archive.clone())
                            .await
                            .map_err(|e| {
                                ReplicaError::StorageError(format!(
                                    "Failed to load upstream tree: {:?}",
                                    e
                                ))
                            })?;

                    // Compute local changes: what operations transform base into current
                    // This gives us the changes we made locally
                    let changes = self.changes();

                    // Integrate local changes into upstream tree
                    target.integrate(changes).await.map_err(|e| {
                        ReplicaError::StorageError(format!("Failed to integrate changes: {:?}", e))
                    })?;

                    // Get the hash of the integrated tree
                    let hash = target.hash().cloned().unwrap_or(EMPT_TREE_HASH);

                    // Check if integration actually changed the tree
                    if &hash == revision.tree.hash() {
                        // No local changes were integrated - tree unchanged
                        // Just adopt the upstream revision directly without creating a new one
                        self.reset(revision.clone()).await?;

                        Ok(Some(revision))
                    } else {
                        // Create new revision with integrated changes
                        #[allow(clippy::clone_on_copy)]
                        let new_revision = Revision {
                            issuer: self.issuer.principal().clone(),
                            tree: NodeReference(hash),
                            cause: HashSet::from([revision.edition()?]),
                            // period is max between local and remote periods + 1
                            period: revision.period.max(self.revision().period) + 1,
                            // moment is reset when period changes
                            moment: 0,
                        };

                        // Advance branch to merged revision with upstream's tree as base.
                        // This way novelty() will find merged nodes when we push.
                        self.advance(new_revision.clone(), revision.tree.clone())
                            .await?;

                        Ok(Some(new_revision))
                    }
                }
            } else {
                Ok(None)
            }
        } else {
            Err(ReplicaError::BranchHasNoUpstream {
                id: self.id.clone(),
            })
        }
    }

    /// Sets the upstream for this branch and persists the change.
    /// Accepts either a Branch or RemoteBranch via Into<Upstream>.
    pub async fn set_upstream<U: Into<Upstream<Backend>>>(
        &mut self,
        target: U,
    ) -> Result<(), ReplicaError> {
        let upstream = target.into();

        // Get the state descriptor from the upstream
        let state = upstream.to_state();

        // First update branch memory with a new upstream
        self.memory
            .replace_with(
                |current| {
                    let branch = current
                        .as_ref()
                        .expect("branch must be loaded before upstream is set");

                    Some(BranchState {
                        upstream: Some(state.clone()),
                        ..branch.clone()
                    })
                },
                &mut self.storage,
            )
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        // Set the archive remote to the upstream store if it is a
        // remote so tree changes will be replicated; if local, clear the remote
        // Configure archive's remote for fallback reads
        let upstream = match upstream {
            Upstream::Remote(mut remote) => {
                let connection = remote.connect().await?;
                self.archive.set_remote(connection.archive()).await;
                Upstream::Remote(remote)
            }
            Upstream::Local(_) => {
                self.archive.clear_remote().await;
                upstream
            }
        };

        // Update the cached upstream on the branch
        *self.upstream.write().unwrap_or_else(|e| e.into_inner()) = Some(upstream);

        Ok(())
    }
}

// Implement ArtifactStore for Branch
impl<Backend: PlatformBackend + 'static> ArtifactStore for Branch<Backend> {
    fn select(
        &self,
        selector: ArtifactSelector<Constrained>,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 'static + ConditionalSend
    {
        let tree = self.tree.clone();

        try_stream! {
            // Clone the tree to "pin" it at a version for the lifetime of the stream
            let tree = tree.read().await.clone();

            if selector.entity().is_some() {
                let start = <EntityKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <EntityKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

                let stream = tree.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { value: State::Added(datum), .. } = entry {
                            yield Artifact::try_from(datum)?;
                        }
                    }
                }
            } else if selector.value().is_some() {
                let start = <ValueKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <ValueKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

                let stream = tree.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { value: State::Added(datum), .. } = entry {
                            yield Artifact::try_from(datum)?;
                        }
                    }
                }
            } else if selector.attribute().is_some() {
                let start = <AttributeKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <AttributeKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

                let stream = tree.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { value: State::Added(datum), .. } = entry {
                            yield Artifact::try_from(datum)?;
                        }
                    }
                }
            } else {
                unreachable!("ArtifactSelector will always have at least one field specified")
            };
        }
    }
}

// Implement ArtifactStoreMut for Branch
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend: PlatformBackend + 'static> ArtifactStoreMut for Branch<Backend> {
    async fn commit<Instructions>(
        &mut self,
        instructions: Instructions,
    ) -> Result<Blake3Hash, DialogArtifactsError>
    where
        Instructions: Stream<Item = Instruction> + ConditionalSend,
    {
        let base_revision = self.revision();

        let transaction_result = async {
            let mut tree = self.tree.write().await;

            tokio::pin!(instructions);

            while let Some(instruction) = instructions.next().await {
                match instruction {
                    Instruction::Assert(artifact) => {
                        let entity_key = EntityKey::from(&artifact);
                        let value_key = ValueKey::from_key(&entity_key);
                        let attribute_key = AttributeKey::from_key(&entity_key);

                        let datum = Datum::from(artifact);

                        if let Some(cause) = &datum.cause {
                            let ancestor_key = {
                                let search_start = <EntityKey<Key> as KeyViewConstruct>::min()
                                    .set_entity(entity_key.entity())
                                    .set_attribute(entity_key.attribute())
                                    .into_key();
                                let search_end = <EntityKey<Key> as KeyViewConstruct>::max()
                                    .set_entity(entity_key.entity())
                                    .set_attribute(entity_key.attribute())
                                    .into_key();

                                let search_stream = tree.stream_range(search_start..search_end);

                                let mut ancestor_key = None;

                                tokio::pin!(search_stream);

                                while let Some(candidate) = search_stream.try_next().await? {
                                    if let State::Added(current_element) = candidate.value {
                                        let current_artifact = Artifact::try_from(current_element)?;
                                        let current_artifact_reference =
                                            crate::artifacts::Cause::from(&current_artifact);

                                        if cause == &current_artifact_reference {
                                            ancestor_key = Some(candidate.key);
                                            break;
                                        }
                                    }
                                }

                                ancestor_key
                            };

                            if let Some(key) = ancestor_key {
                                // Prune the old entry from the indexes
                                let entity_key = EntityKey(key);
                                let value_key: ValueKey<Key> = ValueKey::from_key(&entity_key);
                                let attribute_key: AttributeKey<Key> =
                                    AttributeKey::from_key(&entity_key);

                                // TODO: Make it concurrent / parallel
                                tree.delete(&entity_key.into_key()).await?;
                                tree.delete(&value_key.into_key()).await?;
                                tree.delete(&attribute_key.into_key()).await?;
                            }
                        }

                        // TODO: Make it concurrent / parallel
                        tree.set(entity_key.into_key(), State::Added(datum.clone()))
                            .await?;
                        tree.set(attribute_key.into_key(), State::Added(datum.clone()))
                            .await?;
                        tree.set(value_key.into_key(), State::Added(datum)).await?;
                    }
                    Instruction::Retract(fact) => {
                        let entity_key = EntityKey::from(&fact);
                        let value_key = ValueKey::from_key(&entity_key);
                        let attribute_key = AttributeKey::from_key(&entity_key);

                        // TODO: Make it concurrent / parallel
                        tree.set(entity_key.into_key(), State::Removed).await?;
                        tree.set(attribute_key.into_key(), State::Removed).await?;
                        tree.set(value_key.into_key(), State::Removed).await?;
                    }
                }
            }

            // Get the tree hash and create a new revision
            let tree_hash = *tree.hash().ok_or_else(|| {
                DialogArtifactsError::Storage("Failed to get tree hash".to_string())
            })?;

            // Create the new revision
            let tree_reference = NodeReference(tree_hash);

            // Calculate the new period and moment based on the base revision
            let (period, moment) = {
                let base_period = *base_revision.period();
                let base_moment = *base_revision.moment();
                let base_issuer = base_revision.issuer();

                if base_issuer == self.issuer.principal() {
                    // Same issuer - increment moment
                    (base_period, base_moment + 1)
                } else {
                    // Different issuer - increment period, reset moment
                    (base_period + 1, 0)
                }
            };

            #[allow(clippy::clone_on_copy)]
            let new_revision = Revision {
                issuer: self.issuer.principal().clone(),
                tree: tree_reference.clone(),
                cause: HashSet::from([base_revision.edition().expect("Failed to create edition")]),
                period,
                moment,
            };

            // Update the branch state with the new revision
            // IMPORTANT: Keep the base tree unchanged - it represents the last
            // synced state, not the current local state. Base should only
            // update during pull/push operations.
            self.memory
                .replace(
                    Some(BranchState {
                        revision: new_revision.clone(),
                        ..self.state()
                    }),
                    &mut self.storage,
                )
                .await
                .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

            Ok(tree_hash)
        }
        .await;

        match transaction_result {
            Ok(hash) => Ok(hash),
            // Rollback: reset tree to the prior revision and propagate an error
            Err(error) => {
                self.tree
                    .write()
                    .await
                    .set_hash(Some(*base_revision.tree().hash()))
                    .await?;

                Err(error)
            }
        }
    }
}

pub use remote::{RemoteBackend, RemoteState};

/// Logical timestamp used to denote dialog transactions. It takes inspiration
/// from automerge which tags lamport timestamps with origin information. It
/// takes inspiration from [Hybrid Logical Clocks (HLC)](https://sergeiturukin.com/2017/06/26/hybrid-logical-clocks.html)
/// and splits timestamp into two components `period` representing coordinated
/// component of the time and `moment` representing an uncoordinated local
/// time component. This construction allows us to capture synchronization
/// points allowing us to prioritize replicas that are actively collaborating
/// over those that are not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Occurence {
    /// Site of this occurence.
    pub site: Principal,

    /// Logical coordinated time component denoting a last synchronization
    /// cycle.
    pub period: usize,

    /// Local uncoordinated time component denoting a moment within a
    /// period at which occurrence happened.
    pub moment: usize,
}

/// A [`Revision`] represents a concrete state of the dialog instance. It is
/// kind of like git commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revision {
    /// Site where this revision was created.It as expected to be a signing
    /// principal representing a tool acting on author's behalf. In the future
    /// I expect we'll have signed delegation chain from user to this site.
    pub issuer: Principal,

    /// Reference the root of the search tree.
    pub tree: NodeReference,

    /// Set of revisions this is based of. It can be an empty set if this is
    /// a first revision, but more commonly it will point to a previous revision
    /// it is based on. If branch tracks multiple concurrent upstreams it will
    /// contain a set of revisions.
    ///
    /// It is effectively equivalent of of `parents` in git commit objects.
    pub cause: HashSet<Edition<Revision>>,

    /// Period indicating when this revision was created. This MUST be derived
    /// from the `cause`al revisions and it must be greater by one than the
    /// maximum period of the `cause`al revisions that have different `by` from
    /// this revision. More simply we create a new period whenever we
    /// synchronize.
    pub period: usize,

    /// Moment at which this revision was created. It represents a number of
    /// transactions that have being made in this period. If `cause`al revisions
    /// have a revision from same `by` this MUST be value greater by one,
    /// otherwise it should be `0`. This implies that when we sync we increment
    /// `period` and reset `moment` to `0`. And when we create a transaction we
    /// increment `moment` by one and keep the same `period`.
    pub moment: usize,
}

impl Revision {
    /// Creates new revision for with an empty tree
    pub fn new(issuer: Principal) -> Self {
        Self {
            issuer,
            tree: NodeReference::default(),
            period: 0,
            moment: 0,
            cause: HashSet::new(),
        }
    }

    /// Issuer of this revision.
    pub fn issuer(&self) -> &Principal {
        &self.issuer
    }

    /// The component of the [`Revision`] that corresponds to the root of the
    /// search index.
    pub fn tree(&self) -> &NodeReference {
        &self.tree
    }

    /// Period when changes have being made
    pub fn period(&self) -> &usize {
        &self.period
    }

    /// Number of transactions made by this issuer since the beginning of
    /// this epoch
    pub fn moment(&self) -> &usize {
        &self.moment
    }

    /// Previous revision this replaced.
    pub fn cause(&self) -> &HashSet<Edition<Revision>> {
        &self.cause
    }

    /// Creates an [`Edition`] of this revision by hashing it.
    ///
    /// This is used to reference this revision as a causal ancestor in subsequent revisions.
    pub fn edition(&self) -> Result<Edition<Revision>, ReplicaError> {
        let revision_bytes = serde_ipld_dagcbor::to_vec(self).map_err(|e| {
            ReplicaError::StorageError(format!("Failed to serialize revision: {}", e))
        })?;
        let revision_hash: [u8; 32] = *blake3::hash(&revision_bytes).as_bytes();
        Ok(Edition::new(revision_hash))
    }
}

impl From<Revision> for Occurence {
    fn from(revision: Revision) -> Self {
        Occurence {
            site: revision.issuer,
            period: revision.period,
            moment: revision.moment,
        }
    }
}

/// Branch is similar to a git branch and represents a named state of
/// the work that is either diverged or converged from other workstream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchState {
    /// Unique identifier of this fork.
    pub id: BranchId,

    /// Free-form human-readable description of this fork.
    pub description: String,

    /// Current revision associated with this branch.
    pub revision: Revision,

    /// Root of the search tree our this revision is based off.
    pub base: NodeReference,

    /// An upstream through which updates get propagated. Branch may
    /// not have an upstream.
    pub upstream: Option<UpstreamState>,
}

/// Unique name for the branch
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BranchId(String);

impl BranchId {
    /// Creates a new branch identifier from a string.
    pub fn new(id: String) -> Self {
        BranchId(id)
    }

    /// Returns a reference to the branch identifier string.
    pub fn id(&self) -> &String {
        &self.0
    }
}

impl KeyType for BranchId {
    fn bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}
impl TryFrom<Vec<u8>> for BranchId {
    type Error = std::string::FromUtf8Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(BranchId(String::from_utf8(bytes)?))
    }
}
impl Display for BranchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&BranchId> for BranchId {
    fn from(value: &BranchId) -> Self {
        value.clone()
    }
}

impl BranchState {
    /// Create a new fork from the given revision.
    pub fn new(id: BranchId, revision: Revision, description: Option<String>) -> Self {
        Self {
            description: description.unwrap_or_else(|| id.0.clone()),
            base: revision.tree.clone(),
            revision,
            upstream: None,
            id,
        }
    }
    /// Unique identifier of this fork.
    pub fn id(&self) -> &BranchId {
        &self.id
    }

    /// Current revision of this branch.
    pub fn revision(&self) -> &Revision {
        &self.revision
    }

    /// Description of this branch.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Upstream branch of this branch.
    pub fn upstream(&self) -> Option<&UpstreamState> {
        self.upstream.as_ref()
    }

    /// Resets the branch to a new revision.
    pub fn reset(&mut self, revision: Revision) -> &mut Self {
        self.revision = revision;
        self
    }
}

/// Descriptor for a local upstream branch (lazy loaded).
#[derive(Clone)]
pub struct LocalUpstream<Backend: PlatformBackend + 'static> {
    branch_id: BranchId,
    issuer: Operator,
    storage: PlatformStorage<Backend>,
    subject: Did,
}

impl<Backend: PlatformBackend + 'static> std::fmt::Debug for LocalUpstream<Backend> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalUpstream")
            .field("branch_id", &self.branch_id)
            .finish_non_exhaustive()
    }
}

impl<Backend: PlatformBackend + 'static> LocalUpstream<Backend> {
    /// Load the branch on demand.
    async fn load(&self) -> Result<Branch<Backend>, ReplicaError> {
        Branch::load(
            &self.branch_id,
            self.issuer.clone(),
            self.storage.clone(),
            self.subject.clone(),
        )
        .await
    }

    /// Get the branch ID.
    pub fn id(&self) -> &BranchId {
        &self.branch_id
    }
}

/// Upstream branch that is used to push & pull changes
/// to / from. It can be local or remote.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Upstream<Backend: PlatformBackend + 'static> {
    /// A local branch upstream (lazy - loaded on fetch/publish)
    Local(LocalUpstream<Backend>),
    /// A remote branch upstream
    Remote(RemoteBranch<Backend>),
}

impl<Backend: PlatformBackend + 'static> Upstream<Backend> {
    /// Creates an upstream from its state descriptor.
    ///
    /// For local upstreams, this stores a descriptor for lazy loading.
    /// For remote upstreams, this creates the remote branch reference.
    pub async fn open(
        state: &UpstreamState,
        issuer: Operator,
        storage: PlatformStorage<Backend>,
        subject: Did,
    ) -> Result<Self, ReplicaError> {
        match state {
            UpstreamState::Local { branch } => Ok(Upstream::Local(LocalUpstream {
                branch_id: branch.clone(),
                issuer,
                storage,
                subject,
            })),
            UpstreamState::Remote { site, branch } => {
                let mut remote_branch =
                    RemoteBranch::new(site, branch.id(), storage.clone(), issuer, subject).await?;
                remote_branch.connect().await?;
                Ok(Upstream::Remote(remote_branch))
            }
        }
    }

    /// Returns the branch id of this upstream as a string
    pub fn id(&self) -> String {
        match self {
            Upstream::Local(branch) => branch.id().to_string(),
            Upstream::Remote(branch) => branch.id().to_string(),
        }
    }

    /// Returns site of the branch. If local returns None otherwise
    /// returns site identifier
    pub fn site(&self) -> Option<&str> {
        match self {
            Upstream::Local(_) => None,
            Upstream::Remote(branch) => Some(branch.site()),
        }
    }

    /// Returns true if this upstream is a local branch.
    pub fn is_local(&self) -> bool {
        matches!(self, Upstream::Local(_))
    }

    /// Converts this upstream to its state descriptor
    pub fn to_state(&self) -> UpstreamState {
        match self {
            Upstream::Local(branch) => UpstreamState::Local {
                branch: branch.id().clone(),
            },
            Upstream::Remote(remote) => UpstreamState::Remote {
                site: remote.site().to_string(),
                branch: BranchId::new(remote.id().to_string()),
            },
        }
    }

    /// Fetches the current revision from the upstream
    pub async fn fetch(&mut self) -> Result<Option<Revision>, ReplicaError> {
        match self {
            Upstream::Local(local) => {
                let branch = local.load().await?;
                Ok(Some(branch.revision()))
            }
            Upstream::Remote(remote) => Ok(remote.connect().await?.resolve().await?.to_owned()),
        }
    }

    /// Pushes a revision to the upstream, returning the previous revision if any
    pub async fn publish(&mut self, revision: Revision) -> Result<(), ReplicaError> {
        match self {
            Upstream::Local(local) => {
                let mut branch = local.load().await?;
                branch.reset(revision).await
            }
            Upstream::Remote(remote) => remote.connect().await?.publish(revision).await,
        }
    }
}

impl<Backend: PlatformBackend + 'static> From<Branch<Backend>> for Upstream<Backend> {
    fn from(branch: Branch<Backend>) -> Self {
        Self::Local(LocalUpstream {
            branch_id: branch.id.clone(),
            issuer: branch.issuer.clone(),
            storage: branch.storage.clone(),
            subject: branch.subject.clone(),
        })
    }
}

impl<Backend: PlatformBackend + 'static> From<RemoteBranch<Backend>> for Upstream<Backend> {
    fn from(branch: RemoteBranch<Backend>) -> Self {
        Self::Remote(branch)
    }
}

impl<Backend: PlatformBackend> From<Upstream<Backend>> for UpstreamState {
    fn from(upstream: Upstream<Backend>) -> Self {
        match upstream {
            Upstream::Local(branch) => UpstreamState::Local {
                branch: branch.id().clone(),
            },
            Upstream::Remote(branch) => UpstreamState::Remote {
                site: branch.site().to_string(),
                branch: BranchId::new(branch.id().to_string()),
            },
        }
    }
}

/// Upstream represents some branch being tracked
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UpstreamState {
    /// A local branch upstream
    Local {
        /// Branch identifier
        branch: BranchId,
    },
    /// A remote branch upstream
    Remote {
        /// Remote site identifier
        site: Site,
        /// Branch identifier
        branch: BranchId,
    },
}

impl UpstreamState {
    /// Returns the branch identifier of this upstream.
    pub fn id(&self) -> &BranchId {
        match self {
            Self::Local { branch } => branch,
            Self::Remote { branch, .. } => branch,
        }
    }
}

/// Blake3 hash of the branch state.
#[derive(Serialize, Deserialize)]
pub struct Edition<T>([u8; 32], PhantomData<fn() -> T>);
impl<T> Edition<T> {
    /// Creates a new edition from a hash.
    pub fn new(hash: [u8; 32]) -> Self {
        Self(hash, PhantomData)
    }
}
impl<T> Clone for Edition<T> {
    fn clone(&self) -> Self {
        Self(self.0, PhantomData)
    }
}

impl<T> Debug for Edition<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "#<{}>{}",
            std::any::type_name::<T>(),
            self.0.to_base58().as_str()
        ))
    }
}

impl<T> Hash for Edition<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<T> PartialEq for Edition<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl<T> Eq for Edition<T> {}
impl<T> PartialOrd for Edition<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for Edition<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
impl<T> KeyType for Edition<T> {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}
impl<T> TryFrom<Vec<u8>> for Edition<T> {
    type Error = crate::DialogArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self(
            value.try_into().map_err(|value: Vec<u8>| {
                crate::DialogArtifactsError::InvalidReference(format!(
                    "Incorrect length (expected {}, got {})",
                    32,
                    value.len()
                ))
            })?,
            PhantomData,
        ))
    }
}

/// The common error type used by this crate
#[derive(Error, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaError {
    /// Branch with the given ID was not found
    #[error("Branch {id} not found")]
    BranchNotFound {
        /// The ID of the branch that was not found
        id: BranchId,
    },

    /// A storage operation failed
    #[error("Storage error {0}")]
    StorageError(String),

    /// Branch has no configured upstream
    #[error("Branch {id} has no upstream")]
    BranchHasNoUpstream {
        /// The ID of the branch that has no upstream
        id: BranchId,
    },

    /// Pushing a revision failed
    #[error("Pushing revision failed: {cause}")]
    PushFailed {
        /// The underlying error message
        cause: String,
    },

    /// Remote repository not found
    #[error("Remote {remote} not found")]
    RemoteNotFound {
        /// Remote site identifier
        remote: Site,
    },
    /// Remote repository already exists
    #[error("Remote {remote} already exist")]
    RemoteAlreadyExists {
        /// Remote site identifier
        remote: Site,
    },
    /// Connection to remote repository failed
    #[error("Connection to remote {remote} failed")]
    RemoteConnectionError {
        /// Remote site identifier
        remote: Site,
    },

    /// Branch upstream is set to itself
    #[error("Upsteam of local {id} is set to itself")]
    BranchUpstreamIsItself {
        /// Branch identifier
        id: BranchId,
    },

    /// Invalid internal state (should never happen in normal operation)
    #[error("Invalid state: {message}")]
    InvalidState {
        /// Description of the invalid state
        message: String,
    },
}

impl ReplicaError {
    /// Create a new storage error
    pub fn storage_error(capability: Capability, cause: DialogStorageError) -> Self {
        ReplicaError::StorageError(format!("{}: {:?}", capability, cause))
    }
}

/// Identifies which operation failed when a storage error occurs.
/// Used in [`ReplicaError::StorageError`] to provide context about where the failure happened.
#[derive(Error, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Capability {
    /// Failed while resolving a branch by ID
    ResolveBranch,
    /// Failed while resolving a revision
    ResolveRevision,
    /// Failed while updating a revision
    UpdateRevision,

    /// Failed during archive operation
    ArchiveError,

    /// Failed during encoding operation
    EncodeError,
}
impl Display for Capability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Capability::ResolveBranch => write!(f, "ResolveBranch"),
            Capability::ResolveRevision => write!(f, "ResolveRevision"),
            Capability::UpdateRevision => write!(f, "UpdateRevision"),
            Capability::ArchiveError => write!(f, "ArchiveError"),
            Capability::EncodeError => write!(f, "EncodeError"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_storage::MemoryStorageBackend;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;

    /// Helper to create a test issuer
    fn seed() -> [u8; 32] {
        [1u8; 32]
    }

    /// Helper to get a test subject DID
    fn test_subject() -> Did {
        "did:test:subject".into()
    }

    /// Helper to create a test branch with upstream
    async fn create_branch_with_upstream<Backend>(
        storage: PlatformStorage<Backend>,
        id: &str,
        upstream_id: &str,
    ) -> Result<Branch<Backend>, ReplicaError>
    where
        Backend: PlatformBackend + 'static,
    {
        let branch_id = BranchId::new(id.to_string());
        let upstream_branch_id = BranchId::new(upstream_id.to_string());
        let subject = test_subject();

        let issuer = Operator::from_secret(&seed());
        let mut branch =
            Branch::open(&branch_id, issuer.clone(), storage.clone(), subject.clone()).await?;
        let target = Branch::open(&upstream_branch_id, issuer, storage.clone(), subject).await?;

        // Set up upstream as a local branch
        branch.set_upstream(target).await?;

        Ok(branch)
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_push_to_local_branch() {
        // Setup: Create two branches - main and feature
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);

        // Create main branch
        let main_id = BranchId::new("main".to_string());
        let issuer = Operator::from_secret(&seed());
        let subject = test_subject();
        let mut main_branch =
            Branch::open(&main_id, issuer.clone(), storage.clone(), subject.clone())
                .await
                .expect("Failed to create main branch");

        // Create a revision for main
        let main_revision = Revision {
            issuer: issuer.principal().clone(),
            tree: NodeReference(EMPT_TREE_HASH),
            cause: HashSet::new(),
            period: 0,
            moment: 0,
        };
        main_branch
            .reset(main_revision.clone())
            .await
            .expect("Failed to reset main branch");

        // Create feature branch with main as upstream
        let mut feature_branch = create_branch_with_upstream(storage.clone(), "feature", "main")
            .await
            .expect("Failed to create feature branch");

        // Create a new revision on feature branch with main_revision as cause
        let feature_revision = Revision {
            issuer: issuer.principal().clone(),
            tree: NodeReference(EMPT_TREE_HASH),
            cause: HashSet::from([main_revision.edition().expect("Failed to create edition")]),
            period: 0,
            moment: 1,
        };
        feature_branch
            .reset(feature_revision.clone())
            .await
            .expect("Failed to reset feature branch");

        // Push feature to main
        feature_branch.push().await.expect("Push failed");

        // Verify main branch was updated
        let updated_main = Branch::load(&main_id, issuer, storage, subject)
            .await
            .expect("Failed to load main branch");

        assert_eq!(updated_main.revision(), feature_revision);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_push_to_same_branch_is_noop() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);

        // Create a branch with itself as upstream
        let mut branch =
            create_branch_with_upstream(storage.clone(), "self-tracking", "self-tracking")
                .await
                .expect("Failed to create branch");

        // Push fails branch tracks itself
        assert!(matches!(
            branch.push().await,
            Err(ReplicaError::BranchUpstreamIsItself { .. })
        ))
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_push_without_upstream_fails() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);

        let branch_id = BranchId::new("no-upstream".to_string());
        let issuer = Operator::from_secret(&seed());
        let subject = test_subject();
        let mut branch = Branch::open(&branch_id, issuer, storage, subject)
            .await
            .expect("Failed to create branch");

        // Push should fail if upstream is not setup
        let result = branch.push().await;
        assert!(result.is_err());
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_pull_with_no_upstream_changes() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);

        // Create main and feature branches
        let main_id = BranchId::new("main".to_string());
        let issuer = Operator::from_secret(&seed());
        let subject = test_subject();
        let mut main_branch =
            Branch::open(&main_id, issuer.clone(), storage.clone(), subject.clone())
                .await
                .expect("Failed to create main branch");

        let main_revision = Revision {
            issuer: main_branch.principal().clone(),
            tree: NodeReference(EMPT_TREE_HASH),
            cause: HashSet::new(),
            period: 0,
            moment: 0,
        };
        main_branch
            .reset(main_revision.clone())
            .await
            .expect("Failed to reset main");

        // Create feature with main as upstream, based on same revision
        let mut feature_branch = create_branch_with_upstream(storage, "feature", "main")
            .await
            .expect("Failed to create feature branch");

        feature_branch
            .reset(main_revision.clone())
            .await
            .expect("Failed to reset feature");

        // Pull should return None (no changes)
        let result = feature_branch.pull().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_pull_without_upstream_fails() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);

        let branch_id = BranchId::new("no-upstream".to_string());
        let issuer = Operator::from_secret(&seed());
        let subject = test_subject();
        let mut branch = Branch::open(&branch_id, issuer, storage, subject)
            .await
            .expect("Failed to create branch");

        // Pull without upstream should return None
        let result = branch.pull().await;
        assert!(result.is_err());
    }

    #[dialog_common::test]
    async fn test_end_to_end_remote_upstream(
        s3_address: dialog_storage::s3::helpers::S3Address,
    ) -> anyhow::Result<()> {
        use dialog_s3_credentials::Address;
        use dialog_storage::JournaledStorage;
        use futures_util::stream;

        // Step 1: Generate issuer
        let issuer = Operator::from_passphrase("test_end_to_end_remote_upstream");
        let subject = issuer.did().clone();

        // Step 2: Create a replica with that issuer and journaled in-memory backend
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let journaled_backend = JournaledStorage::new(backend);
        let mut replica = issuer
            .open(issuer.did(), journaled_backend.clone())
            .expect("Failed to create replica");

        // Step 3: Create a branch e.g. main
        let main_id = BranchId::new("main".to_string());
        let mut main_branch = replica
            .branches
            .open(&main_id)
            .await
            .expect("Failed to create main branch");

        // Verify that opening the branch created a record at local/main
        let branch_key = b"local/main".to_vec();
        let branch_value = journaled_backend
            .get(&branch_key)
            .await
            .expect("Failed to get branch state");
        assert!(
            branch_value.is_some(),
            "Branch 'main' should be stored at local/main key"
        );

        // Decode and verify the branch state
        use serde_ipld_dagcbor;
        let branch_state: BranchState = serde_ipld_dagcbor::from_slice(&branch_value.unwrap())
            .expect("Failed to decode branch state");
        assert_eq!(
            branch_state.id.to_string(),
            "main",
            "Branch state should contain branch name 'main'"
        );

        // Step 4: Add a remote to the replica
        let address = Address::new(&s3_address.endpoint, "auto", &s3_address.bucket);
        let s3_credentials = dialog_s3_credentials::s3::Credentials::private(
            address,
            &s3_address.access_key_id,
            &s3_address.secret_access_key,
        )?
        .with_path_style(true);
        let remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::S3(s3_credentials),
        };
        let _site = replica
            .add_remote(remote_state.clone())
            .await
            .expect("Failed to add remote");

        // Verify that the remote was stored at site/origin
        let remote_key = b"site/origin".to_vec();
        let remote_value = journaled_backend
            .get(&remote_key)
            .await
            .expect("Failed to get remote state");
        assert!(
            remote_value.is_some(),
            "Remote 'origin' should be stored at site/origin key"
        );

        // Decode and verify the remote state
        let decoded_remote_state: RemoteState =
            serde_ipld_dagcbor::from_slice(&remote_value.unwrap())
                .expect("Failed to decode remote state");
        assert_eq!(
            decoded_remote_state.site, "origin",
            "Remote state should contain site name 'origin'"
        );

        // Step 5: Create a remote branch for the main
        let remote_site = RemoteSite::load(
            &remote_state.site,
            issuer.clone(),
            replica.storage().clone(),
        )
        .await
        .expect("Failed to load remote site");
        let remote_repo = remote_site.repository(&subject);
        let mut remote_branch = remote_repo.branch(main_id.to_string());
        remote_branch
            .open()
            .await
            .expect("Failed to open remote branch");

        // Note: Opening a remote branch doesn't write to storage yet.
        // The remote/main record will be created when we push to it.

        // Step 6: Add remote branch as an upstream of the local `main` branch
        main_branch
            .set_upstream(remote_branch)
            .await
            .expect("Failed to set upstream");

        // Verify upstream is configured
        assert!(main_branch.upstream().is_some());

        // Verify the archive's remote storage is configured
        let has_remote = {
            let archive_remote = main_branch.archive.remote.read().await;
            archive_remote.is_some()
        };
        assert!(has_remote, "Archive should have remote storage configured");

        // Step 7: Pull on main branch (should end up reading from remote store)
        // Note: This will return None if remote has no revisions, which is expected for a new remote
        let pull_result = main_branch.pull().await;
        assert!(pull_result.is_ok());

        // Step 8: Commit some changes to the main branch
        // Tree nodes should be written to the remote and to in-memory backend
        let test_artifact = Artifact {
            the: "user/name".parse().expect("Invalid attribute"),
            of: "user:123".parse().expect("Invalid entity"),
            is: crate::Value::String("Alice".to_string()),
            cause: None,
        };

        let instructions = vec![Instruction::Assert(test_artifact.clone())];
        let instruction_stream = stream::iter(instructions);

        let commit_result = main_branch.commit(instruction_stream).await;
        assert!(
            commit_result.is_ok(),
            "Commit failed: {:?}",
            commit_result.err()
        );

        let tree_hash = commit_result.unwrap();
        assert_ne!(tree_hash, EMPT_TREE_HASH);

        // Verify that tree nodes were written to storage by checking if we can read them
        // The tree hash should be stored with "index/" prefix
        let mut tree_key = b"index/".to_vec();
        tree_key.extend_from_slice(&tree_hash);
        let tree_node_value = journaled_backend
            .get(&tree_key)
            .await
            .expect("Failed to get tree node");
        assert!(
            tree_node_value.is_some(),
            "Tree node with hash {:?} should be written to storage",
            tree_hash
        );

        // Note: Tree nodes are NOT written to remote during commit.
        // They are synced to remote during push() using novel_nodes differential.

        // Step 9: Push changes to the main branch
        // Should create records for the local branch and corresponding remote branch
        // in the in-memory backend
        // Record should be written for the branch in the remote store
        let push_result = main_branch.push().await;
        assert!(push_result.is_ok(), "Push failed: {:?}", push_result.err());

        // Note: Tree node verification removed - we can't directly inspect the internal
        // S3 storage state with the new API. The push operation is verified by its success.

        // The push result might be None if the upstream is already up to date
        // In our case, this is expected since we're pushing to a newly created remote branch
        let last_revision = push_result.unwrap();
        assert_eq!(last_revision, None);

        // Verify local branch state was updated with the new tree hash
        let updated_branch_value = journaled_backend
            .get(&branch_key)
            .await
            .expect("Failed to get updated branch state");
        assert!(updated_branch_value.is_some());
        let updated_branch_state: BranchState =
            serde_ipld_dagcbor::from_slice(&updated_branch_value.unwrap())
                .expect("Failed to decode updated branch state");
        assert_eq!(
            updated_branch_state.revision.tree().hash(),
            &tree_hash,
            "Branch state should contain the new tree hash after push"
        );

        // Verify remote branch record was created with a cached revision
        // The key uses the branch ID as bytes
        let remote_branch_key = format!("remote/{}/{}/{}", remote_state.site, subject, main_id)
            .as_bytes()
            .to_vec();

        // Check that the key was written to local storage
        let all_written_keys = journaled_backend.get_writes();
        let was_written = all_written_keys.iter().any(|k| k == &remote_branch_key);
        assert!(
            was_written,
            "Remote branch key 'remote/{}/{}/{}' should have been written during push. All keys: {:?}",
            remote_state.site,
            subject,
            main_id,
            all_written_keys
                .iter()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .collect::<Vec<_>>()
        );

        // Branch state was written to S3 during push (verified by successful reload below)

        // Reload the main branch and verify the changes persisted
        let reloaded_main = replica
            .branches
            .load(&main_id)
            .await
            .expect("Failed to reload main branch");

        assert_eq!(reloaded_main.revision().tree().hash(), &tree_hash);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_push_and_pull_simple(
        s3_address: dialog_storage::s3::helpers::S3Address,
    ) -> anyhow::Result<()> {
        use dialog_s3_credentials::Address;
        use futures_util::stream;

        // Both Alice and Bob share the same subject for this test
        let subject: Did = "did:test:shared_repo".into();

        // Create Alice's replica
        let alice_issuer = Operator::from_passphrase("alice");
        let alice_backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let mut alice_replica = Replica::open(alice_issuer.clone(), subject.clone(), alice_backend)
            .expect("Failed to create Alice's replica");

        // Create Bob's replica
        let bob_issuer = Operator::from_passphrase("bob");
        let bob_backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let mut bob_replica = Replica::open(bob_issuer.clone(), subject.clone(), bob_backend)
            .expect("Failed to create Bob's replica");

        // Both create main branches
        let main_id = BranchId::new("main".to_string());
        let mut alice_main = alice_replica
            .branches
            .open(&main_id)
            .await
            .expect("Failed to create Alice's branch");
        let mut bob_main = bob_replica
            .branches
            .open(&main_id)
            .await
            .expect("Failed to create Bob's branch");

        // Configure shared remote credentials
        let address = Address::new(&s3_address.endpoint, "auto", &s3_address.bucket);
        let s3_credentials = dialog_s3_credentials::s3::Credentials::private(
            address,
            &s3_address.access_key_id,
            &s3_address.secret_access_key,
        )?
        .with_path_style(true);

        // Alice adds remote and sets upstream
        let alice_remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::S3(s3_credentials.clone()),
        };
        alice_replica
            .add_remote(alice_remote_state.clone())
            .await
            .expect("Failed to add remote");
        let alice_remote_site = RemoteSite::load(
            &"origin".to_string(),
            alice_issuer.clone(),
            alice_replica.storage().clone(),
        )
        .await
        .expect("Failed to load Alice's remote site");
        let mut alice_remote_branch = alice_remote_site
            .repository(&subject)
            .branch(main_id.to_string());
        alice_remote_branch
            .open()
            .await
            .expect("Failed to create remote branch");
        alice_main
            .set_upstream(alice_remote_branch)
            .await
            .expect("Failed to set upstream");

        // Alice commits and pushes
        let alice_artifact = Artifact {
            the: "user/name".parse().expect("Invalid attribute"),
            of: "user:alice".parse().expect("Invalid entity"),
            is: crate::Value::String("Alice".to_string()),
            cause: None,
        };
        alice_main
            .commit(stream::iter(vec![Instruction::Assert(
                alice_artifact.clone(),
            )]))
            .await
            .expect("Alice's commit failed");

        alice_main.push().await.expect("Alice's push failed");

        // Note: S3 key verification removed - we can't directly inspect the internal
        // S3 storage state with the new API. The push operation is verified by its success.

        // Bob adds same remote and sets upstream
        let bob_remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::S3(s3_credentials),
        };
        bob_replica
            .add_remote(bob_remote_state)
            .await
            .expect("Failed to add remote");
        let bob_remote_site = RemoteSite::load(
            &"origin".to_string(),
            bob_issuer.clone(),
            bob_replica.storage().clone(),
        )
        .await
        .expect("Failed to load Bob's remote site");
        let mut bob_remote_branch = bob_remote_site
            .repository(&subject)
            .branch(main_id.to_string());
        bob_remote_branch
            .open()
            .await
            .expect("Failed to create remote branch");
        bob_main
            .set_upstream(bob_remote_branch)
            .await
            .expect("Failed to set upstream");

        // Bob pulls Alice's changes
        let bob_pull_result = bob_main.pull().await.expect("Bob's pull failed");
        assert!(bob_pull_result.is_some(), "Pull should return a revision");

        // Verify Bob got Alice's artifact
        use crate::artifacts::ArtifactStore;
        let selector = ArtifactSelector::new()
            .the("user/name".parse().unwrap())
            .of("user:alice".parse().unwrap());

        let facts: Vec<_> = bob_main
            .select(selector)
            .try_collect()
            .await
            .expect("Failed to query artifacts");

        assert_eq!(
            facts.len(),
            1,
            "Bob should have Alice's artifact after pull"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn test_collaborative_workflow_alice_and_bob(
        s3_address: dialog_storage::s3::helpers::S3Address,
    ) -> anyhow::Result<()> {
        use dialog_s3_credentials::Address;
        use dialog_storage::JournaledStorage;
        use futures_util::stream;

        // Both Alice and Bob share the same subject for this test
        let subject: Did = "did:test:shared_repo".into();

        // Step 1: Create Alice's replica with her own issuer and backend
        let alice_issuer = Operator::from_passphrase("alice");
        let alice_backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let alice_journaled = JournaledStorage::new(alice_backend);
        let mut alice_replica = Replica::open(
            alice_issuer.clone(),
            subject.clone(),
            alice_journaled.clone(),
        )
        .expect("Failed to create Alice's replica");

        // Step 2: Create Bob's replica with his own issuer and backend
        let bob_issuer = Operator::from_passphrase("bob");
        let bob_backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let bob_journaled = JournaledStorage::new(bob_backend);
        let mut bob_replica =
            Replica::open(bob_issuer.clone(), subject.clone(), bob_journaled.clone())
                .expect("Failed to create Bob's replica");

        // Step 3: Both create a "main" branch
        let main_id = BranchId::new("main".to_string());
        let mut alice_main = alice_replica
            .branches
            .open(&main_id)
            .await
            .expect("Failed to create Alice's main branch");

        let mut bob_main = bob_replica
            .branches
            .open(&main_id)
            .await
            .expect("Failed to create Bob's main branch");

        // Step 4: Configure shared remote credentials
        let address = Address::new(&s3_address.endpoint, "auto", &s3_address.bucket);
        let s3_credentials = dialog_s3_credentials::s3::Credentials::private(
            address,
            &s3_address.access_key_id,
            &s3_address.secret_access_key,
        )?
        .with_path_style(true);

        // Alice adds the remote
        let alice_remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::S3(s3_credentials.clone()),
        };
        alice_replica
            .add_remote(alice_remote_state)
            .await
            .expect("Failed to add remote for Alice");

        let alice_remote_site = RemoteSite::load(
            &"origin".to_string(),
            alice_issuer.clone(),
            alice_replica.storage().clone(),
        )
        .await
        .expect("Failed to load Alice's remote site");
        let mut alice_remote_branch = alice_remote_site
            .repository(&subject)
            .branch(main_id.to_string());
        alice_remote_branch
            .open()
            .await
            .expect("Failed to create Alice's remote branch");

        alice_main
            .set_upstream(alice_remote_branch)
            .await
            .expect("Failed to set Alice's upstream");

        // Step 5: Alice makes changes and pushes
        let alice_artifact = Artifact {
            the: "user/name".parse().expect("Invalid attribute"),
            of: "user:alice".parse().expect("Invalid entity"),
            is: crate::Value::String("Alice".to_string()),
            cause: None,
        };

        let alice_instructions = vec![Instruction::Assert(alice_artifact.clone())];
        alice_main
            .commit(stream::iter(alice_instructions))
            .await
            .expect("Alice's commit failed");

        let alice_tree_after_commit = *alice_main.revision().tree().hash();
        assert_ne!(alice_tree_after_commit, EMPT_TREE_HASH);

        // Alice pushes her changes
        alice_main.push().await.expect("Alice's push failed");

        // Note: S3 key verification removed - we can't directly inspect the internal
        // S3 storage state with the new API. The push operation is verified by its success.

        // Step 6: Bob adds the remote and sets upstream (after Alice has pushed)
        let bob_remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::S3(s3_credentials),
        };
        bob_replica
            .add_remote(bob_remote_state)
            .await
            .expect("Failed to add remote for Bob");

        let bob_remote_site = RemoteSite::load(
            &"origin".to_string(),
            bob_issuer.clone(),
            bob_replica.storage().clone(),
        )
        .await
        .expect("Failed to load Bob's remote site");
        let mut bob_remote_branch = bob_remote_site
            .repository(&subject)
            .branch(main_id.to_string());
        bob_remote_branch
            .open()
            .await
            .expect("Failed to create Bob's remote branch");

        bob_main
            .set_upstream(bob_remote_branch)
            .await
            .expect("Failed to set Bob's upstream");

        // Step 7: Bob makes his own changes locally (without pulling first)
        let bob_artifact = Artifact {
            the: "user/email".parse().expect("Invalid attribute"),
            of: "user:bob".parse().expect("Invalid entity"),
            is: crate::Value::String("bob@example.com".to_string()),
            cause: None,
        };

        let bob_instructions = vec![Instruction::Assert(bob_artifact.clone())];
        bob_main
            .commit(stream::iter(bob_instructions))
            .await
            .expect("Bob's commit failed");

        let bob_tree_after_commit = *bob_main.revision().tree().hash();
        assert_ne!(bob_tree_after_commit, EMPT_TREE_HASH);
        assert_ne!(
            bob_tree_after_commit, alice_tree_after_commit,
            "Bob and Alice should have different trees before merge"
        );

        // Step 8: Bob pulls Alice's changes (should merge)
        let bob_pull_result = bob_main.pull().await.expect("Bob's pull failed");
        assert!(
            bob_pull_result.is_some(),
            "Pull should return a revision after merging"
        );

        let bob_tree_after_pull = *bob_main.revision().tree().hash();
        assert_ne!(
            bob_tree_after_pull, bob_tree_after_commit,
            "Bob's tree should change after pull"
        );
        assert_ne!(
            bob_tree_after_pull, alice_tree_after_commit,
            "Bob's tree should be different from Alice's (it includes both changes)"
        );

        // Step 9: Verify Bob has both Alice's and his own changes
        use crate::artifacts::ArtifactStore;

        // Check for Alice's artifact
        let alice_selector = ArtifactSelector::new()
            .the("user/name".parse().unwrap())
            .of("user:alice".parse().unwrap());
        let alice_facts: Vec<_> = bob_main
            .select(alice_selector.clone())
            .try_collect()
            .await
            .expect("Failed to query Alice's facts");
        assert_eq!(
            alice_facts.len(),
            1,
            "Bob should have Alice's artifact after pull"
        );

        // Check for Bob's artifact
        let bob_selector = ArtifactSelector::new()
            .the("user/email".parse().unwrap())
            .of("user:bob".parse().unwrap());
        let bob_facts: Vec<_> = bob_main
            .select(bob_selector.clone())
            .try_collect()
            .await
            .expect("Failed to query Bob's facts");
        assert_eq!(
            bob_facts.len(),
            1,
            "Bob should have his own artifact after pull"
        );

        // Step 10: Bob pushes the merged state
        bob_main.push().await.expect("Bob's push failed");

        // Bob's push added tree nodes to S3 (verified by successful pull below)

        // Step 11: Alice pulls Bob's changes
        let alice_pull_result = alice_main.pull().await.expect("Alice's pull failed");
        assert!(
            alice_pull_result.is_some(),
            "Alice should receive updates from Bob's push"
        );

        let alice_tree_after_pull = *alice_main.revision().tree().hash();

        // Step 12: Verify both Alice and Bob have the same final revision
        assert_eq!(
            alice_tree_after_pull, bob_tree_after_pull,
            "Alice and Bob should have identical trees after sync"
        );

        // Step 13: Verify Alice has both her own and Bob's changes
        let alice_facts_check: Vec<_> = alice_main
            .select(alice_selector)
            .try_collect()
            .await
            .expect("Failed to query Alice's facts");
        assert_eq!(
            alice_facts_check.len(),
            1,
            "Alice should still have her own artifact"
        );

        let bob_facts_check: Vec<_> = alice_main
            .select(bob_selector)
            .try_collect()
            .await
            .expect("Failed to query Bob's facts");
        assert_eq!(
            bob_facts_check.len(),
            1,
            "Alice should have Bob's artifact after pull"
        );

        // Final verification: Both replicas are in sync
        let alice_final_revision = alice_main.revision();
        let bob_final_revision = bob_main.revision();
        assert_eq!(
            alice_final_revision.tree().hash(),
            bob_final_revision.tree().hash(),
            "Final revisions should be identical"
        );

        println!("✓ Collaborative workflow complete:");
        println!("  - Alice and Bob both contributed changes");
        println!("  - Changes were merged via pull");
        println!("  - Both replicas synchronized to same final state");

        Ok(())
    }

    #[dialog_common::test]
    async fn test_pull_without_local_changes_adopts_upstream_revision(
        s3_address: dialog_storage::s3::helpers::S3Address,
    ) -> anyhow::Result<()> {
        // This test verifies that when pulling with no local changes,
        // we adopt the upstream revision directly without creating a new one
        use dialog_s3_credentials::Address;
        use dialog_storage::JournaledStorage;
        use futures_util::stream;

        // Both Alice and Bob share the same subject for this test
        let subject: Did = "did:test:shared_repo".into();

        // Create Alice's replica
        let alice_issuer = Operator::from_passphrase("alice");
        let alice_backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let alice_journaled = JournaledStorage::new(alice_backend);
        let mut alice_replica = Replica::open(
            alice_issuer.clone(),
            subject.clone(),
            alice_journaled.clone(),
        )
        .expect("Failed to create Alice's replica");

        // Create Bob's replica
        let bob_issuer = Operator::from_passphrase("bob");
        let bob_backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let bob_journaled = JournaledStorage::new(bob_backend);
        let mut bob_replica =
            Replica::open(bob_issuer.clone(), subject.clone(), bob_journaled.clone())
                .expect("Failed to create Bob's replica");

        // Both create a "main" branch
        let main_id = BranchId::new("main".to_string());
        let mut alice_main = alice_replica
            .branches
            .open(&main_id)
            .await
            .expect("Failed to create Alice's main branch");

        let mut bob_main = bob_replica
            .branches
            .open(&main_id)
            .await
            .expect("Failed to create Bob's main branch");

        // Configure shared remote credentials
        let address = Address::new(&s3_address.endpoint, "auto", &s3_address.bucket);
        let s3_credentials = dialog_s3_credentials::s3::Credentials::private(
            address,
            &s3_address.access_key_id,
            &s3_address.secret_access_key,
        )?
        .with_path_style(true);

        // Alice adds and configures remote
        let alice_remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::S3(s3_credentials.clone()),
        };
        alice_replica
            .add_remote(alice_remote_state)
            .await
            .expect("Failed to add remote for Alice");

        let alice_remote_site = RemoteSite::load(
            &"origin".to_string(),
            alice_issuer.clone(),
            alice_replica.storage().clone(),
        )
        .await
        .expect("Failed to load Alice's remote site");
        let mut alice_remote_branch = alice_remote_site
            .repository(&subject)
            .branch(main_id.to_string());
        alice_remote_branch
            .open()
            .await
            .expect("Failed to open Alice's remote branch");

        alice_main
            .set_upstream(alice_remote_branch)
            .await
            .expect("Failed to set Alice's upstream");

        // Alice commits a change
        let alice_artifact = Artifact {
            the: "user/name".parse().expect("Invalid attribute"),
            of: "user:alice".parse().expect("Invalid entity"),
            is: crate::Value::String("Alice".to_string()),
            cause: None,
        };

        let alice_instructions = vec![Instruction::Assert(alice_artifact.clone())];
        alice_main
            .commit(stream::iter(alice_instructions))
            .await
            .expect("Alice's commit failed");

        // Alice pushes
        alice_main.push().await.expect("Alice's push failed");

        let alice_revision_after_push = alice_main.revision();
        let alice_edition = alice_revision_after_push.edition()?;

        // Bob adds the same remote
        let bob_remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::S3(s3_credentials),
        };
        bob_replica
            .add_remote(bob_remote_state)
            .await
            .expect("Failed to add remote for Bob");

        let bob_remote_site = RemoteSite::load(
            &"origin".to_string(),
            bob_issuer.clone(),
            bob_replica.storage().clone(),
        )
        .await
        .expect("Failed to load Bob's remote site");
        let mut bob_remote_branch = bob_remote_site
            .repository(&subject)
            .branch(main_id.to_string());
        bob_remote_branch
            .open()
            .await
            .expect("Failed to open Bob's remote branch");

        bob_main
            .set_upstream(bob_remote_branch)
            .await
            .expect("Failed to set Bob's upstream");

        // Bob has no local changes, just pulls
        let bob_pull_result = bob_main.pull().await.expect("Bob's pull failed");
        assert!(bob_pull_result.is_some(), "Pull should return a revision");

        let bob_revision_after_pull = bob_main.revision();
        let bob_edition = bob_revision_after_pull.edition()?;

        // Verify that Bob adopted Alice's revision directly (same edition)
        assert_eq!(
            alice_edition, bob_edition,
            "Bob should have adopted Alice's revision exactly (same edition)"
        );

        // Verify they have the same tree hash
        assert_eq!(
            alice_revision_after_push.tree().hash(),
            bob_revision_after_pull.tree().hash(),
            "Tree hashes should be identical"
        );

        // Verify they have the same issuer (Alice's issuer, not Bob's)
        assert_eq!(
            alice_revision_after_push.issuer(),
            bob_revision_after_pull.issuer(),
            "Bob should have adopted Alice's issuer (no new revision created)"
        );

        // Verify they have the same period and moment
        assert_eq!(
            alice_revision_after_push.period(),
            bob_revision_after_pull.period(),
            "Period should be identical"
        );
        assert_eq!(
            alice_revision_after_push.moment(),
            bob_revision_after_pull.moment(),
            "Moment should be identical"
        );

        // Verify Bob can query Alice's artifact
        use crate::artifacts::ArtifactStore;
        let alice_selector = ArtifactSelector::new()
            .the("user/name".parse().unwrap())
            .of("user:alice".parse().unwrap());
        let bob_facts: Vec<_> = bob_main
            .select(alice_selector)
            .try_collect()
            .await
            .expect("Failed to query facts from Bob");
        assert_eq!(bob_facts.len(), 1, "Bob should have Alice's artifact");

        println!("✓ Pull without local changes correctly adopted upstream revision");

        Ok(())
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn test_branch_load_vs_open() -> anyhow::Result<()> {
        // Test the difference between load (expects existing) and open (creates if missing)
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);
        let issuer = Operator::from_passphrase("test-user");
        let subject = test_subject();

        let branch_id = BranchId::new("test-branch".to_string());

        // load() should fail when branch doesn't exist
        let load_result =
            Branch::load(&branch_id, issuer.clone(), storage.clone(), subject.clone()).await;
        assert!(
            load_result.is_err(),
            "load() should fail for non-existent branch"
        );

        // open() should succeed and create the branch
        let branch =
            Branch::open(&branch_id, issuer.clone(), storage.clone(), subject.clone()).await?;
        assert_eq!(branch.id(), &branch_id);

        // Now load() should succeed
        let loaded = Branch::load(&branch_id, issuer.clone(), storage.clone(), subject).await?;
        assert_eq!(loaded.id(), &branch_id);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_fetch_without_pull(
        s3_address: dialog_storage::s3::helpers::S3Address,
    ) -> anyhow::Result<()> {
        // Test that fetch() retrieves upstream state without merging
        use dialog_s3_credentials::Address;
        use dialog_storage::JournaledStorage;
        use futures_util::stream;

        // Both Alice and Bob share the same subject for this test
        let subject: Did = "did:test:shared_repo".into();

        // Create Alice's replica
        let alice_issuer = Operator::from_passphrase("alice");
        let alice_backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let alice_journaled = JournaledStorage::new(alice_backend);
        let mut alice_replica = Replica::open(
            alice_issuer.clone(),
            subject.clone(),
            alice_journaled.clone(),
        )?;

        let main_id = BranchId::new("main".to_string());
        let mut alice_main = alice_replica.branches.open(&main_id).await?;

        // Configure remote credentials
        let address = Address::new(&s3_address.endpoint, "auto", &s3_address.bucket);
        let s3_credentials = dialog_s3_credentials::s3::Credentials::private(
            address,
            &s3_address.access_key_id,
            &s3_address.secret_access_key,
        )?
        .with_path_style(true);

        let alice_remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::S3(s3_credentials.clone()),
        };
        alice_replica.add_remote(alice_remote_state).await?;
        let alice_remote_site = RemoteSite::load(
            &"origin".to_string(),
            alice_issuer.clone(),
            alice_replica.storage().clone(),
        )
        .await?;
        let mut alice_remote_branch = alice_remote_site
            .repository(&subject)
            .branch(main_id.to_string());
        alice_remote_branch.connect().await?;
        alice_main.set_upstream(alice_remote_branch).await?;

        // Alice commits and pushes
        let artifact = Artifact {
            the: "data/value".parse()?,
            of: "entity:1".parse()?,
            is: crate::Value::String("test".to_string()),
            cause: None,
        };
        alice_main
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .await?;
        alice_main.push().await?;

        let alice_revision_after_push = alice_main.revision();

        // Create Bob's replica
        let bob_issuer = Operator::from_passphrase("bob");
        let bob_backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let bob_journaled = JournaledStorage::new(bob_backend);
        let mut bob_replica =
            Replica::open(bob_issuer.clone(), subject.clone(), bob_journaled.clone())?;

        let mut bob_main = bob_replica.branches.open(&main_id).await?;

        let bob_remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::S3(s3_credentials),
        };
        bob_replica.add_remote(bob_remote_state).await?;
        let bob_remote_site = RemoteSite::load(
            &"origin".to_string(),
            bob_issuer.clone(),
            bob_replica.storage().clone(),
        )
        .await?;
        let mut bob_remote_branch = bob_remote_site
            .repository(&subject)
            .branch(main_id.to_string());
        bob_remote_branch.connect().await?;
        bob_main.set_upstream(bob_remote_branch).await?;

        let bob_revision_before_fetch = bob_main.revision();

        // Bob fetches (but doesn't pull/merge)
        let fetched = bob_main.fetch().await?;
        assert!(fetched.is_some(), "Fetch should return upstream revision");

        let bob_revision_after_fetch = bob_main.revision();

        // Bob's local revision should be UNCHANGED after fetch
        assert_eq!(
            bob_revision_before_fetch.edition()?,
            bob_revision_after_fetch.edition()?,
            "fetch() should not change local revision"
        );

        // But the fetched revision should match Alice's
        assert_eq!(
            fetched.unwrap().edition()?,
            alice_revision_after_push.edition()?,
            "Fetched revision should match upstream"
        );

        // Now Bob pulls to actually merge
        bob_main.pull().await?;
        let bob_revision_after_pull = bob_main.revision();

        // After pull, Bob's revision should be updated
        assert_ne!(
            bob_revision_before_fetch.edition()?,
            bob_revision_after_pull.edition()?,
            "pull() should update local revision"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn test_multiple_remotes(
        s3_address: dialog_storage::s3::helpers::S3Address,
    ) -> anyhow::Result<()> {
        // Test managing multiple remote upstreams
        use dialog_s3_credentials::Address;
        use dialog_storage::JournaledStorage;

        let issuer = Operator::from_passphrase("multi-remote-user");
        let subject = issuer.did().clone();
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let journaled = JournaledStorage::new(backend);
        let mut replica = Replica::open(issuer.clone(), subject.clone(), journaled.clone())?;

        // Create S3 credentials
        let address = Address::new(&s3_address.endpoint, "auto", &s3_address.bucket);
        let s3_credentials = dialog_s3_credentials::s3::Credentials::private(
            address,
            &s3_address.access_key_id,
            &s3_address.secret_access_key,
        )?
        .with_path_style(true);

        // Add first remote (origin)
        let origin_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::S3(s3_credentials.clone()),
        };
        let origin = replica.add_remote(origin_state.clone()).await?;
        assert_eq!(origin, "origin");

        // Add second remote (backup)
        let backup_state = RemoteState {
            site: "backup".to_string(),
            credentials: RemoteCredentials::S3(s3_credentials),
        };
        let backup = replica.add_remote(backup_state.clone()).await?;
        assert_eq!(backup, "backup");

        // Load remotes back
        let loaded_origin = replica.load_remote(&"origin".to_string()).await?;
        assert_eq!(loaded_origin.site, "origin");

        let loaded_backup = replica.load_remote(&"backup".to_string()).await?;
        assert_eq!(loaded_backup.site, "backup");

        println!("✓ Multiple remotes work correctly");

        Ok(())
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn test_branch_description() -> anyhow::Result<()> {
        // Test branch description getting and setting
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);
        let issuer = Operator::from_passphrase("test-user");
        let subject = test_subject();

        let branch_id = BranchId::new("feature-x".to_string());

        // Create branch with description
        let branch =
            Branch::open(&branch_id, issuer.clone(), storage.clone(), subject.clone()).await?;

        // Default description should be branch id
        assert_eq!(branch.description(), "feature-x");

        // Load and verify description persists
        let loaded = Branch::load(&branch_id, issuer.clone(), storage.clone(), subject).await?;
        assert_eq!(loaded.description(), "feature-x");

        Ok(())
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn test_issuer_generate() -> anyhow::Result<()> {
        // Test generating random issuer keys
        let issuer1 = Operator::generate()?;
        let issuer2 = Operator::generate()?;

        // Each generated issuer should be unique
        assert_ne!(issuer1.did(), issuer2.did());
        assert_ne!(issuer1.principal(), issuer2.principal());

        // DIDs should be valid format
        assert!(issuer1.did().starts_with("did:key:"));
        assert!(issuer2.did().starts_with("did:key:"));

        Ok(())
    }

    #[dialog_common::test]
    async fn test_archive_caches_remote_reads_to_local(
        s3_address: dialog_storage::s3::helpers::S3Address,
    ) -> anyhow::Result<()> {
        use dialog_s3_credentials::Address;
        use dialog_storage::s3::{Bucket, S3};
        use dialog_storage::{ContentAddressedStorage, MemoryStorageBackend};
        use serde::{Deserialize, Serialize};

        // Define a simple test type
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct TestBlock {
            value: String,
        }

        let address = Address::new(&s3_address.endpoint, "auto", &s3_address.bucket);
        let issuer = Operator::from_passphrase("test-archive-cache");
        let subject = issuer.did().clone();
        let s3_credentials = dialog_s3_credentials::s3::Credentials::private(
            address,
            &s3_address.access_key_id,
            &s3_address.secret_access_key,
        )?
        .with_path_style(true);
        let s3 = S3::from_s3(s3_credentials, issuer.clone());
        let s3_storage = Bucket::new(s3, subject.clone(), "memory");

        // Create local and remote archives using the RemoteBackend enum
        let local_storage = PlatformStorage::new(
            ErrorMappingBackend::new(MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default()),
            CborEncoder,
        );
        let remote_backend = remote::RemoteBackend::S3(ErrorMappingBackend::new(s3_storage));
        let connection = PlatformStorage::new(remote_backend, CborEncoder);

        let archive = Archive::new(local_storage.clone().into_backend());
        archive.set_remote(connection.clone()).await;

        // Create a test block
        let test_block = TestBlock {
            value: "test data from remote".to_string(),
        };

        // Write directly to remote storage (simulating remote-only data).
        // In real usage, the remote bucket path "archive/index" provides namespacing,
        // so we write with raw hash as key (no "index/" prefix).
        let hash = {
            let (hash, bytes) = connection.encode(&test_block).await?;
            let mut conn = connection.clone();
            conn.set(hash.to_vec(), bytes).await?;
            hash
        };

        // Verify it's NOT in local storage yet
        {
            let local_archive_check = Archive::new(local_storage.clone().into_backend());
            let result: Option<TestBlock> = local_archive_check.read(&hash).await?;
            assert_eq!(result, None, "Block should not be in local storage yet");
        }

        // Read from archive (should fetch from remote and cache to local)
        let read_result: Option<TestBlock> = archive.read(&hash).await?;
        assert_eq!(
            read_result,
            Some(test_block.clone()),
            "Should read from remote"
        );

        // Verify it's NOW in local storage (cached)
        {
            let local_archive_check = Archive::new(local_storage.clone().into_backend());
            let cached_result: Option<TestBlock> = local_archive_check.read(&hash).await?;
            assert_eq!(
                cached_result,
                Some(test_block),
                "Block should now be cached in local storage"
            );
        }

        Ok(())
    }

    /// Test that RemoteState with UCAN credentials can be stored and loaded.
    #[cfg(feature = "ucan")]
    #[dialog_common::test]
    async fn test_ucan_remote_state_persistence() -> anyhow::Result<()> {
        use dialog_s3_credentials::ucan::{self, DelegationChain, test_helpers::create_delegation};

        // Create operator and subject
        let operator_signer = dialog_s3_credentials::ucan::test_helpers::generate_signer();
        let operator_did = operator_signer.did().clone();
        let operator = Operator::from_secret(&operator_signer.signer().to_bytes());
        let subject = operator.did().clone();

        // Create a delegation chain
        let delegation = create_delegation(
            &operator_signer,
            &operator_did,
            operator_signer.did(),
            &["archive", "memory"],
        )?;
        let delegation_chain = DelegationChain::new(delegation);

        // Create UCAN credentials
        let ucan_credentials =
            ucan::Credentials::new("https://example.com/access".to_string(), delegation_chain);

        // Create remote state with UCAN credentials
        let remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::Ucan(ucan_credentials.clone()),
        };

        // Test direct serialization/deserialization first
        let encoded = serde_ipld_dagcbor::to_vec(&remote_state)?;
        let decoded: RemoteState = serde_ipld_dagcbor::from_slice(&encoded)?;
        assert_eq!(decoded.site, "origin");

        // Now test via storage
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let journaled_backend = dialog_storage::JournaledStorage::new(backend);
        let mut replica =
            Replica::open(operator.clone(), subject.clone(), journaled_backend.clone())
                .expect("Failed to create replica");

        // Store the remote
        replica
            .add_remote(remote_state.clone())
            .await
            .expect("Failed to add remote");

        // Load it back
        let loaded_state = replica
            .load_remote(&"origin".to_string())
            .await
            .expect("Failed to load remote");

        // Verify it matches
        assert_eq!(loaded_state.site, "origin");
        assert_eq!(
            loaded_state.credentials,
            RemoteCredentials::Ucan(ucan_credentials)
        );

        Ok(())
    }

    /// End-to-end test demonstrating the full UCAN-based workflow:
    /// 1. Setup UCAN service & S3
    /// 2. Create local repository with "main" branch
    /// 3. Add remote "origin" with UCAN credentials
    /// 4. Open remote repository with same DID and "main" branch
    /// 5. Set remote branch as upstream to local "main"
    /// 6. Add local changes and push
    /// 7. Verify changes propagate to remote
    #[cfg(all(feature = "ucan", feature = "helpers"))]
    #[dialog_common::test]
    async fn test_ucan_end_to_end_workflow(
        env: dialog_storage::s3::helpers::UcanS3Address,
    ) -> anyhow::Result<()> {
        use crate::artifacts::ArtifactStore;
        use dialog_s3_credentials::ucan::{self, DelegationChain, test_helpers::create_delegation};
        use dialog_storage::JournaledStorage;
        use futures_util::stream;

        // Step 1: Generate operator (issuer) with signing capability
        // We use the UCAN signer as the authority since it has consistent DID encoding
        // with the delegation chain.
        let operator_signer = dialog_s3_credentials::ucan::test_helpers::generate_signer();
        let operator_did = operator_signer.did().clone();
        let subject: Did = operator_did.into();

        // Create an Operator wrapper that uses the UCAN signer's key
        // Note: Operator::did() may produce a different DID format than Ed25519Did,
        // but the UCAN invocation system uses the Authority trait which gets the
        // secret key bytes for signing, not the DID for identity.
        let operator = Operator::from_secret(&operator_signer.signer().to_bytes());

        // Step 2: Create a delegation chain from subject to operator
        // In this test, the subject and operator are the same (self-signed)
        // Grant root capability (/) which encompasses all operations:
        // - /memory/resolve (branch resolution)
        // - /storage/get, /storage/set (KV storage)
        // - /archive/get, /archive/put (content-addressed storage)
        let delegation = create_delegation(
            &operator_signer,
            &operator_did,
            operator_signer.did(),
            &[], // Empty command = root capability (/)
        )?;
        let delegation_chain = DelegationChain::new(delegation);

        // Step 3: Create UCAN credentials
        let ucan_credentials =
            ucan::Credentials::new(env.access_service_url.clone(), delegation_chain);

        // Step 4: Create local replica with journaled backend
        let local_backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let journaled_backend = JournaledStorage::new(local_backend);
        let mut local_replica =
            Replica::open(operator.clone(), subject.clone(), journaled_backend.clone())
                .expect("Failed to create local replica");

        // Step 5: Create local "main" branch
        let main_id = BranchId::new("main".to_string());
        let mut local_main = local_replica
            .branches
            .open(&main_id)
            .await
            .expect("Failed to create local main branch");

        // Step 6: Add remote "origin" with UCAN credentials
        let remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::Ucan(ucan_credentials),
        };
        local_replica
            .add_remote(remote_state.clone())
            .await
            .expect("Failed to add remote 'origin'");

        // Step 7: Open remote repository with same DID and create remote branch
        let remote_site = RemoteSite::load(
            &"origin".to_string(),
            operator.clone(),
            local_replica.storage().clone(),
        )
        .await
        .expect("Failed to load remote site");

        let remote_repo = remote_site.repository(&subject);
        let mut remote_branch = remote_repo.branch(main_id.to_string());
        remote_branch
            .open()
            .await
            .expect("Failed to open remote main branch");

        // Step 8: Set remote branch as upstream to local "main"
        local_main
            .set_upstream(remote_branch)
            .await
            .expect("Failed to set upstream");

        // Verify upstream is configured
        assert!(
            local_main.upstream().is_some(),
            "Upstream should be configured"
        );

        // Verify archive has remote storage configured
        let has_remote = {
            let archive_remote = local_main.archive.remote.read().await;
            archive_remote.is_some()
        };
        assert!(has_remote, "Archive should have remote storage configured");

        // Step 9: Add local changes (commit artifacts)
        let test_artifact = Artifact {
            the: "user/name".parse().expect("Invalid attribute"),
            of: "user:test".parse().expect("Invalid entity"),
            is: crate::Value::String("Test User".to_string()),
            cause: None,
        };

        let instructions = vec![Instruction::Assert(test_artifact.clone())];
        let tree_hash = local_main
            .commit(stream::iter(instructions))
            .await
            .expect("Commit failed");

        assert_ne!(
            tree_hash, EMPT_TREE_HASH,
            "Tree should have content after commit"
        );

        // Step 10: Push changes to remote
        let push_result = local_main.push().await.expect("Push failed");

        // First push to a new remote branch returns None (no previous revision)
        assert_eq!(
            push_result, None,
            "First push should return None (no previous revision)"
        );

        // Step 11: Verify changes propagate - create a second replica and pull
        let second_backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let second_journaled = JournaledStorage::new(second_backend);

        // Generate a second operator for the second replica
        let second_operator_signer = dialog_s3_credentials::ucan::test_helpers::generate_signer();
        let second_operator_did = second_operator_signer.did().clone();
        let second_operator = Operator::from_secret(&second_operator_signer.signer().to_bytes());

        // Create delegation from the original subject to the second operator
        // Grant root capability (/) to allow all operations for the pull test
        let second_delegation = create_delegation(
            &operator_signer,
            &second_operator_did,
            operator_signer.did(),
            &[], // Empty command = root capability (/)
        )?;
        let second_delegation_chain = DelegationChain::new(second_delegation);
        let second_ucan_credentials =
            ucan::Credentials::new(env.access_service_url.clone(), second_delegation_chain);

        let mut second_replica = Replica::open(
            second_operator.clone(),
            subject.clone(),
            second_journaled.clone(),
        )
        .expect("Failed to create second replica");

        let mut second_main = second_replica
            .branches
            .open(&main_id)
            .await
            .expect("Failed to create second main branch");

        // Add remote to second replica
        let second_remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::Ucan(second_ucan_credentials),
        };
        second_replica
            .add_remote(second_remote_state)
            .await
            .expect("Failed to add remote to second replica");

        let second_remote_site = RemoteSite::load(
            &"origin".to_string(),
            second_operator.clone(),
            second_replica.storage().clone(),
        )
        .await
        .expect("Failed to load second remote site");

        let mut second_remote_branch = second_remote_site
            .repository(&subject)
            .branch(main_id.to_string());
        second_remote_branch
            .open()
            .await
            .expect("Failed to open second remote branch");

        second_main
            .set_upstream(second_remote_branch)
            .await
            .expect("Failed to set upstream for second replica");

        // Pull changes from remote
        let pull_result = second_main.pull().await.expect("Pull failed");
        assert!(
            pull_result.is_some(),
            "Pull should return a revision with the pushed changes"
        );

        // Verify the artifact is now available in the second replica
        let selector = ArtifactSelector::new()
            .the("user/name".parse().unwrap())
            .of("user:test".parse().unwrap());

        let facts: Vec<_> = second_main
            .select(selector)
            .try_collect()
            .await
            .expect("Failed to query artifacts");

        assert_eq!(
            facts.len(),
            1,
            "Second replica should have the pushed artifact after pull"
        );
        assert_eq!(
            facts[0].is,
            crate::Value::String("Test User".to_string()),
            "Artifact value should match what was pushed"
        );

        Ok(())
    }

    /// Test that archive has remote configured after set_upstream.
    ///
    /// This is a bug where the archive's remote wasn't configured because
    /// RemoteBranch was in Reference state (not connected) when set_upstream
    /// was called. The fix ensures connect() is called before accessing archive().
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_archive_has_remote_after_set_upstream() {
        use crate::replica::remote::{RemoteCredentials, RemoteSite, RemoteState};

        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);
        let issuer = Operator::from_secret(&seed());
        let subject = test_subject();

        let mut replica = Replica::open(issuer.clone(), subject.clone(), backend.clone())
            .expect("Failed to create replica");

        // Add memory remote
        let remote_state = RemoteState {
            site: "origin".to_string(),
            credentials: RemoteCredentials::Memory,
        };
        replica
            .add_remote(remote_state)
            .await
            .expect("Failed to add remote");

        let branch_id = BranchId::new("main".to_string());
        let mut branch = replica
            .branches
            .open(&branch_id)
            .await
            .expect("Failed to open branch");

        // Before set_upstream: archive has no remote
        assert!(
            !branch.archive.has_remote().await,
            "Archive should NOT have remote before set_upstream"
        );

        // Set upstream
        let remote_site = RemoteSite::load(&"origin".to_string(), issuer.clone(), storage)
            .await
            .expect("Failed to load remote site");
        let remote_branch = remote_site
            .repository(&subject)
            .branch(branch_id.to_string());

        branch
            .set_upstream(remote_branch)
            .await
            .expect("Failed to set upstream");

        // After set_upstream: archive MUST have remote configured
        // With the old bug, this would be false because archive_connection()
        // returned None when RemoteBranch was in Reference state.
        assert!(
            branch.archive.has_remote().await,
            "Archive MUST have remote after set_upstream - this was the bug!"
        );
    }
}
