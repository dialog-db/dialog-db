#![allow(missing_docs)]

use super::platform::Storage as PlatformStorage;
use super::platform::{ErrorMappingBackend, PlatformBackend, TypedStore, TypedStoreResource};
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
use dialog_common::ConditionalSend;
#[cfg(test)]
use dialog_common::ConditionalSync;
use dialog_prolly_tree::{EMPT_TREE_HASH, Entry, GeometricDistribution, KeyType, Tree};
use futures_util::{Stream, StreamExt, TryStreamExt, future::BoxFuture};

use dialog_storage::{
    Blake3Hash, CborEncoder, DialogStorageError, Encoder, Resource, RestStorageBackend,
    RestStorageConfig, StorageBackend,
};
use ed25519_dalek::ed25519::signature::SignerMut;
use ed25519_dalek::{SECRET_KEY_LENGTH, Signature, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

/// Cryptographic identifier like Ed25519 public key representing
/// an principal that produced a change. We may
pub type Principal = [u8; 32];

/// Type alias for the prolly tree index used to store artifacts
/// Uses dialog_storage::Storage directly (not platform::Storage) because content-addressed
/// storage doesn't need key prefixing/namespacing
pub type Index<Backend> =
    Tree<GeometricDistribution, Key, State<Datum>, Blake3Hash, Archive<Backend>>;

/// We reference a tree by the root hash.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

impl From<NodeReference> for Blake3Hash {
    fn from(value: NodeReference) -> Self {
        let NodeReference(hash) = value;
        hash
    }
}

/// Site identifier used to reference remotes.
pub type Site = String;

/// Represents a principal operating a replica.
#[derive(Clone)]
pub struct Issuer {
    id: String,
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

impl Issuer {
    /// Creates a new issuer from a passphrase by hashing it to derive a signing key.
    pub fn from_passphrase(passphrase: &str) -> Self {
        let bytes = passphrase.as_bytes();
        Self::from_secret(blake3::hash(bytes).as_bytes())
    }
    /// Creates a new issuer from a secret key.
    pub fn from_secret(secret: &[u8; SECRET_KEY_LENGTH]) -> Self {
        Issuer::new(SigningKey::from_bytes(secret))
    }
    /// Creates a new issuer from a signing key.
    pub fn new(signing_key: SigningKey) -> Self {
        let verifying_key = signing_key.verifying_key();
        const PREFIX: &str = "z6Mk";
        let id = [
            PREFIX,
            SigningKey::generate(&mut rand::thread_rng())
                .verifying_key()
                .as_bytes()
                .as_ref()
                .to_base58()
                .as_str(),
        ]
        .concat();

        Self {
            id: format!("did:key:{id}"),
            signing_key,
            verifying_key,
        }
    }
    /// Generates a new issuer with a random signing key.
    pub fn generate() -> Result<Self, ReplicaError> {
        Ok(Self::new(SigningKey::generate(&mut rand::thread_rng())))
    }

    /// Signs a payload with this issuer's signing key.
    pub fn sign(&mut self, payload: &[u8]) -> Signature {
        self.signing_key.sign(payload)
    }

    /// Returns the DID (Decentralized Identifier) for this issuer.
    pub fn did(&self) -> &str {
        &self.id
    }

    /// Returns the principal (public key bytes) for this issuer.
    pub fn principal(&self) -> &Principal {
        self.verifying_key.as_bytes()
    }
}

/// A replica represents a local instance of a distributed database.
#[allow(dead_code)]
pub struct Replica<Backend: PlatformBackend> {
    issuer: Issuer,
    storage: PlatformStorage<Backend>,
    remotes: Remotes<Backend>,
    branches: Branches<Backend>,
}

impl<Backend: PlatformBackend + 'static> Replica<Backend> {
    /// Creates a new replica with the given issuer and storage backend.
    pub fn open(issuer: Issuer, backend: Backend) -> Result<Self, ReplicaError> {
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);

        let branches = Branches::new(issuer.clone(), backend.clone());
        let remotes = Remotes::new(backend.clone());
        Ok(Replica {
            issuer,
            storage,
            remotes,
            branches,
        })
    }
}

/// Manages multiple branches within a replica.
#[allow(dead_code)]
pub struct Branches<Backend: PlatformBackend> {
    issuer: Issuer,
    storage: PlatformStorage<Backend>,
}

impl<Backend: PlatformBackend + 'static> Branches<Backend> {
    /// Creates a new instance for the given backend
    pub fn new(issuer: Issuer, backend: Backend) -> Self {
        let storage = PlatformStorage::new(backend, CborEncoder);
        Self { issuer, storage }
    }

    /// Loads a branch with given identifier, produces an error if it does not
    /// exists.
    pub async fn load(&self, id: &BranchId) -> Result<Branch<Backend>, ReplicaError> {
        Branch::load(id, self.issuer.clone(), self.storage.clone()).await
    }

    /// Loads a branch with the given identifier or creates a new one if
    /// it does not already exist.
    pub async fn open(&self, id: &BranchId) -> Result<Branch<Backend>, ReplicaError> {
        Branch::open(id, self.issuer.clone(), self.storage.clone()).await
    }
}

/// Archive represents content addressed storage where search tree
/// nodes are stored. It supports optional remote fallback for on
/// demand replication. Uses Arc to share remote state across clones.
#[derive(Clone)]
pub struct Archive<Backend: PlatformBackend> {
    local: PlatformStorage<Backend>,
    remote: Arc<RwLock<Option<PlatformStorage<RemoteBackend>>>>,
}

impl<Backend: PlatformBackend> Archive<Backend> {
    /// Creates a new Archive with the given backend
    pub fn new(local: PlatformStorage<Backend>) -> Self {
        Self {
            local,
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
impl<Backend: PlatformBackend + 'static> dialog_storage::ContentAddressedStorage
    for Archive<Backend>
{
    type Hash = [u8; 32];
    type Error = dialog_storage::DialogStorageError;

    async fn read<T>(&self, hash: &Self::Hash) -> Result<Option<T>, Self::Error>
    where
        T: serde::de::DeserializeOwned + dialog_common::ConditionalSync,
    {
        // Convert hash to key
        let key = hash.to_vec();

        // Try local first
        if let Some(bytes) =
            self.local.get(&key).await.map_err(|e| {
                dialog_storage::DialogStorageError::StorageBackend(format!("{:?}", e))
            })?
        {
            return self.local.decode(&bytes).await.map(Some);
        }

        // Fall back to remote if available
        let remote_guard = self.remote.read().await;
        if let Some(remote) = remote_guard.as_ref() {
            if let Some(bytes) = remote.get(&key).await.map_err(|e| {
                dialog_storage::DialogStorageError::StorageBackend(format!("{:?}", e))
            })? {
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
        let key = hash.to_vec();

        // Write to local always
        self.local
            .set(key.clone(), bytes.clone())
            .await
            .map_err(|e| dialog_storage::DialogStorageError::StorageBackend(format!("{:?}", e)))?;

        // Write to remote if available
        let mut remote_guard = self.remote.write().await;
        if let Some(remote) = remote_guard.as_mut() {
            remote.set(key, bytes).await.map_err(|e| {
                dialog_storage::DialogStorageError::StorageBackend(format!("{:?}", e))
            })?;
        }

        Ok(hash)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend: PlatformBackend + 'static> dialog_storage::Encoder for Archive<Backend> {
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
pub struct Branch<Backend: PlatformBackend + 'static> {
    issuer: Issuer,
    state: BranchState,
    storage: PlatformStorage<Backend>,
    archive: Archive<Backend>,
    memory: TypedStoreResource<BranchState, Backend>,
    tree: Arc<RwLock<Index<Backend>>>,
    upstream: Option<Box<Upstream<Backend>>>,
}

impl<Backend: PlatformBackend + 'static> Branch<Backend> {
    /// Mounts a typed store for branch state at the appropriate storage location.
    pub fn mount(storage: &PlatformStorage<Backend>) -> TypedStore<BranchState, Backend> {
        storage.at("local").mount()
    }
    /// Loads a branch with a given id or creates one if it does not exist.
    pub async fn open(
        id: &BranchId,
        issuer: Issuer,
        storage: PlatformStorage<Backend>,
    ) -> Result<Branch<Backend>, ReplicaError> {
        let mut memory = Self::mount(&storage)
            .open(&id.to_string().into())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        let archive = Archive::new(storage.clone());

        if let Some(state) = memory.content() {
            // Clone state before moving memory to avoid borrow issues
            let state = state.clone();
            let upstream_state = state.upstream.clone();

            // Load the tree from the revision's tree hash
            let tree = Tree::from_hash(state.revision.tree().hash(), archive.clone())
                .await
                .map_err(|e| ReplicaError::StorageError(format!("Failed to load tree: {:?}", e)))?;

            // Load upstream if configured
            let loaded_upstream = if let Some(ref upstream_state) = upstream_state {
                match Upstream::load(upstream_state, issuer.clone(), storage.clone()).await {
                    Ok(upstream) => {
                        // Configure archive remote if upstream is remote
                        if let Upstream::Remote(ref remote_branch) = upstream {
                            if let Ok(remote_storage) = remote_branch.remote_storage().await {
                                archive.set_remote(remote_storage).await;
                            }
                        }
                        Some(Box::new(upstream))
                    }
                    Err(_) => None,
                }
            } else {
                None
            };

            let branch = Branch {
                issuer: issuer.clone(),
                state,
                storage: storage.clone(),
                archive,
                memory,
                tree: Arc::new(RwLock::new(tree)),
                upstream: loaded_upstream,
            };

            Ok(branch)
        } else {
            // create a new branch with a new revision
            let state = BranchState::new(
                id.clone(),
                Revision::new(issuer.principal().to_owned()),
                None,
            );
            memory
                .replace(Some(state.clone()))
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

            // New branch starts with empty tree
            let tree = Tree::new(archive.clone());

            Ok(Branch {
                issuer,
                state,
                memory,
                storage,
                archive,
                tree: Arc::new(RwLock::new(tree)),
                upstream: None,
            })
        }
    }

    /// Loads a branch from the the the underlaying replica, if branch with a
    /// given id does not exists it produces an error.
    pub async fn load(
        id: &BranchId,
        issuer: Issuer,
        storage: PlatformStorage<Backend>,
    ) -> Result<Branch<Backend>, ReplicaError> {
        let memory = Self::mount(&storage)
            .open(&id.to_string().into())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        let archive = Archive::new(storage.clone());

        if let Some(state) = memory.content() {
            // Clone state before moving memory to avoid borrow issues
            let state = state.clone();
            let upstream_state = state.upstream.clone();

            // Load the tree from the revision's tree hash
            let tree = Tree::from_hash(state.revision.tree().hash(), archive.clone())
                .await
                .map_err(|e| ReplicaError::StorageError(format!("Failed to load tree: {:?}", e)))?;

            // Load upstream if configured
            let loaded_upstream = if let Some(ref upstream_state) = upstream_state {
                match Upstream::load(upstream_state, issuer.clone(), storage.clone()).await {
                    Ok(upstream) => {
                        // Configure archive remote if upstream is remote
                        if let Upstream::Remote(ref remote_branch) = upstream {
                            if let Ok(remote_storage) = remote_branch.remote_storage().await {
                                archive.set_remote(remote_storage).await;
                            }
                        }
                        Some(Box::new(upstream))
                    }
                    Err(_) => None,
                }
            } else {
                None
            };

            let branch = Branch {
                issuer: issuer.clone(),
                state,
                storage: storage.clone(),
                archive,
                memory,
                tree: Arc::new(RwLock::new(tree)),
                upstream: loaded_upstream,
            };

            Ok(branch)
        } else {
            Err(ReplicaError::BranchNotFound { id: id.clone() })
        }
    }

    /// Resets this branch to a given revision and a base tree.
    pub async fn reset(
        &mut self,
        revision: Revision,
        base: NodeReference,
    ) -> Result<&mut Self, ReplicaError> {
        // create new edition from the prior state.
        let state = BranchState {
            revision: revision.clone(),
            id: self.state.id.clone(),
            description: self.state.description.clone(),
            upstream: self.state.upstream.clone(),
            base,
        };

        self.memory
            .replace_with(|_| Some(state.clone()))
            .await
            .map_err(|_| ReplicaError::StorageError("Updating branch failed".into()))?;

        // Reload the tree from the new revision
        let tree = Tree::from_hash(revision.tree().hash(), self.archive.clone())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("Failed to load tree: {:?}", e)))?;
        *self.tree.write().await = tree;

        // If we were able to write a new state update
        self.state = state;

        Ok(self)
    }

    /// Lazily initializes and returns a mutable reference to the upstream.
    /// Returns None if no upstream is configured.
    async fn upstream(&mut self) -> Result<Option<&mut Box<Upstream<Backend>>>, ReplicaError> {
        if self.state.upstream.is_none() {
            return Ok(None);
        }

        if self.upstream.is_none() {
            if let Some(upstream_state) = &self.state.upstream {
                let upstream =
                    Upstream::load(upstream_state, self.issuer.clone(), self.storage.clone())
                        .await?;
                self.upstream = Some(Box::new(upstream));
            }
        }
        Ok(self.upstream.as_mut())
    }

    /// Fetches remote reference of this branch. If this branch has no upstream
    /// setup it will produce an error. If upstream branch is a local one this
    /// operation is a no-op. If it has a remote upsteram it tries to fetch
    /// a revision and update corresponding branch record locally
    pub async fn fetch(&mut self) -> Result<Option<Revision>, ReplicaError> {
        if let Some(upstream) = self.upstream().await? {
            upstream.fetch().await
        } else {
            Err(ReplicaError::BranchNotFound {
                id: self.id().clone(),
            })
        }
    }

    fn state(&self) -> BranchState {
        self.memory.content().clone().unwrap_or(self.state.clone())
    }
    /// Returns the branch identifier.
    pub fn id(&self) -> &BranchId {
        self.state.id()
    }
    /// Returns the current revision of this branch.
    pub fn revision(&self) -> Revision {
        self.state().revision().to_owned()
    }
    /// Returns a description of this branch.
    pub fn description(&self) -> String {
        self.state().description().into()
    }

    /// Pushes the current revision to the upstream branch.
    /// If upstream is local, it updates that branch directly.
    /// If upstream is remote, it publishes to the remote and updates local cache.
    /// Returns None if no upstream is configured or if pushing to self.
    pub async fn push(&mut self) -> Result<Option<Revision>, ReplicaError> {
        // Check if pushing to self
        if let Some(upstream_state) = &self.state.upstream {
            if upstream_state.id() == self.id() {
                return Ok(None);
            }
        }

        let after = self.state.revision.clone();

        // Lazily load and push to upstream
        let before = if let Some(upstream) = self.upstream().await? {
            upstream.publish(after.clone()).await?
        } else {
            // No upstream configured
            return Ok(None);
        };

        // create new branch state with published revision
        let state = BranchState {
            revision: after.clone(),
            // reset a base to published tree
            base: after.tree.clone(),
            ..self.state.clone()
        };

        // update a memory with the latest branch state
        self.memory
            .replace_with(|_| Some(state.clone()))
            .await
            .map_err(|e| ReplicaError::StorageError(format!("Branch update failed {}", e)))?;

        // and update state with latest branch state
        self.state = state;

        Ok(before)
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
        if self.state.upstream.is_some() {
            if let Some(upstream_revision) = self.fetch().await? {
                // Check if the upstream has changed since our last pull
                if self.state.base != upstream_revision.tree {
                    // Load upstream tree into memory
                    let mut upstream_tree: Index<Backend> =
                        Tree::from_hash(upstream_revision.tree.hash(), self.archive.clone())
                            .await
                            .map_err(|e| {
                                ReplicaError::StorageError(format!(
                                    "Failed to load upstream tree: {:?}",
                                    e
                                ))
                            })?;

                    // Load current tree (base tree) to compute local changes
                    let base_tree: Index<Backend> =
                        Tree::from_hash(self.state.base.hash(), self.archive.clone())
                            .await
                            .map_err(|e| {
                                ReplicaError::StorageError(format!(
                                    "Failed to load base tree: {:?}",
                                    e
                                ))
                            })?;

                    // Load our current tree to differentiate
                    let current_tree: Index<Backend> =
                        Tree::from_hash(self.state.revision.tree.hash(), self.archive.clone())
                            .await
                            .map_err(|e| {
                                ReplicaError::StorageError(format!(
                                    "Failed to load current tree: {:?}",
                                    e
                                ))
                            })?;

                    // Compute local changes: diff between base and current
                    let local_changes = current_tree.differentiate(&base_tree);

                    // Integrate local changes into upstream tree
                    upstream_tree.integrate(local_changes).await.map_err(|e| {
                        ReplicaError::StorageError(format!("Failed to integrate changes: {:?}", e))
                    })?;

                    // Compute new period and moment based on issuer
                    let (period, moment) = if upstream_revision.issuer == *self.issuer.principal() {
                        // Same issuer: increment moment, keep period
                        (upstream_revision.period, upstream_revision.moment + 1)
                    } else {
                        // Different issuer: new period (sync point), reset moment
                        (upstream_revision.period + 1, 0)
                    };

                    // Get the hash of the integrated tree
                    let tree_hash = upstream_tree.hash().cloned().unwrap_or(EMPT_TREE_HASH);

                    // Create new revision with integrated changes
                    let new_revision = Revision {
                        issuer: *self.issuer.principal(),
                        tree: NodeReference(tree_hash),
                        cause: HashSet::from([upstream_revision.edition()?]),
                        period,
                        moment,
                    };

                    // Reset branch to the new revision
                    self.reset(new_revision.clone(), upstream_revision.tree.clone())
                        .await?;

                    Ok(Some(new_revision))
                } else {
                    // Base hasn't changed, nothing to pull
                    Ok(None)
                }
            } else {
                // No upstream revision found
                Ok(None)
            }
        } else {
            // No upstream configured
            Ok(None)
        }
    }

    /// Sets the upstream for this branch and persists the change.
    /// Accepts either a Branch or RemoteBranch via Into<Upstream>.
    pub async fn set_upstream<U: Into<Upstream<Backend>>>(
        &mut self,
        upstream: U,
    ) -> Result<(), ReplicaError> {
        let mut upstream = upstream.into();

        // Get the state descriptor from the upstream
        let upstream_state = upstream.to_state();

        // Configure remote archive if upstream is remote
        match &mut upstream {
            Upstream::Remote(remote) => {
                let remote_storage = remote.remote_storage().await?;
                self.archive.set_remote(remote_storage).await;
            }
            Upstream::Local(_) => {
                self.archive.clear_remote().await;
            }
        }

        // Set the cached upstream
        self.upstream = Some(Box::new(upstream));

        // Update the state with the new upstream state
        self.state.upstream = Some(upstream_state);

        // Persist the updated state to memory
        self.memory
            .replace(Some(self.state.clone()))
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

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
                let base_issuer = *base_revision.issuer();
                let current_issuer: Principal =
                    *blake3::hash(self.state.id.0.as_bytes()).as_bytes();

                if base_issuer == current_issuer {
                    // Same issuer - increment moment
                    (base_period, base_moment + 1)
                } else {
                    // Different issuer - increment period, reset moment
                    (base_period + 1, 0)
                }
            };

            let new_revision = Revision {
                issuer: *self.issuer.principal(),
                tree: tree_reference.clone(),
                cause: {
                    let mut set = HashSet::new();
                    set.insert(base_revision.edition().map_err(|e| {
                        DialogArtifactsError::Storage(format!("Failed to create edition: {}", e))
                    })?);
                    set
                },
                period,
                moment,
            };

            // Update the branch state with the new revision
            let new_state = BranchState {
                id: self.state.id.clone(),
                description: self.state.description.clone(),
                revision: new_revision.clone(),
                base: tree_reference.clone(),
                upstream: self.state.upstream.clone(),
            };

            // Save the new state
            self.memory
                .replace(Some(new_state.clone()))
                .await
                .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

            self.state = new_state;

            Ok(tree_hash)
        }
        .await;

        match transaction_result {
            Ok(hash) => Ok(hash),
            Err(error) => {
                // Rollback: reload tree from base revision
                let rollback_tree =
                    Tree::from_hash(base_revision.tree().hash(), self.archive.clone())
                        .await
                        .map_err(|e| {
                            DialogArtifactsError::Storage(format!("Rollback failed: {:?}", e))
                        })?;

                *self.tree.write().await = rollback_tree;
                Err(error)
            }
        }
    }
}

/// Manages remote repositories for synchronization.
#[allow(dead_code)]
pub struct Remotes<Backend: PlatformBackend> {
    storage: PlatformStorage<Backend>,
}

impl<Backend: PlatformBackend> Remotes<Backend> {
    /// Creates a new remotes manager for the given backend.
    pub fn new(backend: Backend) -> Self {
        let storage = PlatformStorage::new(backend, CborEncoder);
        Self { storage }
    }

    /// Loads an existing remote repository by name.
    pub async fn load(&self, site: &Site) -> Result<Remote<Backend>, ReplicaError> {
        Remote::setup(site, self.storage.clone()).await
    }

    /// Adds a new remote repository with the given name and address.
    pub async fn add(&mut self, state: RemoteState) -> Result<Remote<Backend>, ReplicaError> {
        Remote::add(state, self.storage.clone()).await
    }
}

/// Represents remote storage
pub type RemoteBackend = ErrorMappingBackend<RestStorageBackend<Vec<u8>, Vec<u8>>>;

/// Represents a connection to a remote repository.
#[allow(dead_code)]
pub struct Remote<Backend: PlatformBackend> {
    state: RemoteState,
    storage: PlatformStorage<Backend>,
    memory: TypedStoreResource<RemoteState, Backend>,
}
impl<Backend: PlatformBackend> Remote<Backend> {
    pub fn site(&self) -> &Site {
        &self.state.site
    }
    /// Mounts a typed store for remote state.
    pub fn mount(storage: PlatformStorage<Backend>) -> TypedStore<RemoteState, Backend> {
        storage.at("site").mount()
    }
    /// Loads a remote repository by the site name.
    pub async fn setup(
        site: &Site,
        storage: PlatformStorage<Backend>,
    ) -> Result<Remote<Backend>, ReplicaError> {
        let memory = Self::mount(storage.clone())
            .open(&site.to_string().into_bytes().to_vec())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        if let Some(state) = memory.content().clone() {
            Ok(Remote {
                state,
                memory,
                storage,
            })
        } else {
            Err(ReplicaError::RemoteNotFound {
                remote: site.into(),
            })
        }
    }

    /// Adds a new remote repository with the given configuration.
    pub async fn add(
        state: RemoteState,
        storage: PlatformStorage<Backend>,
    ) -> Result<Remote<Backend>, ReplicaError> {
        let mut memory = Self::mount(storage.clone())
            .open(&state.site.as_bytes().to_vec())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        if memory.content().is_some() {
            Err(ReplicaError::RemoteAlreadyExists {
                remote: state.site.to_string(),
            })
        } else {
            let state = RemoteState {
                site: state.site.to_string(),
                address: state.address,
            };
            memory
                .replace(Some(state.clone()))
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

            Ok(Remote {
                state,
                memory,
                storage,
            })
        }
    }

    pub fn connect(&self) -> Result<PlatformStorage<RemoteBackend>, ReplicaError> {
        let backend: RestStorageBackend<Vec<u8>, Vec<u8>> =
            RestStorageBackend::new(self.state.address.clone()).map_err(|_| {
                ReplicaError::RemoteConnectionError {
                    remote: self.state.site.clone(),
                }
            })?;

        Ok(PlatformStorage::new(
            ErrorMappingBackend::new(backend),
            CborEncoder,
        ))
    }

    /// Opens a branch at this remote
    pub async fn open(&self, id: &BranchId) -> Result<RemoteBranch<Backend>, ReplicaError> {
        RemoteBranch::open(self.site(), id, self.storage.clone()).await
    }

    /// Loads a branch at this remote
    pub async fn load(&self, id: &BranchId) -> Result<RemoteBranch<Backend>, ReplicaError> {
        RemoteBranch::load(self.site(), id, self.storage.clone()).await
    }
}

/// Represents a branch on a remote repository.
#[allow(dead_code)]
pub struct RemoteBranch<Backend: PlatformBackend> {
    site: Site,
    id: BranchId,
    revision: Option<Revision>,
    storage: PlatformStorage<Backend>,
    /// Local cache for the revision currently branch has
    cache: TypedStoreResource<Revision, Backend>,
    /// Canonical revision, which is created lazily on fetch.
    canonical: Option<TypedStoreResource<Revision, RemoteBackend>>,
}

impl<Backend: PlatformBackend> RemoteBranch<Backend> {
    /// Mounts a typed store for remote branch state.
    pub fn mount<B: StorageBackend>(storage: &PlatformStorage<B>) -> TypedStore<Revision, B> {
        storage.at("remote").mount()
    }
    /// Loads a remote branch by name.
    pub async fn load(
        site: &Site,
        id: &BranchId,
        storage: PlatformStorage<Backend>,
    ) -> Result<RemoteBranch<Backend>, ReplicaError> {
        // Open a localy stored revision for this branch
        let memory = Self::mount(&storage)
            .open(&id.to_string().into_bytes().to_vec())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        if let Some(revision) = memory.content().clone() {
            Ok(Self {
                site: site.clone(),
                id: id.clone(),
                storage,
                revision: Some(revision),
                cache: memory,
                canonical: None,
            })
        } else {
            Err(ReplicaError::RemoteNotFound {
                remote: id.to_string(),
            })
        }
    }

    pub async fn open(
        site: &Site,
        id: &BranchId,
        storage: PlatformStorage<Backend>,
    ) -> Result<RemoteBranch<Backend>, ReplicaError> {
        // Open a localy stored revision for this branch
        let memory = Self::mount(&storage)
            .open(&id.to_string().into_bytes().to_vec())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        Ok(Self {
            site: site.clone(),
            id: id.clone(),
            storage,
            revision: memory.content().clone(),
            cache: memory,
            canonical: None,
        })
    }

    /// Returns the site for this remote branch
    pub fn site(&self) -> &Site {
        &self.site
    }

    /// Returns the branch id
    pub fn id(&self) -> &BranchId {
        &self.id
    }

    /// Returns the current revision
    pub fn revision(&self) -> &Option<Revision> {
        &self.revision
    }

    /// Returns the remote storage for this branch
    pub async fn remote_storage(&self) -> Result<PlatformStorage<RemoteBackend>, ReplicaError> {
        let remote = Remote::setup(&self.site, self.storage.clone()).await?;
        remote.connect()
    }

    pub async fn connect(
        &mut self,
    ) -> Result<&TypedStoreResource<Revision, RemoteBackend>, ReplicaError> {
        if self.canonical.is_none() {
            // Load a remote for this branch
            let remote = Remote::setup(&self.site, self.storage.clone()).await?;

            let canonical = Self::mount(&remote.connect()?)
                .open(&self.id.to_string().into_bytes().to_vec())
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

            self.canonical = Some(canonical);
        }

        Ok(self
            .canonical
            .as_ref()
            .expect("canonical was initialized above"))
    }

    /// Fetcher remote revision for this branch. If remote revision is different
    /// from local revision updates local one to match the remote. Returns
    /// revision of this branch.
    pub async fn fetch(&mut self) -> Result<&Option<Revision>, ReplicaError> {
        self.connect().await?;
        let canonical = self.canonical.as_mut().expect("connected");
        let revision = canonical.content().clone();
        // update local record for the revision.
        let _ = self.cache.replace_with(|_| revision.clone()).await;
        self.revision = revision;

        Ok(self.revision())
    }

    /// Publishes new canonical revision. If published revision is different
    /// from current (local) revision for this branch previous revision is
    /// returned otherwise None is returned.
    pub async fn publish(&mut self, revision: Revision) -> Result<Option<Revision>, ReplicaError> {
        self.connect().await?;
        let before = self.revision.clone();
        let canonical = self.canonical.as_mut().expect("connected");

        // if revision is different we update
        if canonical.content().as_ref() != Some(&revision) {
            canonical
                .replace(Some(revision.clone()))
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
        }

        // if local state is different we update it also
        if before.as_ref() != Some(&revision) {
            self.cache
                .replace_with(|_| Some(revision.clone()))
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

            self.revision = Some(revision);

            Ok(before)
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// State information for a remote repository connection.
pub struct RemoteState {
    /// Name for this remote.
    site: Site,

    /// Address used to configure this remote
    address: RestStorageConfig,
}

/// Logical timestamp used to denote dialog transactions. It takes inspiration
/// from automerge which tags lamport timestamps with origin information. It
/// takes inspiration from [Hybrid Logical Clocks (HLC)](https://sergeiturukin.com/2017/06/26/hybrid-logical-clocks.html)
/// and splits timestamp into two components `period` representing coordinated
/// component of the time and `moment` representing an uncoordinated local
/// time component. This construction allows us to capture synchronization
/// points allowing us to prioritize replicas that are actively collaborating
/// over those that are not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Occurence {
    /// Site of this occurence.
    site: Principal,

    /// Logical coordinated time component denoting a last synchronization
    /// cycle.
    period: usize,

    /// Local uncoordinated time component denoting a moment within a
    /// period at which occurrence happened.
    moment: usize,
}

/// A [`Revision`] represents a concrete state of the dialog instance. It is
/// kind of like git commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Revision {
    /// Site where this revision was created.It as expected to be a signing
    /// principal representing a tool acting on author's behalf. In the future
    /// I expect we'll have signed delegation chain from user to this site.
    issuer: Principal,

    /// Reference the root of the search tree.
    tree: NodeReference,

    /// Set of revisions this is based of. It can be an empty set if this is
    /// a first revision, but more commonly it will point to a previous revision
    /// it is based on. If branch tracks multiple concurrent upstreams it will
    /// contain a set of revisions.
    ///
    /// It is effectively equivalent of of `parents` in git commit objects.
    cause: HashSet<Edition<Revision>>,

    /// Period indicating when this revision was created. This MUST be derived
    /// from the `cause`al revisions and it must be greater by one than the
    /// maximum period of the `cause`al revisions that have different `by` from
    /// this revision. More simply we create a new period whenever we
    /// synchronize.
    period: usize,

    /// Moment at which this revision was created. It represents a number of
    /// transactions that have being made in this period. If `cause`al revisions
    /// have a revision from same `by` this MUST be value greater by one,
    /// otherwise it should be `0`. This implies that when we sync we increment
    /// `period` and reset `moment` to `0`. And when we create a transaction we
    /// increment `moment` by one and keep the same `period`.
    moment: usize,
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
    id: BranchId,

    /// Free-form human-readable description of this fork.
    description: String,

    /// Current revision associated with this branch.
    revision: Revision,

    /// Root of the search tree our this revision is based off.
    base: NodeReference,

    /// An upstream through which updates get propagated. Branch may
    /// not have an upstream.
    upstream: Option<UpstreamState>,
}

/// Unique name for the branch
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BranchId(String);

impl BranchId {
    pub fn new(id: String) -> Self {
        BranchId(id)
    }

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

    pub fn reset(&mut self, revision: Revision) -> &mut Self {
        self.revision = revision;
        self
    }
}

/// Upstream branch that is used to push & pull changes
/// to / from. It can be local or remote.
pub enum Upstream<Backend: PlatformBackend + 'static> {
    Local(Branch<Backend>),
    Remote(RemoteBranch<Backend>),
}

impl<Backend: PlatformBackend + 'static> Upstream<Backend> {
    /// Loads an upstream from its state descriptor
    pub fn load(
        state: &UpstreamState,
        issuer: Issuer,
        storage: PlatformStorage<Backend>,
    ) -> BoxFuture<'_, Result<Self, ReplicaError>> {
        Box::pin(async move {
            match state {
                UpstreamState::Local { branch } => {
                    let branch = Branch::load(branch, issuer, storage).await?;
                    Ok(Upstream::Local(branch))
                }
                UpstreamState::Remote { site, branch } => {
                    let remote_branch = RemoteBranch::load(site, branch, storage).await?;
                    Ok(Upstream::Remote(remote_branch))
                }
            }
        })
    }

    /// Returns the branch id of this upstream
    pub fn id(&self) -> &BranchId {
        match self {
            Upstream::Local(branch) => branch.id(),
            Upstream::Remote(remote) => remote.id(),
        }
    }

    /// Converts this upstream to its state descriptor
    pub fn to_state(&self) -> UpstreamState {
        match self {
            Upstream::Local(branch) => UpstreamState::Local {
                branch: branch.id().clone(),
            },
            Upstream::Remote(remote) => UpstreamState::Remote {
                site: remote.site().clone(),
                branch: remote.id().clone(),
            },
        }
    }

    /// Fetches the current revision from the upstream
    pub async fn fetch(&mut self) -> Result<Option<Revision>, ReplicaError> {
        match self {
            Upstream::Local(branch) => Ok(Some(branch.revision())),
            Upstream::Remote(remote) => Ok(remote.fetch().await?.to_owned()),
        }
    }

    /// Pushes a revision to the upstream, returning the previous revision if any
    pub async fn publish(&mut self, revision: Revision) -> Result<Option<Revision>, ReplicaError> {
        match self {
            Upstream::Local(branch) => {
                let before = branch.revision();
                branch.reset(revision, branch.state.base.clone()).await?;
                Ok(Some(before))
            }
            Upstream::Remote(remote) => remote.publish(revision).await,
        }
    }
}

impl<Backend: PlatformBackend + 'static> From<Branch<Backend>> for Upstream<Backend> {
    fn from(branch: Branch<Backend>) -> Self {
        Self::Local(branch)
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
                site: branch.site.clone(),
                branch: branch.id().clone(),
            },
        }
    }
}

/// Upstream represents some branch being tracked
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UpstreamState {
    Local { branch: BranchId },
    Remote { site: Site, branch: BranchId },
}

impl UpstreamState {
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
    pub fn new(hash: [u8; 32]) -> Self {
        Self(hash, PhantomData)
    }
}
impl<T> Clone for Edition<T> {
    fn clone(&self) -> Self {
        Self(self.0, PhantomData)
    }
}

impl<T> std::fmt::Debug for Edition<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Edition").field(&self.0).finish()
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
    #[error("Pushing revision failed cause {cause}")]
    PushFailed {
        /// The underlying error
        cause: DialogStorageError,
    },

    #[error("Remote {remote} not found")]
    RemoteNotFound { remote: Site },
    #[error("Remote {remote} already exist")]
    RemoteAlreadyExists { remote: Site },
    #[error("Connection to remote {remote} failed")]
    RemoteConnectionError { remote: Site },
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

    ArchiveError,

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
    fn test_issuer() -> Principal {
        [1u8; 32]
    }

    /// Helper to create a test branch with upstream
    async fn create_branch_with_upstream<Backend>(
        storage: PlatformStorage<Backend>,
        id: &str,
        upstream_id: &str,
    ) -> Result<Branch<Backend>, ReplicaError>
    where
        Backend: PlatformBackend + 'static,
        Backend::Error: ConditionalSync,
        Backend::Resource: ConditionalSync + ConditionalSend,
    {
        let branch_id = BranchId::new(id.to_string());
        let upstream_branch_id = BranchId::new(upstream_id.to_string());

        let issuer = Issuer::from_secret(&test_issuer());
        let mut branch = Branch::open(&branch_id, issuer, storage.clone()).await?;

        // Set up upstream as a local branch
        branch.state.upstream = Some(UpstreamState::Local {
            branch: upstream_branch_id,
        });

        // Save the updated state
        branch
            .memory
            .replace(Some(branch.state.clone()))
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

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
        let issuer = Issuer::from_secret(&test_issuer());
        let mut main_branch = Branch::open(&main_id, issuer.clone(), storage.clone())
            .await
            .expect("Failed to create main branch");

        // Create a revision for main
        let main_revision = Revision {
            issuer: test_issuer(),
            tree: NodeReference(EMPT_TREE_HASH),
            cause: HashSet::new(),
            period: 0,
            moment: 0,
        };
        main_branch
            .reset(main_revision.clone(), NodeReference(EMPT_TREE_HASH))
            .await
            .expect("Failed to reset main branch");

        // Create feature branch with main as upstream
        let mut feature_branch = create_branch_with_upstream(storage.clone(), "feature", "main")
            .await
            .expect("Failed to create feature branch");

        // Create a new revision on feature branch with main_revision as cause
        let feature_revision = Revision {
            issuer: test_issuer(),
            tree: NodeReference(EMPT_TREE_HASH),
            cause: HashSet::from([main_revision.edition().expect("Failed to create edition")]),
            period: 0,
            moment: 1,
        };
        feature_branch
            .reset(feature_revision.clone(), NodeReference(EMPT_TREE_HASH))
            .await
            .expect("Failed to reset feature branch");

        // Push feature to main
        feature_branch.push().await.expect("Push failed");

        // Verify main branch was updated
        let updated_main = Branch::load(&main_id, issuer, storage)
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

        let original_revision = branch.revision();

        // Push should be a no-op
        branch.push().await.expect("Push failed");

        // Revision should be unchanged
        assert_eq!(branch.revision(), original_revision);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_push_without_upstream_fails() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);

        let branch_id = BranchId::new("no-upstream".to_string());
        let issuer = Issuer::from_secret(&test_issuer());
        let mut branch = Branch::open(&branch_id, issuer, storage)
            .await
            .expect("Failed to create branch");

        // Push should return None without upstream
        let result = branch.push().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_pull_with_no_upstream_changes() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);

        // Create main and feature branches
        let main_id = BranchId::new("main".to_string());
        let issuer = Issuer::from_secret(&test_issuer());
        let mut main_branch = Branch::open(&main_id, issuer, storage.clone())
            .await
            .expect("Failed to create main branch");

        let main_revision = Revision {
            issuer: test_issuer(),
            tree: NodeReference(EMPT_TREE_HASH),
            cause: HashSet::new(),
            period: 0,
            moment: 0,
        };
        main_branch
            .reset(main_revision.clone(), NodeReference(EMPT_TREE_HASH))
            .await
            .expect("Failed to reset main");

        // Create feature with main as upstream, based on same revision
        let mut feature_branch = create_branch_with_upstream(storage, "feature", "main")
            .await
            .expect("Failed to create feature branch");

        feature_branch
            .reset(main_revision.clone(), NodeReference(EMPT_TREE_HASH))
            .await
            .expect("Failed to reset feature");

        // Pull should return None (no changes)
        let result = feature_branch.pull().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_pull_without_upstream_returns_none() {
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let storage = PlatformStorage::new(backend.clone(), CborEncoder);

        let branch_id = BranchId::new("no-upstream".to_string());
        let issuer = Issuer::from_secret(&test_issuer());
        let mut branch = Branch::open(&branch_id, issuer, storage)
            .await
            .expect("Failed to create branch");

        // Pull without upstream should return None
        let result = branch.pull().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn test_end_to_end_remote_upstream() -> anyhow::Result<()> {
        use dialog_storage::{AuthMethod, JournaledStorage, RestStorageConfig};
        use futures_util::stream;

        // Start a local S3-compatible test server
        let s3_service = dialog_storage::s3_test_server::start().await?;

        // Step 1: Generate issuer
        let issuer = Issuer::from_passphrase("test_end_to_end_remote_upstream");

        // Step 2: Create a replica with that issuer and journaled in-memory backend
        let backend = MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        let journaled_backend = JournaledStorage::new(backend);
        let mut replica = Replica::open(issuer.clone(), journaled_backend.clone())
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
        let remote_state = RemoteState {
            site: "origin".to_string(),
            address: RestStorageConfig {
                endpoint: s3_service.endpoint().to_string(),
                auth_method: AuthMethod::None,
                bucket: Some("test-bucket".to_string()),
                key_prefix: Some("test".to_string()),
                headers: vec![],
                timeout_seconds: Some(30),
            },
        };
        let remote = replica
            .remotes
            .add(remote_state)
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
        assert_eq!(
            decoded_remote_state.address.endpoint,
            s3_service.endpoint(),
            "Remote state should contain correct endpoint"
        );

        // Step 5: Create a remote branch for the main
        let remote_branch = remote
            .open(&main_id)
            .await
            .expect("Failed to create remote branch");

        // Note: Opening a remote branch doesn't write to storage yet.
        // The remote/main record will be created when we push to it.

        // Step 6: Add remote branch as an upstream of the local `main` branch
        main_branch
            .set_upstream(remote_branch)
            .await
            .expect("Failed to set upstream");

        // Verify upstream is configured
        assert!(main_branch.state.upstream.is_some());
        assert!(main_branch.upstream.is_some());

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
        // The tree hash should be a key in the storage
        let tree_node_value = journaled_backend
            .get(&tree_hash.to_vec())
            .await
            .expect("Failed to get tree node");
        assert!(
            tree_node_value.is_some(),
            "Tree node with hash {:?} should be written to storage",
            tree_hash
        );

        // Step 9: Push changes to the main branch
        // Should create records for the local branch and corresponding remote branch
        // in the in-memory backend
        // Record should be written for the branch in the remote store
        let push_result = main_branch.push().await;
        assert!(push_result.is_ok(), "Push failed: {:?}", push_result.err());

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
        let remote_branch_key = format!("remote/{}", main_id.to_string())
            .as_bytes()
            .to_vec();

        // Check that the key was written (this is the important verification for this test)
        let all_written_keys = journaled_backend.get_writes();
        let was_written = all_written_keys.iter().any(|k| k == &remote_branch_key);
        assert!(
            was_written,
            "Remote branch key 'remote/{}' should have been written during push. All keys: {:?}",
            main_id.to_string(),
            all_written_keys
                .iter()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .collect::<Vec<_>>()
        );

        // Reload the main branch and verify the changes persisted
        let reloaded_main = replica
            .branches
            .load(&main_id)
            .await
            .expect("Failed to reload main branch");

        assert_eq!(reloaded_main.revision().tree().hash(), &tree_hash);

        Ok(())
    }
}
