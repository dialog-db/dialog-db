//! Branch management for the replica system.
//!
//! This module contains the Branch type and related types for managing
//! named lines of development within a replica.

use async_stream::try_stream;
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use futures_util::future::BoxFuture;
#[cfg(target_arch = "wasm32")]
use futures_util::future::LocalBoxFuture;
use futures_util::{Stream, StreamExt, TryStreamExt};
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::RwLock;

use dialog_common::{ConditionalSend, SharedCell};
use dialog_prolly_tree::{Differential, Entry, GeometricDistribution, Node, Tree, TreeDifference};
use dialog_storage::Blake3Hash;

pub use dialog_prolly_tree::EMPT_TREE_HASH;

use crate::artifacts::selector::Constrained;
use crate::artifacts::{
    Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMut, Datum, Instruction, MatchCandidate,
};
use crate::platform::{PlatformBackend, Storage as PlatformStorage, TypedStoreResource};
use crate::{
    AttributeKey, DialogArtifactsError, EntityKey, FromKey, KeyView, KeyViewConstruct, KeyViewMut,
    State, ValueKey,
};

use super::archive::Archive;
use super::error::ReplicaError;
use super::issuer::Issuer;
use super::remote::RemoteBranch;
use super::types::{BranchId, BranchState, NodeReference, Occurence, Principal, Revision};
use super::upstream::UpstreamState;

/// Type alias for the prolly tree index used to store artifacts.
pub type Index<Backend> =
    Tree<GeometricDistribution, crate::Key, State<Datum>, Blake3Hash, Archive<Backend>>;

/// Upstream represents a branch being tracked (local or remote).
#[derive(Debug)]
pub enum Upstream<Backend: PlatformBackend + 'static> {
    /// A local branch upstream
    Local(Branch<Backend>),
    /// A remote branch upstream
    Remote(RemoteBranch<Backend>),
}

impl<Backend: PlatformBackend + 'static> Upstream<Backend> {
    /// Loads an upstream from its state descriptor
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(
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
                    let remote_branch = RemoteBranch::open(site, branch, storage).await?;
                    Ok(Upstream::Remote(remote_branch))
                }
            }
        })
    }

    /// Loads an upstream from its state descriptor
    #[cfg(target_arch = "wasm32")]
    pub fn open(
        state: &UpstreamState,
        issuer: Issuer,
        storage: PlatformStorage<Backend>,
    ) -> LocalBoxFuture<'_, Result<Self, ReplicaError>> {
        Box::pin(async move {
            match state {
                UpstreamState::Local { branch } => {
                    let branch = Branch::open(branch, issuer, storage).await?;
                    Ok(Upstream::Local(branch))
                }
                UpstreamState::Remote { site, branch } => {
                    let remote_branch = RemoteBranch::open(site, branch, storage).await?;
                    Ok(Upstream::Remote(remote_branch))
                }
            }
        })
    }

    /// Returns the branch id of this upstream
    pub fn id(&self) -> &BranchId {
        match self {
            Upstream::Local(branch) => branch.id(),
            Upstream::Remote(branch) => branch.id(),
        }
    }

    /// Returns revision this branch is at
    pub fn revision(&self) -> Option<Revision> {
        match self {
            Upstream::Local(branch) => Some(branch.revision()),
            Upstream::Remote(branch) => branch.revision(),
        }
    }

    /// Returns site of the branch. If local returns None otherwise
    /// returns site identifier
    pub fn site(&self) -> Option<&super::types::Site> {
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
    pub async fn publish(&mut self, revision: Revision) -> Result<(), ReplicaError> {
        match self {
            Upstream::Local(branch) => branch.reset(revision).await,
            Upstream::Remote(remote) => remote.publish(revision).await,
        }
    }
}

impl<Backend: PlatformBackend + 'static> Clone for Upstream<Backend> {
    fn clone(&self) -> Self {
        match self {
            Upstream::Local(branch) => Upstream::Local(branch.clone()),
            Upstream::Remote(branch) => Upstream::Remote(branch.clone()),
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

/// A branch represents a named line of development within a replica.
#[derive(Clone, Debug)]
pub struct Branch<Backend: PlatformBackend + 'static> {
    issuer: Issuer,
    id: BranchId,
    storage: PlatformStorage<Backend>,
    archive: Archive<Backend>,
    memory: TypedStoreResource<BranchState, Backend>,
    tree: Arc<RwLock<Index<Backend>>>,
    upstream: Arc<SharedCell<Option<Upstream<Backend>>>>,
}

impl<Backend: PlatformBackend + 'static> Branch<Backend> {
    async fn mount(
        id: &BranchId,
        storage: &PlatformStorage<Backend>,
        default_state: Option<BranchState>,
    ) -> Result<TypedStoreResource<BranchState, Backend>, ReplicaError> {
        let key = format!("local/{}", id);
        let mut memory = storage
            .open::<BranchState>(&key.into())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        // if we branch does not exist yet and we have default state we create
        // a branch.
        if let (None, Some(state)) = (memory.content(), default_state) {
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
        issuer: Issuer,
        storage: PlatformStorage<Backend>,
        default_state: Option<BranchState>,
    ) -> Result<Branch<Backend>, ReplicaError> {
        let memory = Self::mount(id, &storage, default_state).await?;
        let archive = Archive::new(storage.clone());

        // if we have a memory of this branch we initialize it otherwise
        // we produce an error.
        if let Some(state) = memory.content() {
            // Load the tree from the revision's tree hash
            let tree = Tree::from_hash(state.revision.tree().hash(), archive.clone())
                .await
                .map_err(|e| ReplicaError::StorageError(format!("Failed to load tree: {:?}", e)))?;

            // If branch has an upstream setup we load it up and configure
            // archive's remote
            let upstream = if let Some(state) = &state.upstream {
                let upstream = Upstream::open(state, issuer.clone(), storage.clone()).await?;

                if let Upstream::Remote(branch) = &upstream {
                    archive.set_remote(branch.remote_storage.clone()).await;
                }

                Some(upstream)
            } else {
                None
            };

            Ok(Branch {
                id: id.clone(),
                issuer: issuer.clone(),
                memory,
                archive,
                storage: storage.clone(),
                upstream: Arc::new(SharedCell::new(upstream)),
                tree: Arc::new(RwLock::new(tree)),
            })
        } else {
            Err(ReplicaError::BranchNotFound { id: id.clone() })
        }
    }

    /// Loads a branch with a given id or creates one if it does not exist.
    pub async fn open(
        id: &BranchId,
        issuer: Issuer,
        storage: PlatformStorage<Backend>,
    ) -> Result<Branch<Backend>, ReplicaError> {
        let default_state = Some(BranchState::new(
            id.clone(),
            #[allow(clippy::clone_on_copy)]
            Revision::new(issuer.principal().clone()),
            None,
        ));

        let branch = Self::load_with_default(id, issuer, storage, default_state).await?;

        Ok(branch)
    }

    /// Loads a branch from storage, producing an error if it doesn't exist.
    pub async fn load(
        id: &BranchId,
        issuer: Issuer,
        storage: PlatformStorage<Backend>,
    ) -> Result<Branch<Backend>, ReplicaError> {
        let branch = Self::load_with_default(id, issuer, storage, None).await?;

        Ok(branch)
    }

    /// Advances the branch to a given revision with an explicit base tree.
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
                &self.storage,
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

    /// Advances the branch to a given revision.
    pub async fn reset(&mut self, revision: Revision) -> Result<(), ReplicaError> {
        self.advance(revision.clone(), revision.tree.clone()).await
    }

    /// Returns the upstream if configured.
    pub fn upstream(&self) -> Option<Upstream<Backend>> {
        self.upstream.read().clone()
    }

    /// Fetches remote reference of this branch.
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
        self.memory.content().unwrap_or_else(|| {
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

    /// Returns principal issuing changes on this branch.
    pub fn principal(&self) -> &Principal {
        self.issuer.principal()
    }

    /// Returns the current revision of this branch.
    pub fn revision(&self) -> Revision {
        self.state().revision().to_owned()
    }

    /// Logical time on this branch.
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
    fn novelty(
        &self,
    ) -> impl Stream<Item = Result<Node<crate::Key, State<Datum>, Blake3Hash>, ReplicaError>> + '_
    {
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

    /// Returns a stream of changes since the last sync.
    fn changes(&self) -> impl Differential<crate::Key, State<Datum>> + '_ {
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
    pub async fn push(&mut self) -> Result<Option<Revision>, ReplicaError> {
        if let Some(upstream) = &mut self.upstream() {
            match upstream {
                Upstream::Local(target) => {
                    if target.id() == self.id() {
                        Err(ReplicaError::BranchUpstreamIsItself {
                            id: target.id().clone(),
                        })
                    } else {
                        let before = target.revision();
                        if target.revision().tree() == &self.base() {
                            target.reset(self.revision()).await?;
                            self.reset(self.revision()).await?;
                            Ok(Some(before))
                        } else {
                            Ok(None)
                        }
                    }
                }
                Upstream::Remote(target) => {
                    let before = target.revision();
                    let after = self.revision().clone();
                    if before.as_ref() != Some(&after) {
                        // Replicate novel blocks to a remote target
                        target.import(self.novelty()).await?;
                        // Now that all blocks are synced, publish the revision
                        target.publish(after.clone()).await?;
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
    pub async fn pull(&mut self) -> Result<Option<Revision>, ReplicaError> {
        use std::collections::HashSet;

        if let Some(_revision) = &mut self.upstream() {
            if let Some(revision) = self.fetch().await? {
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

                    // Compute local changes
                    let changes = self.changes();

                    // Integrate local changes into upstream tree
                    target.integrate(changes).await.map_err(|e| {
                        ReplicaError::StorageError(format!("Failed to integrate changes: {:?}", e))
                    })?;

                    // Get the hash of the integrated tree
                    let hash = target.hash().cloned().unwrap_or(EMPT_TREE_HASH);

                    if &hash == revision.tree.hash() {
                        self.reset(revision.clone()).await?;
                        Ok(Some(revision))
                    } else {
                        #[allow(clippy::clone_on_copy)]
                        let new_revision = Revision {
                            issuer: self.issuer.principal().clone(),
                            tree: NodeReference::new(hash),
                            cause: HashSet::from([revision.edition()?]),
                            period: revision.period.max(self.revision().period) + 1,
                            moment: 0,
                        };

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

    /// Sets the upstream for this branch.
    pub async fn set_upstream<U: Into<Upstream<Backend>>>(
        &mut self,
        target: U,
    ) -> Result<(), ReplicaError> {
        let upstream = target.into();
        let state = upstream.to_state();

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
                &self.storage,
            )
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        match &upstream {
            Upstream::Remote(remote) => {
                self.archive.set_remote(remote.remote_storage.clone()).await;
            }
            Upstream::Local(_) => {
                self.archive.clear_remote().await;
            }
        }

        *self.upstream.write() = Some(upstream);

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
            let tree = tree.read().await.clone();

            if selector.entity().is_some() {
                let start = <EntityKey<crate::Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <EntityKey<crate::Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

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
                let start = <ValueKey<crate::Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <ValueKey<crate::Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

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
                let start = <AttributeKey<crate::Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <AttributeKey<crate::Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

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
        use std::collections::HashSet;

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
                                let search_start = <EntityKey<crate::Key> as KeyViewConstruct>::min()
                                    .set_entity(entity_key.entity())
                                    .set_attribute(entity_key.attribute())
                                    .into_key();
                                let search_end = <EntityKey<crate::Key> as KeyViewConstruct>::max()
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
                                let entity_key = EntityKey(key);
                                let value_key: ValueKey<crate::Key> = ValueKey::from_key(&entity_key);
                                let attribute_key: AttributeKey<crate::Key> =
                                    AttributeKey::from_key(&entity_key);

                                tree.delete(&entity_key.into_key()).await?;
                                tree.delete(&value_key.into_key()).await?;
                                tree.delete(&attribute_key.into_key()).await?;
                            }
                        }

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

                        tree.set(entity_key.into_key(), State::Removed).await?;
                        tree.set(attribute_key.into_key(), State::Removed).await?;
                        tree.set(value_key.into_key(), State::Removed).await?;
                    }
                }
            }

            let tree_hash = *tree.hash().ok_or_else(|| {
                DialogArtifactsError::Storage("Failed to get tree hash".to_string())
            })?;

            let tree_reference = NodeReference::new(tree_hash);

            let (period, moment) = {
                let base_period = *base_revision.period();
                let base_moment = *base_revision.moment();
                let base_issuer = base_revision.issuer();

                if base_issuer == self.issuer.principal() {
                    (base_period, base_moment + 1)
                } else {
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

            self.memory
                .replace(
                    Some(BranchState {
                        revision: new_revision.clone(),
                        ..self.state()
                    }),
                    &self.storage,
                )
                .await
                .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

            Ok(tree_hash)
        }
        .await;

        match transaction_result {
            Ok(hash) => Ok(hash),
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
