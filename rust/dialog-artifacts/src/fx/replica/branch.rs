//! Effectful branch management for the replica system.
//!
//! This module contains the effectful Branch type that mirrors the original
//! Branch API but uses algebraic effects for all storage operations.
//!
//! # Shared State
//!
//! Multiple `Branch` instances for the same address share state via
//! `transactional_memory::Cell`. Changes made through one instance are
//! immediately visible to others.

use super::error::ReplicaError;
use super::session::BranchSession;
use super::types::{
    BranchId, BranchState, NodeReference, Occurence, Principal, Revision, EMPT_TREE_HASH,
};
use super::upstream::Upstream;
use super::Replica;
use crate::fx::archive::Archive;
use crate::fx::archive_store::ArchiveStore;
use crate::fx::effects::{effectful, LocalBackend, Memory, RemoteBackend, Store};
use crate::fx::local::Address as LocalAddress;
use crate::fx::remote::Address as RemoteAddress;
use crate::fx::transactional_memory::Cell;
use crate::{Datum, DialogArtifactsError, State};
use dialog_common::fx::Effect;
use dialog_common::ConditionalSync;
use dialog_prolly_tree::{GeometricDistribution, Tree, TreeDifference};
use dialog_storage::{Blake3Hash, CborEncoder, Encoder, StorageBackend};
use futures_util::StreamExt;
use std::collections::HashSet;
use std::pin::pin;

/// A branch represents a named line of development within a replica.
///
/// This is the effectful version that works with algebraic effects
/// instead of holding direct storage references.
///
/// Multiple `Branch` instances for the same address share state via
/// `Cell<BranchState>`. Changes made through one instance are immediately
/// visible to others.
#[derive(Debug, Clone)]
pub struct Branch {
    /// The replica this branch belongs to.
    replica: Replica,
    /// Branch identifier.
    id: BranchId,
    /// Shared cell for branch state.
    cell: Cell<BranchState>,
    /// Upstream branch if configured.
    upstream: Option<Box<Upstream>>,
}

impl Branch {
    /// Returns the replica this branch belongs to.
    pub fn replica(&self) -> &Replica {
        &self.replica
    }

    /// Returns the local storage address for this branch.
    pub fn address(&self) -> &LocalAddress {
        self.replica.address()
    }

    /// Get the current state (synchronous read from cache).
    fn state(&self) -> BranchState {
        self.cell
            .read()
            .expect("Branch cell should always have a value")
    }

    /// Loads a branch from storage without loading its upstream.
    ///
    /// Creates the branch with the provided default state if it doesn't exist.
    /// This is an internal helper used to avoid infinite recursion when loading
    /// upstream branches.
    #[effectful(Memory<LocalAddress>)]
    fn load_with_default_no_upstream(
        id: BranchId,
        replica: Replica,
        default_state: Option<BranchState>,
    ) -> Result<Self, ReplicaError> {
        let key = format!("local/{}", id).into_bytes();

        // Open cell from storage
        let cell = perform!(Cell::<BranchState>::open(replica.address().clone(), key))?;

        // Check if we have a value or need to create with default
        if cell.read().is_none() {
            if let Some(default) = default_state {
                // Write the default state
                perform!(cell.replace(Some(default)))?;
            } else {
                return Err(ReplicaError::BranchNotFound { id });
            }
        }

        Ok(Branch {
            replica,
            id,
            cell,
            upstream: None,
        })
    }

    /// Loads a branch from storage, creating it with the provided default state if it doesn't exist.
    ///
    /// This automatically loads the upstream if one is configured.
    #[effectful(Memory<LocalAddress>)]
    pub fn load_with_default(
        id: BranchId,
        replica: Replica,
        default_state: Option<BranchState>,
    ) -> Result<Self, ReplicaError> {
        let mut branch = perform!(Self::load_with_default_no_upstream(
            id,
            replica.clone(),
            default_state
        ))?;

        // Load upstream if configured
        if let Some(upstream_state) = branch.state().upstream.clone() {
            let upstream = match upstream_state {
                crate::replica::UpstreamState::Local {
                    branch: upstream_id,
                } => {
                    // Load local upstream (without loading its upstream to avoid recursion)
                    let loaded_branch = perform!(Branch::load_with_default_no_upstream(
                        upstream_id,
                        replica.clone(),
                        None
                    ))?;
                    Some(Box::new(Upstream::Local(loaded_branch)))
                }
                crate::replica::UpstreamState::Remote {
                    site,
                    branch: upstream_id,
                } => {
                    // Remote branches are loaded via RemoteBranch::open
                    let remote_branch =
                        perform!(super::remote::RemoteBranch::open(replica.address().clone(), site, upstream_id))?;
                    Some(Box::new(Upstream::Remote(remote_branch)))
                }
            };
            branch.upstream = upstream;
        }

        Ok(branch)
    }

    /// Opens a branch with a given id or creates one if it does not exist.
    ///
    /// This automatically loads the upstream if one is configured.
    #[effectful(Memory<LocalAddress>)]
    pub fn open(id: BranchId, replica: Replica) -> Result<Self, ReplicaError> {
        #[allow(clippy::clone_on_copy)]
        let default_state = Some(BranchState::new(
            id.clone(),
            Revision::new(replica.principal().clone()),
            None,
        ));

        perform!(Self::load_with_default(id, replica, default_state))
    }

    /// Loads a branch from storage, producing an error if it doesn't exist.
    ///
    /// This automatically loads the upstream if one is configured.
    #[effectful(Memory<LocalAddress>)]
    pub fn load(id: BranchId, replica: Replica) -> Result<Self, ReplicaError> {
        perform!(Self::load_with_default(id, replica, None))
    }

    /// Returns the branch identifier.
    pub fn id(&self) -> &BranchId {
        &self.id
    }

    /// Returns principal issuing changes on this branch.
    pub fn principal(&self) -> &Principal {
        self.replica.principal()
    }

    /// Returns the current revision of this branch.
    pub fn revision(&self) -> Revision {
        self.state().revision.clone()
    }

    /// Logical time on this branch.
    pub fn occurence(&self) -> Occurence {
        self.revision().into()
    }

    /// Returns the base tree reference for this branch.
    pub fn base(&self) -> NodeReference {
        self.state().base.clone()
    }

    /// Returns a description of this branch.
    pub fn description(&self) -> String {
        self.state().description.clone()
    }

    /// Returns the upstream if configured.
    pub fn upstream(&self) -> Option<&Upstream> {
        self.upstream.as_deref()
    }

    /// Advances the branch to a given revision with an explicit base tree.
    ///
    /// Returns the updated branch.
    #[effectful(Memory<LocalAddress>)]
    pub fn advance(self, revision: Revision, base: NodeReference) -> Result<Self, ReplicaError> {
        let current = self.state();
        let new_state = BranchState {
            revision,
            id: self.id.clone(),
            description: current.description,
            upstream: current.upstream,
            base,
        };

        perform!(self.cell.replace(Some(new_state)))?;

        Ok(self)
    }

    /// Resets the branch to a given revision (base becomes revision.tree).
    ///
    /// Returns the updated branch.
    #[effectful(Memory<LocalAddress>)]
    pub fn reset(self, revision: Revision) -> Result<Self, ReplicaError> {
        let base = revision.tree.clone();
        perform!(self.advance(revision, base))
    }

    /// Fetches the remote reference of this branch.
    ///
    /// Returns the updated branch and the fetched revision.
    #[effectful(Memory<LocalAddress> + Memory<RemoteAddress>)]
    pub fn fetch(self) -> Result<(Self, Option<Revision>), ReplicaError> {
        match self.upstream {
            Some(upstream) => {
                let (upstream, rev) = perform!((*upstream).fetch())?;
                Ok((
                    Self {
                        upstream: Some(Box::new(upstream)),
                        ..self
                    },
                    rev,
                ))
            }
            None => Err(ReplicaError::BranchHasNoUpstream {
                id: self.id.clone(),
            }),
        }
    }

    /// Sets the upstream for this branch.
    ///
    /// Returns the updated branch.
    #[effectful(Memory<LocalAddress>)]
    pub fn set_upstream(self, target: Upstream) -> Result<Self, ReplicaError> {
        let upstream_state = target.to_state();

        let current = self.state();
        let new_state = BranchState {
            upstream: Some(upstream_state),
            ..current
        };

        perform!(self.cell.replace(Some(new_state)))?;

        Ok(Self {
            upstream: Some(Box::new(target)),
            ..self
        })
    }

    /// Pushes the current revision to the upstream branch.
    ///
    /// Returns the updated branch and the previous upstream revision (if any).
    ///
    /// For local upstream: resets the target branch to our revision.
    /// For remote upstream: computes novel blocks, imports them to remote, and publishes.
    #[effectful(Memory<LocalAddress> + Memory<RemoteAddress> + LocalBackend + RemoteBackend + Store<RemoteAddress>)]
    pub fn push(self) -> Result<(Self, Option<Revision>), ReplicaError>
    where
        Capability: Memory<LocalAddress> + Memory<RemoteAddress> + LocalBackend + RemoteBackend + Store<RemoteAddress>,
        <Capability as LocalBackend>::Backend:
            StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
        <<Capability as LocalBackend>::Backend as StorageBackend>::Error:
            Into<DialogArtifactsError> + Send,
        <Capability as RemoteBackend>::Backend:
            StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
        <<Capability as RemoteBackend>::Backend as StorageBackend>::Error:
            Into<DialogArtifactsError> + Send,
    {
        let upstream = self
            .upstream
            .clone()
            .ok_or_else(|| ReplicaError::BranchHasNoUpstream {
                id: self.id.clone(),
            })?;

        match *upstream {
            Upstream::Local(ref target) => {
                if target.id() == self.id() {
                    return Err(ReplicaError::BranchUpstreamIsItself {
                        id: target.id().clone(),
                    });
                }

                let before = target.revision();
                let self_base = self.base();
                let self_revision = self.revision();

                if target.revision().tree() == &self_base {
                    // Clone the target to reset it
                    let target_branch = target.clone();
                    let updated_target = perform!(target_branch.reset(self_revision.clone()))?;
                    let updated_self = perform!(self.reset(self_revision))?;

                    Ok((
                        Self {
                            upstream: Some(Box::new(Upstream::Local(updated_target))),
                            ..updated_self
                        },
                        Some(before),
                    ))
                } else {
                    Ok((self, None))
                }
            }
            Upstream::Remote(ref target) => {
                let before = target.revision().cloned();
                let after = self.revision();

                if before.as_ref() != Some(&after) {
                    // Create archive with remote fallback if available
                    let mut archive_config = Archive::new(self.address().clone());
                    if let Some(remote_addr) = target.remote_address() {
                        archive_config.add_remote(remote_addr.clone());
                    }

                    // Acquire archive store with backends
                    let archive = perform!(archive_config.acquire())?;

                    // Load base tree
                    let base_ref = self.base();
                    let base_hash = base_ref.hash();
                    let base: Tree<
                        GeometricDistribution,
                        crate::Key,
                        State<Datum>,
                        Blake3Hash,
                        ArchiveStore<
                            <Capability as LocalBackend>::Backend,
                            <Capability as RemoteBackend>::Backend,
                        >,
                    > = if base_hash == &EMPT_TREE_HASH {
                        Tree::new(archive.clone())
                    } else {
                        Tree::from_hash(base_hash, archive.clone())
                            .await
                            .map_err(|e| {
                                ReplicaError::StorageError(format!("Failed to load base tree: {:?}", e))
                            })?
                    };

                    // Load current tree
                    let current_hash = after.tree().hash();
                    let current: Tree<
                        GeometricDistribution,
                        crate::Key,
                        State<Datum>,
                        Blake3Hash,
                        ArchiveStore<
                            <Capability as LocalBackend>::Backend,
                            <Capability as RemoteBackend>::Backend,
                        >,
                    > = if current_hash == &EMPT_TREE_HASH {
                        Tree::new(archive.clone())
                    } else {
                        Tree::from_hash(current_hash, archive.clone())
                            .await
                            .map_err(|e| {
                                ReplicaError::StorageError(format!(
                                    "Failed to load current tree: {:?}",
                                    e
                                ))
                            })?
                    };

                    // Compute novel nodes
                    let difference = TreeDifference::compute(&base, &current).await.map_err(|e| {
                        ReplicaError::StorageError(format!("Failed to compute diff: {:?}", e))
                    })?;

                    // Collect novel blocks
                    let mut blocks = Vec::new();
                    let encoder = CborEncoder;

                    let mut novel_nodes = pin!(difference.novel_nodes());
                    while let Some(result) = novel_nodes.next().await {
                        let node = result.map_err(|e| {
                            ReplicaError::StorageError(format!("Failed to load node: {:?}", e))
                        })?;

                        let hash = node.hash();
                        let mut key = b"index/".to_vec();
                        key.extend_from_slice(hash);

                        let (_hash, bytes) = encoder.encode(node.block()).await.map_err(|e| {
                            ReplicaError::StorageError(format!("Failed to encode block: {:?}", e))
                        })?;

                        blocks.push((key, bytes));
                    }

                    // Get remote address
                    let remote_addr = target
                        .remote_address()
                        .cloned()
                        .ok_or_else(|| ReplicaError::RemoteNotFound {
                            remote: target.site().clone(),
                        })?;

                    // Import blocks to remote
                    if !blocks.is_empty() {
                        perform!(Store::<RemoteAddress>().import(remote_addr, blocks))?;
                    }

                    // Publish revision to remote
                    let target_branch = target.clone();
                    let target_branch = perform!(target_branch.publish(after.clone()))?;

                    // Update local base
                    let updated_self = perform!(self.reset(after))?;

                    Ok((
                        Self {
                            upstream: Some(Box::new(Upstream::Remote(target_branch))),
                            ..updated_self
                        },
                        before,
                    ))
                } else {
                    Ok((self, before))
                }
            }
        }
    }

    /// Pulls changes from the upstream branch.
    ///
    /// Returns the updated branch and the merged revision.
    ///
    /// This properly integrates local changes into the upstream tree:
    /// 1. Fetches the upstream revision
    /// 2. If no local changes, fast-forwards to upstream
    /// 3. If local changes exist, integrates them into upstream tree
    #[effectful(Memory<LocalAddress> + Memory<RemoteAddress> + LocalBackend + RemoteBackend)]
    pub fn pull(self) -> Result<(Self, Option<Revision>), ReplicaError>
    where
        Capability: Memory<LocalAddress> + Memory<RemoteAddress> + LocalBackend + RemoteBackend,
        <Capability as LocalBackend>::Backend:
            StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
        <<Capability as LocalBackend>::Backend as StorageBackend>::Error:
            Into<DialogArtifactsError> + Send,
        <Capability as RemoteBackend>::Backend:
            StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
        <<Capability as RemoteBackend>::Backend as StorageBackend>::Error:
            Into<DialogArtifactsError> + Send,
    {
        if self.upstream.is_none() {
            return Err(ReplicaError::BranchHasNoUpstream {
                id: self.id.clone(),
            });
        }

        let base = self.base();
        let revision = self.revision();

        // Fetch upstream revision
        let (updated_self, upstream_revision) = perform!(self.fetch())?;

        let upstream_revision = match upstream_revision {
            Some(rev) => rev,
            None => return Ok((updated_self, None)),
        };

        // If base matches upstream, no changes
        if base.hash() == upstream_revision.tree().hash() {
            return Ok((updated_self, None));
        }

        // Check if we have local changes
        let has_local_changes = revision.tree.hash() != base.hash();

        if !has_local_changes {
            // Fast-forward: just reset to upstream
            let updated_self = perform!(updated_self.reset(upstream_revision.clone()))?;
            Ok((updated_self, Some(upstream_revision)))
        } else {
            // Three-way merge: integrate local changes into upstream tree

            // Create archive with remote fallback
            let mut archive_config = Archive::new(updated_self.address().clone());
            if let Some(upstream) = &updated_self.upstream {
                if let Upstream::Remote(remote_branch) = upstream.as_ref() {
                    if let Some(remote_addr) = remote_branch.remote_address() {
                        archive_config.add_remote(remote_addr.clone());
                    }
                }
            }

            // Acquire archive store with backends
            let archive = perform!(archive_config.acquire())?;

            // Load upstream tree
            let mut target: Tree<
                GeometricDistribution,
                crate::Key,
                State<Datum>,
                Blake3Hash,
                ArchiveStore<
                    <Capability as LocalBackend>::Backend,
                    <Capability as RemoteBackend>::Backend,
                >,
            > = if upstream_revision.tree().hash() == &EMPT_TREE_HASH {
                Tree::new(archive.clone())
            } else {
                Tree::from_hash(upstream_revision.tree().hash(), archive.clone())
                    .await
                    .map_err(|e| {
                        ReplicaError::StorageError(format!("Failed to load upstream tree: {:?}", e))
                    })?
            };

            // Load base tree for computing local changes
            let base_tree: Tree<
                GeometricDistribution,
                crate::Key,
                State<Datum>,
                Blake3Hash,
                ArchiveStore<
                    <Capability as LocalBackend>::Backend,
                    <Capability as RemoteBackend>::Backend,
                >,
            > = if base.hash() == &EMPT_TREE_HASH {
                Tree::new(archive.clone())
            } else {
                Tree::from_hash(base.hash(), archive.clone())
                    .await
                    .map_err(|e| {
                        ReplicaError::StorageError(format!("Failed to load base tree: {:?}", e))
                    })?
            };

            // Load current tree
            let current_tree: Tree<
                GeometricDistribution,
                crate::Key,
                State<Datum>,
                Blake3Hash,
                ArchiveStore<
                    <Capability as LocalBackend>::Backend,
                    <Capability as RemoteBackend>::Backend,
                >,
            > = if revision.tree.hash() == &EMPT_TREE_HASH {
                Tree::new(archive.clone())
            } else {
                Tree::from_hash(revision.tree.hash(), archive.clone())
                    .await
                    .map_err(|e| {
                        ReplicaError::StorageError(format!("Failed to load current tree: {:?}", e))
                    })?
            };

            // Compute local changes (diff from base to current)
            let changes = base_tree.differentiate(&current_tree);

            // Integrate local changes into upstream tree
            target.integrate(changes).await.map_err(|e| {
                ReplicaError::StorageError(format!("Failed to integrate changes: {:?}", e))
            })?;

            // Get the hash of the integrated tree
            let hash = target.hash().cloned().unwrap_or(EMPT_TREE_HASH);

            if &hash == upstream_revision.tree().hash() {
                // Local changes already present in upstream - just reset
                let updated_self = perform!(updated_self.reset(upstream_revision.clone()))?;
                Ok((updated_self, Some(upstream_revision)))
            } else {
                // Create merged revision
                #[allow(clippy::clone_on_copy)]
                let merged_revision = Revision {
                    issuer: updated_self.replica.principal().clone(),
                    tree: NodeReference::new(hash),
                    cause: HashSet::from([upstream_revision.edition()?]),
                    period: upstream_revision.period.max(*revision.period()) + 1,
                    moment: 0,
                };

                // Advance with upstream as new base
                let updated_self = perform!(
                    updated_self.advance(merged_revision.clone(), upstream_revision.tree.clone())
                )?;

                Ok((updated_self, Some(merged_revision)))
            }
        }
    }

    /// Commits a new revision with the given tree hash.
    ///
    /// Returns the updated branch and the new revision.
    #[effectful(Memory<LocalAddress>)]
    pub fn commit(self, tree_hash: [u8; 32]) -> Result<(Self, Revision), ReplicaError> {
        let base_revision = self.revision();

        let (period, moment) = {
            let base_period = *base_revision.period();
            let base_moment = *base_revision.moment();
            let base_issuer = base_revision.issuer();

            if base_issuer == self.replica.principal() {
                (base_period, base_moment + 1)
            } else {
                (base_period + 1, 0)
            }
        };

        #[allow(clippy::clone_on_copy)]
        let new_revision = Revision {
            issuer: self.replica.principal().clone(),
            tree: NodeReference::new(tree_hash),
            cause: HashSet::from([base_revision.edition()?]),
            period,
            moment,
        };

        // Update state (keep same base - it's updated on push/pull)
        let current = self.state();
        let new_state = BranchState {
            revision: new_revision.clone(),
            ..current
        };

        perform!(self.cell.replace(Some(new_state)))?;

        Ok((self, new_revision))
    }

    /// Creates a session for querying and transacting artifacts on this branch.
    ///
    /// The session provides `ArtifactStore` (for queries) and `ArtifactStoreMut`
    /// (for transactions) capabilities, backed by a prolly tree.
    ///
    /// If the branch has a remote upstream configured, the session will use it
    /// as a fallback for blocks that aren't available locally. This enables
    /// on-demand replication: blocks are fetched from the remote and cached
    /// locally when accessed.
    ///
    /// Returns the branch session. The branch itself is not consumed and can
    /// continue to be used for other operations.
    ///
    /// # Note on Upstream Changes
    ///
    /// The session captures the upstream configuration at creation time. If you
    /// call `set_upstream()` after creating a session, you have two options:
    ///
    /// 1. Create a new session with `session()` to pick up the new upstream
    /// 2. Update the existing session's storage directly:
    ///    ```ignore
    ///    let store = session.storage().await;
    ///    store.add_remote(new_remote_backend).await;
    ///    ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// let session = branch.session().perform(&mut env).await?;
    ///
    /// // Query artifacts
    /// let artifacts = session.select(selector).collect::<Vec<_>>().await;
    ///
    /// // Commit changes
    /// session.commit(instructions).await?;
    /// ```
    #[effectful(LocalBackend + RemoteBackend)]
    pub fn session(
        &self,
    ) -> Result<
        BranchSession<
            ArchiveStore<
                <Capability as LocalBackend>::Backend,
                <Capability as RemoteBackend>::Backend,
            >,
        >,
        ReplicaError,
    >
    where
        Capability: LocalBackend + RemoteBackend,
        <Capability as LocalBackend>::Backend:
            StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
        <<Capability as LocalBackend>::Backend as StorageBackend>::Error:
            Into<DialogArtifactsError> + Send,
        <Capability as RemoteBackend>::Backend:
            StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
        <<Capability as RemoteBackend>::Backend as StorageBackend>::Error:
            Into<DialogArtifactsError> + Send,
    {
        // Acquire local storage backend
        let local_backend = perform!(LocalBackend.backend(self.address().clone()))?;

        // Check if we have a remote upstream and acquire its backend
        let remotes = if let Some(upstream) = &self.upstream {
            if let Upstream::Remote(remote_branch) = upstream.as_ref() {
                if let Some(remote_addr) = remote_branch.remote_address() {
                    // Try to acquire the remote backend
                    match perform!(RemoteBackend.backend(remote_addr.clone())) {
                        Ok(remote_backend) => vec![remote_backend],
                        Err(_) => vec![], // Skip remote on connection failure
                    }
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        // Create ArchiveStore with local and optional remote backends
        let store = if remotes.is_empty() {
            ArchiveStore::new(local_backend)
        } else {
            ArchiveStore::with_remotes(local_backend, remotes)
        };

        let session = BranchSession::new(self.replica.issuer().clone(), self.revision(), store)
            .await
            .map_err(|e| ReplicaError::StorageError(e.to_string()))?;

        Ok(session)
    }
}
