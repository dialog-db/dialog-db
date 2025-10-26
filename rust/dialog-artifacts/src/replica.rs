pub use super::uri::Uri;
use crate::artifacts::NULL_REVISION_HASH as EMPTY_INDEX;
use crate::constants::HASH_SIZE;
use crate::{Artifacts, Datum, Index, Key, State};
use async_stream::stream;
use dialog_common::ConditionalSync;
use dialog_prolly_tree::{Change, DialogProllyTreeError, Differential, Node, Tree};
use dialog_storage::{
    AtomicStorageBackend, Blake3Hash, CborEncoder, DialogStorageError, Storage, StorageBackend,
};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use thiserror::Error;
/// Cryptographic identifier like Ed25519 public key representing
/// an principal that produced a change. We may
pub type Principal = [u8; 32];

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
        Self(EMPTY_INDEX)
    }
}

impl From<NodeReference> for Blake3Hash {
    fn from(value: NodeReference) -> Self {
        let NodeReference(hash) = value;
        hash.clone()
    }
}

/// Fork identifier
pub type BranchId = String;

/// Site identifier used to reference remotes.
pub type Site = String;

/// A [`Revision`] represents a concrete state of the dialog instance. It is
/// kind of like git commit.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct Revision {
    /// Reference to the root of the search tree
    index: NodeReference,
    /// Version is local counter of edits to the index.
    version: usize,
}
impl Revision {
    /// The component of the [`Revision`] that corresponds to the root of the
    /// search index.
    pub fn index(&self) -> &NodeReference {
        &self.index
    }

    /// The component of the [`Revision`] that corresponds to the version of the
    /// index.
    pub fn version(&self) -> usize {
        self.version
    }
}

/// Fork is somewhat similar to a git branch and represents a diverged
/// state of the that has a name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Branch {
    /// Unique identifier of this fork.
    id: BranchId,

    /// Free-form human-readable description of this fork.
    description: String,

    /// Revision at which this fork diverged from the source.
    base: Revision,

    /// Current revision of this fork.
    revision: Revision,

    /// An upstream through which updates get propagated if this fork
    /// has one.
    upstream: Option<Upstream>,
}

impl Branch {
    /// Create a new fork from the given revision.
    pub fn new(id: BranchId, description: Option<String>) -> Self {
        Self {
            description: description.unwrap_or_else(|| id.clone()),
            base: Revision::default(),
            revision: Revision::default(),
            upstream: None,
            id,
        }
    }
    /// Unique identifier of this fork.
    pub fn id(&self) -> &BranchId {
        &self.id
    }
    /// Revision at which this fork diverged from the source.
    pub fn base(&self) -> &Revision {
        &self.base
    }
    /// Current revision of this fork.
    pub fn revision(&self) -> &Revision {
        &self.revision
    }
}

/// Represents an open fork that can be operated on.
pub struct BranchView<'a, P: Platform> {
    model: Branch,
    platform: &'a P,
}
impl<'a, P: Platform> BranchView<'a, P> {
    /// Loads a branch with a given id or creates one if it does not exist.
    pub async fn open(id: BranchId, platform: &'a P) -> Result<Self, ReplicaError> {
        let model = platform
            .forks()
            .get(&id)
            .await
            .map_err(|e| ReplicaError::storage_error(Capability::ResolveBranch, e))?
            .unwrap_or_else(|| Branch::new(id, None));

        Ok(Self { model, platform })
    }

    /// Loads a branch from the the the underlaying replica, if branch with a
    /// given id does not exists it produces an error.
    pub async fn load(id: BranchId, platform: &'a P) -> Result<Self, ReplicaError> {
        if let Some(branch) = platform
            .forks()
            .get(&id)
            .await
            .map_err(|e| ReplicaError::storage_error(Capability::ResolveBranch, e))?
        {
            Ok(Self {
                model: branch,
                platform,
            })
        } else {
            Err(ReplicaError::BranchNotFound { id })
        }
    }

    /// Returns unique identifier of this fork.
    pub fn id(&self) -> &BranchId {
        &self.model.id
    }

    /// Fetches remote reference of this fork. If this branch has no upstream
    /// setup it will produce an error. If upstream branch is a local one this
    /// operation is a no-op.
    pub async fn fetch(&self) -> Result<Option<Revision>, ReplicaError> {
        if let Some(upstream) = &self.model.upstream {
            match &upstream.origin {
                // Fetch from a local branch is a no-op.
                Origin::Local => {
                    // update revision based on what we have fetched.
                    let revision = self
                        .platform
                        .revisions()
                        .get(&(self.id().into(), None))
                        .await
                        .map_err(|error| ReplicaError::StorageError {
                            capability: Capability::ResolveRevision,
                            cause: error,
                        })?;

                    Ok(revision)
                }
                Origin::Remote(remote) => {
                    let address = remote.address();
                    let id = remote.id();

                    let revision = self
                        .platform
                        .revisions()
                        .get(&(self.id().into(), Some(id.into())))
                        .await
                        .map_err(|error| ReplicaError::StorageError {
                            capability: Capability::ResolveRevision,
                            cause: error,
                        })?;

                    if let Some(revision) = &revision {
                        // update revision based on what we have fetched.
                        self.platform
                            .revisions()
                            .set((self.id().into(), Some(id.into())), revision.clone())
                            .await
                            .map_err(|error| ReplicaError::StorageError {
                                capability: Capability::UpdateRevision,
                                cause: error,
                            })?;
                    }

                    Ok(revision)
                }
            }
        } else {
            Err(ReplicaError::BranchNotFound {
                id: self.id().clone(),
            })
        }
    }

    /// Computes all the changes that have occured on this branch since last
    /// pull. It assumes that current revision is based of the base revision
    /// and that subtrees that were updated are available locally, which would
    /// have been fetched in order to produce an update.
    pub fn difference(&self) -> impl Differential<Key, State<Datum>> {
        let archive = self.platform.archive();
        stream! {
            let before:Index<Key, Datum, P::Storage> = Tree::from_hash(self.model.base().index().hash(), archive.clone()).await?;
            let after = Tree::from_hash(self.model.revision().index().hash(), archive.clone()).await?;
            for await change in after.difference(before) {
                yield change;
            }
        }
    }
}

/// Remote represents a remote replica of the dialog instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Remote {
    /// Unique identifier of this remote.
    id: Site,
    /// Address of the remote.
    address: Uri,
}
impl Remote {
    /// Creates a new remote instance.
    pub fn new(id: Site, address: Uri) -> Self {
        Self { id, address }
    }
    /// Returns the unique identifier of this remote.
    pub fn id(&self) -> &Site {
        &self.id
    }
    /// Returns the address of this remote.
    pub fn address(&self) -> &Uri {
        &self.address
    }
}

/// Upstream represents some branch being tracked
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Upstream {
    id: BranchId,
    origin: Origin,
}

/// Describes origin of the replica that is either local or a remote.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Origin {
    /// Implies local replica
    Local,
    /// Reference to a remote replica
    Remote(Remote),
}

type Archive<Backend> = Storage<HASH_SIZE, CborEncoder, Backend>;

/// Platform represents a platform capabilities dialog requires for
/// it's operations
pub trait Platform {
    /// Archive represents a storage of the index tree nodes.
    type Storage: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + 'static;

    /// Revision information for both local and remote replicas.
    type Revisions: StorageBackend<Key = (BranchId, Option<Site>), Value = Revision, Error = DialogStorageError>;

    /// Revisions represents a storage for tracking mutable references across
    /// local and remote replicas.
    type Releases: AtomicStorageBackend<
            Key = (BranchId, Option<Site>),
            Value = Revision,
            Error = DialogStorageError,
        >;

    /// Tracked forks
    type Forks: StorageBackend<Key = BranchId, Value = Branch, Error = DialogStorageError>
        + ConditionalSync
        + 'static;

    /// State tracking all the remotes
    type Remotes: StorageBackend<Key = Site, Value = Remote, Error = DialogStorageError>
        + ConditionalSync
        + 'static;

    /// Get a reference-counted pointer to the internal search tree index
    fn archive(&self) -> &mut Archive<Self::Storage>;

    /// Gets a reference to revision store.
    fn revisions(&self) -> &mut Self::Revisions;

    /// Gets a reference to release store.
    fn releases(&self) -> &mut Self::Releases;

    /// Gets a reference to forks store.
    fn forks(&self) -> &mut Self::Forks;

    /// Gets a reference to remotes store.
    fn remotes(&self) -> &mut Self::Remotes;
}

/// Replica represents a replica of the dialog instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Replica<'a, P: Platform> {
    platform: &'a P,
    /// Cryptographic authority managing this replica
    authority: Principal,
}

impl<'a, P: Platform> Replica<'a, P> {
    /// Creates or opens a fork in the replica.
    pub async fn open(&self, id: BranchId) -> Result<BranchView<'a, P>, DialogStorageError> {
        let forks = self.platform.forks();
        let fork = if let Some(fork) = forks.get(&id).await? {
            fork
        } else {
            let fork = Branch::new(id.clone(), None);
            forks.set(id, fork.clone()).await?;
            fork
        };

        Ok(BranchView {
            model: fork,
            platform: self.platform,
        })
    }
}

/// The common error type used by this crate
#[derive(Error, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaError {
    #[error("Branch {id} not found")]
    BranchNotFound { id: BranchId },

    #[error("Capability {capability} failed cause {cause}")]
    StorageError {
        capability: Capability,
        cause: DialogStorageError,
    },

    #[error("Branch {id} has no upstream")]
    BranchHasNoUpstream { id: BranchId },
}

impl ReplicaError {
    pub fn storage_error(capability: Capability, cause: DialogStorageError) -> Self {
        ReplicaError::StorageError { capability, cause }
    }
}

#[derive(Error, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Capability {
    ResolveBranch,
    ResolveRevision,
    UpdateRevision,
}
impl Display for Capability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Capability::ResolveBranch => write!(f, "ResolveBranch"),
            Capability::ResolveRevision => write!(f, "ResolveRevision"),
            Capability::UpdateRevision => write!(f, "UpdateRevision"),
        }
    }
}
