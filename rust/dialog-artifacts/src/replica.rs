pub use super::uri::Uri;
use crate::artifacts::NULL_REVISION_HASH as EMPTY_INDEX;
use crate::constants::HASH_SIZE;
use crate::{Datum, Index, Key, State};
use async_stream::stream;
use dialog_common::ConditionalSync;
use dialog_prolly_tree::{Differential, KeyType, Tree, ValueType};
use dialog_storage::{
    AtomicStorageBackend, Blake3Hash, CborEncoder, DialogStorageError, Encoder, Storage,
    StorageBackend,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt::{Display, Formatter, format};
use std::marker::PhantomData;
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
        hash
    }
}

/// Site identifier used to reference remotes.
pub type Site = String;

/// A [`Revision`] represents a concrete state of the dialog instance. It is
/// kind of like git commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Revision {
    /// Reference for the source tree at the beginning of the epoch.
    source: NodeReference,

    /// Reference for the root of the search tree.
    tree: NodeReference,

    /// Epoch when changes have being made
    epoch: usize,

    /// Issuer of this revision.
    issuer: Principal,

    /// Number of transactions made by this issuer since the beginning of
    /// this epoch
    since: usize,

    /// Previous revision this replaced.
    cause: Option<Edition<Revision>>,
}

impl Revision {
    /// The component of the [`Revision`] that corresponds to the root of the
    /// search index.
    pub fn tree(&self) -> &NodeReference {
        &self.tree
    }

    /// Epoch when changes have being made
    pub fn epoch(&self) -> &usize {
        &self.epoch
    }

    /// Issuer of this revision.
    pub fn issuer(&self) -> &Principal {
        &self.issuer
    }

    /// Number of transactions made by this issuer since the beginning of
    /// this epoch
    pub fn since(&self) -> &usize {
        &self.since
    }

    /// Previous revision this replaced.
    pub fn cause(&self) -> &Option<Edition<Revision>> {
        &self.cause
    }
}

/// Record used to keep track of the remote branch. It is different from the
/// local branch record and is only a wrapper around `Revision` to hold
/// information about the branch and a target site.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteBranchState {
    id: RemoteBranchId,
    revision: Revision,
    cause: Option<Edition<RemoteBranchState>>,
}
impl Record for RemoteBranchState {
    type Key = RemoteBranchId;
    type Edition = Edition<Self>;
    type Error = crate::DialogStorageError;

    fn key(&self) -> &Self::Key {
        &self.id
    }
    async fn encode(&self) -> Result<(Self::Edition, Vec<u8>), Self::Error> {
        let (out, bytes) = CborEncoder.encode(self).await?;
        Ok((Edition::new(out), bytes))
    }
    fn cause(&self) -> Option<&Self::Edition> {
        self.cause.as_ref()
    }
}

/// Unique name for the branch
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RemoteBranchId {
    site: Site,
    branch: BranchId,
}
impl RemoteBranchId {
    pub fn id(&self) -> String {
        format!("{}@{}", self.branch, self.site)
    }
}
impl KeyType for RemoteBranchId {
    fn bytes(&self) -> &[u8] {
        self.id().as_bytes()
    }
}
impl TryFrom<Vec<u8>> for RemoteBranchId {
    type Error = std::string::FromUtf8Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let id = String::from_utf8(bytes)?;
        let (site, branch) = match id.split("/").collect::<Vec<&str>>().as_slice() {
            [site, id] => (site.to_string(), BranchId(id.to_string())),
            [site, path @ ..] => (site.to_string(), BranchId(path.join("/"))),
            _ => panic!("must have two components"),
        };

        Ok(RemoteBranchId { site, branch })
    }
}
impl Display for RemoteBranchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.branch, self.site)
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

    /// Local revision of this branch.
    revision: Revision,

    /// An upstream through which updates get propagated if this fork
    /// has one.
    upstream: Option<Upstream>,

    /// Previous state of this branch.
    cause: Option<Edition<BranchState>>,
}

/// Unique name for the branch
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
struct BranchId(String);
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

impl Record for BranchState {
    type Key = BranchId;
    type Edition = Edition<BranchState>;
    type Error = crate::DialogStorageError;

    fn key(&self) -> &Self::Key {
        &self.id
    }
    async fn encode(&self) -> Result<(Self::Edition, Vec<u8>), Self::Error> {
        let (out, bytes) = CborEncoder.encode(self).await?;
        Ok((Edition::new(out), bytes))
    }
    fn cause(&self) -> Option<&Self::Edition> {
        self.cause.as_ref()
    }
}

impl BranchState {
    /// Create a new fork from the given revision.
    pub fn new(id: BranchId, description: Option<String>) -> Self {
        Self {
            description: description.unwrap_or_else(|| id.0.clone()),
            revision: Revision::default(),
            cause: None,
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
    pub fn upstream(&self) -> Option<&Upstream> {
        self.upstream.as_ref()
    }

    /// Cause of this branch.
    pub fn cause(&self) -> Option<&Edition<BranchState>> {
        self.cause.as_ref()
    }
}

/// Represents an open fork that can be operated on.
pub struct Branch<'a, P: Platform> {
    state: BranchState,
    platform: &'a P,
}
impl<'a, P: Platform> Branch<'a, P> {
    /// Loads a branch with a given id or creates one if it does not exist.
    pub async fn open(id: &BranchId, platform: &'a P) -> Result<Self, ReplicaError> {
        let state = platform
            .branches()
            .read(&id)
            .await
            .map_err(|e| ReplicaError::storage_error(Capability::ResolveBranch, e))?
            .unwrap_or_else(|| BranchState::new(id.clone(), None));

        Ok(Self { state, platform })
    }

    /// Loads a branch from the the the underlaying replica, if branch with a
    /// given id does not exists it produces an error.
    pub async fn load(id: BranchId, platform: &'a P) -> Result<Self, ReplicaError> {
        if let Some(state) = platform
            .branches()
            .read(&id)
            .await
            .map_err(|e| ReplicaError::storage_error(Capability::ResolveBranch, e))?
        {
            Ok(Self { state, platform })
        } else {
            Err(ReplicaError::BranchNotFound { id })
        }
    }

    /// Returns unique identifier of this fork.
    pub fn id(&self) -> &BranchId {
        &self.state.id
    }

    /// Fetches remote reference of this fork. If this branch has no upstream
    /// setup it will produce an error. If upstream branch is a local one this
    /// operation is a no-op.
    pub async fn fetch(&self) -> Result<Option<Revision>, ReplicaError> {
        if let Some(upstream) = &self.state.upstream {
            match &upstream.origin {
                // Fetch from a local branch is a no-op.
                Origin::Local => {
                    let state = self
                        .platform
                        .branches()
                        .read(&upstream.id)
                        .await
                        .map_err(|e| ReplicaError::storage_error(Capability::ResolveBranch, e))?;

                    if let Some(state) = state {
                        // TODO: Revision does not fit a desired structure
                        Ok(Some(state.revision().clone()))
                    } else {
                        Err(ReplicaError::BranchNotFound {
                            id: upstream.id.clone(),
                        })
                    }
                }
                Origin::Remote(remote) => {
                    let _address = remote.address();

                    let revision = self
                        .platform
                        .revisions()
                        .get(&(self.id().clone(), Some(remote.id().into())))
                        .await
                        .map_err(|error| ReplicaError::StorageError {
                            capability: Capability::ResolveRevision,
                            cause: error,
                        })?;

                    if let Some(revision) = &revision {
                        // update revision based on what we have fetched.
                        self.platform
                            .revisions()
                            .set(
                                (self.id().clone(), Some(remote.id().into())),
                                revision.clone(),
                            )
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
    pub fn differentiate(&self) -> impl Differential<Key, State<Datum>> {
        let archive = self.platform.archive();
        stream! {
            let before:Index<Key, Datum, P::Storage> = Tree::from_hash(self.state.revision().source.hash(), archive.clone()).await?;
            let after:Index<Key, Datum, P::Storage> = Tree::from_hash(self.state.revision().tree.hash(), archive.clone()).await?;

            let diff = before.differentiate(&after);
            for await change in diff {
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

/// Blake3 hash of the branch state.
#[derive(Serialize, Deserialize)]
struct Edition<T>([u8; 32], PhantomData<fn() -> T>);
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
                    crate::HASH_SIZE,
                    value.len()
                ))
            })?,
            PhantomData,
        ))
    }
}

/// Transactional memory abstraction.
trait TransactionalMemory: ConditionalSync + 'static {
    type Key: KeyType;
    type Error: Into<DialogStorageError>;
    type Record: Record<Key = Self::Key, Error = Self::Error>;

    /// Gets the record for the given id. Returns `None` if record
    /// does not exists.
    async fn read(&self, id: &Self::Key) -> Result<Option<Self::Record>, Self::Error>;

    /// Performs an optimistic write of a record expecting that the currently
    /// record has a hash corresponding to the current record. If assumed record
    /// does not match existing record error should be returned.
    async fn write(&mut self, record: Self::Record) -> Result<(), Self::Error>;
}

trait Record:
    std::fmt::Debug + ConditionalSync + Clone + PartialEq + Serialize + DeserializeOwned
{
    type Key: KeyType;
    type Edition: KeyType;
    type Error: Into<DialogStorageError>;

    /// Returns a unique identifier for this record.
    fn key(&self) -> &Self::Key;

    /// Returns serialized representation of this record and unique identifier
    /// for a specific version of this record, which can be a hash an Etag,
    /// sequence number or anything else as long as it's unique.
    async fn encode(&self) -> Result<(Self::Edition, Vec<u8>), Self::Error>;

    /// Causal reference to an edition of this record this is replacing.
    fn cause(&self) -> Option<&Self::Edition>;
}

/// Platform repres~ents a platform capabilities dialog requires for
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

    /// Persisted information about branches.
    type Branches: TransactionalMemory<Key = BranchId, Record = BranchState, Error = DialogStorageError>;

    /// State tracking all the remotes
    type Remotes: StorageBackend<Key = Site, Value = Remote, Error = DialogStorageError>
        + ConditionalSync
        + 'static;

    /// Get a reference-counted pointer to the internal search tree index
    #[allow(clippy::mut_from_ref)]
    fn archive(&self) -> &mut Archive<Self::Storage>;

    /// Gets a reference to revision store.
    #[allow(clippy::mut_from_ref)]
    fn revisions(&self) -> &mut Self::Revisions;

    /// Gets a reference to release store.
    #[allow(clippy::mut_from_ref)]
    fn releases(&self) -> &mut Self::Releases;

    /// Gets a reference to forks store.
    #[allow(clippy::mut_from_ref)]
    fn branches(&self) -> &mut Self::Branches;

    /// Gets a reference to remotes store.
    #[allow(clippy::mut_from_ref)]
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
    /// Opens a branch in this replica. If branch does not exist it creates a new one.
    pub async fn open(&'a self, id: &BranchId) -> Result<Branch<'a, P>, ReplicaError> {
        Branch::open(id, self.platform).await
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
    #[error("Capability {capability} failed cause {cause}")]
    StorageError {
        /// The capability that was being exercised when the error occurred
        capability: Capability,
        /// The underlying storage error
        cause: DialogStorageError,
    },

    /// Branch has no configured upstream
    #[error("Branch {id} has no upstream")]
    BranchHasNoUpstream {
        /// The ID of the branch that has no upstream
        id: BranchId,
    },
}

impl ReplicaError {
    /// Create a new storage error
    pub fn storage_error(capability: Capability, cause: DialogStorageError) -> Self {
        ReplicaError::StorageError { capability, cause }
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
