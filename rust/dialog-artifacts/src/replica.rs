use super::platform::{
    ErrorMappingBackend, PlatformBackend, Storage, TypedStore, TypedStoreResource,
};
pub use super::uri::Uri;
use crate::artifacts::NULL_REVISION_HASH as EMPTY_INDEX;
use base58::ToBase58;
use blake3;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_prolly_tree::KeyType;

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

/// Represents a principal operating a replica.
pub struct Issuer {
    id: String,
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

impl Issuer {
    pub fn from_passphrase(passphrase: &str) -> Self {
        let bytes = passphrase.as_bytes();
        Self::from_secret(blake3::hash(bytes).as_bytes())
    }
    pub fn from_secret(secret: &[u8; SECRET_KEY_LENGTH]) -> Self {
        Issuer::new(SigningKey::from_bytes(secret))
    }
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
    pub fn generate() -> Result<Self, ReplicaError> {
        Ok(Self::new(SigningKey::generate(&mut rand::thread_rng())))
    }

    pub fn sign(&mut self, payload: &[u8]) -> Signature {
        self.signing_key.sign(payload)
    }

    pub fn did(&self) -> &str {
        &self.id
    }

    pub fn principal(&self) -> &Principal {
        self.verifying_key.as_bytes()
    }
}

pub struct Replica<Backend: PlatformBackend>
where
    Backend::Error: ConditionalSync,
    Backend::Resource: ConditionalSync + ConditionalSend,
{
    storage: Storage<Backend>,
    remotes: Remotes<Backend>,
    branches: Branches<Backend>,
}

impl<Backend: PlatformBackend> Replica<Backend> {
    pub fn new(backend: Backend) -> Result<Self, ReplicaError> {
        let storage = Storage::new(backend.clone(), CborEncoder);

        let branches = Branches::new(backend.clone());
        let remotes = Remotes::new(backend.clone());
        Ok(Replica {
            storage,
            remotes,
            branches,
        })
    }

    /// Opens or creates a new named branch
    pub async fn open(&self, id: BranchId) -> Result<Branch<Backend>, ReplicaError> {
        Ok(self.branches.open(&id).await?)
    }
}

pub struct Branches<Backend: PlatformBackend> {
    storage: Storage<Backend>,
    store: TypedStore<BranchState, Backend>,
}

impl<Backend: PlatformBackend> Branches<Backend> {
    /// Creates a new instance for the given backend
    pub fn new(backend: Backend) -> Self {
        let storage = Storage::new(backend, CborEncoder);
        let store = storage.at("revisions").at("local").mount();
        Self { storage, store }
    }

    /// Loads a branch with given identifier, produces an error if it does not
    /// exists.
    pub async fn load(&self, id: BranchId) -> Result<Branch<Backend>, ReplicaError> {
        Branch::load(id, self.storage.clone()).await
    }

    /// Loads a branch with the given identifier or creates a new one if
    /// it does not already exist.
    pub async fn open(&self, id: &BranchId) -> Result<Branch<Backend>, ReplicaError> {
        Branch::open(id, self.storage.clone()).await
    }
}

pub struct Branch<Backend: PlatformBackend> {
    state: BranchState,
    storage: Storage<Backend>,
    memory: TypedStoreResource<BranchState, Backend>,
}

impl<Backend: PlatformBackend> Branch<Backend> {
    pub fn mount(storage: &Storage<Backend>) -> TypedStore<BranchState, Backend> {
        storage.at("branch").at("local").mount()
    }
    /// Loads a branch with a given id or creates one if it does not exist.
    pub async fn open(
        id: &BranchId,
        storage: Storage<Backend>,
    ) -> Result<Branch<Backend>, ReplicaError> {
        let mut memory = Self::mount(&storage)
            .open(&id.to_string().into())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        if let Some(state) = memory.content() {
            Ok(Branch {
                state: state.clone(),
                storage,
                memory,
            })
        } else {
            let state = BranchState::new(id.clone(), None);
            memory
                .replace(Some(state.clone()))
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
            Ok(Branch {
                state,
                memory,
                storage,
            })
        }
    }

    /// Loads a branch from the the the underlaying replica, if branch with a
    /// given id does not exists it produces an error.
    pub async fn load(
        id: BranchId,
        storage: Storage<Backend>,
    ) -> Result<Branch<Backend>, ReplicaError> {
        let memory = Self::mount(&storage)
            .open(&id.to_string().into())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        if let Some(state) = memory.content() {
            Ok(Branch {
                state: state.clone(),
                storage,
                memory,
            })
        } else {
            Err(ReplicaError::BranchNotFound { id })
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
            revision,
            id: self.state.id.clone(),
            description: self.state.description.clone(),
            upstream: self.state.upstream.clone(),
            base,
        };

        self.memory
            .replace_with(|_| Some(state.clone()))
            .await
            .map_err(|_| ReplicaError::StorageError("Updating branch failed".into()))?;

        // If we were able to write a new state update
        self.state = state;

        Ok(self)
    }

    /// Fetches remote reference of this branch. If this branch has no upstream
    /// setup it will produce an error. If upstream branch is a local one this
    /// operation is a no-op. If it has a remote upsteram it tries to fetch
    /// a revision and update corresponding branch record locally
    pub async fn fetch(&mut self) -> Result<Option<Revision>, ReplicaError> {
        if let Some(upstream) = &self.state.upstream {
            match &upstream.origin {
                // Fetch from a local branch is a no-op.
                Origin::Local => {
                    let revision = Branch::load(upstream.id.clone(), self.storage.clone())
                        .await?
                        .revision();
                    Ok(Some(revision))
                }
                Origin::Remote(origin) => {
                    let remote = RepositoryRemote::load(&origin.id, self.storage.clone()).await?;
                    // resolve revision from the upstream
                    if let Some(revision) = remote.resolve(&upstream.id).await? {
                        let _target = RemoteBranch::set(
                            &upstream.id.to_string(),
                            RemoteBranchState {
                                id: upstream.id.clone(),
                                revision: revision.clone(),
                            },
                            &mut self.storage,
                        )
                        .await?;

                        Ok(Some(revision))
                    } else {
                        Err(ReplicaError::BranchNotFound {
                            id: upstream.id.clone(),
                        })
                    }
                }
            }
        } else {
            Err(ReplicaError::BranchNotFound {
                id: self.id().clone(),
            })
        }
    }

    fn state(&self) -> BranchState {
        self.memory.content().clone().unwrap_or(self.state.clone())
    }
    pub fn id(&self) -> &BranchId {
        self.state.id()
    }
    pub fn revision(&self) -> Revision {
        self.state().revision().to_owned()
    }
    pub fn description(&self) -> String {
        self.state().description().into()
    }
}

pub struct Remotes<Backend: PlatformBackend> {
    storage: Storage<Backend>,
    store: TypedStore<RemoteState, Backend>,
}

impl<Backend> Remotes<Backend>
where
    Backend: PlatformBackend,
    Backend::Error: ConditionalSync,
    Backend::Resource: ConditionalSync + ConditionalSend,
{
    pub fn new(backend: Backend) -> Self {
        let storage = Storage::new(backend, CborEncoder);
        let store = storage.at("connection").mount();
        Self { storage, store }
    }

    pub async fn add(
        &self,
        name: &str,
        address: RestStorageConfig,
    ) -> Result<RepositoryRemote<Backend>, ReplicaError> {
        RepositoryRemote::add(name, address, self.storage.clone()).await
    }
    pub async fn load(&self, name: &str) -> Result<RepositoryRemote<Backend>, ReplicaError> {
        RepositoryRemote::load(name, self.storage.clone()).await
    }
}

pub struct RepositoryRemote<Backend: PlatformBackend> {
    state: RemoteState,
    storage: Storage<Backend>,
    memory: TypedStoreResource<RemoteState, Backend>,
}
impl<Backend: PlatformBackend> RepositoryRemote<Backend> {
    pub fn mount(storage: Storage<Backend>) -> TypedStore<RemoteState, Backend> {
        storage.at("address").mount()
    }
    pub async fn load(
        name: &str,
        storage: Storage<Backend>,
    ) -> Result<RepositoryRemote<Backend>, ReplicaError> {
        let memory = Self::mount(storage.clone())
            .open(&name.to_string().into_bytes().to_vec())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        if let Some(state) = memory.content().clone() {
            Ok(RepositoryRemote {
                state,
                memory,
                storage,
            })
        } else {
            Err(ReplicaError::RemoteNotFound {
                remote: name.to_string(),
            })
        }
    }

    pub async fn add(
        name: &str,
        address: RestStorageConfig,
        storage: Storage<Backend>,
    ) -> Result<RepositoryRemote<Backend>, ReplicaError> {
        let mut memory = Self::mount(storage.clone())
            .open(&name.as_bytes().to_vec())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        if memory.content().is_some() {
            Err(ReplicaError::RemoteAlreadyExists {
                remote: name.to_string(),
            })
        } else {
            let state = RemoteState {
                id: name.to_string(),
                address,
            };
            memory
                .replace(Some(state.clone()))
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

            Ok(RepositoryRemote {
                state,
                memory,
                storage,
            })
        }
    }

    pub async fn resolve(&self, id: &BranchId) -> Result<Option<Revision>, ReplicaError> {
        let backend: RestStorageBackend<Vec<u8>, Vec<u8>> =
            RestStorageBackend::new(self.state.address.clone()).map_err(|_| {
                ReplicaError::RemoteConnectionError {
                    remote: self.state.id.clone(),
                }
            })?;

        let connection = ErrorMappingBackend::new(backend);
        let storage = Storage::new(connection, CborEncoder);
        let store = storage.mount::<Revision>();

        let key = id.to_string().into_bytes();
        let revision = store
            .get(&key)
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        Ok(revision)
    }
}

pub struct RemoteBranch<Backend: PlatformBackend> {
    state: RemoteBranchState,
    memory: TypedStoreResource<RemoteBranchState, Backend>,
}

impl<Backend: PlatformBackend> RemoteBranch<Backend> {
    pub fn mount(storage: &Storage<Backend>) -> TypedStore<RemoteBranchState, Backend> {
        storage.at("remote").mount()
    }
    pub async fn load(
        name: &str,
        storage: Storage<Backend>,
    ) -> Result<RemoteBranch<Backend>, ReplicaError> {
        let memory = Self::mount(&storage)
            .open(&name.to_string().into_bytes().to_vec())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        if let Some(state) = memory.content().clone() {
            Ok(Self { state, memory })
        } else {
            Err(ReplicaError::RemoteNotFound {
                remote: name.to_string(),
            })
        }
    }

    pub async fn set(
        name: &str,
        state: RemoteBranchState,
        storage: &mut Storage<Backend>,
    ) -> Result<RemoteBranch<Backend>, ReplicaError> {
        let mut memory = Self::mount(&storage)
            .open(&name.as_bytes().to_vec())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        memory
            .replace_with(|_| Some(state.clone()))
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        Ok(Self { state, memory })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteState {
    /// Name for this remote.
    id: Site,

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

/// Record used to keep track of the remote branch. It is different from the
/// local branch record and is only a wrapper around `Revision` to hold
/// information about the branch and a target site.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteBranchState {
    id: BranchId,
    revision: Revision,
}

/// Unique name for the branch
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RemoteBranchId {
    site: Site,
    branch: BranchId,
    id: String,
}
impl RemoteBranchId {
    /// Create a new RemoteBranchId
    pub fn new(site: Site, branch: BranchId) -> Self {
        let id = format!("{}@{}", branch.0, site);
        Self { site, branch, id }
    }

    /// Site of this RemoteBranchId
    pub fn site(&self) -> &Site {
        &self.site
    }

    /// Branch of this RemoteBranchId
    pub fn branch(&self) -> &BranchId {
        &self.branch
    }
}
impl KeyType for RemoteBranchId {
    fn bytes(&self) -> &[u8] {
        self.id.as_bytes()
    }
}
impl TryFrom<Vec<u8>> for RemoteBranchId {
    type Error = std::string::FromUtf8Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let id = String::from_utf8(bytes)?;
        let (branch, site) = match id.split("@").collect::<Vec<&str>>().as_slice() {
            [branch_str, site_str] => (BranchId(branch_str.to_string()), site_str.to_string()),
            _ => panic!("must have format: branch@site"),
        };

        Ok(RemoteBranchId::new(site, branch))
    }
}
impl Display for RemoteBranchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
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
    upstream: Option<Upstream>,
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
    pub fn new(id: BranchId, description: Option<String>) -> Self {
        let revision = Revision::default();
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
    pub fn upstream(&self) -> Option<&Upstream> {
        self.upstream.as_ref()
    }
}

// Branch struct and implementation moved to platform.rs
// The complex git-like operations (fetch, pull, push, sync) will be reimplemented
// using the new Resource-based API

/* Commented out - needs migration to Resource-based API
pub struct Branch<'a, P: Platform> {
    state: BranchState,
    issuer: Principal,
    platform: &'a P,
}
impl<'a, P: Platform> Branch<'a, P> {
    pub fn new(state: BranchState, platform: &'a P) -> Self {
        Self {
            issuer: blake3::hash(state.id().bytes()).into(),
            state,
            platform,
        }
    }

    /// Loads a branch with a given id or creates one if it does not exist.
    pub async fn open(id: &BranchId, platform: &'a P) -> Result<Self, ReplicaError> {
        let state = platform
            .branches()
            .read(&id)
            .await
            .map_err(|e| ReplicaError::storage_error(Capability::ResolveBranch, e))?
            .unwrap_or_else(|| BranchState::new(id.clone(), None));

        Ok(Self::new(state, platform))
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
            Ok(Self::new(state, platform))
        } else {
            Err(ReplicaError::BranchNotFound { id })
        }
    }

    /// Returns unique identifier of this fork.
    pub fn id(&self) -> &BranchId {
        &self.state.id
    }

    /// Resets this branch to a given revision and a base tree.
    pub async fn reset(
        &mut self,
        revision: Revision,
        base: NodeReference,
    ) -> Result<&mut Self, ReplicaError> {
        // derive edition from the current state.
        let (current, _) = self
            .state
            .encode()
            .await
            .map_err(|e| ReplicaError::StorageError {
                capability: Capability::ArchiveError,
                cause: e,
            })?;

        // create new edition from the prior state.
        let state = BranchState {
            revision,
            id: self.state.id.clone(),
            description: self.state.description.clone(),
            upstream: self.state.upstream.clone(),
            base,
        };

        self.platform.branches().write(state.clone()).await;

        // If we were able to write a new state update
        self.state = state;

        Ok(self)
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
                        Ok(Some(state.revision().clone()))
                    } else {
                        Err(ReplicaError::BranchNotFound {
                            id: upstream.id.clone(),
                        })
                    }
                }
                Origin::Remote(remote) => {
                    // Read revision from the remote site.
                    let upstream = self
                        .platform
                        .announcements()
                        .read(&(remote.id.clone(), self.id().clone()))
                        .await
                        .map_err(|error| ReplicaError::StorageError {
                            capability: Capability::ResolveRevision,
                            cause: error,
                        })?;

                    let local = self
                        .platform
                        .revisions()
                        .read(&(remote.id.clone(), self.id().clone()))
                        .await
                        .map_err(|error| ReplicaError::StorageError {
                            capability: Capability::ResolveBranch,
                            cause: error,
                        })?;

                    let revision =
                        match (upstream, local) {
                            (Some(current), Some(local)) => {
                                let (prior, _) = local.encode().await.map_err(|e| {
                                    ReplicaError::StorageError {
                                        capability: Capability::EncodeError,
                                        cause: e,
                                    }
                                })?;
                                self.platform
                                    .revisions()
                                    .write(RemoteBranchState {
                                        id: current.id.clone(),
                                        revision: current.revision.clone(),
                                        prior: Some(prior),
                                    })
                                    .await
                                    .map_err(|error| ReplicaError::StorageError {
                                        capability: Capability::UpdateRevision,
                                        cause: error,
                                    })?;

                                Some(current.revision)
                            }
                            (Some(current), None) => {
                                self.platform
                                    .revisions()
                                    .write(current.clone())
                                    .await
                                    .map_err(|error| ReplicaError::StorageError {
                                        capability: Capability::UpdateRevision,
                                        cause: error,
                                    })?;

                                Some(current.revision)
                            }
                            (None, Some(prior)) => Some(prior.revision),
                            (None, None) => None,
                        };

                    Ok(revision)
                }
            }
        } else {
            Err(ReplicaError::BranchNotFound {
                id: self.id().clone(),
            })
        }
    }

    pub async fn pull(&mut self) -> Result<Option<&Revision>, ReplicaError> {
        if self.state.upstream.is_some() {
            if let Some(base) = self.fetch().await? {
                // If revision has changed since our last pull
                // we got to rebase our changes
                if self.state.base != base.tree {
                    let archive = self.platform.archive();
                    // load upstream tree into memory
                    let mut tree: Index<Key, Datum, P::Storage> =
                        Tree::from_hash(base.tree.hash(), archive.clone())
                            .await
                            .map_err(|error| ReplicaError::StorageError {
                                capability: Capability::ResolveBranch,
                                cause: error.into(),
                            })?;

                    // Integrate local changes into an upstream tree.
                    tree.integrate(self.differentiate()).await;

                    // Compute a new revision and replace the local one
                    let (period, moment) = if base.issuer == self.issuer {
                        (base.period, base.moment + 1)
                    } else {
                        (base.period + 1, 0)
                    };

                    self.reset(
                        Revision {
                            issuer: self.issuer,
                            tree: NodeReference(tree.hash().expect("should have hash").clone()),
                            cause: HashSet::from([Edition::<Revision>::new(
                                base.tree.hash().clone(),
                            )]),
                            period,
                            moment,
                        },
                        base.tree.clone(),
                    )
                    .await;

                    Ok(Some(&self.state.revision))
                }
                // if base is the same as our last revision there
                // is nothing to do.
                else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub async fn push(&mut self) -> Result<&mut Self, ReplicaError> {
        if let Some(upstream) = &self.state.upstream {
            let revision = self.state.revision.clone();
            match &upstream.origin {
                Origin::Local => {
                    if upstream.id() != self.id() {
                        // Load target branch that we will update.
                        let mut target = Branch::load(upstream.id().clone(), self.platform).await?;
                        // And reset it's revision to the current branch's revision
                        target.reset(revision, target.state.base.clone()).await?;
                    }
                    return Ok(self);
                }
                Origin::Remote(remote) => {
                    let key = (remote.id.clone(), self.id().clone());

                    // Get the current state to use as prior
                    let current = self
                        .platform
                        .revisions()
                        .read(&key)
                        .await
                        .map_err(|cause| ReplicaError::StorageError {
                            capability: Capability::ResolveBranch,
                            cause,
                        })?;

                    let prior = if let Some(current) = current {
                        let (edition, _) =
                            current
                                .encode()
                                .await
                                .map_err(|e| ReplicaError::StorageError {
                                    capability: Capability::EncodeError,
                                    cause: e,
                                })?;
                        Some(edition)
                    } else {
                        None
                    };

                    let state = RemoteBranchState {
                        id: RemoteBranchId::new(remote.id.clone(), self.id().clone()),
                        revision: revision.clone(),
                        prior,
                    };

                    // announce new revision to the collaborators
                    self.platform
                        .announcements()
                        .write(state.clone())
                        .await
                        .map_err(|e| ReplicaError::PushFailed { cause: e })?;
                    // if we were able to publish the revision we need to
                    // update local state
                    self.platform
                        .revisions()
                        .write(state)
                        .await
                        .map_err(|cause| ReplicaError::StorageError {
                            capability: Capability::ArchiveError,
                            cause,
                        })?;
                }
            };
        }
        return Ok(self);
    }

    pub async fn sync(&mut self) -> Result<&mut Self, ReplicaError> {
        // try pushing 10 times if all fail due to concurrency we
        // propagate error otherwise keep retrying.
        for attempt in 0..10 {
            match self.push().await {
                Ok(_) => {
                    return Ok(self);
                }
                Err(ReplicaError::PushFailed { cause }) => {
                    self.pull().await?;
                    if attempt == 9 {
                        return Err(ReplicaError::PushFailed { cause })?;
                    }
                }
                Err(reason) => return Err(reason),
            };
        }

        return Ok(self);
    }

    /// Computes all the changes that have occured on this branch since last
    /// pull. It assumes that current revision is based of the base revision
    /// and that subtrees that were updated are available locally, which would
    /// have been fetched in order to produce an update.
    pub fn differentiate(&self) -> impl Differential<Key, State<Datum>> {
        let archive = self.platform.archive();
        stream! {
            let before:Index<Key, Datum, P::Storage> = Tree::from_hash(self.state.base.hash(), archive.clone()).await?;
            let after:Index<Key, Datum, P::Storage> = Tree::from_hash(self.state.revision().tree.hash(), archive.clone()).await?;

            let diff = before.differentiate(&after);
            for await change in diff {
                yield change;
            }
        }
    }
}
*/

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

// pub struct RemoteConnection<Backend: AtomicStorageBackend<Key = String, Value = Vec<u8>>> {
//     remote: Backend,
//     local: Backend,
// }
// impl<Backend: AtomicStorageBackend<Key = String, Value = Vec<u8>>> RemoteConnection<Backend> {
//     /// Resolves revision for the given branch from the local cache of this remote.
//     /// It will not fetch the revision from the remote, if fetch is desired call `fetch`.
//     /// instead.
//     pub async fn resolve(&self, key: &BranchId) -> Result<Option<Revision>, Backend::Error> {
//         if let Some(bytes) = self.local.resolve(&key.0).await? {
//             let revision: Revision = CborEncoder
//                 .decode(&bytes)
//                 .await
//                 .map_err(|e| panic!("Failed to decode revision: {}", e))?;
//             Ok(Some(revision))
//         } else {
//             Ok(None)
//         }
//     }

//     /// Fetch a revision for the given branch from the remote.
//     pub async fn fetch(&mut self, key: &BranchId) -> Result<Option<Revision>, Backend::Error> {
//         if let Some(bytes) = self.remote.resolve(&key.0).await? {
//             let revision: Revision = CborEncoder
//                 .decode(&bytes)
//                 .await
//                 .map_err(|e| panic!("Failed to decode revision: {}", e))?;
//             // Update local cache
//             self.local
//                 .swap(
//                     key.0.clone(),
//                     Some(bytes),
//                     self.local.resolve(&key.0).await?,
//                 )
//                 .await?;

//             Ok(Some(revision))
//         } else {
//             Ok(None)
//         }
//     }

//     /// Publish revision to the remote branch.
//     pub async fn push(
//         &mut self,
//         key: &BranchId,
//         revision: &Revision,
//     ) -> Result<(), Backend::Error> {
//         let prior = self.local.resolve(&key.0).await?;
//         let (_, bytes) = CborEncoder
//             .encode(revision)
//             .await
//             .map_err(|e| panic!("Failed to encode revision: {}", e))?;
//         self.remote
//             .swap(key.0.clone(), Some(bytes.clone()), prior.clone())
//             .await;
//         self.local.swap(key.0.clone(), Some(bytes), prior).await;

//         Ok(())
//     }
// }

/// Upstream represents some branch being tracked
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Upstream {
    id: BranchId,
    origin: Origin,
}

impl Upstream {
    pub fn id(&self) -> &BranchId {
        &self.id
    }
}

/// Describes origin of the replica that is either local or a remote.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Origin {
    /// Implies local replica
    Local,
    /// Reference to a remote replica
    Remote(Remote),
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
