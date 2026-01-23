//! Remote repository credentials and configuration.
//!
//! This module defines the credentials types used to connect to remote
//! repositories for synchronization.

use std::fmt::Debug;

use dialog_common::capability::Did;
use dialog_common::{DialogAsyncError, TaskQueue};
use dialog_prolly_tree::{KeyType, Node};
use dialog_s3_credentials::s3;
#[cfg(feature = "ucan")]
pub use dialog_s3_credentials::{ucan, ucan::DelegationChain};
#[cfg(feature = "s3")]
use dialog_storage::s3::{Bucket, S3};
use dialog_storage::{Blake3Hash, CborEncoder, Encoder, StorageBackend};
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use super::{Operator, PlatformBackend, RemoteBackend, RemoteState, Revision};
use crate::replica::ReplicaError;
use crate::{ErrorMappingBackend, PlatformStorage, TypedStoreResource};

/// A named remote site identifier.
pub type Site = String;

/// Represents a configured remote site with its credentials.
///
/// This is the persisted state for a remote, storing the site name
/// and the credentials needed to connect to it.
pub struct RemoteSite<Backend: PlatformBackend> {
    /// The site name.
    pub name: Site,
    /// Memory cell storing the remote state.
    memory: TypedStoreResource<RemoteState, Backend>,
    /// Storage for persistence (cloned, cheap).
    storage: PlatformStorage<Backend>,
    /// Issuer for signing requests.
    issuer: Operator,
    /// Subject DID for this replica.
    subject: Did,
}

impl<Backend: PlatformBackend + 'static> RemoteSite<Backend> {
    /// Add a new remote site, persisting its state to storage.
    pub async fn add(
        state: RemoteState,
        mut storage: PlatformStorage<Backend>,
        issuer: Operator,
        subject: Did,
    ) -> Result<Self, ReplicaError> {
        let memory = Self::mount(&state.site, &mut storage).await?;

        // Check if remote already exists
        if let Some(existing_state) = memory.read() {
            if state != existing_state {
                return Err(ReplicaError::RemoteAlreadyExists {
                    remote: state.site.clone(),
                });
            }
            // Same state, just return the existing site
            return Ok(Self {
                name: state.site,
                memory,
                storage,
                issuer,
                subject,
            });
        }

        // Persist the new state
        memory
            .replace(Some(state.clone()), &mut storage)
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        Ok(Self {
            name: state.site,
            memory,
            storage,
            issuer,
            subject,
        })
    }

    /// Load an existing remote site from storage.
    pub async fn load(
        site: &Site,
        mut storage: PlatformStorage<Backend>,
        issuer: Operator,
        subject: Did,
    ) -> Result<Self, ReplicaError> {
        let memory = Self::mount(site, &mut storage).await?;

        if memory.read().is_some() {
            Ok(Self {
                name: site.clone(),
                memory,
                storage,
                issuer,
                subject,
            })
        } else {
            Err(ReplicaError::RemoteNotFound {
                remote: site.clone(),
            })
        }
    }

    /// Get the remote state.
    pub fn state(&self) -> Option<RemoteState> {
        self.memory.read()
    }

    /// Connect to the remote S3 storage.
    #[cfg(feature = "s3")]
    pub fn connect(&self) -> Result<S3<Operator>, ReplicaError> {
        if let Some(state) = self.memory.read() {
            let s3 = S3::new(state.credentials.clone(), self.issuer.clone());
            Ok(s3)
        } else {
            Err(ReplicaError::RemoteNotFound {
                remote: self.name.clone(),
            })
        }
    }

    /// Mount the transactional memory cell for a remote site.
    async fn mount(
        site: &Site,
        storage: &mut PlatformStorage<Backend>,
    ) -> Result<TypedStoreResource<RemoteState, Backend>, ReplicaError> {
        let address = format!("site/{}", site);
        storage
            .open::<RemoteState>(&address.into_bytes())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))
    }

    /// Start building a reference to a repository at this remote site.
    ///
    /// The `subject` is the DID identifying the repository owner.
    pub fn repository(&self, subject: impl Into<Did>) -> RemoteRepository<Backend> {
        RemoteRepository {
            site_name: self.name.clone(),
            subject: subject.into(),
            storage: self.storage.clone(),
            issuer: self.issuer.clone(),
            state: self.memory.read(),
        }
    }
}

/// A reference to a repository on a remote site.
///
/// This is a builder step for accessing remote branches.
#[derive(Clone)]
pub struct RemoteRepository<Backend: PlatformBackend> {
    /// The subject DID identifying the repository owner.
    pub subject: Did,
    /// The remote site name.
    pub site_name: Site,
    /// Storage for persistence (cloned, cheap).
    storage: PlatformStorage<Backend>,
    /// Issuer for signing requests.
    issuer: Operator,
    /// The remote state (credentials).
    state: Option<RemoteState>,
}

impl<Backend: PlatformBackend + 'static> RemoteRepository<Backend> {
    /// Reference a branch within this remote repository.
    pub fn branch(&self, name: impl Into<String>) -> RemoteBranch<Backend> {
        RemoteBranch::Reference {
            name: name.into(),
            site_name: self.site_name.clone(),
            subject: self.subject.clone(),
            storage: self.storage.clone(),
            issuer: self.issuer.clone(),
            state: self.state.clone(),
        }
    }
}

/// A reference to a branch at a remote repository.
///
/// This is the final builder step that identifies a specific branch.
#[derive(Clone)]
pub enum RemoteBranch<Backend: PlatformBackend> {
    Reference {
        /// The branch name.
        name: String,
        /// The site name.
        site_name: Site,
        /// The subject DID.
        subject: Did,
        /// Storage for persistence (cloned, cheap).
        storage: PlatformStorage<Backend>,
        /// Issuer for signing requests.
        issuer: Operator,
        /// The remote state (credentials).
        state: Option<RemoteState>,
    },
    #[cfg(feature = "s3")]
    Open {
        /// The branch name.
        name: String,
        /// The site name.
        site_name: Site,
        /// The subject DID.
        subject: Did,
        /// Storage for persistence (cloned, cheap).
        storage: PlatformStorage<Backend>,
        /// Issuer for signing requests.
        issuer: Operator,

        /// Remote connection (memory storage).
        connection: PlatformStorage<RemoteBackend>,

        /// Remote tree index store.
        index: Bucket<Operator>,

        /// Local cache for the revision currently branch has.
        down: TypedStoreResource<Revision, Backend>,

        /// Canonical revision on the remote.
        up: TypedStoreResource<Revision, RemoteBackend>,
    },
}

impl<Backend: PlatformBackend> Debug for RemoteBranch<Backend> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reference {
                name,
                site_name,
                subject,
                ..
            } => f
                .debug_struct("RemoteBranch::Reference")
                .field("name", name)
                .field("site_name", site_name)
                .field("subject", subject)
                .finish_non_exhaustive(),
            #[cfg(feature = "s3")]
            Self::Open {
                name,
                site_name,
                subject,
                ..
            } => f
                .debug_struct("RemoteBranch::Open")
                .field("name", name)
                .field("site_name", site_name)
                .field("subject", subject)
                .finish_non_exhaustive(),
        }
    }
}

impl<Backend: PlatformBackend + 'static> RemoteBranch<Backend> {
    /// Get the branch name.
    pub fn name(&self) -> &str {
        match self {
            Self::Reference { name, .. } => name,
            #[cfg(feature = "s3")]
            Self::Open { name, .. } => name,
        }
    }

    /// Get the site name.
    pub fn site(&self) -> &Site {
        match self {
            Self::Reference { site_name, .. } => site_name,
            #[cfg(feature = "s3")]
            Self::Open { site_name, .. } => site_name,
        }
    }

    /// Get the branch id.
    pub fn id(&self) -> &str {
        self.name()
    }

    /// Get the current revision (from local cache).
    pub fn revision(&self) -> Option<Revision> {
        match self {
            Self::Reference { .. } => None,
            #[cfg(feature = "s3")]
            Self::Open { down, .. } => down.read(),
        }
    }

    /// Get the connection to the remote storage (only available when open).
    #[cfg(feature = "s3")]
    pub fn connection(&self) -> Option<PlatformStorage<RemoteBackend>> {
        match self {
            Self::Reference { .. } => None,
            Self::Open { connection, .. } => Some(connection.clone()),
        }
    }

    /// Open a connection to the remote branch.
    #[cfg(feature = "s3")]
    pub async fn open(self) -> Result<Self, ReplicaError> {
        match self {
            Self::Reference {
                name,
                site_name,
                subject,
                mut storage,
                issuer,
                state,
            } => {
                let state = state.ok_or_else(|| ReplicaError::RemoteNotFound {
                    remote: site_name.clone(),
                })?;

                // Mount local storage for caching the revision
                let down = Self::mount_local(&site_name, &subject, &name, &mut storage).await?;

                // Connect to remote
                let s3 = S3::new(state.credentials.clone(), issuer.clone());
                let memory_bucket = Bucket::new(s3.clone(), &subject, "memory");
                let mut connection =
                    PlatformStorage::new(ErrorMappingBackend::new(memory_bucket), CborEncoder);
                let index = Bucket::new(s3, &subject, "archive/index");

                // Open remote revision storage
                let up = connection
                    .open::<Revision>(&format!("local/{}", &name).into_bytes())
                    .await
                    .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

                Ok(Self::Open {
                    name,
                    site_name,
                    subject,
                    storage,
                    issuer,
                    connection,
                    index,
                    down,
                    up,
                })
            }
            Self::Open { .. } => Ok(self),
        }
    }

    /// Mount the transactional memory for a remote branch from local storage.
    async fn mount_local(
        site: &Site,
        subject: &Did,
        branch: &str,
        storage: &mut PlatformStorage<Backend>,
    ) -> Result<TypedStoreResource<Revision, Backend>, ReplicaError> {
        let address = format!("remote/{}/{}/{}", site, subject, branch);
        storage
            .open::<Revision>(&address.into_bytes())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))
    }

    /// Resolves remote revision for this branch. If remote revision is different
    /// from local revision, updates local one to match the remote. Returns
    /// revision of this branch.
    #[cfg(feature = "s3")]
    pub async fn resolve(&mut self) -> Result<Option<Revision>, ReplicaError> {
        // Ensure we're open
        let this = std::mem::replace(self, unsafe { std::mem::zeroed() });
        *self = this.open().await?;

        match self {
            Self::Open {
                storage,
                connection,
                down,
                up,
                ..
            } => {
                // Force reload from storage to ensure we get fresh data
                let _ = up.reload(connection).await;
                let revision = up.read();

                // Update local record for the revision
                down.replace_with(|_| revision.clone(), storage)
                    .await
                    .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

                Ok(down.read())
            }
            _ => unreachable!("We just opened"),
        }
    }

    /// Publishes new canonical revision. Returns error if publishing fails.
    #[cfg(feature = "s3")]
    pub async fn publish(&mut self, revision: Revision) -> Result<(), ReplicaError> {
        // Ensure we're open
        let this = std::mem::replace(self, unsafe { std::mem::zeroed() });
        *self = this.open().await?;

        match self {
            Self::Open {
                storage,
                connection,
                down,
                up,
                ..
            } => {
                let prior = down.read();

                // We only need to publish to upstream if desired revision is different
                // from the last revision we have read from upstream.
                if up.read().as_ref() != Some(&revision) {
                    up.replace(Some(revision.clone()), connection)
                        .await
                        .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
                }

                // If revision for the remote branch is different from one published,
                // we got to update local revision.
                if prior.as_ref() != Some(&revision) {
                    down.replace_with(|_| Some(revision.clone()), storage)
                        .await
                        .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
                }

                Ok(())
            }
            _ => unreachable!("We just opened"),
        }
    }

    /// Uploads novel nodes from a stream into remote storage.
    ///
    /// This method takes a stream of tree nodes (typically from `TreeDifference::novel_nodes()`)
    /// and pushes them concurrently to the remote storage. Use this before publishing a new
    /// revision to ensure all tree blocks are available on the remote.
    #[cfg(feature = "s3")]
    pub async fn upload<Key, Value, E, S>(&mut self, nodes: S) -> Result<(), ReplicaError>
    where
        Key: KeyType + 'static,
        Value: dialog_prolly_tree::ValueType,
        E: std::fmt::Debug,
        S: Stream<Item = Result<Node<Key, Value, Blake3Hash>, E>>,
    {
        use futures_util::pin_mut;

        // Ensure we're open
        let this = std::mem::replace(self, unsafe { std::mem::zeroed() });
        *self = this.open().await?;

        match self {
            Self::Open { index, .. } => {
                let mut queue = TaskQueue::default();
                pin_mut!(nodes);

                while let Some(result) = nodes.next().await {
                    let node =
                        result.map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

                    // Build the key for this block
                    let hash = *node.hash();

                    // Encode the block using the standard encoder
                    let (_hash, bytes) = CborEncoder.encode(node.block()).await.map_err(|e| {
                        ReplicaError::StorageError(format!("Failed to encode block: {:?}", e))
                    })?;

                    // Clone what we need for the spawned task
                    let mut store = index.clone();

                    // Spawn concurrent upload task
                    queue.spawn(async move {
                        store
                            .set(hash.as_slice().to_vec(), bytes.into())
                            .await
                            .map_err(|_| DialogAsyncError::JoinError)
                    });
                }

                // Wait for all uploads to complete
                queue
                    .join()
                    .await
                    .map_err(|e| ReplicaError::StorageError(format!("Upload failed: {:?}", e)))?;

                Ok(())
            }
            _ => unreachable!("We just opened"),
        }
    }

    /// Create a new RemoteBranch from site, branch and storage (for Upstream::open).
    #[cfg(feature = "s3")]
    pub async fn new(
        site: &Site,
        branch: &str,
        storage: PlatformStorage<Backend>,
        issuer: Operator,
        subject: Did,
    ) -> Result<Self, ReplicaError> {
        // Load the remote site to get credentials
        let site_obj =
            RemoteSite::load(site, storage.clone(), issuer.clone(), subject.clone()).await?;
        let state = site_obj.state();

        Ok(Self::Reference {
            name: branch.to_string(),
            site_name: site.clone(),
            subject,
            storage,
            issuer,
            state,
        })
    }
}

/// Credentials for connecting to a remote repository.
///
/// This enum stores the credentials configuration that can be persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoteCredentials {
    /// Direct S3 access with optional signing credentials.
    S3(s3::Credentials),
    /// UCAN-based access via an authorization service.
    #[cfg(feature = "ucan")]
    Ucan(ucan::Credentials),
}

impl RemoteCredentials {
    /// Create S3 credentials for public access.
    pub fn s3_public(
        endpoint: impl Into<String>,
        region: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Result<Self, ReplicaError> {
        let address = s3::Address::new(endpoint, region, bucket);
        s3::Credentials::public(address)
            .map(Self::S3)
            .map_err(|e| ReplicaError::StorageError(e.to_string()))
    }

    /// Create S3 credentials with signing keys.
    pub fn s3_private(
        endpoint: impl Into<String>,
        region: impl Into<String>,
        bucket: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Result<Self, ReplicaError> {
        let address = s3::Address::new(endpoint, region, bucket);
        s3::Credentials::private(address, access_key_id, secret_access_key)
            .map(Self::S3)
            .map_err(|e| ReplicaError::StorageError(e.to_string()))
    }

    /// Create UCAN credentials from an optional delegation chain.
    #[cfg(feature = "ucan")]
    pub fn ucan(endpoint: impl Into<String>, delegation: DelegationChain) -> Self {
        Self::Ucan(ucan::Credentials::new(endpoint.into(), delegation))
    }
}
