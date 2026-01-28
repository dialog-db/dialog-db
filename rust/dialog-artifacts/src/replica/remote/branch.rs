//! Remote branch reference and connection.

use std::fmt::Debug;

use dialog_common::capability::Did;
use dialog_prolly_tree::{KeyType, Node};
use dialog_storage::{Blake3Hash, CborEncoder, Encoder, StorageBackend};
use futures_util::{Stream, StreamExt};

use super::{
    Connection, Operator, PlatformBackend, PlatformStorage, RemoteBackend, RemoteSite, RemoteState,
    Revision, Site,
};
use crate::TypedStoreResource;
use crate::replica::ReplicaError;

/// A reference to a branch at a remote repository.
///
/// This is the final builder step that identifies a specific branch.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum RemoteBranch<Backend: PlatformBackend> {
    /// A reference to a remote branch that hasn't been opened yet.
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
    /// An open connection to a remote branch.
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

        /// Remote connection (backend-specific).
        connection: Connection,

        /// Local cache for the revision currently branch has.
        down: TypedStoreResource<Revision, Backend>,

        /// Canonical revision on the remote.
        up: TypedStoreResource<Revision, RemoteBackend>,
    },
}

impl<Backend: PlatformBackend> RemoteBranch<Backend> {
    /// Create a new reference to a remote branch.
    pub(super) fn reference(
        name: String,
        site_name: Site,
        subject: Did,
        storage: PlatformStorage<Backend>,
        issuer: Operator,
        state: Option<RemoteState>,
    ) -> Self {
        Self::Reference {
            name,
            site_name,
            subject,
            storage,
            issuer,
            state,
        }
    }
}

impl<Backend: PlatformBackend> Debug for RemoteBranch<Backend> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteBranch")
            .field("name", &self.name())
            .field("site_name", &self.site())
            .field("subject", &self.subject())
            .finish_non_exhaustive()
    }
}

impl<Backend: PlatformBackend + 'static> RemoteBranch<Backend> {
    /// Get the branch name.
    pub fn name(&self) -> &str {
        match self {
            Self::Reference { name, .. } => name,
            Self::Open { name, .. } => name,
        }
    }

    /// Get the site name.
    pub fn site(&self) -> &str {
        match self {
            Self::Reference { site_name, .. } => site_name,
            Self::Open { site_name, .. } => site_name,
        }
    }

    /// Subject repository this is a branch of
    pub fn subject(&self) -> &Did {
        match self {
            Self::Reference { subject, .. } => subject,
            Self::Open { subject, .. } => subject,
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
            Self::Open { down, .. } => down.read(),
        }
    }

    /// Get the connection to the remote memory storage (only available when open).
    /// This is used for storing branch revision state.
    pub fn connection(&self) -> Option<PlatformStorage<RemoteBackend>> {
        match self {
            Self::Reference { .. } => None,
            Self::Open { connection, .. } => Some(connection.memory()),
        }
    }

    /// Get the connection to the remote archive/index storage (only available when open).
    /// This is used by the Archive for storing and retrieving tree blocks.
    pub fn archive_connection(&self) -> Option<PlatformStorage<RemoteBackend>> {
        match self {
            Self::Reference { .. } => None,
            Self::Open { connection, .. } => Some(connection.archive()),
        }
    }

    /// Open a connection to the remote branch and return a mutable reference for chaining.
    ///
    /// If already open, this is a no-op. If in Reference state, opens the connection
    /// and transitions to Open state. On error, self remains unchanged.
    pub async fn open(&mut self) -> Result<&mut Self, ReplicaError> {
        // If already open, nothing to do
        if matches!(self, Self::Open { .. }) {
            return Ok(self);
        }

        // Clone and try to open - if successful, replace self
        // This avoids unsafe code and keeps self valid on error
        let opened = self.clone().into_open().await?;
        *self = opened;
        Ok(self)
    }

    /// Consume self and return Open variant or error.
    async fn into_open(self) -> Result<Self, ReplicaError> {
        match self {
            Self::Reference {
                name,
                site_name,
                subject,
                mut storage,
                issuer,
                state,
            } => {
                let remote_state = state.ok_or_else(|| ReplicaError::RemoteNotFound {
                    remote: site_name.clone(),
                })?;

                // Mount local storage for caching the revision
                let down = Self::mount_local(&site_name, &subject, &name, &mut storage).await?;

                // Connect to remote using credentials
                let connection = remote_state.credentials.connect(issuer.clone(), &subject)?;

                // Open remote revision storage
                let mut memory = connection.memory();
                let up = memory
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
    pub async fn resolve(&mut self) -> Result<Option<Revision>, ReplicaError> {
        // Ensure we're open
        self.open().await?;

        match self {
            Self::Open {
                storage,
                connection,
                down,
                up,
                ..
            } => {
                // Reload from upstream to get latest revision before we read.
                let mut memory = connection.memory();
                up.reload(&mut memory)
                    .await
                    .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
                let revision = up.read();

                // Update local record for the upstream revision
                down.replace_with(|_| revision.clone(), storage)
                    .await
                    .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

                Ok(down.read())
            }
            Self::Reference { .. } => Err(ReplicaError::InvalidState {
                message: "Branch should be open after successful open() call".to_string(),
            }),
        }
    }

    /// Publishes new canonical revision. Returns error if publishing fails.
    pub async fn publish(&mut self, revision: Revision) -> Result<(), ReplicaError> {
        // Ensure we're open
        self.open().await?;

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
                    let mut memory = connection.memory();
                    up.replace(Some(revision.clone()), &mut memory)
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
            Self::Reference { .. } => Err(ReplicaError::InvalidState {
                message: "Branch should be open after successful open() call".to_string(),
            }),
        }
    }

    /// Uploads novel nodes from a stream into remote storage.
    ///
    /// This method takes a stream of tree nodes (typically from `TreeDifference::novel_nodes()`)
    /// and uploads them to the remote storage. Use this before publishing a new
    /// revision to ensure all tree blocks are available on the remote.
    pub async fn upload<Key, Value, E, S>(&mut self, nodes: S) -> Result<(), ReplicaError>
    where
        Key: KeyType + 'static,
        Value: dialog_prolly_tree::ValueType,
        E: std::fmt::Debug,
        S: Stream<Item = Result<Node<Key, Value, Blake3Hash>, E>>,
    {
        // Ensure we're open
        self.open().await?;

        match self {
            Self::Open { connection, .. } => {
                tokio::pin!(nodes);

                // Get the archive storage backend
                let mut archive = connection.archive();

                while let Some(result) = nodes.next().await {
                    let node =
                        result.map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

                    // Use hash directly as key
                    let key = node.hash().to_vec();

                    // Encode the block using the standard encoder
                    let (_hash, bytes) = CborEncoder.encode(node.block()).await.map_err(|e| {
                        ReplicaError::StorageError(format!("Failed to encode block: {:?}", e))
                    })?;

                    // Upload the block using StorageBackend trait
                    StorageBackend::set(&mut archive, key, bytes)
                        .await
                        .map_err(|e| {
                            ReplicaError::StorageError(format!("Upload failed: {:?}", e))
                        })?;
                }

                Ok(())
            }
            Self::Reference { .. } => Err(ReplicaError::InvalidState {
                message: "Branch should be open after successful open() call".to_string(),
            }),
        }
    }

    /// Create a new RemoteBranch from site, branch and storage (for Upstream::open).
    pub async fn new(
        name: &Site,
        branch: &str,
        storage: PlatformStorage<Backend>,
        issuer: Operator,
        subject: Did,
    ) -> Result<Self, ReplicaError> {
        Ok(Self::Reference {
            // Load the remote site as we need it's credentials
            state: RemoteSite::load(name, issuer.clone(), storage.clone())
                .await?
                .state(),

            name: branch.to_string(),
            site_name: name.clone(),
            subject,
            storage,
            issuer,
        })
    }
}
