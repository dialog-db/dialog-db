//! Remote branch reference and connection.

use std::fmt::Debug;

use dialog_capability::{Authority, Did};
use dialog_prolly_tree::{KeyType, Node};
use dialog_storage::{Blake3Hash, CborEncoder, Encoder, StorageBackend};
use futures_util::{Stream, StreamExt};

use super::{
    Connection, Operator, PlatformBackend, PlatformStorage, RemoteBackend, RemoteCredentials,
    RemoteSite, Revision, Site,
};
use crate::TypedStoreResource;
use crate::replica::ReplicaError;

/// Descriptor for a remote branch that hasn't been connected yet.
///
/// This holds the configuration needed to establish a connection.
#[derive(Clone)]
pub struct RemoteBranchDescriptor<Backend: PlatformBackend, A: Authority + Clone + Debug = Operator>
{
    name: String,
    site_name: Site,
    subject: Did,
    storage: PlatformStorage<Backend>,
    issuer: A,
    credentials: Option<RemoteCredentials>,
}

impl<Backend: PlatformBackend, A: Authority + Clone + Debug> RemoteBranchDescriptor<Backend, A> {
    /// Get the branch name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the site name.
    pub fn site(&self) -> &str {
        &self.site_name
    }

    /// Subject repository this is a branch of.
    pub fn subject(&self) -> &Did {
        &self.subject
    }
}

impl<Backend: PlatformBackend, A: Authority + Clone + Debug> Debug
    for RemoteBranchDescriptor<Backend, A>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteBranchDescriptor")
            .field("name", &self.name)
            .field("site_name", &self.site_name)
            .field("subject", &self.subject)
            .finish_non_exhaustive()
    }
}

/// An open connection to a remote branch.
///
/// This holds the active connection and revision tracking resources.
pub struct RemoteBranchConnection<Backend: PlatformBackend, A: Authority + Clone + Debug = Operator>
{
    descriptor: RemoteBranchDescriptor<Backend, A>,
    connection: Connection,
    down: TypedStoreResource<Revision, Backend>,
    up: TypedStoreResource<Revision, RemoteBackend>,
}

impl<Backend: PlatformBackend, A: Authority + Clone + Debug> Debug
    for RemoteBranchConnection<Backend, A>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteBranchConnection")
            .field("name", &self.descriptor.name)
            .field("site_name", &self.descriptor.site_name)
            .field("subject", &self.descriptor.subject)
            .finish_non_exhaustive()
    }
}

impl<Backend: PlatformBackend, A: Authority + Clone + Debug> Clone
    for RemoteBranchConnection<Backend, A>
{
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            connection: self.connection.clone(),
            down: self.down.clone(),
            up: self.up.clone(),
        }
    }
}

impl<Backend: PlatformBackend + 'static, A: Authority + Clone + Debug + 'static>
    RemoteBranchConnection<Backend, A>
{
    /// Get the branch name.
    pub fn name(&self) -> &str {
        &self.descriptor.name
    }

    /// Get the site name.
    pub fn site(&self) -> &str {
        &self.descriptor.site_name
    }

    /// Subject repository this is a branch of.
    pub fn subject(&self) -> &Did {
        &self.descriptor.subject
    }

    /// Get the branch id.
    pub fn id(&self) -> &str {
        &self.descriptor.name
    }

    /// Get the current revision (from local cache).
    pub fn revision(&self) -> Option<Revision> {
        self.down.read()
    }

    /// Get the archive storage backend for reading/writing tree blocks.
    pub fn archive(&self) -> PlatformStorage<RemoteBackend> {
        self.connection.archive()
    }

    /// Get the memory storage backend.
    pub fn memory(&self) -> PlatformStorage<RemoteBackend> {
        self.connection.memory()
    }

    /// Resolves remote revision for this branch. If remote revision is different
    /// from local revision, updates local one to match the remote. Returns
    /// revision of this branch.
    pub async fn resolve(&mut self) -> Result<Option<Revision>, ReplicaError> {
        // Reload from upstream to get latest revision before we read.
        let mut memory = self.connection.memory();
        self.up
            .reload(&mut memory)
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
        let revision = self.up.read();

        // Update local record for the upstream revision
        self.down
            .replace_with(|_| revision.clone(), &mut self.descriptor.storage.clone())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        Ok(self.down.read())
    }

    /// Publishes new canonical revision. Returns error if publishing fails.
    pub async fn publish(&mut self, revision: Revision) -> Result<(), ReplicaError> {
        let prior = self.down.read();

        // We only need to publish to upstream if desired revision is different
        // from the last revision we have read from upstream.
        if self.up.read().as_ref() != Some(&revision) {
            let mut memory = self.connection.memory();
            self.up
                .replace(Some(revision.clone()), &mut memory)
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
        }

        // If revision for the remote branch is different from one published,
        // we got to update local revision.
        if prior.as_ref() != Some(&revision) {
            self.down
                .replace_with(
                    |_| Some(revision.clone()),
                    &mut self.descriptor.storage.clone(),
                )
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
        }

        Ok(())
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
        tokio::pin!(nodes);

        // Get the archive storage backend
        let mut archive = self.connection.archive();

        while let Some(result) = nodes.next().await {
            let node = result.map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

            // Use hash directly as key
            let key = node.hash().to_vec();

            // Encode the block using the standard encoder
            let (_hash, bytes) = CborEncoder.encode(node.block()).await.map_err(|e| {
                ReplicaError::StorageError(format!("Failed to encode block: {:?}", e))
            })?;

            // Upload the block using StorageBackend trait
            StorageBackend::set(&mut archive, key, bytes)
                .await
                .map_err(|e| ReplicaError::StorageError(format!("Upload failed: {:?}", e)))?;
        }

        Ok(())
    }
}

/// A reference to a branch at a remote repository.
///
/// Can be either a descriptor (not yet connected) or an open connection.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum RemoteBranch<Backend: PlatformBackend, A: Authority + Clone + Debug = Operator> {
    /// A reference to a remote branch that hasn't been connected yet.
    Reference(RemoteBranchDescriptor<Backend, A>),
    /// An open connection to a remote branch.
    Open(RemoteBranchConnection<Backend, A>),
}

impl<Backend: PlatformBackend, A: Authority + Clone + Debug> Debug for RemoteBranch<Backend, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reference(desc) => f
                .debug_struct("RemoteBranch::Reference")
                .field("name", &desc.name)
                .field("site_name", &desc.site_name)
                .field("subject", &desc.subject)
                .finish_non_exhaustive(),
            Self::Open(conn) => f
                .debug_struct("RemoteBranch::Open")
                .field("name", &conn.descriptor.name)
                .field("site_name", &conn.descriptor.site_name)
                .field("subject", &conn.descriptor.subject)
                .finish_non_exhaustive(),
        }
    }
}

impl<Backend: PlatformBackend, A: Authority + Clone + Debug + 'static> RemoteBranch<Backend, A> {
    /// Create a new reference to a remote branch.
    pub(super) fn reference(
        name: String,
        site_name: Site,
        subject: Did,
        storage: PlatformStorage<Backend>,
        issuer: A,
        credentials: Option<RemoteCredentials>,
    ) -> Self {
        Self::Reference(RemoteBranchDescriptor {
            name,
            site_name,
            subject,
            storage,
            issuer,
            credentials,
        })
    }

    /// Get the descriptor, whether connected or not.
    fn descriptor(&self) -> &RemoteBranchDescriptor<Backend, A> {
        match self {
            Self::Reference(desc) => desc,
            Self::Open(conn) => &conn.descriptor,
        }
    }
}

impl<Backend: PlatformBackend + 'static, A: Authority + Clone + Debug + 'static>
    RemoteBranch<Backend, A>
{
    /// Get the branch name.
    pub fn name(&self) -> &str {
        self.descriptor().name()
    }

    /// Get the site name.
    pub fn site(&self) -> &str {
        self.descriptor().site()
    }

    /// Subject repository this is a branch of.
    pub fn subject(&self) -> &Did {
        self.descriptor().subject()
    }

    /// Get the branch id.
    pub fn id(&self) -> &str {
        self.descriptor().name()
    }

    /// Connect to the remote and return the connection.
    ///
    /// This establishes the connection if needed (transitioning from Reference to Open)
    /// and returns a reference to the connection.
    pub async fn open(&mut self) -> Result<&mut RemoteBranchConnection<Backend, A>, ReplicaError> {
        if let Self::Reference(desc) = self {
            let credentials =
                desc.credentials
                    .as_ref()
                    .ok_or_else(|| ReplicaError::RemoteNotFound {
                        remote: desc.site_name.clone(),
                    })?;

            // Mount local storage for caching the revision
            let address = format!("remote/{}/{}/{}", desc.site_name, desc.subject, desc.name);
            let down = desc
                .storage
                .clone()
                .open::<Revision>(&address.into_bytes())
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

            // Connect to remote using credentials.
            // Remote S3 operations require an Operator with secret key access.
            // Construct one from the Authority's secret key bytes if available.
            let operator = match desc.issuer.secret_key_bytes() {
                Some(bytes) => Operator::from_secret(&bytes),
                None => {
                    return Err(ReplicaError::StorageError(
                        "Remote operations require an authority with extractable key material"
                            .to_string(),
                    ));
                }
            };
            let connection = credentials.connect(operator, &desc.subject)?;

            // Open remote revision storage
            let mut memory = connection.memory();
            let up = memory
                .open::<Revision>(&format!("local/{}", &desc.name).into_bytes())
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

            // Transition to Open state
            *self = Self::Open(RemoteBranchConnection {
                descriptor: desc.clone(),
                connection,
                down,
                up,
            });
        }

        match self {
            Self::Open(connection) => Ok(connection),
            Self::Reference(_) => unreachable!("Should be Open after connection"),
        }
    }

    /// Create a new RemoteBranch from site, branch and storage (for Upstream::open).
    pub async fn new(
        site_name: &Site,
        branch: &str,
        storage: PlatformStorage<Backend>,
        issuer: A,
        subject: Did,
    ) -> Result<Self, ReplicaError> {
        // Load the remote site to get credentials
        let credentials = RemoteSite::load(site_name, issuer.clone(), storage.clone())
            .await?
            .state()
            .map(|s| s.credentials);

        Ok(Self::Reference(RemoteBranchDescriptor {
            name: branch.to_string(),
            site_name: site_name.clone(),
            subject,
            storage,
            issuer,
            credentials,
        }))
    }
}
