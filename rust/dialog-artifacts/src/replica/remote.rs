//! Remote repository credentials and configuration.
//!
//! This module defines the credentials types used to connect to remote
//! repositories for synchronization.

use dialog_common::capability::{Capability, Subject};
use dialog_common::{ConditionalSend, DialogAsyncError, TaskQueue};
use dialog_prolly_tree::{
    Differential, EMPT_TREE_HASH, Entry, GeometricDistribution, KeyType, Node, Tree, TreeDifference,
};
use dialog_s3_credentials::capability::{archive, memory};
use dialog_s3_credentials::{AccessError, credentials, s3};
#[cfg(feature = "ucan")]
pub use dialog_s3_credentials::{ucan, ucan::DelegationChain};
#[cfg(feature = "s3")]
use dialog_storage::s3::{Bucket, S3};
use dialog_storage::{Blake3Hash, CborEncoder, DialogStorageError, Encoder, StorageBackend};
use futures_util::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use url::Url;

use super::{Operator, RemoteBackend, RemoteState, Replica};
use crate::replica::{ReplicaError, Revision};
use crate::{ErrorMappingBackend, PlatformBackend, PlatformStorage, TypedStoreResource};

/// A named remote site identifier.
pub type Site = String;

/// Represents a configured remote site with its credentials.
///
/// This is the persisted state for a remote, storing the site name
/// and the credentials needed to connect to it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteSite<'a, Backend: PlatformBackend> {
    pub name: String,
    pub memory: TypedStoreResource<RemoteState, Backend>,

    session: &'a mut Replica<Backend>,
}

impl<'a, Backend: PlatformBackend> RemoteSite<'a, Backend> {
    pub async fn add(
        state: RemoteState,
        session: &'a mut Replica<Backend>,
    ) -> Result<Self, ReplicaError> {
        let memory = Self::mount(&state.site, &session.storage).await?;
        let mut alread_exists = false;
        if let Some(existing_state) = memory.read() {
            alread_exists = true;
            if state != existing_state {
                return Err(ReplicaError::RemoteAlreadyExists {
                    remote: state.site.to_string(),
                });
            }
        }

        let site = Self {
            name: state.site,
            memory,
            session,
        };

        if !alread_exists {
            memory
                .replace(Some(state.clone()), &mut session.storage)
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
        }

        Ok(site)
    }

    pub async fn load(
        site: impl Into<Site>,
        session: &'a mut Replica<Backend>,
    ) -> Result<Self, ReplicaError> {
        let memory = Self::mount(site, &session.storage).await?;
        if let Some(state) = memory.read().clone() {
            Ok(Self {
                name: site.into(),
                memory,
                session,
            })
        } else {
            Err(ReplicaError::RemoteNotFound {
                remote: site.into(),
            })
        }
    }

    #[cfg(feature = "s3")]
    pub async fn connect(&mut self) -> Result<S3<Operator>, ReplicaError> {
        if let Some(state) = self.memory.read() {
            let s3 = S3::new(state.credentials.clone(), self.session.issuer.clone());
            Ok(s3)
        } else {
            ReplicaError::RemoteNotFound { remote: self.name }
        }
    }

    /// Mounts the transactional memory for a remote site from storage.
    pub async fn mount(
        site: impl Into<Site>,
        storage: &PlatformStorage<Backend>,
    ) -> Result<TypedStoreResource<RemoteState, Backend>, ReplicaError> {
        let address = format!("site/{}", site);
        let memory = storage
            .open::<RemoteState>(&address.into_bytes())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        Ok(memory)
    }

    /// Start building a reference to a repository at this remote site.
    ///
    /// The `subject` is the DID identifying the repository owner.
    pub fn repository(&'a self, subject: impl Into<String>) -> RemoteRepository<'a, Backend> {
        RemoteRepository {
            site: self,
            subject: subject.into(),
        }
    }
}

/// A reference to a repository on a remote site.
///
/// This is a builder step for accessing remote branches.
#[derive(Debug, Clone)]
pub struct RemoteRepository<'a, Backend: PlatformBackend> {
    /// The subject DID identifying the repository owner.
    pub subject: String,
    /// The remote site this repository is on.
    pub site: RemoteSite<'a, Backend>,
}

impl<'a, Backend: PlatformBackend> RemoteRepository<'a, Backend> {
    /// Reference a branch within this remote repository.
    pub fn branch(&'a self, name: impl Into<String>) -> RemoteBranch<'a, Backend> {
        RemoteBranch::Reference {
            name: name.into(),
            repository: self,
        }
    }
}

/// A reference to a branch at a remote repository.
///
/// This is the final builder step that identifies a specific branch.
/// Named `RemoteBranchRef` to distinguish from `RemoteBranch<Backend>`
/// which is the actual connected branch.
#[derive(Debug, Clone)]
pub enum RemoteBranch<'a, Backend: PlatformBackend> {
    Reference {
        /// The branch name.
        name: String,
        /// The remote repository this branch is in.
        repository: &'a RemoteRepository<'a, Backend>,
    },
    Open {
        /// The branch name.
        name: String,
        /// The remote repository this branch is in.
        repository: &'a RemoteRepository<'a, Backend>,

        /// Remote connnection
        connection: PlatformStorage<RemoteBackend>,

        /// Remote tree index store
        index: Bucket<Operator>,

        /// Local cache for the revision currently branch has
        down: TypedStoreResource<Revision, Backend>,

        /// Canonical revision, which is created lazily on fetch.
        up: TypedStoreResource<Revision, RemoteBackend>,
    },
}

impl<'a, Backend: PlatformBackend> RemoteBranch<'a, Backend> {
    pub fn name(&self) {
        match self {
            Self::Reference { name, .. } => name,
            Self::Open { name, .. } => name,
        }
    }
    pub async fn open(mut self) -> Result<Self, ReplicaError> {
        Ok(match self {
            Self::Reference { name, repository } => {
                let down = Self::mount(
                    &repository.site.name,
                    &repository.subject,
                    name,
                    &mut repository.site.session.storage,
                )
                .await?;

                let remote = repository.site.connect().await?;
                let memory = Bucket::new(remote.clone(), &repository.subject, "memory");
                let connection =
                    PlatformStorage::new(ErrorMappingBackend::new(memory), CborEncoder);
                let index = Bucket::new(remote, &repository.subject, "archive/index");

                let up = connection
                    .open::<Revision>(&format!("local/{}", &name).into_bytes())
                    .await
                    .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

                *self = Self::Open {
                    name,
                    repository,
                    connection,
                    index,
                    down,
                    up,
                };

                self
            }
            Self::Open { .. } => self,
        })
    }

    /// Mounts the transactional memory for a remote branch from local storage.
    async fn mount(
        site: impl Into<Site>,
        repository: impl Into<Did>,
        branch: impl Into<String>,
        storage: &PlatformStorage<Backend>,
    ) -> Result<TypedStoreResource<Revision, Backend>, ReplicaError> {
        // Open a localy stored revision for this branch
        let address = format!("remote/{}/{}/{}", site, repository, branch);
        let memory = storage
            .open::<Revision>(&address.into_bytes())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        Ok(memory)
    }

    /// Resolves remote revision for this branch. If remote revision is different
    /// from local revision updates local one to match the remote. Returns
    /// revision of this branch.
    pub async fn resolve(&mut self) -> Result<Option<Revision>, ReplicaError> {
        match self.open().await? {
            Self::Open {
                name,
                repository,
                connection,
                index,
                down,
                up,
            } => {
                // Force reload from storage to ensure we get fresh data
                let _ = up.reload(&connection).await;
                let revision = up.read().clone();
                // update local record for the revision.
                down.replace_with(|_| revision.clone(), &repository.site.session.storage)
                    .await
                    .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

                Ok(down.read())
            }
            _ => unreachable!("We just opened"),
        }
    }

    /// Publishes new canonical revision. Returns error if publishing fails.
    pub async fn publish(&mut self, revision: Revision) -> Result<(), ReplicaError> {
        match self.open().await? {
            Self::Open {
                name,
                repository,
                connection,
                index,
                down,
                up,
            } => {
                let prior = down.read();

                // we only need to publish to upstream if desired revision is different
                // from the last revision we have read from upstream.
                if up.read().as_ref() != Some(&revision) {
                    up.replace(Some(revision.clone()), &mut connection)
                        .await
                        .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
                }

                // if revision for the remote branch is different from one published
                // we got to update local revision. We return revision we replaced
                if prior.as_ref() != Some(&revision) {
                    down.replace_with(
                        |_| Some(revision.clone()),
                        &mut repository.site.session.storage,
                    )
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
    ///
    /// # Arguments
    /// * `nodes` - A stream of nodes to import
    ///
    /// # Example
    ///
    /// ```text
    /// // After computing a TreeDifference, import novel nodes:
    /// remote.upload(diff.novel_nodes()).await?;
    /// ```
    pub async fn upload<Key, Value, E, S>(&mut self, nodes: S) -> Result<(), ReplicaError>
    where
        Key: KeyType + 'static,
        Value: dialog_prolly_tree::ValueType,
        E: std::fmt::Debug,
        S: Stream<Item = Result<Node<Key, Value, Blake3Hash>, E>>,
    {
        use futures_util::pin_mut;

        match self.open().await? {
            Self::Open {
                name,
                repository,
                connection,
                index,
                down,
                up,
            } => {
                let mut queue = TaskQueue::default();
                pin_mut!(nodes);

                while let Some(result) = nodes.next().await {
                    let node =
                        result.map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

                    // Build the key for this block
                    let hash = node.hash();

                    // Encode the block using the standard encoder
                    let (_hash, bytes) = CborEncoder.encode(node.block()).await.map_err(|e| {
                        ReplicaError::StorageError(format!("Failed to encode block: {:?}", e))
                    })?;

                    // Clone what we need for the spawned task
                    let mut store = index.clone();

                    // Spawn concurrent upload task
                    queue.spawn(async move {
                        index
                            .set(hash, bytes)
                            .await
                            .map_err(|_| DialogAsyncError::JoinError)
                    });
                }

                // Wait for all uploads to complete
                queue
                    .join()
                    .await
                    .map_err(|e| ReplicaError::StorageError(format!("Import failed: {:?}", e)))?;

                Ok(())
            }
            _ => unreachable!("Just opened"),
        }
    }
}

pub struct Index {
    issuer: Operator,
    credentials: RemoteCredentials,
    archive: Capability<archive::Catalog>,
}

impl Index {
    pub fn get(&self, digest: Blake3Hash) -> Capability<archive::Get> {
        self.archive
            .invoke(archive::Get { digest })
            .acquire(self.credentials)
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
        endpoint: impl Into<Url>,
        region: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Self {
        let address = s3::Address::new(endpoint, region, bucket);
        Self::S3(s3::PublicCredentials::new(address))
    }

    /// Create S3 credentials with signing keys.
    pub fn s3_private(
        endpoint: impl Into<Url>,
        region: impl Into<String>,
        bucket: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Result<Self, AccessError> {
        let address = s3::Address::new(endpoint, region, bucket);

        let credentials = s3::PrivateCredentials::new(address, access_key_id, secret_access_key)?;

        Self::S3(credentials)
    }

    /// Create UCAN credentials from an optional delegation chain.
    #[cfg(feature = "ucan")]
    pub fn ucan(endpoint: impl Into<Url>, delegation: Option<DelegationChain>) -> Self {
        Self::Ucan(ucan::Credentials::new(endpoint.into(), delegation));
    }
}

#[cfg(test)]
mod tests {
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_uses_new_remote_add() {
        use dialog_storage::JournaledStorage;

        // Create a replica
        let issuer = Operator::from_passphrase("test_remotes_add_v2");
        let subject = issuer.did().to_string();
        let backend = MemoryStorageBackend::default();
        let journaled = JournaledStorage::new(backend);
        let mut replica = Replica::open(issuer.clone(), subject, journaled.clone())
            .expect("Failed to create replica");

        // Add a remote using the new add_v2 API with S3 credentials
        let credentials = RemoteCredentials::S3 {
            endpoint: "https://s3.us-east-1.amazonaws.com".parse().unwrap(),
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            access_key_id: Some("AKIATEST".to_string()),
            secret_access_key: Some("secret123".to_string()),
        };

        let origin = replica
            .remotes
            .add_v2("origin", credentials.clone())
            .await
            .expect("Failed to add remote");

        assert_eq!(origin.name, "origin");
        assert_eq!(origin.credentials, credentials);

        // Adding same remote again with same credentials should succeed
        let origin_again = replica
            .remotes
            .add_v2("origin", credentials.clone())
            .await
            .expect("Should succeed with same credentials");

        assert_eq!(origin_again.name, "origin");

        // Adding same remote with different credentials should fail
        let different_credentials = RemoteCredentials::S3 {
            endpoint: "https://s3.eu-west-1.amazonaws.com".parse().unwrap(),
            region: "eu-west-1".to_string(),
            bucket: "different-bucket".to_string(),
            access_key_id: None,
            secret_access_key: None,
        };

        let result = replica
            .remotes
            .add_v2("origin", different_credentials)
            .await;

        assert!(
            result.is_err(),
            "Should fail when adding remote with different credentials"
        );

        // Adding a different remote should succeed
        let backup_credentials = RemoteCredentials::S3 {
            endpoint: "https://backup.example.com".parse().unwrap(),
            region: "auto".to_string(),
            bucket: "backup-bucket".to_string(),
            access_key_id: None,
            secret_access_key: None,
        };

        let backup = replica
            .remotes
            .add_v2("backup", backup_credentials.clone())
            .await
            .expect("Failed to add backup remote");

        assert_eq!(backup.name, "backup");
        assert_eq!(backup.credentials, backup_credentials);
    }

    #[cfg(feature = "ucan")]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_remotes_add_v2_ucan() {
        use dialog_storage::JournaledStorage;

        // Create a replica
        let issuer = Operator::from_passphrase("test_remotes_add_v2_ucan");
        let subject = issuer.did().to_string();
        let backend = MemoryStorageBackend::default();
        let journaled = JournaledStorage::new(backend);
        let mut replica = Replica::open(issuer.clone(), subject, journaled.clone())
            .expect("Failed to create replica");

        // Add a remote using UCAN credentials (without delegation for now)
        let credentials = RemoteCredentials::Ucan {
            endpoint: "https://access.example.com".parse().unwrap(),
            delegation: None,
        };

        let origin = replica
            .remotes
            .add_v2("origin", credentials.clone())
            .await
            .expect("Failed to add UCAN remote");

        assert_eq!(origin.name, "origin");
        assert_eq!(origin.credentials, credentials);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_remote_fluent_api() {
        use dialog_storage::JournaledStorage;

        // Create a replica
        let issuer = Operator::from_passphrase("test_remote_fluent_api");
        let subject = issuer.did().to_string();
        let backend = MemoryStorageBackend::default();
        let journaled = JournaledStorage::new(backend);
        let mut replica = Replica::open(issuer.clone(), subject, journaled.clone())
            .expect("Failed to create replica");

        // Add a remote
        let credentials = RemoteCredentials::S3 {
            endpoint: "https://s3.us-east-1.amazonaws.com".parse().unwrap(),
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            access_key_id: None,
            secret_access_key: None,
        };

        let origin = replica
            .remotes
            .add_v2("origin", credentials)
            .await
            .expect("Failed to add remote");

        // Use the fluent API to reference a remote branch
        let remote_did = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
        let remote_branch = origin.repository(remote_did).branch("main");

        assert_eq!(remote_branch.name, "main");
        assert_eq!(remote_branch.repository.subject, remote_did);
        assert_eq!(remote_branch.repository.site.name, "origin");

        // Test capability builders
        let index_cap = remote_branch.index();
        assert_eq!(index_cap.subject(), remote_did);
        // Catalog is a Policy (not Attenuation), so only Archive contributes to ability
        assert_eq!(index_cap.ability(), "/archive");

        let revision_cap = remote_branch.revision();
        assert_eq!(revision_cap.subject(), remote_did);
        // Space and Cell are Policies, so only Memory contributes to ability
        assert_eq!(revision_cap.ability(), "/memory");
    }
}
