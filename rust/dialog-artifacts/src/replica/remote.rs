//! Remote repository management.
//!
//! This module contains types for managing remote repositories
//! and branches on remote replicas.

use dialog_common::{DialogAsyncError, TaskQueue};
use dialog_prolly_tree::{KeyType, Node};
use dialog_storage::Blake3Hash;
use dialog_storage::{CborEncoder, Encoder, StorageBackend};
use futures_util::Stream;

use crate::platform::{PlatformBackend, Storage as PlatformStorage, TypedStoreResource};

use super::error::ReplicaError;
use super::remote_types::{RemoteBackend, RemoteState};
use super::types::{BranchId, Revision, Site};

/// Represents a connection to a remote repository.
#[derive(Debug)]
pub struct Remote<Backend: PlatformBackend> {
    /// Site of the remote
    pub site: Site,
    memory: TypedStoreResource<RemoteState, Backend>,
    storage: PlatformStorage<Backend>,
    connection: PlatformStorage<RemoteBackend>,
}

impl<Backend: PlatformBackend> Remote<Backend> {
    /// Returns the site identifier for this remote.
    pub fn site(&self) -> &Site {
        &self.site
    }

    /// Mounts the transactional memory for a remote site from storage.
    pub async fn mount(
        site: &Site,
        storage: &PlatformStorage<Backend>,
    ) -> Result<TypedStoreResource<RemoteState, Backend>, ReplicaError> {
        let address = format!("site/{}", site);
        let memory = storage
            .open::<RemoteState>(&address.into_bytes())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        Ok(memory)
    }

    /// Loads a remote repository by the site name.
    pub async fn setup(
        site: &Site,
        storage: PlatformStorage<Backend>,
    ) -> Result<Remote<Backend>, ReplicaError> {
        let memory = Self::mount(site, &storage).await?;
        if let Some(state) = memory.content().clone() {
            Ok(Remote {
                site: state.site.clone(),
                connection: state.connect()?,
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
        let mut memory = Self::mount(&state.site, &storage).await?;
        let mut already_exists = false;

        if let Some(existing_state) = memory.content() {
            already_exists = true;
            if state != existing_state {
                return Err(ReplicaError::RemoteAlreadyExists {
                    remote: state.site.to_string(),
                });
            }
        }

        let state = RemoteState {
            site: state.site.to_string(),
            address: state.address,
        };
        let site = state.site.clone();
        let connection = state.connect()?;

        if !already_exists {
            memory
                .replace(Some(state.clone()), &storage)
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
        }

        Ok(Remote {
            site,
            connection,
            memory,
            storage,
        })
    }

    /// Updates the remote address configuration.
    pub async fn update_address(
        &mut self,
        address: dialog_storage::RestStorageConfig,
    ) -> Result<(), ReplicaError> {
        let new_state = RemoteState {
            site: self.site.clone(),
            address,
        };

        self.memory
            .replace(Some(new_state.clone()), &self.storage)
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        self.connection = new_state.connect()?;

        Ok(())
    }

    /// Opens a branch at this remote.
    pub async fn open(&self, id: &BranchId) -> Result<RemoteBranch<Backend>, ReplicaError> {
        RemoteBranch::open(self.site(), id, self.storage.clone()).await
    }
}

/// Represents a branch on a remote repository.
#[derive(Debug, Clone)]
pub struct RemoteBranch<Backend: PlatformBackend> {
    /// Name of the remote this branch is part of
    pub site: Site,
    /// Branch id on the remote it's on
    pub id: BranchId,

    /// Local storage where updates are stored
    pub storage: PlatformStorage<Backend>,

    /// Remote storage for canonical operations
    pub remote_storage: PlatformStorage<RemoteBackend>,

    /// Local cache for the revision currently branch has
    pub cache: TypedStoreResource<Revision, Backend>,
    /// Canonical revision, which is created lazily on fetch.
    pub canonical: Option<TypedStoreResource<Revision, RemoteBackend>>,
}

impl<Backend: PlatformBackend> RemoteBranch<Backend> {
    /// Mounts the transactional memory for a remote branch from local storage.
    pub async fn mount(
        site: &Site,
        id: &BranchId,
        storage: &PlatformStorage<Backend>,
    ) -> Result<TypedStoreResource<Revision, Backend>, ReplicaError> {
        let address = format!("remote/{}/{}", site, id);
        let memory = storage
            .open::<Revision>(&address.into_bytes())
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        Ok(memory)
    }

    /// Opens a remote branch, creating it if it doesn't exist.
    pub async fn open(
        site: &Site,
        id: &BranchId,
        storage: PlatformStorage<Backend>,
    ) -> Result<RemoteBranch<Backend>, ReplicaError> {
        let memory = Self::mount(site, id, &storage).await?;
        let remote = Remote::setup(site, storage.clone()).await?;

        Ok(Self {
            site: site.clone(),
            id: id.clone(),
            storage,
            cache: memory,
            canonical: None,
            remote_storage: remote.connection,
        })
    }

    /// Returns the site for this remote branch.
    pub fn site(&self) -> &Site {
        &self.site
    }

    /// Returns the branch id.
    pub fn id(&self) -> &BranchId {
        &self.id
    }

    /// Returns the current revision.
    pub fn revision(&self) -> Option<Revision> {
        self.cache.read()
    }

    /// Connects to the canonical remote storage for this branch.
    pub async fn connect(
        &mut self,
    ) -> Result<&TypedStoreResource<Revision, RemoteBackend>, ReplicaError> {
        if self.canonical.is_none() {
            let address = format!("local/{}", self.id);
            let canonical = self
                .remote_storage
                .open::<Revision>(&address.into_bytes())
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

            self.canonical = Some(canonical);
        }

        Ok(self
            .canonical
            .as_ref()
            .expect("canonical was initialized above"))
    }

    /// Fetches remote revision for this branch.
    pub async fn fetch(&mut self) -> Result<Option<Revision>, ReplicaError> {
        self.connect().await?;
        let canonical = self.canonical.as_mut().expect("connected");

        let _ = canonical.reload(&self.remote_storage).await;

        let revision = canonical.content().clone();

        self.cache
            .replace_with(|_| revision.clone(), &self.storage)
            .await
            .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

        Ok(self.revision())
    }

    /// Publishes new canonical revision.
    pub async fn publish(&mut self, revision: Revision) -> Result<(), ReplicaError> {
        self.connect().await?;
        let prior = self.revision().clone();
        let canonical = self.canonical.as_mut().expect("connected");

        if canonical.content().as_ref() != Some(&revision) {
            canonical
                .replace(Some(revision.clone()), &self.remote_storage)
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
        }

        if prior.as_ref() != Some(&revision) {
            self.cache
                .replace_with(|_| Some(revision.clone()), &self.storage)
                .await
                .map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;
        }

        Ok(())
    }

    /// Imports novel nodes from a stream into remote storage.
    pub async fn import<Key, Value, E, S>(&mut self, nodes: S) -> Result<(), ReplicaError>
    where
        Key: KeyType + 'static,
        Value: dialog_prolly_tree::ValueType,
        E: std::fmt::Debug,
        S: Stream<Item = Result<Node<Key, Value, Blake3Hash>, E>>,
    {
        use futures_util::{pin_mut, StreamExt};

        let mut queue = TaskQueue::default();
        pin_mut!(nodes);

        while let Some(result) = nodes.next().await {
            let node = result.map_err(|e| ReplicaError::StorageError(format!("{:?}", e)))?;

            let hash = node.hash();
            let mut key = b"index/".to_vec();
            key.extend_from_slice(hash);

            let (_hash, bytes) = CborEncoder.encode(node.block()).await.map_err(|e| {
                ReplicaError::StorageError(format!("Failed to encode block: {:?}", e))
            })?;

            let mut remote = self.remote_storage.clone();

            queue.spawn(async move {
                remote
                    .set(key, bytes)
                    .await
                    .map_err(|_| DialogAsyncError::JoinError)
            });
        }

        queue
            .join()
            .await
            .map_err(|e| ReplicaError::StorageError(format!("Import failed: {:?}", e)))?;

        Ok(())
    }
}
