//! Environment implementation composing local and remote sites.

use super::connection::Connection;
use super::connectors::RestBackend;
use super::connectors::RestConnector;
use super::effects::{Env, LocalBackend, Memory, RemoteBackend, Store};
use super::errors::MemoryError;
use super::local::Address as LocalAddress;
use super::remote::Address as RemoteAddress;
use super::site::Site;
use dialog_common::{ConditionalSync, DialogAsyncError, TaskQueue};
use dialog_storage::{DialogStorageError, StorageBackend, TransactionalMemoryBackend};

#[cfg(not(target_arch = "wasm32"))]
use super::connectors::{FileSystemBackend, FileSystemConnector};

#[cfg(target_arch = "wasm32")]
use super::connectors::{IndexedDbBackend, IndexedDbConnector};

/// Environment composes a local site and a remote site.
///
/// This struct provides a unified environment for local and remote storage operations.
/// It delegates to the appropriate site based on the operation:
/// - `LocalStore` and `LocalMemory` delegate to the local site
/// - `RemoteStore` and `RemoteMemory` delegate to the remote site
///
/// # Type Parameters
///
/// - `L`: Local site type
/// - `R`: Remote site type
///
/// # Example
///
/// ```ignore
/// use dialog_artifacts::fx::{Environment, Site};
///
/// let local_site = Site::new(local_store_connector, local_memory_connector);
/// let remote_site = Site::new(remote_store_connector, remote_memory_connector);
/// let env = Environment::new(local_site, remote_site);
/// ```
#[derive(Clone)]
pub struct Environment<L, R> {
    /// Local site for local storage operations.
    pub local: L,
    /// Remote site for network operations.
    pub remote: R,
}

impl<L, R> Environment<L, R> {
    /// Create a new environment with the given local and remote sites.
    pub fn new(local: L, remote: R) -> Self {
        Self { local, remote }
    }
}


// =============================================================================
// Trait implementations for Environment
// =============================================================================

// In the new design, just implementing the trait is enough - the `#[effect]`
// macro generates blanket `Effect` implementations for any type implementing
// the Provider trait.

impl<LS, LM, LSC, LMC, RS, RM, RSC, RMC> Env
    for Environment<
        Site<LS, LM, LSC, LMC, LocalAddress>,
        Site<RS, RM, RSC, RMC, RemoteAddress>,
    >
where
    LS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    LS::Error: Into<DialogStorageError>,
    LM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    LM::Error: Into<DialogStorageError>,
    LSC: Connection<LS, Address = LocalAddress> + Clone + Send + Sync,
    LSC::Error: Into<MemoryError>,
    LMC: Connection<LM, Address = LocalAddress> + Clone + Send + Sync,
    LMC::Error: Into<MemoryError>,
    RS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    RS::Error: Into<DialogStorageError>,
    RM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    RM::Error: Into<DialogStorageError>,
    RSC: Connection<RS, Address = RemoteAddress> + Clone + Send + Sync,
    RSC::Error: Into<MemoryError>,
    RMC: Connection<RM, Address = RemoteAddress> + Clone + Send + Sync,
    RMC::Error: Into<MemoryError>,
{
}

impl<LS, LM, LSC, LMC, RS, RM, RSC, RMC> Store<LocalAddress>
    for Environment<
        Site<LS, LM, LSC, LMC, LocalAddress>,
        Site<RS, RM, RSC, RMC, RemoteAddress>,
    >
where
    LS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    LS::Error: Into<DialogStorageError>,
    LM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    LM::Error: Into<DialogStorageError>,
    LSC: Connection<LS, Address = LocalAddress> + Clone + Send + Sync,
    LSC::Error: Into<MemoryError>,
    LMC: Connection<LM, Address = LocalAddress> + Clone + Send + Sync,
    LMC::Error: Into<MemoryError>,
    RS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    RS::Error: Into<DialogStorageError>,
    RM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    RM::Error: Into<DialogStorageError>,
    RSC: Connection<RS, Address = RemoteAddress> + Clone + Send + Sync,
    RSC::Error: Into<MemoryError>,
    RMC: Connection<RM, Address = RemoteAddress> + Clone + Send + Sync,
    RMC::Error: Into<MemoryError>,
{
    async fn get(&self, did: LocalAddress, key: Vec<u8>) -> Result<Option<Vec<u8>>, MemoryError> {
        let store = self.local.store(&did).await.map_err(Into::into)?;
        store.get(&key).await.map_err(|e| MemoryError::from(e.into()))
    }

    async fn set(&mut self, did: LocalAddress, key: Vec<u8>, value: Vec<u8>) -> Result<(), MemoryError> {
        let mut store = self.local.store(&did).await.map_err(Into::into)?;
        store.set(key, value).await.map_err(|e| MemoryError::from(e.into()))
    }

    async fn import(
        &mut self,
        did: LocalAddress,
        blocks: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), MemoryError> {
        let store = self.local.store(&did).await.map_err(Into::into)?;

        let mut queue = TaskQueue::default();
        for (key, value) in blocks {
            let mut store = store.clone();
            queue.spawn(async move {
                store
                    .set(key, value)
                    .await
                    .map_err(|_| DialogAsyncError::JoinError)
            });
        }

        queue
            .join()
            .await
            .map_err(|e| MemoryError::Storage(format!("Import failed: {:?}", e)))?;

        Ok(())
    }
}

impl<LS, LM, LSC, LMC, RS, RM, RSC, RMC> Memory<LocalAddress>
    for Environment<
        Site<LS, LM, LSC, LMC, LocalAddress>,
        Site<RS, RM, RSC, RMC, RemoteAddress>,
    >
where
    LS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    LS::Error: Into<DialogStorageError>,
    LM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    LM::Error: Into<DialogStorageError>,
    LSC: Connection<LS, Address = LocalAddress> + Clone + Send + Sync,
    LSC::Error: Into<MemoryError>,
    LMC: Connection<LM, Address = LocalAddress> + Clone + Send + Sync,
    LMC::Error: Into<MemoryError>,
    RS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    RS::Error: Into<DialogStorageError>,
    RM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    RM::Error: Into<DialogStorageError>,
    RSC: Connection<RS, Address = RemoteAddress> + Clone + Send + Sync,
    RSC::Error: Into<MemoryError>,
    RMC: Connection<RM, Address = RemoteAddress> + Clone + Send + Sync,
    RMC::Error: Into<MemoryError>,
{
    async fn resolve(&self, did: LocalAddress, key: Vec<u8>) -> Result<Option<(Vec<u8>, Vec<u8>)>, MemoryError> {
        let memory = self.local.memory(&did).await.map_err(Into::into)?;
        memory.resolve(&key).await.map_err(|e| MemoryError::from(e.into()))
    }

    async fn replace(
        &mut self,
        did: LocalAddress,
        key: Vec<u8>,
        edition: Option<Vec<u8>>,
        content: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, MemoryError> {
        let memory = self.local.memory(&did).await.map_err(Into::into)?;
        memory.replace(&key, edition.as_ref(), content)
            .await
            .map_err(|e| MemoryError::from(e.into()))
    }
}

impl<LS, LM, LSC, LMC, RS, RM, RSC, RMC> Store<RemoteAddress>
    for Environment<
        Site<LS, LM, LSC, LMC, LocalAddress>,
        Site<RS, RM, RSC, RMC, RemoteAddress>,
    >
where
    LS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    LS::Error: Into<DialogStorageError>,
    LM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    LM::Error: Into<DialogStorageError>,
    LSC: Connection<LS, Address = LocalAddress> + Clone + Send + Sync,
    LSC::Error: Into<MemoryError>,
    LMC: Connection<LM, Address = LocalAddress> + Clone + Send + Sync,
    LMC::Error: Into<MemoryError>,
    RS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    RS::Error: Into<DialogStorageError>,
    RM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    RM::Error: Into<DialogStorageError>,
    RSC: Connection<RS, Address = RemoteAddress> + Clone + Send + Sync,
    RSC::Error: Into<MemoryError>,
    RMC: Connection<RM, Address = RemoteAddress> + Clone + Send + Sync,
    RMC::Error: Into<MemoryError>,
{
    async fn get(&self, site: RemoteAddress, key: Vec<u8>) -> Result<Option<Vec<u8>>, MemoryError> {
        let store = self.remote.store(&site).await.map_err(Into::into)?;
        store.get(&key).await.map_err(|e| MemoryError::from(e.into()))
    }

    async fn set(&mut self, site: RemoteAddress, key: Vec<u8>, value: Vec<u8>) -> Result<(), MemoryError> {
        let mut store = self.remote.store(&site).await.map_err(Into::into)?;
        store.set(key, value).await.map_err(|e| MemoryError::from(e.into()))
    }

    async fn import(
        &mut self,
        site: RemoteAddress,
        blocks: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), MemoryError> {
        let store = self.remote.store(&site).await.map_err(Into::into)?;

        let mut queue = TaskQueue::default();
        for (key, value) in blocks {
            let mut store = store.clone();
            queue.spawn(async move {
                store
                    .set(key, value)
                    .await
                    .map_err(|_| DialogAsyncError::JoinError)
            });
        }

        queue
            .join()
            .await
            .map_err(|e| MemoryError::Network(format!("Import failed: {:?}", e)))?;

        Ok(())
    }
}

impl<LS, LM, LSC, LMC, RS, RM, RSC, RMC> Memory<RemoteAddress>
    for Environment<
        Site<LS, LM, LSC, LMC, LocalAddress>,
        Site<RS, RM, RSC, RMC, RemoteAddress>,
    >
where
    LS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    LS::Error: Into<DialogStorageError>,
    LM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    LM::Error: Into<DialogStorageError>,
    LSC: Connection<LS, Address = LocalAddress> + Clone + Send + Sync,
    LSC::Error: Into<MemoryError>,
    LMC: Connection<LM, Address = LocalAddress> + Clone + Send + Sync,
    LMC::Error: Into<MemoryError>,
    RS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    RS::Error: Into<DialogStorageError>,
    RM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    RM::Error: Into<DialogStorageError>,
    RSC: Connection<RS, Address = RemoteAddress> + Clone + Send + Sync,
    RSC::Error: Into<MemoryError>,
    RMC: Connection<RM, Address = RemoteAddress> + Clone + Send + Sync,
    RMC::Error: Into<MemoryError>,
{
    async fn resolve(
        &self,
        site: RemoteAddress,
        key: Vec<u8>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MemoryError> {
        let memory = self.remote.memory(&site).await.map_err(Into::into)?;
        memory.resolve(&key).await.map_err(|e| MemoryError::from(e.into()))
    }

    async fn replace(
        &mut self,
        site: RemoteAddress,
        key: Vec<u8>,
        edition: Option<Vec<u8>>,
        content: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, MemoryError> {
        let memory = self.remote.memory(&site).await.map_err(Into::into)?;
        memory.replace(&key, edition.as_ref(), content)
            .await
            .map_err(|e| MemoryError::from(e.into()))
    }
}

// =============================================================================
// Temporary bridge effects for ContentAddressedStorage integration
// =============================================================================

impl<LS, LM, LSC, LMC, RS, RM, RSC, RMC> LocalBackend
    for Environment<
        Site<LS, LM, LSC, LMC, LocalAddress>,
        Site<RS, RM, RSC, RMC, RemoteAddress>,
    >
where
    LS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    LS::Error: Into<DialogStorageError>,
    LM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    LM::Error: Into<DialogStorageError>,
    LSC: Connection<LS, Address = LocalAddress> + Clone + Send + Sync,
    LSC::Error: Into<MemoryError>,
    LMC: Connection<LM, Address = LocalAddress> + Clone + Send + Sync,
    LMC::Error: Into<MemoryError>,
    RS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    RS::Error: Into<DialogStorageError>,
    RM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    RM::Error: Into<DialogStorageError>,
    RSC: Connection<RS, Address = RemoteAddress> + Clone + Send + Sync,
    RSC::Error: Into<MemoryError>,
    RMC: Connection<RM, Address = RemoteAddress> + Clone + Send + Sync,
    RMC::Error: Into<MemoryError>,
{
    type Backend = LS;

    async fn backend(&self, did: LocalAddress) -> Result<Self::Backend, MemoryError> {
        self.local.store(&did).await.map_err(Into::into)
    }
}

impl<LS, LM, LSC, LMC, RS, RM, RSC, RMC> RemoteBackend
    for Environment<
        Site<LS, LM, LSC, LMC, LocalAddress>,
        Site<RS, RM, RSC, RMC, RemoteAddress>,
    >
where
    LS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    LS::Error: Into<DialogStorageError>,
    LM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    LM::Error: Into<DialogStorageError>,
    LSC: Connection<LS, Address = LocalAddress> + Clone + Send + Sync,
    LSC::Error: Into<MemoryError>,
    LMC: Connection<LM, Address = LocalAddress> + Clone + Send + Sync,
    LMC::Error: Into<MemoryError>,
    RS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    RS::Error: Into<DialogStorageError>,
    RM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    RM::Error: Into<DialogStorageError>,
    RSC: Connection<RS, Address = RemoteAddress> + Clone + Send + Sync,
    RSC::Error: Into<MemoryError>,
    RMC: Connection<RM, Address = RemoteAddress> + Clone + Send + Sync,
    RMC::Error: Into<MemoryError>,
{
    type Backend = RS;

    async fn backend(&self, site: RemoteAddress) -> Result<Self::Backend, MemoryError> {
        self.remote.store(&site).await.map_err(Into::into)
    }
}

// =============================================================================
// Platform-specific environment type aliases
// =============================================================================

/// Local site type for native (filesystem-based) environments.
#[cfg(not(target_arch = "wasm32"))]
pub type NativeLocalSite = Site<
    FileSystemBackend,
    FileSystemBackend,
    FileSystemConnector,
    FileSystemConnector,
    LocalAddress,
>;

/// Remote site type using REST backends.
pub type RestRemoteSite = Site<
    RestBackend,
    RestBackend,
    RestConnector,
    RestConnector,
    RemoteAddress,
>;

/// Native environment using filesystem storage locally and REST remotely.
///
/// This is the standard environment for desktop/server applications.
///
/// # Example
///
/// ```ignore
/// use dialog_artifacts::fx::{NativeEnv, FileSystemConnector, RestConnector};
/// use std::path::PathBuf;
///
/// let local_connector = FileSystemConnector::new(PathBuf::from("/data/dialog"));
/// let remote_connector = RestConnector::new();
///
/// let env = NativeEnv::new(local_connector, remote_connector);
/// ```
#[cfg(not(target_arch = "wasm32"))]
pub type NativeEnv = Environment<NativeLocalSite, RestRemoteSite>;

#[cfg(not(target_arch = "wasm32"))]
impl NativeEnv {
    /// Create a new native environment with the given base path for local storage.
    ///
    /// Uses filesystem storage for local operations and REST for remote operations.
    pub fn with_path(base_path: impl Into<std::path::PathBuf>) -> Self {
        let base = base_path.into();
        let local_store = FileSystemConnector::new(base.join("store"));
        let local_memory = FileSystemConnector::new(base.join("memory"));
        let remote_store = RestConnector::new();
        let remote_memory = RestConnector::new();

        let local = Site::new(local_store, local_memory);
        let remote = Site::new(remote_store, remote_memory);

        Environment::new(local, remote)
    }
}

/// Local site type for web (IndexedDB-based) environments.
#[cfg(target_arch = "wasm32")]
pub type WebLocalSite = Site<
    IndexedDbBackend,
    IndexedDbBackend,
    IndexedDbConnector,
    IndexedDbConnector,
    LocalAddress,
>;

/// Web environment using IndexedDB storage locally and REST remotely.
///
/// This is the standard environment for browser-based applications.
///
/// # Example
///
/// ```ignore
/// use dialog_artifacts::fx::{WebEnv, IndexedDbConnector, RestConnector};
///
/// let local_connector = IndexedDbConnector::new("my-app");
/// let remote_connector = RestConnector::new();
///
/// let env = WebEnv::new(local_connector, remote_connector);
/// ```
#[cfg(target_arch = "wasm32")]
pub type WebEnv = Environment<WebLocalSite, RestRemoteSite>;

#[cfg(target_arch = "wasm32")]
impl WebEnv {
    /// Create a new web environment with the given database name prefix.
    ///
    /// Uses IndexedDB for local operations and REST for remote operations.
    pub fn with_db_prefix(db_prefix: impl Into<String>) -> Self {
        let prefix = db_prefix.into();
        let local_store = IndexedDbConnector::with_store_name(format!("{}-store", prefix), "blocks");
        let local_memory = IndexedDbConnector::with_store_name(format!("{}-memory", prefix), "state");
        let remote_store = RestConnector::new();
        let remote_memory = RestConnector::new();

        let local = Site::new(local_store, local_memory);
        let remote = Site::new(remote_store, remote_memory);

        Environment::new(local, remote)
    }
}
