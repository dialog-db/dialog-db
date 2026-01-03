//! Environment implementation composing local and remote sites.

use super::connection::Connection;
use super::effects::{Env, LocalMemory, LocalStore, RemoteMemory, RemoteStore};
use super::errors::{NetworkError, StorageError};
use super::local::Address as LocalAddress;
use super::remote::Address as RemoteAddress;
use super::site::Site;
use dialog_common::{ConditionalSync, DialogAsyncError, TaskQueue};
use dialog_storage::{DialogStorageError, StorageBackend, TransactionalMemoryBackend};

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
    LSC::Error: Into<StorageError>,
    LMC: Connection<LM, Address = LocalAddress> + Clone + Send + Sync,
    LMC::Error: Into<StorageError>,
    RS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    RS::Error: Into<DialogStorageError>,
    RM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    RM::Error: Into<DialogStorageError>,
    RSC: Connection<RS, Address = RemoteAddress> + Clone + Send + Sync,
    RSC::Error: Into<NetworkError>,
    RMC: Connection<RM, Address = RemoteAddress> + Clone + Send + Sync,
    RMC::Error: Into<NetworkError>,
{
}

impl<LS, LM, LSC, LMC, RS, RM, RSC, RMC> LocalStore
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
    LSC::Error: Into<StorageError>,
    LMC: Connection<LM, Address = LocalAddress> + Clone + Send + Sync,
    LMC::Error: Into<StorageError>,
    RS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    RS::Error: Into<DialogStorageError>,
    RM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    RM::Error: Into<DialogStorageError>,
    RSC: Connection<RS, Address = RemoteAddress> + Clone + Send + Sync,
    RSC::Error: Into<NetworkError>,
    RMC: Connection<RM, Address = RemoteAddress> + Clone + Send + Sync,
    RMC::Error: Into<NetworkError>,
{
    async fn get(&self, did: LocalAddress, key: Vec<u8>) -> Result<Option<Vec<u8>>, StorageError> {
        let store = self.local.store(&did).await.map_err(Into::into)?;
        store.get(&key).await.map_err(|e| StorageError::from(e.into()))
    }

    async fn set(&mut self, did: LocalAddress, key: Vec<u8>, value: Vec<u8>) -> Result<(), StorageError> {
        let mut store = self.local.store(&did).await.map_err(Into::into)?;
        store.set(key, value).await.map_err(|e| StorageError::from(e.into()))
    }
}

impl<LS, LM, LSC, LMC, RS, RM, RSC, RMC> LocalMemory
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
    LSC::Error: Into<StorageError>,
    LMC: Connection<LM, Address = LocalAddress> + Clone + Send + Sync,
    LMC::Error: Into<StorageError>,
    RS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    RS::Error: Into<DialogStorageError>,
    RM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    RM::Error: Into<DialogStorageError>,
    RSC: Connection<RS, Address = RemoteAddress> + Clone + Send + Sync,
    RSC::Error: Into<NetworkError>,
    RMC: Connection<RM, Address = RemoteAddress> + Clone + Send + Sync,
    RMC::Error: Into<NetworkError>,
{
    async fn resolve(&self, did: LocalAddress, address: Vec<u8>) -> Result<Option<(Vec<u8>, Vec<u8>)>, StorageError> {
        let memory = self.local.memory(&did).await.map_err(Into::into)?;
        memory.resolve(&address).await.map_err(|e| StorageError::from(e.into()))
    }

    async fn replace(
        &mut self,
        did: LocalAddress,
        address: Vec<u8>,
        edition: Option<Vec<u8>>,
        content: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let memory = self.local.memory(&did).await.map_err(Into::into)?;
        memory.replace(&address, edition.as_ref(), content)
            .await
            .map_err(|e| StorageError::from(e.into()))
    }
}

impl<LS, LM, LSC, LMC, RS, RM, RSC, RMC> RemoteStore
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
    LSC::Error: Into<StorageError>,
    LMC: Connection<LM, Address = LocalAddress> + Clone + Send + Sync,
    LMC::Error: Into<StorageError>,
    RS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    RS::Error: Into<DialogStorageError>,
    RM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    RM::Error: Into<DialogStorageError>,
    RSC: Connection<RS, Address = RemoteAddress> + Clone + Send + Sync,
    RSC::Error: Into<NetworkError>,
    RMC: Connection<RM, Address = RemoteAddress> + Clone + Send + Sync,
    RMC::Error: Into<NetworkError>,
{
    async fn get(&self, site: RemoteAddress, key: Vec<u8>) -> Result<Option<Vec<u8>>, NetworkError> {
        let store = self.remote.store(&site).await.map_err(Into::into)?;
        store.get(&key).await.map_err(|e| NetworkError::from(e.into()))
    }

    async fn import(
        &mut self,
        site: RemoteAddress,
        blocks: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), NetworkError> {
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
            .map_err(|e| NetworkError::Network(format!("Import failed: {:?}", e)))?;

        Ok(())
    }
}

impl<LS, LM, LSC, LMC, RS, RM, RSC, RMC> RemoteMemory
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
    LSC::Error: Into<StorageError>,
    LMC: Connection<LM, Address = LocalAddress> + Clone + Send + Sync,
    LMC::Error: Into<StorageError>,
    RS: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    RS::Error: Into<DialogStorageError>,
    RM: TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>, Edition = Vec<u8>> + Clone + ConditionalSync + 'static,
    RM::Error: Into<DialogStorageError>,
    RSC: Connection<RS, Address = RemoteAddress> + Clone + Send + Sync,
    RSC::Error: Into<NetworkError>,
    RMC: Connection<RM, Address = RemoteAddress> + Clone + Send + Sync,
    RMC::Error: Into<NetworkError>,
{
    async fn resolve(
        &self,
        site: RemoteAddress,
        address: Vec<u8>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, NetworkError> {
        let memory = self.remote.memory(&site).await.map_err(Into::into)?;
        memory.resolve(&address).await.map_err(|e| NetworkError::from(e.into()))
    }

    async fn replace(
        &mut self,
        site: RemoteAddress,
        address: Vec<u8>,
        edition: Option<Vec<u8>>,
        content: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, NetworkError> {
        let memory = self.remote.memory(&site).await.map_err(Into::into)?;
        memory.replace(&address, edition.as_ref(), content)
            .await
            .map_err(|e| NetworkError::from(e.into()))
    }
}
