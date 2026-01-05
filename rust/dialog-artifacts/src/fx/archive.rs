//! Archive provides content-addressed storage with remote fallback.
//!
//! Archive implements the "archive" pattern: try local first, fall back to
//! remote if not found, and cache locally on remote hits. This is the core
//! abstraction for on-demand replication.

use super::archive_store::ArchiveStore;
use super::effects::{LocalBackend, RemoteBackend, Store, effectful};
use super::errors::MemoryError;
use super::local::Address as LocalAddress;
use super::remote::Address as RemoteAddress;
use dialog_common::fx::Effect;
use thiserror::Error;

/// Error type for archive operations.
#[derive(Debug, Clone, Error)]
pub enum ArchiveError {
    /// Storage or network operation failed.
    #[error("{0}")]
    Memory(#[from] MemoryError),
}

/// Archive provides content-addressed storage with remote fallback.
///
/// It implements the "archive" pattern:
/// - Read: try local first, fall back to remotes in order, cache locally on hit
/// - Write: local only (remote sync happens during push)
///
/// # Example
///
/// ```ignore
/// use dialog_artifacts::fx::{Archive, local, remote, TestEnv, LocalStore};
/// use dialog_common::fx::Effect;
///
/// let mut env = TestEnv::default();
/// let archive = Archive::new(local::Address::did("did:test:123"));
///
/// // Write to local
/// archive.write(b"key".to_vec(), b"value".to_vec())
///     .perform(&mut env)
///     .await?;
///
/// // Read from local
/// let value = archive.read(b"key".to_vec())
///     .perform(&mut env)
///     .await?;
/// ```
#[derive(Clone, Debug)]
pub struct Archive {
    /// The local address (DID) for this archive's storage.
    did: LocalAddress,
    /// Remote addresses to try for fallback reads.
    remotes: Vec<RemoteAddress>,
}

impl Archive {
    /// Create a new archive for the given local address.
    pub fn new(did: LocalAddress) -> Self {
        Self {
            did,
            remotes: Vec::new(),
        }
    }

    /// Get the local address for this archive.
    pub fn did(&self) -> &LocalAddress {
        &self.did
    }

    /// Get the list of remote addresses.
    pub fn remotes(&self) -> &[RemoteAddress] {
        &self.remotes
    }

    /// Add a remote address for fallback reads.
    pub fn add_remote(&mut self, remote: RemoteAddress) {
        self.remotes.push(remote);
    }

    /// Clear all remote addresses.
    pub fn clear_remotes(&mut self) {
        self.remotes.clear();
    }

    /// Set a single remote address (replacing any existing remotes).
    pub fn set_remote(&mut self, remote: RemoteAddress) {
        self.remotes.clear();
        self.remotes.push(remote);
    }

    /// Check if this archive has any remotes configured.
    pub fn has_remote(&self) -> bool {
        !self.remotes.is_empty()
    }

    /// Acquire an ArchiveStore from an environment.
    ///
    /// This opens the local storage backend for this archive's DID and
    /// optionally opens remote storage backends for each configured remote.
    /// The returned ArchiveStore implements ContentAddressedStorage and can
    /// be used directly with prolly trees.
    ///
    /// **Note**: This method uses temporary bridge effects (`LocalBackend`, `RemoteBackend`)
    /// that will be removed once prolly trees are made effectful.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use dialog_artifacts::fx::{Archive, local, NativeEnv, LocalBackend, RemoteBackend};
    /// use dialog_common::fx::Effect;
    ///
    /// let mut env = NativeEnv::with_path("/data/dialog");
    /// let mut archive = Archive::new(local::Address::did("did:test:123"));
    /// archive.add_remote(remote_addr);
    ///
    /// let store = archive.acquire().perform(&mut env).await?;
    /// // store now implements ContentAddressedStorage
    /// ```
    #[effectful(LocalBackend + RemoteBackend)]
    pub fn acquire(
        &self,
    ) -> Result<
        ArchiveStore<<Capability as LocalBackend>::Backend, <Capability as RemoteBackend>::Backend>,
        ArchiveError,
    >
    where
        Capability: LocalBackend + RemoteBackend,
    {
        // Open local storage backend
        let local = perform!(LocalBackend.backend(self.did.clone()))?;

        // Open remote storage backends (skip any that fail to connect)
        let mut remotes = Vec::new();
        for remote_addr in &self.remotes {
            if let Ok(remote) = perform!(RemoteBackend.backend(remote_addr.clone())) {
                remotes.push(remote);
            }
        }

        Ok(ArchiveStore::with_remotes(local, remotes))
    }

    /// Read a value by key.
    ///
    /// Tries local storage first. If not found and remotes are configured,
    /// tries each remote in order. On remote hit, caches the value locally.
    #[effectful(Store<LocalAddress> + Store<RemoteAddress>)]
    pub fn read(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ArchiveError> {
        // Try local first
        if let Some(value) = perform!(Store::<LocalAddress>().get(self.did.clone(), key.clone()))? {
            return Ok(Some(value));
        }

        // Try each remote in order
        for remote in &self.remotes {
            match perform!(Store::<RemoteAddress>().get(remote.clone(), key.clone())) {
                Ok(Some(value)) => {
                    // Cache locally
                    perform!(Store::<LocalAddress>().set(self.did.clone(), key, value.clone()))?;
                    return Ok(Some(value));
                }
                Ok(None) => continue,
                Err(_) => continue, // Try next remote on error
            }
        }

        Ok(None)
    }

    /// Write a value by key to local storage.
    ///
    /// This only writes locally. Remote sync happens during push operations.
    #[effectful(Store<LocalAddress>)]
    pub fn write(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), ArchiveError> {
        perform!(Store::<LocalAddress>().set(self.did.clone(), key, value))?;
        Ok(())
    }

    /// Check if a key exists locally.
    #[effectful(Store<LocalAddress>)]
    pub fn contains(&self, key: Vec<u8>) -> Result<bool, ArchiveError> {
        let result = perform!(Store::<LocalAddress>().get(self.did.clone(), key))?;
        Ok(result.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fx::{local, memory::TestEnv, remote};
    use dialog_common::fx::Effect;
    use dialog_storage::{AuthMethod, RestStorageConfig};

    fn test_did() -> LocalAddress {
        local::Address::did("did:test:archive")
    }

    fn test_remote() -> RemoteAddress {
        remote::Address::rest(RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: None,
            headers: vec![],
            timeout_seconds: None,
        })
    }

    #[tokio::test]
    async fn it_writes_and_reads_locally() {
        let mut env = TestEnv::default();
        let archive = Archive::new(test_did());

        // Write
        archive
            .write(b"key".to_vec(), b"value".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        // Read
        let result = archive
            .read(b"key".to_vec())
            .perform(&mut env)
            .await
            .unwrap();
        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn it_returns_none_for_missing_key() {
        let mut env = TestEnv::default();
        let archive = Archive::new(test_did());

        let result = archive
            .read(b"missing".to_vec())
            .perform(&mut env)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn it_checks_key_existence() {
        let mut env = TestEnv::default();
        let archive = Archive::new(test_did());

        assert!(
            !archive
                .contains(b"key".to_vec())
                .perform(&mut env)
                .await
                .unwrap()
        );

        archive
            .write(b"key".to_vec(), b"value".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        assert!(
            archive
                .contains(b"key".to_vec())
                .perform(&mut env)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn it_falls_back_to_remote_and_caches() {
        use crate::fx::effects::Store;

        let mut env = TestEnv::default();
        let site = test_remote();

        // Put data in remote only
        Store::<RemoteAddress>()
            .import(
                site.clone(),
                vec![(b"remote-key".to_vec(), b"remote-value".to_vec())],
            )
            .perform(&mut env)
            .await
            .unwrap();

        // Create archive with remote
        let mut archive = Archive::new(test_did());
        archive.add_remote(site);

        // Read should find it via remote and cache locally
        let result = archive
            .read(b"remote-key".to_vec())
            .perform(&mut env)
            .await
            .unwrap();
        assert_eq!(result, Some(b"remote-value".to_vec()));

        // Clear remotes - should still find cached value locally
        archive.clear_remotes();
        let cached = archive
            .read(b"remote-key".to_vec())
            .perform(&mut env)
            .await
            .unwrap();
        assert_eq!(cached, Some(b"remote-value".to_vec()));
    }

    #[tokio::test]
    async fn it_prefers_local_over_remote() {
        use crate::fx::effects::Store;

        let mut env = TestEnv::default();
        let site = test_remote();

        // Put different values in local and remote
        let mut archive = Archive::new(test_did());
        archive
            .write(b"key".to_vec(), b"local-value".to_vec())
            .perform(&mut env)
            .await
            .unwrap();

        Store::<RemoteAddress>()
            .import(
                site.clone(),
                vec![(b"key".to_vec(), b"remote-value".to_vec())],
            )
            .perform(&mut env)
            .await
            .unwrap();

        archive.add_remote(site);

        // Should get local value, not remote
        let result = archive
            .read(b"key".to_vec())
            .perform(&mut env)
            .await
            .unwrap();
        assert_eq!(result, Some(b"local-value".to_vec()));
    }

    #[tokio::test]
    async fn it_acquires_archive_store_effectfully() {
        use dialog_storage::ContentAddressedStorage;

        let mut env = TestEnv::default();
        let site = test_remote();

        // Create archive with a remote
        let mut archive = Archive::new(test_did());
        archive.add_remote(site);

        // Acquire the store effectfully
        let mut store = archive.acquire().perform(&mut env).await.unwrap();

        // Verify it implements ContentAddressedStorage by writing and reading
        let data = b"test content".to_vec();
        let hash = store.write(&data).await.unwrap();
        let retrieved = store.read(&hash).await.unwrap();

        assert_eq!(retrieved, Some(data));
    }
}
