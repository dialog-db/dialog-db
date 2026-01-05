//! ArchiveStore provides content-addressed storage with remote fallback.
//!
//! This module bridges the effectful Archive with the prolly tree's
//! `ContentAddressedStorage` trait requirement.
//!
//! Use `Archive::acquire()` to obtain an `ArchiveStore` from an environment.
//! Once prolly trees are made effectful, this bridge will no longer be needed.

use async_trait::async_trait;
use dialog_common::ConditionalSync;
use dialog_storage::{
    CborEncoder, ContentAddressedStorage, DialogStorageError, Encoder, StorageBackend,
};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::RwLock;

/// ArchiveStore provides content-addressed storage with local-first reads
/// and remote fallback.
///
/// This struct wraps local and optional remote storage backends and implements
/// the archive pattern:
/// - Read: try local first, fall back to remote, cache locally on hit
/// - Write: local only (remote sync happens during push)
///
/// # Type Parameters
///
/// - `L`: Local storage backend type
/// - `R`: Remote storage backend type
#[derive(Clone)]
pub struct ArchiveStore<L, R>
where
    L: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone,
    R: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone,
{
    /// Local storage backend.
    local: L,
    /// Remote storage backends for fallback reads.
    remotes: Arc<RwLock<Vec<R>>>,
    /// Encoder for content-addressed operations.
    encoder: CborEncoder,
}

impl<L, R> ArchiveStore<L, R>
where
    L: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone,
    R: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone,
{
    /// Create a new ArchiveStore with just local storage.
    pub fn new(local: L) -> Self {
        Self {
            local,
            remotes: Arc::new(RwLock::new(Vec::new())),
            encoder: CborEncoder,
        }
    }

    /// Create a new ArchiveStore with local storage and remote fallbacks.
    pub fn with_remotes(local: L, remotes: Vec<R>) -> Self {
        Self {
            local,
            remotes: Arc::new(RwLock::new(remotes)),
            encoder: CborEncoder,
        }
    }

    /// Add a remote storage backend for fallback reads.
    pub async fn add_remote(&self, remote: R) {
        self.remotes.write().await.push(remote);
    }

    /// Clear all remote storage backends.
    pub async fn clear_remotes(&self) {
        self.remotes.write().await.clear();
    }

    /// Check if any remotes are configured.
    pub async fn has_remote(&self) -> bool {
        !self.remotes.read().await.is_empty()
    }

    /// Get a value by key with archive pattern (local first, remote fallback).
    async fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DialogStorageError>
    where
        L: ConditionalSync,
        L::Error: Into<DialogStorageError>,
        R: ConditionalSync,
        R::Error: Into<DialogStorageError>,
    {
        // Try local first
        if let Some(value) = self.local.get(&key.to_vec()).await.map_err(Into::into)? {
            return Ok(Some(value));
        }

        // Try each remote in order
        let remotes = self.remotes.read().await.clone();
        for remote in remotes {
            match remote.get(&key.to_vec()).await {
                Ok(Some(value)) => {
                    // Cache locally
                    let mut local = self.local.clone();
                    local
                        .set(key.to_vec(), value.clone())
                        .await
                        .map_err(Into::into)?;
                    return Ok(Some(value));
                }
                Ok(None) => continue,
                Err(_) => continue, // Try next remote on error
            }
        }

        Ok(None)
    }

    /// Set a value by key (local only).
    async fn set_raw(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DialogStorageError>
    where
        L: ConditionalSync,
        L::Error: Into<DialogStorageError>,
    {
        self.local.set(key, value).await.map_err(Into::into)
    }
}

impl<L, R> Debug for ArchiveStore<L, R>
where
    L: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + Debug,
    R: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchiveStore")
            .field("local", &self.local)
            .field("remotes", &"[...]")
            .finish()
    }
}

// Encoder implementation
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<L, R> Encoder for ArchiveStore<L, R>
where
    L: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    L::Error: Into<DialogStorageError> + Send,
    R: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    R::Error: Into<DialogStorageError> + Send,
{
    type Bytes = Vec<u8>;
    type Hash = [u8; 32];
    type Error = DialogStorageError;

    async fn encode<T>(&self, block: &T) -> Result<(Self::Hash, Self::Bytes), Self::Error>
    where
        T: serde::Serialize + ConditionalSync + std::fmt::Debug,
    {
        self.encoder.encode(block).await
    }

    async fn decode<T>(&self, bytes: &[u8]) -> Result<T, Self::Error>
    where
        T: serde::de::DeserializeOwned + ConditionalSync,
    {
        self.encoder.decode(bytes).await
    }
}

// ContentAddressedStorage implementation directly (not via blanket impl)
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<L, R> ContentAddressedStorage for ArchiveStore<L, R>
where
    L: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    L::Error: Into<DialogStorageError> + Send,
    R: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Clone + ConditionalSync + 'static,
    R::Error: Into<DialogStorageError> + Send,
{
    type Hash = [u8; 32];
    type Error = DialogStorageError;

    async fn read<T>(&self, hash: &Self::Hash) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync,
    {
        let Some(bytes) = self.get_raw(hash).await? else {
            return Ok(None);
        };
        let value = self.encoder.decode(&bytes).await?;
        Ok(Some(value))
    }

    async fn write<T>(&mut self, block: &T) -> Result<Self::Hash, Self::Error>
    where
        T: Serialize + ConditionalSync + Debug,
    {
        let (hash, bytes) = self.encoder.encode(block).await?;
        self.set_raw(hash.to_vec(), bytes).await?;
        Ok(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_storage::ContentAddressedStorage;
    use dialog_storage::MemoryStorageBackend;

    type TestBackend = MemoryStorageBackend<Vec<u8>, Vec<u8>>;

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestBlock {
        value: u32,
    }

    #[tokio::test]
    async fn it_writes_and_reads_content() {
        let local = TestBackend::default();
        let mut store: ArchiveStore<TestBackend, TestBackend> = ArchiveStore::new(local);

        let block = TestBlock { value: 42 };
        let hash = store.write(&block).await.unwrap();

        let result: Option<TestBlock> = store.read(&hash).await.unwrap();
        assert_eq!(result, Some(block));
    }

    #[tokio::test]
    async fn it_returns_none_for_missing_content() {
        let local = TestBackend::default();
        let store: ArchiveStore<TestBackend, TestBackend> = ArchiveStore::new(local);

        let hash = [0u8; 32];
        let result: Option<TestBlock> = store.read(&hash).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn it_falls_back_to_remote() {
        let local = TestBackend::default();
        let mut remote = TestBackend::default();

        // Write to remote directly
        let block = TestBlock { value: 123 };
        let encoder = CborEncoder;
        let (hash, bytes) = encoder.encode(&block).await.unwrap();
        remote.set(hash.to_vec(), bytes).await.unwrap();

        // Create store with remote
        let store: ArchiveStore<TestBackend, TestBackend> =
            ArchiveStore::with_remotes(local, vec![remote]);

        // Should find via remote
        let result: Option<TestBlock> = store.read(&hash).await.unwrap();
        assert_eq!(result, Some(block));
    }

    #[tokio::test]
    async fn it_caches_remote_hits_locally() {
        let local = TestBackend::default();
        let mut remote = TestBackend::default();

        // Write to remote directly
        let block = TestBlock { value: 456 };
        let encoder = CborEncoder;
        let (hash, bytes) = encoder.encode(&block).await.unwrap();
        remote.set(hash.to_vec(), bytes).await.unwrap();

        // Create store with remote
        let store: ArchiveStore<TestBackend, TestBackend> =
            ArchiveStore::with_remotes(local.clone(), vec![remote]);

        // Read from remote (will cache locally)
        let _: Option<TestBlock> = store.read(&hash).await.unwrap();

        // Create new store with just local - should find cached value
        let local_only: ArchiveStore<TestBackend, TestBackend> = ArchiveStore::new(local);
        let cached: Option<TestBlock> = local_only.read(&hash).await.unwrap();
        assert_eq!(cached, Some(block));
    }

    #[tokio::test]
    async fn it_prefers_local_over_remote() {
        let mut local = TestBackend::default();
        let mut remote = TestBackend::default();

        let encoder = CborEncoder;

        // Write different values to local and remote
        let local_block = TestBlock { value: 100 };
        let remote_block = TestBlock { value: 200 };

        let (hash, local_bytes) = encoder.encode(&local_block).await.unwrap();
        local.set(hash.to_vec(), local_bytes).await.unwrap();

        let (_, remote_bytes) = encoder.encode(&remote_block).await.unwrap();
        remote.set(hash.to_vec(), remote_bytes).await.unwrap();

        // Create store with both
        let store: ArchiveStore<TestBackend, TestBackend> =
            ArchiveStore::with_remotes(local, vec![remote]);

        // Should get local value
        let result: Option<TestBlock> = store.read(&hash).await.unwrap();
        assert_eq!(result, Some(local_block));
    }
}
