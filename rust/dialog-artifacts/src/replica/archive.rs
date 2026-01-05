//! Archive provides content-addressed storage with remote fallback.
//!
//! Archive implements the "archive" pattern: try local first, fall back to
//! remote if not found, and cache locally on remote hits. This is the core
//! abstraction for on-demand replication.

use async_trait::async_trait;
use dialog_storage::{Encoder, StorageBackend};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::platform::{PlatformBackend, Storage as PlatformStorage};

use super::remote_types::RemoteBackend;

/// Archive represents content addressed storage where search tree
/// nodes are stored. It supports optional remote fallback for on
/// demand replication. Uses Arc to share remote state across clones.
#[derive(Clone, Debug)]
pub struct Archive<Backend: PlatformBackend> {
    local: Arc<PlatformStorage<Backend>>,
    remote: Arc<RwLock<Option<PlatformStorage<RemoteBackend>>>>,
}

impl<Backend: PlatformBackend> Archive<Backend> {
    /// Creates a new Archive with the given backend.
    pub fn new(local: PlatformStorage<Backend>) -> Self {
        Self {
            local: Arc::new(local),
            remote: Arc::new(RwLock::new(None)),
        }
    }

    /// Sets the remote storage for fallback reads and replicated writes.
    pub async fn set_remote(&self, remote: PlatformStorage<RemoteBackend>) {
        *self.remote.write().await = Some(remote);
    }

    /// Clears the remote storage.
    pub async fn clear_remote(&self) {
        *self.remote.write().await = None;
    }

    /// Checks if a remote storage is configured.
    pub async fn has_remote(&self) -> bool {
        self.remote.read().await.is_some()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend: PlatformBackend + 'static> dialog_storage::ContentAddressedStorage
    for Archive<Backend>
{
    type Hash = [u8; 32];
    type Error = dialog_storage::DialogStorageError;

    async fn read<T>(&self, hash: &Self::Hash) -> Result<Option<T>, Self::Error>
    where
        T: serde::de::DeserializeOwned + dialog_common::ConditionalSync,
    {
        // Convert hash to key with "index/" prefix
        let mut key = b"index/".to_vec();
        key.extend_from_slice(hash);

        // Try local first
        if let Some(bytes) = self.local.get(&key).await.map_err(|e| {
            dialog_storage::DialogStorageError::StorageBackend(format!("{:?}", e))
        })? {
            return self.local.decode(&bytes).await.map(Some);
        }

        // Fall back to remote if available - clone to avoid holding lock across await
        let remote_storage = {
            let remote_guard = self.remote.read().await;
            remote_guard.clone()
        };

        if let Some(remote) = remote_storage.as_ref() {
            if let Some(bytes) = remote.get(&key).await.map_err(|e| {
                dialog_storage::DialogStorageError::StorageBackend(format!("{:?}", e))
            })? {
                // Cache the remote value to local storage
                // Clone the Arc to get a mutable copy that shares the backend's interior state
                let mut local = (*self.local).clone();
                local.set(key, bytes.clone()).await?;

                return remote.decode(&bytes).await.map(Some);
            }
        }

        Ok(None)
    }

    async fn write<T>(&mut self, block: &T) -> Result<Self::Hash, Self::Error>
    where
        T: serde::Serialize + dialog_common::ConditionalSync + std::fmt::Debug,
    {
        // Encode and hash the block
        let (hash, bytes) = self.local.encode(block).await?;

        // Prefix key with "index/"
        let mut key = b"index/".to_vec();
        key.extend_from_slice(&hash);

        // Write to local storage only - remote sync happens during push()
        // and that is when new blocks will be propagated to the remote.
        {
            let mut local = (*self.local).clone();
            local.set(key, bytes).await.map_err(|e| {
                dialog_storage::DialogStorageError::StorageBackend(format!("{:?}", e))
            })?;
        }

        Ok(hash)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend: PlatformBackend + 'static> dialog_storage::Encoder for Archive<Backend> {
    type Bytes = Vec<u8>;
    type Hash = [u8; 32];
    type Error = dialog_storage::DialogStorageError;

    async fn encode<T>(&self, block: &T) -> Result<(Self::Hash, Self::Bytes), Self::Error>
    where
        T: serde::Serialize + dialog_common::ConditionalSync + std::fmt::Debug,
    {
        self.local.encode(block).await
    }

    async fn decode<T>(&self, bytes: &[u8]) -> Result<T, Self::Error>
    where
        T: serde::de::DeserializeOwned + dialog_common::ConditionalSync,
    {
        self.local.decode(bytes).await
    }
}
