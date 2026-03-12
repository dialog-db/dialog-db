use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Catalog, Get, Put};
use dialog_effects::remote::RemoteInvocation;
use dialog_storage::{
    Blake3Hash, CborEncoder, ContentAddressedStorage, DialogStorageError, Encoder,
};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

use crate::environment::Address;
use crate::repository::remote::RemoteBranch;

/// A content-addressed store that reads from local first, falls back to remote.
///
/// On a remote cache miss that hits remotely, the fetched bytes are written
/// to the local store (cache-through). Writes go to local only.
pub struct FallbackStore<'a, Env> {
    env: &'a Env,
    encoder: CborEncoder,
    local_catalog: Capability<Catalog>,
    remote_branch: &'a RemoteBranch,
}

impl<Env> Clone for FallbackStore<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            env: self.env,
            encoder: self.encoder.clone(),
            local_catalog: self.local_catalog.clone(),
            remote_branch: self.remote_branch,
        }
    }
}

impl<'a, Env> FallbackStore<'a, Env> {
    /// Create a new FallbackStore.
    pub fn new(
        env: &'a Env,
        local_catalog: Capability<Catalog>,
        remote_branch: &'a RemoteBranch,
    ) -> Self {
        Self {
            env,
            encoder: CborEncoder,
            local_catalog,
            remote_branch,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> ContentAddressedStorage for FallbackStore<'_, Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<RemoteInvocation<Get, Address>>
        + ConditionalSync
        + 'static,
{
    type Hash = Blake3Hash;
    type Error = DialogStorageError;

    async fn read<T>(&self, hash: &Self::Hash) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync,
    {
        // Try local first
        let local_get = self.local_catalog.clone().invoke(Get::new(*hash));
        let local_result: Result<Option<Vec<u8>>, _> = local_get.perform(self.env).await;
        let local_result =
            local_result.map_err(|e| DialogStorageError::StorageBackend(e.to_string()))?;

        if let Some(bytes) = local_result {
            let value: T = self
                .encoder
                .decode(&bytes)
                .await
                .map_err(|e| DialogStorageError::DecodeFailed(e.to_string()))?;
            return Ok(Some(value));
        }

        // Fall back to remote
        let remote_result = self
            .remote_branch
            .download_block(*hash, self.env)
            .await
            .map_err(|e| DialogStorageError::StorageBackend(e.to_string()))?;

        match remote_result {
            Some(bytes) => {
                // Cache locally
                let local_put = self
                    .local_catalog
                    .clone()
                    .invoke(Put::new(*hash, bytes.clone()));
                let _: Result<(), _> = local_put.perform(self.env).await;

                let value: T = self
                    .encoder
                    .decode(&bytes)
                    .await
                    .map_err(|e| DialogStorageError::DecodeFailed(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    async fn write<T>(&mut self, block: &T) -> Result<Self::Hash, Self::Error>
    where
        T: Serialize + ConditionalSync + Debug,
    {
        // Write to local only
        let (hash, bytes) = self.encoder.encode(block).await?;

        let effect = self.local_catalog.clone().invoke(Put::new(hash, bytes));
        let result: Result<(), _> = effect.perform(self.env).await;
        result.map_err(|e| DialogStorageError::StorageBackend(e.to_string()))?;

        Ok(hash)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Encoder for FallbackStore<'_, Env>
where
    Env: ConditionalSync + 'static,
{
    type Bytes = Vec<u8>;
    type Hash = Blake3Hash;
    type Error = DialogStorageError;

    async fn encode<T>(&self, block: &T) -> Result<(Self::Hash, Self::Bytes), Self::Error>
    where
        T: Serialize + ConditionalSync + Debug,
    {
        self.encoder.encode(block).await
    }

    async fn decode<T>(&self, bytes: &[u8]) -> Result<T, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync,
    {
        self.encoder.decode(bytes).await
    }
}
