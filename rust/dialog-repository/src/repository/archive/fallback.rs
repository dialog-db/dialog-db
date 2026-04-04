use async_trait::async_trait;
use dialog_capability::access::{Allow, Claim};
use dialog_capability::fork::Fork;
use dialog_capability::site::{Site, SiteAddress};
use dialog_capability::ucan::Ucan;
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{self as archive_fx, Catalog, Get, Put};
use dialog_remote_s3::S3;
use dialog_storage::{
    Blake3Hash, CborEncoder, ContentAddressedStorage, DialogStorageError, Encoder,
};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

use crate::SiteAddress as SiteAddressEnum;
use crate::repository::remote::RemoteRepository;

pub struct FallbackStore<'a, Env> {
    env: &'a Env,
    encoder: CborEncoder,
    local_catalog: Capability<Catalog>,
    remote: Option<RemoteRepository>,
}

impl<Env> Clone for FallbackStore<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            env: self.env,
            encoder: self.encoder.clone(),
            local_catalog: self.local_catalog.clone(),
            remote: self.remote.clone(),
        }
    }
}

impl<'a, Env> FallbackStore<'a, Env> {
    /// Create a fallback store. If `remote` is `Some`, reads that miss
    /// locally will fall back to the remote and cache the result.
    pub fn new(
        env: &'a Env,
        local_catalog: Capability<Catalog>,
        remote: Option<RemoteRepository>,
    ) -> Self {
        Self {
            env,
            encoder: CborEncoder,
            local_catalog,
            remote,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> ContentAddressedStorage for FallbackStore<'_, Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Fork<S3, Get>>
        + Provider<Fork<dialog_remote_ucan_s3::UcanSite, Get>>
        + Provider<Claim<Get, Allow>>
        + Provider<Claim<Get, Ucan>>
        + ConditionalSync
        + 'static,
{
    type Hash = Blake3Hash;
    type Error = DialogStorageError;

    async fn read<T>(&self, hash: &Self::Hash) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync,
    {
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

        let remote = match &self.remote {
            Some(r) => r,
            None => return Ok(None),
        };

        let address = remote.address();
        let remote_catalog = dialog_capability::Subject::from(address.subject.clone())
            .attenuate(archive_fx::Archive)
            .attenuate(Catalog::new("index"));

        let remote_result = match address.address {
            SiteAddressEnum::S3(ref addr) => {
                download_block(&remote_catalog, addr, *hash, self.env).await
            }
            #[cfg(feature = "ucan")]
            SiteAddressEnum::Ucan(ref addr) => {
                download_block(&remote_catalog, addr, *hash, self.env).await
            }
        }
        .map_err(|e| DialogStorageError::StorageBackend(e.to_string()))?;

        match remote_result {
            Some(bytes) => {
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
        let (hash, bytes) = self.encoder.encode(block).await?;
        let effect = self.local_catalog.clone().invoke(Put::new(hash, bytes));
        let result: Result<(), _> = effect.perform(self.env).await;
        result.map_err(|e| DialogStorageError::StorageBackend(e.to_string()))?;
        Ok(hash)
    }
}

async fn download_block<A, Env>(
    catalog: &Capability<Catalog>,
    address: &A,
    hash: Blake3Hash,
    env: &Env,
) -> Result<Option<Vec<u8>>, crate::DialogArtifactsError>
where
    A: SiteAddress,
    A::Site: Site,
    Env: Provider<Fork<A::Site, Get>>
        + Provider<Claim<Get, <A::Site as Site>::Protocol>>
        + ConditionalSync,
{
    catalog
        .clone()
        .invoke(Get::new(hash))
        .fork(address)
        .perform(env)
        .await
        .map_err(|e| {
            crate::DialogArtifactsError::Storage(format!("Remote download failed: {}", e))
        })?
        .map_err(|e| crate::DialogArtifactsError::Storage(format!("Remote download failed: {}", e)))
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
