use async_trait::async_trait;
use dialog_capability::fork::Fork;
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::{ArchiveExt, ArchiveSubjectExt, CatalogExt};
use dialog_effects::archive::{Catalog, Get, Put};
use dialog_remote_s3::S3;
use dialog_storage::{Blake3Hash, ContentAddressedStorage, DialogStorageError, Encoder};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

use super::local::LocalIndex;
use crate::SiteAddress as SiteAddressEnum;
use crate::repository::remote::RemoteRepository;

/// Content-addressed index with on-demand remote replication.
///
/// Wraps a [`LocalIndex`] and adds transparent remote fallback: reads
/// that miss locally are fetched from the remote and cached. Writes
/// always go to the local index only.
///
/// When no remote is configured, behaves identically to [`LocalIndex`].
pub struct NetworkedIndex<'a, Env> {
    local: LocalIndex<'a, Env>,
    remote: Option<RemoteRepository>,
}

impl<Env> Clone for NetworkedIndex<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            local: self.local.clone(),
            remote: self.remote.clone(),
        }
    }
}

impl<'a, Env> NetworkedIndex<'a, Env> {
    /// Create a networked index. If `remote` is `Some`, reads that miss
    /// locally will fall back to the remote and cache the result.
    pub fn new(env: &'a Env, index: Capability<Catalog>, remote: Option<RemoteRepository>) -> Self {
        Self {
            local: LocalIndex::new(env, index),
            remote,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> ContentAddressedStorage for NetworkedIndex<'_, Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Fork<S3, Get>>
        + Provider<Fork<dialog_remote_ucan_s3::UcanSite, Get>>
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
        if let Some(value) = self.local.read::<T>(hash).await? {
            return Ok(Some(value));
        }

        // Fall back to offloaded nodes
        let remote = match &self.remote {
            Some(r) => r,
            None => return Ok(None),
        };

        let address = remote.address();
        let remote_catalog = address.subject.clone().archive().catalog("index");

        let env = self.local.env();
        let remote_result = match address.address {
            SiteAddressEnum::S3(ref addr) => {
                remote_catalog
                    .clone()
                    .get(*hash)
                    .fork(addr)
                    .perform(env)
                    .await
            }
            SiteAddressEnum::Ucan(ref addr) => {
                remote_catalog
                    .clone()
                    .get(*hash)
                    .fork(addr)
                    .perform(env)
                    .await
            }
        }
        .map_err(|e| DialogStorageError::StorageBackend(e.to_string()))?;

        match remote_result {
            Some(bytes) => {
                // Cache locally
                let cache = self.local.catalog().clone().put(*hash, bytes.clone());
                let _: Result<(), _> = cache.perform(self.local.env()).await;

                let value: T = self
                    .local
                    .encoder()
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
        self.local.write(block).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Encoder for NetworkedIndex<'_, Env>
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
        self.local.encoder().encode(block).await
    }

    async fn decode<T>(&self, bytes: &[u8]) -> Result<T, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync,
    {
        self.local.encoder().decode(bytes).await
    }
}
