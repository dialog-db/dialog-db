use crate::RemoteSite;
use async_trait::async_trait;
use dialog_capability::Fork;
use dialog_capability::{Capability, Provider};
use dialog_common::{Buffer, ConditionalSync};
use dialog_effects::archive::prelude::{ArchiveExt, ArchiveSubjectExt, CatalogExt};
use dialog_effects::archive::{Catalog, Get, Put};
use dialog_storage::{Blake3Hash, DialogStorageError, Encoder, StorageBackend};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

use super::local::LocalIndex;
use crate::RemoteRepository;

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

/// Raw block access for the search tree, with the same transparent
/// remote fallback as the content-addressed `read`: reads that miss
/// locally are fetched from the remote and cached, writes go to the
/// local index only. Node buffers pass through without the CBOR encoder.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> StorageBackend for NetworkedIndex<'_, Env>
where
    Env:
        Provider<Get> + Provider<Put> + Provider<Fork<RemoteSite, Get>> + ConditionalSync + 'static,
{
    type Key = Blake3Hash;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        StorageBackend::set(&mut self.local, key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        if let Some(bytes) = StorageBackend::get(&self.local, key).await? {
            return Ok(Some(bytes));
        }

        let remote = match &self.remote {
            Some(remote) => remote,
            None => return Ok(None),
        };

        let address = remote.address();
        let remote_catalog = address.subject.clone().archive().catalog("index");

        let env = self.local.env();
        let remote_result = remote_catalog
            .clone()
            .get(*key)
            .fork(&address.address)
            .perform(env)
            .await
            .map_err(|e| DialogStorageError::StorageBackend(e.to_string()))?;

        match remote_result {
            Some(bytes) => {
                // Cache locally
                let cache = self
                    .local
                    .catalog()
                    .clone()
                    .put(Buffer::from(bytes.as_slice()));
                let _: Result<(), _> = cache.perform(self.local.env()).await;
                Ok(Some(bytes))
            }
            None => Ok(None),
        }
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
