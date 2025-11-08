use std::sync::Arc;

use async_trait::async_trait;
use dialog_common::ConditionalSync;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::Mutex;

use crate::{DialogStorageError, Encoder, HashType, StorageBackend};

/// A [ContentAddressedStorage] is able to store and/or retrieve a value -
/// called a block - by a self-evident, deterministically derivable value: its
/// hash.
///
/// A blanket implementation is provided for all types that also implement
/// [Encoder] and [StorageBackend] in a compatible fashion.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ContentAddressedStorage: ConditionalSync + 'static {
    /// The type of hash that is produced by this [ContentAddressedStorage]
    type Hash: HashType + ConditionalSync;
    /// The type of error that is produced by this [ContentAddressedStorage]
    type Error: Into<DialogStorageError>;

    /// Retrieve a block by its hash
    async fn read<T>(&self, hash: &Self::Hash) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync;
    /// Store a block and receive its hash
    async fn write<T>(&mut self, block: &T) -> Result<Self::Hash, Self::Error>
    where
        T: Serialize + ConditionalSync + std::fmt::Debug;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Bytes, Hash, EncoderError, BackendError, U> ContentAddressedStorage for U
where
    Hash: HashType + ConditionalSync,
    Bytes: AsRef<[u8]> + 'static + ConditionalSync,
    EncoderError: Into<DialogStorageError>,
    BackendError: Into<DialogStorageError>,
    U: Encoder<Bytes = Bytes, Hash = Hash, Error = EncoderError>
        + StorageBackend<Key = Hash, Value = Bytes, Error = BackendError>
        + ConditionalSync
        + 'static,
{
    type Hash = Hash;
    type Error = DialogStorageError;

    async fn read<T>(&self, hash: &Self::Hash) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync,
    {
        let Some(encoded_bytes) = self.get(hash).await.map_err(|error| error.into())? else {
            return Ok(None);
        };

        Ok(Some(
            self.decode(encoded_bytes.as_ref())
                .await
                .map_err(|error| error.into())?,
        ))
    }
    async fn write<T>(&mut self, block: &T) -> Result<Self::Hash, Self::Error>
    where
        T: Serialize + ConditionalSync + std::fmt::Debug,
    {
        let (hash, encoded_bytes) = self.encode(block).await.map_err(|error| error.into())?;
        self.set(hash.clone(), encoded_bytes)
            .await
            .map_err(|error| error.into())?;
        Ok(hash)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Bytes, Hash, EncoderError, BackendError, U> ContentAddressedStorage for Arc<Mutex<U>>
where
    Hash: HashType + ConditionalSync,
    Bytes: AsRef<[u8]> + 'static + ConditionalSync,
    EncoderError: Into<DialogStorageError>,
    BackendError: Into<DialogStorageError>,
    U: Encoder<Bytes = Bytes, Hash = Hash, Error = EncoderError>
        + StorageBackend<Key = Hash, Value = Bytes, Error = BackendError>
        + ConditionalSync
        + 'static,
{
    type Hash = Hash;
    type Error = DialogStorageError;

    async fn read<T>(&self, hash: &Self::Hash) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync,
    {
        let storage = self.lock().await;
        let Some(encoded_bytes) = storage.get(hash).await.map_err(|error| error.into())? else {
            return Ok(None);
        };

        Ok(Some(
            storage
                .decode(encoded_bytes.as_ref())
                .await
                .map_err(|error| error.into())?,
        ))
    }
    async fn write<T>(&mut self, block: &T) -> Result<Self::Hash, Self::Error>
    where
        T: Serialize + ConditionalSync + std::fmt::Debug,
    {
        let mut storage = self.lock().await;
        let (hash, encoded_bytes) = storage.encode(block).await.map_err(|error| error.into())?;
        storage
            .set(hash.clone(), encoded_bytes)
            .await
            .map_err(|error| error.into())?;
        Ok(hash)
    }
}
