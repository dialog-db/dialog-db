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
pub trait ContentAddressedStorage<const HASH_SIZE: usize>: ConditionalSync + 'static {
    /// The type of hash that is produced by this [ContentAddressedStorage]
    type Hash: HashType<HASH_SIZE> + ConditionalSync;
    /// The type of error that is produced by this [ContentAddressedStorage]
    type Error: Into<DialogStorageError>;

    /// Retrieve a block by its hash
    async fn read<T>(&self, hash: &Self::Hash) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync;
    /// Store a block and receive its hash
    async fn write<T>(&mut self, block: &T) -> Result<Self::Hash, Self::Error>
    where
        T: Serialize + ConditionalSync;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<const HASH_SIZE: usize, Bytes, Hash, EncoderError, BackendError, U>
    ContentAddressedStorage<HASH_SIZE> for U
where
    Hash: HashType<HASH_SIZE> + ConditionalSync,
    Bytes: AsRef<[u8]> + 'static + ConditionalSync,
    EncoderError: Into<DialogStorageError>,
    BackendError: Into<DialogStorageError>,
    U: Encoder<HASH_SIZE, Bytes = Bytes, Hash = Hash, Error = EncoderError>
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
        T: Serialize + ConditionalSync,
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
impl<const HASH_SIZE: usize, Bytes, Hash, EncoderError, BackendError, U>
    ContentAddressedStorage<HASH_SIZE> for Arc<Mutex<U>>
where
    Hash: HashType<HASH_SIZE> + ConditionalSync,
    Bytes: AsRef<[u8]> + 'static + ConditionalSync,
    EncoderError: Into<DialogStorageError>,
    BackendError: Into<DialogStorageError>,
    U: Encoder<HASH_SIZE, Bytes = Bytes, Hash = Hash, Error = EncoderError>
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
        T: Serialize + ConditionalSync,
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
