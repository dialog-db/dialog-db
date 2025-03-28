use async_trait::async_trait;
use x_common::ConditionalSync;

use crate::{Encoder, HashType, StorageBackend, XStorageError};

/// A [ContentAddressedStorage] is able to store and/or retrieve a value -
/// called a block - by a self-evident, deterministically derivable value: its
/// hash.
///
/// A blanket implementation is provided for all types that also implement
/// [Encoder] and [StorageBackend] in a compatible fashion.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ContentAddressedStorage<const HASH_SIZE: usize> {
    /// The type of block that is able to be stored
    type Block: ConditionalSync;
    /// The type of hash that is produced by this [ContentAddressedStorage]
    type Hash: HashType<HASH_SIZE> + ConditionalSync;
    /// The type of error that is produced by this [ContentAddressedStorage]
    type Error: Into<XStorageError>;

    /// Retrieve a block by its hash
    async fn read(&self, hash: &Self::Hash) -> Result<Option<Self::Block>, Self::Error>;
    /// Store a block and receive its hash
    async fn write(&mut self, block: &Self::Block) -> Result<Self::Hash, Self::Error>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<const HASH_SIZE: usize, Bl, By, H, Ee, Es, T> ContentAddressedStorage<HASH_SIZE> for T
where
    H: HashType<HASH_SIZE> + ConditionalSync,
    Bl: ConditionalSync,
    By: AsRef<[u8]> + 'static + ConditionalSync,
    Ee: Into<XStorageError>,
    Es: Into<XStorageError>,
    T: Encoder<HASH_SIZE, Block = Bl, Bytes = By, Hash = H, Error = Ee>
        + StorageBackend<Key = H, Value = By, Error = Es>
        + ConditionalSync,
{
    type Block = Bl;
    type Hash = H;
    type Error = XStorageError;

    async fn read(&self, hash: &Self::Hash) -> Result<Option<Self::Block>, Self::Error> {
        let Some(encoded_bytes) = self.get(hash).await.map_err(|error| error.into())? else {
            return Ok(None);
        };

        Ok(Some(
            self.decode(encoded_bytes.as_ref())
                .await
                .map_err(|error| error.into())?,
        ))
    }
    async fn write(&mut self, block: &Self::Block) -> Result<Self::Hash, Self::Error> {
        let (hash, encoded_bytes) = self.encode(&block).await.map_err(|error| error.into())?;
        self.set(hash.clone(), encoded_bytes)
            .await
            .map_err(|error| error.into())?;
        Ok(hash)
    }
}
