use async_trait::async_trait;
use x_common::ConditionalSync;

use crate::Encoder;

mod backend;
pub use backend::*;

mod content_addressed;
pub use content_addressed::*;

/// A universal envelope for all compatible combinations of [Encoder] and
/// [StorageBackend] implementations. See the crate documentation for
/// a practical example of usage.
pub struct Storage<const HASH_SIZE: usize, E, S>
where
    E: Encoder<HASH_SIZE>,
    S: StorageBackend,
{
    /// The [Encoder] used by the [Storage]
    pub encoder: E,
    /// The [StorageBackend] used by the [Storage]
    pub backend: S,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<const HASH_SIZE: usize, E, S> Encoder<HASH_SIZE> for Storage<HASH_SIZE, E, S>
where
    E: Encoder<HASH_SIZE>,
    S: StorageBackend,
    Self: ConditionalSync,
{
    type Block = E::Block;
    type Bytes = E::Bytes;
    type Hash = E::Hash;
    type Error = E::Error;

    async fn encode(&self, block: &Self::Block) -> Result<(Self::Hash, Self::Bytes), Self::Error> {
        self.encoder.encode(block).await
    }

    async fn decode(&self, bytes: &[u8]) -> Result<Self::Block, Self::Error> {
        self.encoder.decode(bytes).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<const HASH_SIZE: usize, E, S> StorageBackend for Storage<HASH_SIZE, E, S>
where
    E: Encoder<HASH_SIZE>,
    S: StorageBackend,
    Self: ConditionalSync,
{
    type Key = S::Key;
    type Value = S::Value;
    type Error = S::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.backend.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        self.backend.get(key).await
    }
}

#[cfg(test)]
mod tests {
    use super::{ContentAddressedStorage, Encoder, MemoryStorageBackend, Storage};
    use crate::XStorageError;
    use anyhow::Result;
    use async_trait::async_trait;

    #[derive(PartialEq, Debug)]
    struct TestBlock {
        pub value: u32,
    }

    struct TestEncoder;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl Encoder<32> for TestEncoder {
        type Block = TestBlock;
        type Bytes = Vec<u8>;
        type Hash = [u8; 32];
        type Error = XStorageError;

        async fn encode(
            &self,
            block: &Self::Block,
        ) -> Result<(Self::Hash, Self::Bytes), Self::Error> {
            let bytes = block.value.to_le_bytes().to_vec();
            let hash = blake3::hash(&bytes).as_bytes().to_owned();

            Ok((hash, bytes))
        }

        async fn decode(&self, bytes: &[u8]) -> Result<Self::Block, Self::Error> {
            let value = u32::from_le_bytes(
                bytes
                    .try_into()
                    .map_err(|error| XStorageError::DecodeFailed(format!("{error}")))?,
            );
            Ok(TestBlock { value })
        }
    }

    #[tokio::test]
    async fn it_manifests_content_addressed_storage_from_an_encoder_and_backend() -> Result<()> {
        let mut storage = Storage {
            encoder: TestEncoder,
            backend: MemoryStorageBackend::<[u8; 32], Vec<u8>>::default(),
        };

        let hash = storage.write(&TestBlock { value: 123 }).await?;

        let value = storage.read(&hash).await?;

        assert_eq!(Some(TestBlock { value: 123 }), value);

        Ok(())
    }
}
