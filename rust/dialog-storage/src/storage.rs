use async_trait::async_trait;
use dialog_common::ConditionalSync;

mod backend;
pub use backend::*;

mod cache;
pub use cache::*;

mod measure;
pub use measure::*;

mod transfer;
pub use transfer::*;

mod content_addressed;
pub use content_addressed::*;

/// A universal envelope for all compatible combinations of [Encoder] and
/// [StorageBackend] implementations. See the crate documentation for
/// a practical example of usage.
#[derive(Clone)]
pub struct Storage<const HASH_SIZE: usize, Encoder, Backend>
where
    Encoder: crate::Encoder<HASH_SIZE>,
    Backend: StorageBackend,
{
    /// The [Encoder] used by the [Storage]
    pub encoder: Encoder,
    /// The [StorageBackend] used by the [Storage]
    pub backend: Backend,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<const HASH_SIZE: usize, Encoder, Backend> crate::Encoder<HASH_SIZE>
    for Storage<HASH_SIZE, Encoder, Backend>
where
    Encoder: crate::Encoder<HASH_SIZE>,
    Backend: StorageBackend,
    Self: ConditionalSync,
{
    type Block = Encoder::Block;
    type Bytes = Encoder::Bytes;
    type Hash = Encoder::Hash;
    type Error = Encoder::Error;

    async fn encode(&self, block: &Self::Block) -> Result<(Self::Hash, Self::Bytes), Self::Error> {
        self.encoder.encode(block).await
    }

    async fn decode(&self, bytes: &[u8]) -> Result<Self::Block, Self::Error> {
        self.encoder.decode(bytes).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<const HASH_SIZE: usize, Encoder, Backend> StorageBackend
    for Storage<HASH_SIZE, Encoder, Backend>
where
    Encoder: crate::Encoder<HASH_SIZE>,
    Backend: StorageBackend,
    Self: ConditionalSync,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Error = Backend::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.backend.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        self.backend.get(key).await
    }
}

#[cfg(test)]
mod tests {
    use crate::DialogStorageError;
    use crate::{ContentAddressedStorage, Encoder, MemoryStorageBackend, Storage};
    use anyhow::Result;
    use async_trait::async_trait;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[derive(PartialEq, Debug)]
    struct TestBlock {
        pub value: u32,
    }

    #[derive(Clone)]
    struct TestEncoder;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl Encoder<32> for TestEncoder {
        type Block = TestBlock;
        type Bytes = Vec<u8>;
        type Hash = [u8; 32];
        type Error = DialogStorageError;

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
                    .map_err(|error| DialogStorageError::DecodeFailed(format!("{error}")))?,
            );
            Ok(TestBlock { value })
        }
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
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
