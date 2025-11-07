use async_trait::async_trait;
use dialog_common::ConditionalSync;

mod backend;
pub use backend::*;

mod cache;
pub use cache::*;

mod compress;
pub use compress::*;

mod overlay;
pub use overlay::*;

mod measure;
pub use measure::*;

mod journal;
pub use journal::*;

mod transfer;
use serde::{Serialize, de::DeserializeOwned};
pub use transfer::*;

mod content_addressed;
pub use content_addressed::*;

mod transactional_memory;
pub use transactional_memory::*;

/// A universal envelope for all compatible combinations of [Encoder] and
/// [StorageBackend] implementations. See the crate documentation for
/// a practical example of usage.
#[derive(Clone)]
pub struct Storage<Encoder, Backend>
where
    Encoder: crate::Encoder,
    Backend: StorageBackend,
{
    /// The [Encoder] used by the [Storage]
    pub encoder: Encoder,
    /// The [StorageBackend] used by the [Storage]
    pub backend: Backend,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Encoder, Backend> crate::Encoder for Storage<Encoder, Backend>
where
    Encoder: crate::Encoder,
    Backend: StorageBackend,
    Self: ConditionalSync,
{
    type Bytes = Encoder::Bytes;
    type Hash = Encoder::Hash;
    type Error = Encoder::Error;

    async fn encode<T>(&self, block: &T) -> Result<(Self::Hash, Self::Bytes), Self::Error>
    where
        T: Serialize + ConditionalSync + std::fmt::Debug,
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Encoder, Backend> StorageBackend for Storage<Encoder, Backend>
where
    Encoder: crate::Encoder,
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Encoder, Backend> TransactionalMemoryBackend for Storage<Encoder, Backend>
where
    Encoder: crate::Encoder,
    Backend: StorageBackend
        + TransactionalMemoryBackend<
            Address = <Backend as StorageBackend>::Key,
            Value = <Backend as StorageBackend>::Value,
            Error = <Backend as StorageBackend>::Error,
        >,
    Self: ConditionalSync,
{
    type Address = <Backend as StorageBackend>::Key;
    type Value = <Backend as StorageBackend>::Value;
    type Error = <Backend as StorageBackend>::Error;
    type Edition = <Backend as TransactionalMemoryBackend>::Edition;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        self.backend.resolve(address).await
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        self.backend.replace(address, edition, content).await
    }
}

#[cfg(test)]
mod tests {
    use crate::DialogStorageError;
    use crate::{ContentAddressedStorage, Encoder, MemoryStorageBackend, Storage};
    use anyhow::Result;
    use async_trait::async_trait;

    use dialog_common::ConditionalSync;
    use serde::de::DeserializeOwned;
    use serde::{Deserialize, Serialize};
    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[derive(PartialEq, Debug, Serialize, Deserialize)]
    struct TestBlock {
        pub value: u32,
    }

    #[derive(Clone)]
    struct TestEncoder;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl Encoder for TestEncoder {
        type Bytes = Vec<u8>;
        type Hash = [u8; 32];
        type Error = DialogStorageError;

        async fn encode<T>(&self, block: &T) -> Result<(Self::Hash, Self::Bytes), Self::Error>
        where
            T: Serialize + ConditionalSync + std::fmt::Debug,
        {
            let bytes = serde_ipld_dagcbor::to_vec(block)
                .map_err(|error| DialogStorageError::EncodeFailed(format!("{error}")))?;
            let hash = blake3::hash(&bytes).as_bytes().to_owned();

            Ok((hash, bytes))
        }

        async fn decode<T>(&self, bytes: &[u8]) -> Result<T, Self::Error>
        where
            T: DeserializeOwned + ConditionalSync,
        {
            serde_ipld_dagcbor::from_slice::<T>(bytes)
                .map_err(|error| DialogStorageError::DecodeFailed(format!("{error}")))
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
