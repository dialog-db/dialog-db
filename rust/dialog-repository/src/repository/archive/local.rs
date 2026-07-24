use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_common::{Buffer, ConditionalSync};
use dialog_effects::archive::prelude::*;
use dialog_effects::archive::{Catalog, Get, Put};
use dialog_storage::{Blake3Hash, CborEncoder, DialogStorageError, Encoder, StorageBackend};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

/// Local content-addressed index backed by archive capabilities.
///
/// Bridges the capability system (`Get`/`Put` effects) with the search
/// tree's `ContentAddressedStorage` trait. All reads and writes go to
/// the local archive only.
pub struct LocalIndex<'a, Env> {
    env: &'a Env,
    encoder: CborEncoder,
    catalog: Capability<Catalog>,
}

impl<Env> Clone for LocalIndex<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            env: self.env,
            encoder: self.encoder.clone(),
            catalog: self.catalog.clone(),
        }
    }
}

impl<'a, Env> LocalIndex<'a, Env> {
    /// Create a local index for the given catalog capability.
    pub fn new(env: &'a Env, catalog: Capability<Catalog>) -> Self {
        Self {
            env,
            encoder: CborEncoder,
            catalog,
        }
    }

    /// The catalog capability this index operates on.
    pub fn catalog(&self) -> &Capability<Catalog> {
        &self.catalog
    }

    /// The environment reference.
    pub fn env(&self) -> &'a Env {
        self.env
    }

    /// The encoder used for serialization.
    pub fn encoder(&self) -> &CborEncoder {
        &self.encoder
    }
}

/// Raw block access for the search tree: node buffers are already
/// serialized, so they pass straight through the catalog effects without
/// the CBOR encoder. The encoder remains in use for typed blocks like
/// revisions (via `dialog_storage::ContentAddressedStorage`).
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> StorageBackend for LocalIndex<'_, Env>
where
    Env: Provider<Get> + Provider<Put> + ConditionalSync + 'static,
{
    type Key = Blake3Hash;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn set(&mut self, _key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.catalog
            .clone()
            .put(Buffer::from(value))
            .perform(self.env)
            .await?;
        Ok(())
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        Ok(self.catalog.clone().get(*key).perform(self.env).await?)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Encoder for LocalIndex<'_, Env>
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

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use dialog_storage::ContentAddressedStorage;

    use super::*;
    use anyhow::Result;
    use dialog_capability::Subject;
    use dialog_storage::provider::Volatile;
    use dialog_varsig::did;

    fn test_catalog(name: &str) -> Capability<Catalog> {
        Subject::from(did!("key:zArchiveCasTest"))
            .archive()
            .catalog(name)
    }

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestBlock {
        value: u32,
        label: String,
    }

    #[dialog_common::test]
    async fn it_writes_and_reads_block() -> Result<()> {
        let env = Volatile::new();
        let mut archive = LocalIndex::new(&env, test_catalog("index"));

        let block = TestBlock {
            value: 42,
            label: "hello".into(),
        };

        let hash = archive.write(&block).await?;
        let result: Option<TestBlock> = archive.read(&hash).await?;

        assert_eq!(result, Some(block));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_hash() -> Result<()> {
        let env = Volatile::new();
        let archive = LocalIndex::new(&env, test_catalog("index"));

        let missing_hash = [0u8; 32];
        let result: Option<TestBlock> = archive.read(&missing_hash).await?;

        assert!(result.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_catalogs() -> Result<()> {
        let env = Volatile::new();

        let block = TestBlock {
            value: 99,
            label: "isolated".into(),
        };

        let hash = {
            let mut archive = LocalIndex::new(&env, test_catalog("a"));
            archive.write(&block).await?
        };

        {
            let archive = LocalIndex::new(&env, test_catalog("b"));
            let result: Option<TestBlock> = archive.read(&hash).await?;
            assert!(result.is_none());
        }

        {
            let archive = LocalIndex::new(&env, test_catalog("a"));
            let result: Option<TestBlock> = archive.read(&hash).await?;
            assert_eq!(result, Some(block));
        }

        Ok(())
    }
}
