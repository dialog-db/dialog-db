use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::CatalogExt;
use dialog_effects::archive::{Catalog, Get, Put};
use dialog_storage::{
    Blake3Hash, CborEncoder, ContentAddressedStorage, DialogStorageError, Encoder,
};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

/// Local content-addressed index backed by archive capabilities.
///
/// Bridges the capability system (`Get`/`Put` effects) with the prolly
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> ContentAddressedStorage for LocalIndex<'_, Env>
where
    Env: Provider<Get> + Provider<Put> + ConditionalSync + 'static,
{
    type Hash = Blake3Hash;
    type Error = DialogStorageError;

    async fn read<T>(&self, hash: &Self::Hash) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync,
    {
        let result: Option<Vec<u8>> = self.catalog.clone().get(*hash).perform(self.env).await?;

        match result {
            Some(bytes) => {
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
        self.catalog
            .clone()
            .put(hash, bytes)
            .perform(self.env)
            .await?;
        Ok(hash)
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
    use super::*;
    use dialog_capability::{Did, Subject};
    use dialog_effects::archive::Archive;
    use dialog_storage::provider::Volatile;

    fn test_catalog(name: &str) -> Capability<Catalog> {
        let did: Did = "did:test:archive-cas".parse().unwrap();
        Subject::from(did)
            .attenuate(Archive)
            .attenuate(Catalog::new(name))
    }

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestBlock {
        value: u32,
        label: String,
    }

    #[dialog_common::test]
    async fn it_writes_and_reads_block() -> anyhow::Result<()> {
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
    async fn it_returns_none_for_missing_hash() -> anyhow::Result<()> {
        let env = Volatile::new();
        let archive = LocalIndex::new(&env, test_catalog("index"));

        let missing_hash = [0u8; 32];
        let result: Option<TestBlock> = archive.read(&missing_hash).await?;

        assert!(result.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_catalogs() -> anyhow::Result<()> {
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
