use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Catalog, Get, Put};
use dialog_storage::{
    Blake3Hash, CborEncoder, ContentAddressedStorage, DialogStorageError, Encoder,
};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;
use tokio::sync::Mutex;

/// CAS (Content-Addressed Storage) adapter that bridges the capability system
/// with the prolly tree's `ContentAddressedStorage` trait.
///
/// This is the **only** struct in the capability-based repository that captures
/// `Env`. It exists because the prolly tree requires an owned
/// `ContentAddressedStorage` implementation, but we want all storage to go
/// through capability effects.
///
/// Uses `Arc<Mutex<Env>>` for interior mutability since `Provider::execute`
/// requires `&mut Env` but `ContentAddressedStorage::read` takes `&self`.
///
/// Created on-the-fly inside `perform()` methods when tree operations are
/// needed, and dropped when those operations complete.
pub struct ContentAddressedStore<Env> {
    env: Arc<Mutex<Env>>,
    encoder: CborEncoder,
    catalog: Capability<Catalog>,
}

impl<Env> Clone for ContentAddressedStore<Env> {
    fn clone(&self) -> Self {
        Self {
            env: self.env.clone(),
            encoder: self.encoder.clone(),
            catalog: self.catalog.clone(),
        }
    }
}

impl<Env> ContentAddressedStore<Env> {
    /// Create a new ContentAddressedStore that delegates storage operations to
    /// capability effects on the given environment.
    pub fn new(env: Arc<Mutex<Env>>, catalog: Capability<Catalog>) -> Self {
        Self {
            env,
            encoder: CborEncoder,
            catalog,
        }
    }

    /// Unwrap the inner environment, consuming this archive.
    /// Panics if there are other references to the Arc.
    pub fn into_inner(self) -> Env {
        Arc::try_unwrap(self.env)
            .ok()
            .expect("ContentAddressedStore: other references still exist")
            .into_inner()
    }

    /// Get a reference to the shared environment.
    pub fn env(&self) -> &Arc<Mutex<Env>> {
        &self.env
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> ContentAddressedStorage for ContentAddressedStore<Env>
where
    Env: Provider<Get> + Provider<Put> + ConditionalSync + 'static,
{
    type Hash = Blake3Hash;
    type Error = DialogStorageError;

    async fn read<T>(&self, hash: &Self::Hash) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync,
    {
        let effect = self.catalog.clone().invoke(Get::new(*hash));

        let mut env = self.env.lock().await;
        let result: Result<Option<Vec<u8>>, _> = effect.perform(&mut *env).await;
        let result = result.map_err(|e| DialogStorageError::StorageBackend(e.to_string()))?;

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
        T: Serialize + ConditionalSync + std::fmt::Debug,
    {
        let (hash, bytes) = self.encoder.encode(block).await?;

        let effect = self.catalog.clone().invoke(Put::new(hash, bytes.clone()));

        let mut env = self.env.lock().await;
        let result: Result<(), _> = effect.perform(&mut *env).await;
        result.map_err(|e| DialogStorageError::StorageBackend(e.to_string()))?;

        Ok(hash)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Encoder for ContentAddressedStore<Env>
where
    Env: ConditionalSync + 'static,
{
    type Bytes = Vec<u8>;
    type Hash = Blake3Hash;
    type Error = DialogStorageError;

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
        let env = Arc::new(Mutex::new(Volatile::new()));
        let mut archive = ContentAddressedStore::new(env, test_catalog("index"));

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
        let env = Arc::new(Mutex::new(Volatile::new()));
        let archive = ContentAddressedStore::new(env, test_catalog("index"));

        let missing_hash = [0u8; 32];
        let result: Option<TestBlock> = archive.read(&missing_hash).await?;

        assert!(result.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_catalogs() -> anyhow::Result<()> {
        let env = Arc::new(Mutex::new(Volatile::new()));

        let block = TestBlock {
            value: 99,
            label: "isolated".into(),
        };

        // Write to catalog "a"
        let hash = {
            let mut archive = ContentAddressedStore::new(env.clone(), test_catalog("a"));
            archive.write(&block).await?
        };

        // Read from catalog "b" — should not find it
        {
            let archive = ContentAddressedStore::new(env.clone(), test_catalog("b"));
            let result: Option<TestBlock> = archive.read(&hash).await?;
            assert!(result.is_none());
        }

        // Read from catalog "a" — should find it
        {
            let archive = ContentAddressedStore::new(env.clone(), test_catalog("a"));
            let result: Option<TestBlock> = archive.read(&hash).await?;
            assert_eq!(result, Some(block));
        }

        Ok(())
    }
}
