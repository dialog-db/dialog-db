use async_trait::async_trait;
use dialog_artifacts::{DialogArtifactsError, LoadBlob};
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer, ConditionalSync};
use dialog_effects::archive::prelude::CatalogScope;
use dialog_effects::archive::{ArchiveError, Get};
use dialog_search_tree::{DialogSearchTreeError, Load};

/// Local content-addressed index backed by archive capabilities.
///
/// Loads a branch's tree nodes ([`Load`]) and spilled values ([`LoadBlob`])
/// from one catalog of the local archive, performing `Get` against the
/// environment it borrows. It never writes: a commit writes what its batch
/// staged.
pub struct LocalIndex<'a, Env> {
    env: &'a Env,
    catalog: CatalogScope,
}

impl<Env> Clone for LocalIndex<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            env: self.env,
            catalog: self.catalog.clone(),
        }
    }
}

impl<'a, Env> LocalIndex<'a, Env> {
    /// Create a local index for the given catalog capability.
    pub fn new(env: &'a Env, catalog: CatalogScope) -> Self {
        Self { env, catalog }
    }

    /// The catalog capability this index operates on.
    pub fn catalog(&self) -> &CatalogScope {
        &self.catalog
    }

    /// The environment reference.
    pub fn env(&self) -> &'a Env {
        self.env
    }
}

impl<Env> LocalIndex<'_, Env>
where
    Env: Provider<Get> + ConditionalSync + 'static,
{
    /// The block stored under `hash` in the local archive, if any.
    pub async fn load(&self, hash: &Blake3Hash) -> Result<Option<Buffer>, ArchiveError> {
        Ok(self
            .catalog
            .clone()
            .get(hash.clone())
            .perform(self.env)
            .await?
            .map(Buffer::from))
    }
}

/// Tree nodes load from the local archive.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<Load> for LocalIndex<'_, Env>
where
    Env: Provider<Get> + ConditionalSync + 'static,
{
    async fn execute(&self, hash: Blake3Hash) -> Result<Option<Buffer>, DialogSearchTreeError> {
        self.load(&hash)
            .await
            .map_err(|error| DialogSearchTreeError::Storage(error.into()))
    }
}

/// Spilled values load from the local archive, where commits write them
/// beside the tree's nodes.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<LoadBlob> for LocalIndex<'_, Env>
where
    Env: Provider<Get> + ConditionalSync + 'static,
{
    async fn execute(&self, hash: Blake3Hash) -> Result<Option<Buffer>, DialogArtifactsError> {
        Ok(self.load(&hash).await?)
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use anyhow::Result;
    use dialog_capability::Subject;
    use dialog_effects::archive::prelude::ArchiveScope;
    use dialog_storage::provider::Volatile;
    use dialog_varsig::did;

    fn test_catalog(name: &str) -> CatalogScope {
        ArchiveScope::new(Subject::from(did!("key:zArchiveCasTest"))).catalog(name)
    }

    async fn put(env: &Volatile, catalog: &CatalogScope, block: &Buffer) -> Result<()> {
        catalog.clone().put(block.clone()).perform(env).await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn it_loads_a_stored_block_through_both_lanes() -> Result<()> {
        let env = Volatile::new();
        let catalog = test_catalog("index");
        let block = Buffer::from(b"a block".to_vec());
        put(&env, &catalog, &block).await?;

        let index = LocalIndex::new(&env, catalog);
        let node = Provider::<Load>::execute(&index, block.blake3_hash().clone()).await?;
        let blob = Provider::<LoadBlob>::execute(&index, block.blake3_hash().clone()).await?;

        assert_eq!(node, Some(block.clone()));
        assert_eq!(blob, Some(block));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_loads_nothing_for_a_missing_hash() -> Result<()> {
        let env = Volatile::new();
        let index = LocalIndex::new(&env, test_catalog("index"));

        let missing = Buffer::from(b"never stored".to_vec());
        let node = Provider::<Load>::execute(&index, missing.blake3_hash().clone()).await?;

        assert!(node.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_catalogs() -> Result<()> {
        let env = Volatile::new();
        let block = Buffer::from(b"isolated".to_vec());
        put(&env, &test_catalog("a"), &block).await?;

        let other = LocalIndex::new(&env, test_catalog("b"));
        assert!(
            Provider::<Load>::execute(&other, block.blake3_hash().clone())
                .await?
                .is_none()
        );

        let same = LocalIndex::new(&env, test_catalog("a"));
        assert_eq!(
            Provider::<Load>::execute(&same, block.blake3_hash().clone()).await?,
            Some(block)
        );
        Ok(())
    }
}
