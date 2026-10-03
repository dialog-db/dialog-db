use async_trait::async_trait;
use dialog_artifacts::{DialogArtifactsError, LoadBlob};
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer, ConditionalSync};
use dialog_effects::archive::prelude::CatalogScope;
use dialog_effects::archive::{ArchiveError, Get};
use dialog_effects::blob::{BlobError, BlobReader, Read as BlobRead};
use dialog_search_tree::{DialogSearchTreeError, LoadBlock};

use crate::sealing::{SealedReadError, TreeSpace, open_node};

/// Local content-addressed index backed by archive capabilities.
///
/// Loads a branch's tree nodes ([`LoadBlock`]) and spilled values ([`LoadBlob`])
/// from one catalog of the local archive, performing `Get` against the
/// environment it borrows. It never writes: a commit writes what its batch
/// staged.
///
/// On a sealed line ([`sealed`](Self::sealed)) the catalog holds envelopes
/// and sealed values, not nodes and values: a load finds where the node or
/// value lives through the line's [`TreeSpace`], fetches that, and opens it.
pub struct LocalIndex<'a, Env> {
    env: &'a Env,
    catalog: CatalogScope,
    sealing: Option<TreeSpace>,
}

impl<Env> Clone for LocalIndex<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            env: self.env,
            catalog: self.catalog.clone(),
            sealing: self.sealing.clone(),
        }
    }
}

impl<'a, Env> LocalIndex<'a, Env> {
    /// Create a local index for the given catalog capability.
    pub fn new(env: &'a Env, catalog: CatalogScope) -> Self {
        Self {
            env,
            catalog,
            sealing: None,
        }
    }

    /// Read a sealed line through `sealing`, or a plain one with `None`.
    #[must_use]
    pub fn sealed(mut self, sealing: Option<TreeSpace>) -> Self {
        self.sealing = sealing;
        self
    }

    /// The keys this index opens what it loads with, on a sealed line.
    pub fn sealing(&self) -> Option<&TreeSpace> {
        self.sealing.as_ref()
    }

    /// The catalog capability this index operates on.
    pub fn catalog(&self) -> &CatalogScope {
        &self.catalog
    }

    /// The environment reference.
    pub fn env(&self) -> &'a Env {
        self.env
    }

    /// Where the archive holds the node `identity`: under its identity on
    /// a plain line, under its envelope's address on a sealed one. `None`
    /// when a sealed line has not located it.
    pub(crate) fn node_address(&self, identity: &Blake3Hash) -> Option<Blake3Hash> {
        match &self.sealing {
            None => Some(identity.clone()),
            Some(space) => space.locate(identity).map(|at| at.address),
        }
    }

    /// Where the archive holds the spilled value hashing to `reference`:
    /// under the reference on a plain line, under its sealed copy's
    /// address on a sealed one. `None` when a sealed line has not located
    /// it.
    pub(crate) fn value_address(&self, reference: &Blake3Hash) -> Option<Blake3Hash> {
        match &self.sealing {
            None => Some(reference.clone()),
            Some(space) => space.locate_value(reference),
        }
    }
}

impl<Env> LocalIndex<'_, Env>
where
    Env: Provider<Get> + ConditionalSync + 'static,
{
    /// The block stored under `hash` in the local archive, if any, as the
    /// archive holds it: unverified, and sealed on a sealed line. Readers
    /// outside the crate load through [`LoadBlock`] or [`LoadBlob`], which
    /// open and check it.
    pub(crate) async fn load(&self, hash: &Blake3Hash) -> Result<Option<Buffer>, ArchiveError> {
        Ok(self
            .catalog
            .clone()
            .get(hash.clone())
            .perform(self.env)
            .await?
            .map(Buffer::from))
    }

    /// The node `identity` names, opened on a sealed line: the envelope
    /// this line's space locates it at, opened. On a plain line, the block
    /// stored under `identity`. A node a sealed line has not located is
    /// absent: the line has not reached it from any head it read.
    ///
    /// # Errors
    ///
    /// [`SealedReadError::Keyring`] when an envelope does not open for this
    /// line's keys, and [`SealedReadError::Archive`] when the archive fails.
    pub async fn load_node(
        &self,
        identity: &Blake3Hash,
    ) -> Result<Option<Buffer>, SealedReadError> {
        let Some(space) = &self.sealing else {
            return Ok(self.load(identity).await?);
        };
        let Some(at) = space.locate(identity) else {
            return Ok(None);
        };
        let Some(bytes) = self.load(&at.address).await? else {
            return Ok(None);
        };
        Ok(Some(open_node(space, identity, bytes.as_ref())?))
    }
}

impl<Env> LocalIndex<'_, Env>
where
    Env: Provider<Get> + Provider<BlobRead> + ConditionalSync + 'static,
{
    /// The spilled value hashing to `reference`, opened on a sealed line;
    /// on a plain one, the value stored under `reference`.
    ///
    /// # Errors
    ///
    /// As for [`load_node`](Self::load_node).
    pub async fn load_value(
        &self,
        reference: &Blake3Hash,
    ) -> Result<Option<Buffer>, SealedReadError> {
        let Some(space) = &self.sealing else {
            return Ok(self.load_blob(reference).await?);
        };
        let Some(address) = space.locate_value(reference) else {
            return Ok(None);
        };
        let Some(bytes) = self.load_blob(&address).await? else {
            return Ok(None);
        };
        Ok(Some(space.open_value(reference, bytes.as_ref())?))
    }

    /// The blob stored under `hash` locally, if any, as the archive holds
    /// it: a spilled value on a plain line, a sealed one on a sealed line.
    ///
    /// Spilled values live in the archive's blob store. Values spilled before
    /// they moved there are still blocks in this catalog, so a blob-store
    /// miss falls back to it.
    pub async fn load_blob(&self, hash: &Blake3Hash) -> Result<Option<Buffer>, ArchiveError> {
        match self
            .catalog
            .archive()
            .blob()
            .read(hash.clone())
            .perform(self.env)
            .await
        {
            Ok(reader) => Ok(Some(Buffer::from(
                read_all(reader).await.map_err(archive_error)?,
            ))),
            Err(BlobError::NotFound(_)) => self.load(hash).await,
            Err(error) => Err(archive_error(error)),
        }
    }
}

/// Every byte `reader` yields, in order.
pub(crate) async fn read_all(mut reader: BlobReader) -> Result<Vec<u8>, BlobError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next().await? {
        bytes.extend(chunk);
    }
    Ok(bytes)
}

/// A blob-store failure as the archive failure a reader of this archive
/// reports, keeping authorization and rejection decisions intact.
pub(crate) fn archive_error(error: BlobError) -> ArchiveError {
    match error {
        BlobError::Authorization(error) => ArchiveError::Authorization(error),
        BlobError::Rejected(error) => ArchiveError::Rejected(error),
        error => ArchiveError::Storage(error.to_string()),
    }
}

/// Tree nodes load from the local archive.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<LoadBlock> for LocalIndex<'_, Env>
where
    Env: Provider<Get> + ConditionalSync + 'static,
{
    async fn execute(
        &self,
        LoadBlock { hash }: LoadBlock,
    ) -> Result<Option<Buffer>, DialogSearchTreeError> {
        self.load_node(&hash)
            .await
            .map_err(|error| DialogSearchTreeError::Storage(ArchiveError::from(error).into()))
    }
}

/// Spilled values load from the local blob store, falling back to the
/// block catalog for values spilled before they moved to blobs.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<LoadBlob> for LocalIndex<'_, Env>
where
    Env: Provider<Get> + Provider<BlobRead> + ConditionalSync + 'static,
{
    async fn execute(
        &self,
        LoadBlob { hash }: LoadBlob,
    ) -> Result<Option<Buffer>, DialogArtifactsError> {
        Ok(self.load_value(&hash).await.map_err(ArchiveError::from)?)
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use anyhow::Result;
    use dialog_capability::Subject;
    use dialog_effects::archive::Put;
    use dialog_effects::archive::prelude::ArchiveScope;
    use dialog_effects::blob::Import as BlobImport;
    use dialog_effects::storage::{Directory, Location};
    use dialog_peer::helpers::unique_name;
    use dialog_storage::provider::{FileSystem, Volatile};
    use dialog_storage::resource::Resource as _;
    use dialog_varsig::did;

    fn test_catalog(name: &str) -> CatalogScope {
        ArchiveScope::new(Subject::from(did!("key:zArchiveCasTest"))).catalog(name)
    }

    async fn put(env: &Volatile, catalog: &CatalogScope, block: &Buffer) -> Result<()> {
        catalog.clone().put(block.clone()).perform(env).await?;
        Ok(())
    }

    /// A local store the spill-lane tests run against: an archive catalog
    /// beside a blob store.
    trait Store: Provider<Get> + Provider<Put> + Provider<BlobRead> + Provider<BlobImport> {}
    impl<T> Store for T where
        T: Provider<Get> + Provider<Put> + Provider<BlobRead> + Provider<BlobImport>
    {
    }

    /// A filesystem store in a fresh temp directory; OPFS on the web.
    async fn filesystem(name: &str) -> Result<FileSystem> {
        Ok(FileSystem::open(&Location::new(Directory::Temp, unique_name(name))).await?)
    }

    /// A value spilled before values moved to the blob store is a block in
    /// the catalog, and still loads as a spill.
    async fn loads_a_legacy_spill_block<Env>(env: &Env) -> Result<()>
    where
        Env: Store + ConditionalSync + 'static,
    {
        let catalog = test_catalog("index");
        let block = Buffer::from(b"a block".to_vec());
        catalog.clone().put(block.clone()).perform(env).await?;

        let index = LocalIndex::new(env, catalog);
        let node = LoadBlock::new(block.blake3_hash().clone())
            .perform(&index)
            .await?;
        let blob = LoadBlob::new(block.blake3_hash().clone())
            .perform(&index)
            .await?;

        assert_eq!(node, Some(block.clone()));
        assert_eq!(blob, Some(block));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_loads_a_stored_block_through_both_lanes() -> Result<()> {
        loads_a_legacy_spill_block(&Volatile::new()).await
    }

    #[dialog_common::test]
    async fn it_loads_a_stored_block_through_both_lanes_on_the_filesystem() -> Result<()> {
        loads_a_legacy_spill_block(&filesystem("legacy-spill").await?).await
    }

    /// A spilled value in the blob store loads as a spill but is not a tree
    /// node: the blob store and the block catalog are separate lanes.
    async fn loads_a_spill_from_the_blob_store<Env>(env: &Env) -> Result<()>
    where
        Env: Store + ConditionalSync + 'static,
    {
        let catalog = test_catalog("index");
        let value = Buffer::from(b"a spilled value".to_vec());
        let digest = value.blake3_hash().clone();
        let mut writer = catalog
            .archive()
            .blob()
            .import(digest.clone(), value.as_ref().len() as u64)
            .perform(env)
            .await?;
        writer.write_all(value.as_ref()).await?;
        writer.finish().await?;

        let index = LocalIndex::new(env, catalog);
        let blob = LoadBlob::new(digest.clone()).perform(&index).await?;
        let node = LoadBlock::new(digest).perform(&index).await?;

        assert_eq!(blob, Some(value));
        assert!(node.is_none(), "a spill is not a tree node");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_loads_a_spill_from_the_blob_store() -> Result<()> {
        loads_a_spill_from_the_blob_store(&Volatile::new()).await
    }

    #[dialog_common::test]
    async fn it_loads_a_spill_from_the_blob_store_on_the_filesystem() -> Result<()> {
        loads_a_spill_from_the_blob_store(&filesystem("blob-spill").await?).await
    }

    #[dialog_common::test]
    async fn it_loads_nothing_for_a_missing_hash() -> Result<()> {
        let env = Volatile::new();
        let index = LocalIndex::new(&env, test_catalog("index"));

        let missing = Buffer::from(b"never stored".to_vec());
        let node = LoadBlock::new(missing.blake3_hash().clone())
            .perform(&index)
            .await?;

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
            LoadBlock::new(block.blake3_hash().clone())
                .perform(&other)
                .await?
                .is_none()
        );

        let same = LocalIndex::new(&env, test_catalog("a"));
        assert_eq!(
            LoadBlock::new(block.blake3_hash().clone())
                .perform(&same)
                .await?,
            Some(block)
        );
        Ok(())
    }
}
