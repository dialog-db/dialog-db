//! A store's answer to [`archive::Load`] and [`blob::Load`]: the read of
//! what it holds, since a store knows no other place to look.
//!
//! A store holds what it holds and nothing else, so it answers a load as
//! the plain read: [`Get`] for a block, [`Read`](blob::Read) for a spilled
//! value (then the block catalog, for a value spilled before values moved
//! to the blob store). An environment that knows where a subject's blocks
//! live besides (a peer whose branches track a remote) answers the loads
//! itself, and reads its store through these on the way.

use dialog_capability::{Capability, Provider};
use dialog_common::{Buffer, ConditionalSync};
use dialog_effects::archive::prelude::{ArchiveScope, GetExt as _};
use dialog_effects::archive::{self, ArchiveError, Get};
use dialog_effects::blob::{self, BlobError, BlobReader};

/// The block `load` names, as `env` holds it.
pub async fn load_block<Env>(
    env: &Env,
    load: &archive::Load,
) -> Result<Option<Buffer>, ArchiveError>
where
    Env: Provider<Get> + ConditionalSync + ?Sized,
{
    Ok(load.capability().perform(env).await?.map(Buffer::from))
}

/// The spilled value `load` names, as `env` holds it: its blob, or else the
/// block it was stored as in the subject's default catalog, before values
/// moved to the blob store.
pub async fn load_spill<Env>(env: &Env, load: &blob::Load) -> Result<Option<Buffer>, ArchiveError>
where
    Env: Provider<Get> + Provider<blob::Read> + ConditionalSync + ?Sized,
{
    match load.capability().perform(env).await {
        Ok(reader) => Ok(Some(Buffer::from(
            read_all(reader).await.map_err(archive_error)?,
        ))),
        Err(BlobError::NotFound(_)) => legacy_spill(env, &load.archive, &load.digest).await,
        Err(error) => Err(archive_error(error)),
    }
}

/// The value spilled under `digest` before values moved to the blob store:
/// a block in `archive`'s default catalog.
async fn legacy_spill<Env>(
    env: &Env,
    archive: &ArchiveScope,
    digest: &dialog_common::Blake3Hash,
) -> Result<Option<Buffer>, ArchiveError>
where
    Env: Provider<Get> + ConditionalSync + ?Sized,
{
    let get: Capability<Get> = archive.index().get(digest.clone());
    debug_assert_eq!(get.catalog(), archive::prelude::DEFAULT_CATALOG);
    Ok(get.perform(env).await?.map(Buffer::from))
}

/// Every byte `reader` yields, in order.
pub async fn read_all(mut reader: BlobReader) -> Result<Vec<u8>, BlobError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next().await? {
        bytes.extend(chunk);
    }
    Ok(bytes)
}

/// A blob-store failure as the archive failure a reader of the archive
/// reports, keeping authorization and rejection decisions intact.
pub fn archive_error(error: BlobError) -> ArchiveError {
    match error {
        BlobError::Authorization(error) => ArchiveError::Authorization(error),
        BlobError::Rejected(error) => ArchiveError::Rejected(error),
        error => ArchiveError::Storage(error.to_string()),
    }
}

/// A store answers [`archive::Load`] and, when it keeps blobs,
/// [`blob::Load`] from what it holds.
macro_rules! loads_locally {
    ($store:ty) => {
        #[dialog_capability::provider]
        impl dialog_capability::Provider<dialog_effects::archive::Load> for $store {
            async fn execute(
                &self,
                load: dialog_effects::archive::Load,
            ) -> Result<Option<dialog_common::Buffer>, dialog_effects::archive::ArchiveError> {
                $crate::provider::load::load_block(self, &load).await
            }
        }
    };
    ($store:ty, blobs) => {
        loads_locally!($store);

        #[dialog_capability::provider]
        impl dialog_capability::Provider<dialog_effects::blob::Load> for $store {
            async fn execute(
                &self,
                load: dialog_effects::blob::Load,
            ) -> Result<Option<dialog_common::Buffer>, dialog_effects::archive::ArchiveError> {
                $crate::provider::load::load_spill(self, &load).await
            }
        }
    };
}

loads_locally!(super::Volatile, blobs);
loads_locally!(super::FileSystem, blobs);
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
loads_locally!(super::IndexedDb);

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_capability::{Provider, Subject};
    use dialog_common::{Buffer, ConditionalSync};
    use dialog_effects::archive::prelude::{ArchiveScope, CatalogScope};
    use dialog_effects::archive::{self, Get, Put};
    use dialog_effects::blob::{self, Import as BlobImport, Read as BlobRead};
    use dialog_effects::storage::{Directory, Location};
    use dialog_varsig::did;

    use crate::helpers::unique_name;
    use crate::provider::{FileSystem, Volatile};
    use crate::resource::Resource as _;

    fn test_catalog(name: &str) -> CatalogScope {
        ArchiveScope::new(Subject::from(did!("key:zArchiveCasTest"))).catalog(name)
    }

    /// A local store the spill-lane tests run against: an archive catalog
    /// beside a blob store, answering both loads.
    trait Store:
        Provider<Get>
        + Provider<Put>
        + Provider<BlobRead>
        + Provider<BlobImport>
        + Provider<archive::Load>
        + Provider<blob::Load>
    {
    }
    impl<T> Store for T where
        T: Provider<Get>
            + Provider<Put>
            + Provider<BlobRead>
            + Provider<BlobImport>
            + Provider<archive::Load>
            + Provider<blob::Load>
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

        let node = archive::Load::new(catalog.clone(), block.blake3_hash().clone())
            .perform(env)
            .await?;
        let spill = blob::Load::new(catalog.archive(), block.blake3_hash().clone())
            .perform(env)
            .await?;

        assert_eq!(node, Some(block.clone()));
        assert_eq!(spill, Some(block));
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

        let spill = blob::Load::new(catalog.archive(), digest.clone())
            .perform(env)
            .await?;
        let node = archive::Load::new(catalog, digest).perform(env).await?;

        assert_eq!(spill, Some(value));
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
        let missing = Buffer::from(b"never stored".to_vec());
        let node = archive::Load::new(test_catalog("index"), missing.blake3_hash().clone())
            .perform(&env)
            .await?;

        assert!(node.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_catalogs() -> Result<()> {
        let env = Volatile::new();
        let block = Buffer::from(b"isolated".to_vec());
        test_catalog("a").put(block.clone()).perform(&env).await?;

        let digest = block.blake3_hash().clone();
        assert!(
            archive::Load::new(test_catalog("b"), digest.clone())
                .perform(&env)
                .await?
                .is_none()
        );
        assert_eq!(
            archive::Load::new(test_catalog("a"), digest)
                .perform(&env)
                .await?,
            Some(block)
        );
        Ok(())
    }
}
