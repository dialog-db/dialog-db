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
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
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

        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
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
