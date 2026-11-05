//! Writing what a batch staged into the archive, each lane into its store.

use dialog_artifacts::tree::ArtifactNodeCache;
use dialog_artifacts::{ArchiveDelta, DialogArtifactsError};
use dialog_capability::Provider;
use dialog_common::{Buffer, ConditionalSync, Holds};
use dialog_effects::archive::Import;
use dialog_effects::archive::prelude::CatalogScope;
use dialog_effects::blob::Import as BlobImport;
use futures_util::{StreamExt as _, TryStreamExt as _, stream};

use super::hydrate::write_blob;

/// How many spilled values a persist writes at once.
const BLOB_WRITES: usize = 16;

/// Write everything `delta` staged into `catalog`'s archive: the spilled
/// values into its blob store, then the tree's nodes into its catalog.
///
/// The spilled values land first because nodes reference them: a node
/// must never be durable before the values its entries name, the same
/// order push keeps on a remote. Each spilled value is imported under the
/// hash its key carries and verified against it.
///
/// The nodes a seal stages are already in the node cache `env` holds for
/// the catalog's subject (the seal persisted through it), so the next edit
/// opens them without reading them back. A flush that fails leaves the
/// archive without them, so they are forgotten there, through the same
/// cache: the cache outlives the handle, and a
/// scope must hold only what its archive has, or a later read (a pull
/// merging a tree that names the same bytes, say) would be answered with
/// a block the store never received and never fetch it.
pub(crate) async fn persist<Env>(
    catalog: &CatalogScope,
    delta: &mut ArchiveDelta,
    env: &Env,
) -> Result<(), DialogArtifactsError>
where
    Env: Provider<Import> + Provider<BlobImport> + Holds + ConditionalSync + ?Sized + 'static,
{
    let staged = delta.blocks().keys();
    let flushed = flush(catalog, delta, env).await;
    if flushed.is_err()
        && let Some(nodes) = ArtifactNodeCache::of(env, catalog.subject())
    {
        for hash in &staged {
            nodes.forget(hash);
        }
    }
    flushed
}

/// The flush itself: spilled values, then nodes.
async fn flush<Env>(
    index: &CatalogScope,
    delta: &mut ArchiveDelta,
    env: &Env,
) -> Result<(), DialogArtifactsError>
where
    Env: Provider<Import> + Provider<BlobImport> + ConditionalSync + ?Sized + 'static,
{
    let blobs: Vec<Buffer> = delta.flush_blobs().collect();
    stream::iter(blobs)
        .map(|blob| async move {
            write_blob(env, index, blob.blake3_hash(), blob.as_ref())
                .await
                .map_err(DialogArtifactsError::from)
        })
        .buffer_unordered(BLOB_WRITES)
        .try_collect::<()>()
        .await?;
    index
        .import(delta.flush_blocks())
        .perform(env)
        .await
        .map_err(DialogArtifactsError::from)?;
    Ok(())
}
