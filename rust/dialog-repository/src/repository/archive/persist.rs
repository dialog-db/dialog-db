//! Writing what a batch staged into the archive, each lane into its store.

use dialog_artifacts::tree::ArtifactNodeCache;
use dialog_artifacts::{ArchiveDelta, DialogArtifactsError};
use dialog_capability::Provider;
use dialog_common::{Buffer, ConditionalSync};
use dialog_effects::archive::Import;
use dialog_effects::archive::prelude::CatalogScope;
use dialog_effects::blob::Import as BlobImport;
use futures_util::{StreamExt as _, TryStreamExt as _, stream};

use super::networked::write_blob;

/// How many spilled values a persist writes at once.
const BLOB_WRITES: usize = 16;

/// Write everything `delta` staged into `index`'s archive: the spilled
/// values into its blob store, then the tree's nodes into `index`.
///
/// The spilled values land first because nodes reference them: a node
/// must never be durable before the values its entries name, the same
/// order push keeps on a remote. Each spilled value is imported under the
/// hash its key carries and verified against it.
///
/// The nodes a seal stages are already in `cache`, the tree's view of the
/// node cache its environment holds, so the next edit opens them without
/// reading them back. A flush that fails leaves the archive without them,
/// so they are forgotten there: the cache outlives the handle, and a
/// scope must hold only what its archive has, or a later read (a pull
/// merging a tree that names the same bytes, say) would be answered with
/// a block the store never received and never fetch it.
pub(crate) async fn persist<Env>(
    index: &CatalogScope,
    cache: &ArtifactNodeCache,
    delta: &mut ArchiveDelta,
    env: &Env,
) -> Result<(), DialogArtifactsError>
where
    Env: Provider<Import> + Provider<BlobImport> + ConditionalSync + 'static,
{
    let staged = delta.blocks().keys();
    let flushed = flush(index, delta, env).await;
    if flushed.is_err() {
        for hash in &staged {
            cache.forget(hash);
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
    Env: Provider<Import> + Provider<BlobImport> + ConditionalSync + 'static,
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
