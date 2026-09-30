//! Writing what a batch staged into the archive, each lane into its store.

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
pub(crate) async fn persist<Env>(
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
