//! Writing what a batch staged into the archive, each lane into its store.

use dialog_artifacts::{ArchiveDelta, DialogArtifactsError};
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer, ConditionalSync};
use dialog_effects::archive::Import;
use dialog_effects::archive::prelude::CatalogScope;
use dialog_effects::blob::Import as BlobImport;
use dialog_keyring::KeyringError;
use futures_util::{StreamExt as _, TryStreamExt as _, stream};

use super::networked::write_blob;
use crate::SealedTree;
use crate::sealing::{TreeSpace, seal_line, sealed_tree};

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

/// Write everything `delta` staged for the tree rooted at `root` into
/// `index`'s archive, as [`persist`] does on a plain line (`sealing` is
/// `None`), and return where the sealed tree starts on a sealed one.
///
/// On a sealed line nothing staged is written as it is. Every node
/// reachable from `root`, and every spilled value those nodes name, is
/// sealed first; the sealed values land in the blob store, then the
/// envelopes in `index`, in the same order and for the same reason as
/// [`persist`]. A staged block the tree does not reach is dropped: it was
/// never going to be read. Sealing refuses before anything is written, so a
/// refused persist leaves the archive and `delta` as they were.
///
/// # Errors
///
/// A [`KeyringError`] when sealing refuses (no writer, a node or value
/// neither staged nor reached), and the archive's error when a write fails.
pub(crate) async fn persist_line<Env, E>(
    index: &CatalogScope,
    delta: &mut ArchiveDelta,
    sealing: Option<&TreeSpace>,
    root: &Blake3Hash,
    env: &Env,
) -> Result<Option<SealedTree>, E>
where
    Env: Provider<Import> + Provider<BlobImport> + ConditionalSync + 'static,
    E: From<DialogArtifactsError> + From<KeyringError>,
{
    let Some(space) = sealing else {
        persist(index, delta, env).await?;
        return Ok(None);
    };
    let sealing = seal_line(space, delta.staged_blocks(), delta.staged_blobs(), root)?;
    // Owned, as `persist` streams its blobs: a future borrowing from the
    // closure's argument would leave the whole commit future's `Send`
    // unprovable.
    let values: Vec<Buffer> = sealing
        .values()
        .map(|value| Buffer::from(value.to_vec()))
        .collect();
    stream::iter(values)
        .map(|value| async move {
            write_blob(env, index, value.blake3_hash(), value.as_ref())
                .await
                .map_err(DialogArtifactsError::from)
        })
        .buffer_unordered(BLOB_WRITES)
        .try_collect::<()>()
        .await?;
    index
        .import(
            sealing
                .envelopes()
                .map(|bytes| Buffer::from(bytes.to_vec())),
        )
        .perform(env)
        .await
        .map_err(DialogArtifactsError::from)?;
    delta.flush_blocks().for_each(drop);
    delta.flush_blobs().for_each(drop);
    Ok(Some(sealed_tree(&space.settle(sealing))))
}
