//! Writing what a batch staged into the archive, each lane into its store.

use dialog_artifacts::tree::CorpusNode;
use dialog_artifacts::{ArchiveDelta, DialogArtifactsError};
use dialog_capability::Provider;
use dialog_common::{Buffer, ConditionalSync};
use dialog_effects::archive::prelude::CatalogScope;
use dialog_effects::blob::Import as BlobImport;
use dialog_search_tree::nodes;
use futures_util::{StreamExt as _, TryStreamExt as _, stream};

use super::hydrate::write_blob;

/// How many spilled values a persist writes at once.
const BLOB_WRITES: usize = 16;

/// Write everything `delta` staged into `catalog`'s archive: the spilled
/// values into its blob store, then the tree's nodes through the node
/// layer's write ([`nodes::Write`](dialog_search_tree::nodes::Write)).
///
/// The spilled values land first because nodes reference them: a node
/// must never be durable before the values its entries name, the same
/// order push keeps on a remote. Each spilled value is imported under the
/// hash its key carries and verified against it.
///
/// The nodes are the environment's to store and keep readable: one that
/// holds a node cache keeps them there, so the next edit opens them
/// without reading them back, and a write that fails keeps nothing, so a
/// scope holds only what its archive has.
pub(crate) async fn persist<Env>(
    catalog: &CatalogScope,
    delta: &mut ArchiveDelta,
    env: &Env,
) -> Result<(), DialogArtifactsError>
where
    Env: Provider<nodes::Write<CorpusNode>>
        + Provider<BlobImport>
        + ConditionalSync
        + ?Sized
        + 'static,
{
    let blobs: Vec<Buffer> = delta.flush_blobs().collect();
    stream::iter(blobs)
        .map(|blob| async move {
            write_blob(env, catalog, blob.blake3_hash(), blob.as_ref())
                .await
                .map_err(DialogArtifactsError::from)
        })
        .buffer_unordered(BLOB_WRITES)
        .try_collect::<()>()
        .await?;
    nodes::Write::<CorpusNode>::new(catalog.clone(), delta.flush_nodes())
        .perform(env)
        .await
        .map_err(DialogArtifactsError::from)
}
