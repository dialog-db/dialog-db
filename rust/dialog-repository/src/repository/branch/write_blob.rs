use crate::{
    Branch, CommitError, EMPTY_TREE_HASH, Index, NetworkedIndex, RemoteSite,
    RepositoryArchiveExt as _, RepositoryMemoryExt, Revision, TreeReference, Upstream,
};
use dialog_artifacts::{BlobIndexExt as _, BlobRecord, DialogArtifactsError};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::prelude::CatalogExt as _;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Identify, OperatorExt as _};
use dialog_effects::blob::Write as BlobWrite;
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, BlobExt as _};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::Delta;
use futures_util::{Stream, StreamExt};

/// Ingest a blob into the local store and record it in the blob index, as one
/// new published revision.
///
/// Created by [`Branch::write_blob`]. Execute with `.perform(&env)`.
pub struct WriteBlob<'a, S> {
    branch: &'a Branch,
    source: S,
}

impl Branch {
    /// Ingest a blob from a stream of byte chunks and record it in the branch's
    /// blob index.
    ///
    /// The blob's bytes land in the local store first; then a
    /// [`BlobRecord`](dialog_artifacts::BlobRecord) carrying its size is
    /// written into the blob index and a new [`Revision`] is published. The
    /// blob's content hash (discovered while writing) is returned.
    pub fn write_blob<S>(&self, source: S) -> WriteBlob<'_, S> {
        WriteBlob {
            branch: self,
            source,
        }
    }
}

impl<S> WriteBlob<'_, S>
where
    S: Stream<Item = Result<Vec<u8>, dialog_effects::blob::BlobError>> + ConditionalSend + Unpin,
{
    /// Execute the write, returning the blob's discovered content hash.
    ///
    /// Streams the source into the local blob store (hashing and counting bytes
    /// as it goes), records the resulting `{size}` in the blob index, then
    /// publishes a new revision CAS'd against the head this write was built on —
    /// so the bytes are durable before any revision references them, and a
    /// concurrent write that advanced the head makes this publish fail loudly
    /// rather than clobber it.
    pub async fn perform<Env>(mut self, env: &Env) -> Result<dialog_common::Blake3Hash, CommitError>
    where
        Env: Provider<BlobWrite>
            + Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;

        // 1. Stream the bytes into the local blob store. The hash is discovered
        //    as the bytes are written; the size is counted alongside. The bytes
        //    are durable once `finish` returns, before any revision points at
        //    the record below.
        let mut sink = branch.archive().blob().write().perform(env).await?;
        let mut size: u64 = 0;
        while let Some(chunk) = self.source.next().await {
            let chunk = chunk?;
            size += chunk.len() as u64;
            sink.write_all(&chunk).await?;
        }
        let hash = sink.finish().await?;

        // 2. Record the blob in the index. Mirror `commit`: checkpoint the head
        //    so the publish below CAS's against it, walk from the current tree
        //    root (or the empty tree), put the record, flush the new nodes, then
        //    publish the advanced revision.
        let head = branch.revision.checkpoint();
        let base_revision = branch.revision();

        // A branch tracking a remote upstream may need to read remote-only
        // nodes to open its tree; fall back to the remote when a node is missing
        // locally, as `commit` does.
        let remote = match branch.upstream() {
            Some(Upstream::Remote { remote: name, .. }) => {
                branch.subject().remote(name).load().perform(env).await.ok()
            }
            _ => None,
        };
        let mut store = NetworkedIndex::new(env, branch.archive().index(), remote);

        let base_tree_hash = base_revision
            .as_ref()
            .map(|rev| *rev.tree.hash())
            .unwrap_or(EMPTY_TREE_HASH);
        let mut tree = Index::from_hash(NodeHash::from(base_tree_hash));

        let mut delta = Delta::zero();
        let index_hash: dialog_storage::Blake3Hash = *hash.as_bytes();
        tree.put_blob(&mut store, &mut delta, &index_hash, BlobRecord::new(size))
            .await?;

        // Persist the tree's pending nodes before referencing the root in a
        // revision; a revision must only point at durable blocks.
        branch
            .archive()
            .index()
            .import(delta.flush().map(|(_, buffer)| buffer))
            .perform(env)
            .await
            .map_err(DialogArtifactsError::from)?;

        let tree = TreeReference::from(*tree.root().as_bytes());

        let authority = Identify.perform(env).await?;
        let issuer = authority.did();
        let profile = authority.profile().clone();

        let revision = match base_revision {
            Some(base) => base.advance(tree, issuer, profile),
            None => Revision::new(tree, branch.of().clone(), issuer, profile),
        };

        head.publish(revision, env).await?;

        Ok(hash)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::helpers::unique_name;
    use crate::{Index, LocalIndex, RepositoryArchiveExt as _, RepositoryExt as _};
    use anyhow::Result;
    use dialog_artifacts::BlobIndexExt as _;
    use dialog_capability::Subject;
    use dialog_common::Blake3Hash as NodeHash;
    use dialog_network::Network;
    use dialog_operator::Profile;
    use dialog_storage::provider::storage::Storage;
    use futures_util::stream;

    // Blob effects stream through a real filesystem, so this test builds the
    // operator over a temp-dir native space (`Storage::temp()`) rather than the
    // volatile (memory) space used elsewhere: the volatile space has no blob
    // provider. Mirrors dialog-operator's `blob_tests`.
    #[dialog_common::test]
    async fn it_writes_a_blob_and_records_it_in_the_index() -> Result<()> {
        let storage = Storage::temp();
        let profile = Profile::open(unique_name("write-blob"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"test")
            .allow(Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;
        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let payload: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let chunks: Vec<Vec<u8>> = payload.chunks(8192).map(|c| c.to_vec()).collect();
        let expected = dialog_common::Blake3Hash::hash(&payload);

        let hash = branch
            .write_blob(stream::iter(chunks.into_iter().map(Ok)))
            .perform(&operator)
            .await?;
        assert_eq!(hash, expected);

        // The revision advanced.
        assert!(branch.revision().is_some());

        // The blob record landed in the index: reload the tree from the new
        // revision and read the record back directly.
        let revision = branch.revision().expect("revision after write_blob");
        let store = LocalIndex::new(&operator, branch.archive().index());
        let tree = Index::from_hash(NodeHash::from(*revision.tree.hash()));
        let index_hash: dialog_storage::Blake3Hash = *hash.as_bytes();
        let record = tree.get_blob(&store, &index_hash).await?;
        assert_eq!(record.map(|r| r.size), Some(payload.len() as u64));

        let size = branch.blob_size(&hash).perform(&operator).await?;
        assert_eq!(size, Some(payload.len() as u64));
        Ok(())
    }
}
