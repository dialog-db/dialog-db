use crate::{
    Branch, CommitError, Index, NetworkedIndex, RemoteSite, RepositoryArchiveExt as _,
    RepositoryMemoryExt as _, Upstream,
};
use dialog_artifacts::{BlobIndexExt as _, BlobRecord};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, BlobExt as _};
use dialog_effects::blob::{
    BlobError, BlobReader, ByteRange, Import as BlobImport, Read as BlobRead,
};
use dialog_effects::memory::Resolve;
use futures_util::TryStreamExt as _;

/// Look up a blob's size from the blob index, without fetching its bytes.
///
/// Created by [`Branch::blob_size`]. Execute with `.perform(&env)`.
pub struct BlobSize<'a> {
    branch: &'a Branch,
    hash: Blake3Hash,
}

/// List every blob referenced by the branch's current tree, with its record.
///
/// Created by [`Branch::list_blobs`]. Execute with `.perform(&env)`.
pub struct ListBlobs<'a> {
    branch: &'a Branch,
}

/// Read a blob's bytes, optionally a byte range, hydrating from the remote
/// upstream on a local miss.
///
/// Created by [`Branch::read_blob`]. Execute with `.perform(&env)`.
pub struct ReadBlob<'a> {
    branch: &'a Branch,
    hash: Blake3Hash,
    range: Option<ByteRange>,
}

impl Branch {
    /// Look up a blob's size from the index, or `None` if the branch's current
    /// tree does not reference it.
    ///
    /// Answered from the blob index alone — no blob bytes are fetched. Index
    /// nodes missing locally hydrate from the remote upstream, if any.
    pub fn blob_size(&self, hash: &Blake3Hash) -> BlobSize<'_> {
        BlobSize {
            branch: self,
            hash: hash.clone(),
        }
    }

    /// List every blob referenced by the branch's current tree, paired with its
    /// [`BlobRecord`](dialog_artifacts::BlobRecord).
    ///
    /// Answered from the blob index alone — no blob bytes are fetched. Index
    /// nodes missing locally hydrate from the remote upstream, if any.
    pub fn list_blobs(&self) -> ListBlobs<'_> {
        ListBlobs { branch: self }
    }

    /// Read a blob's bytes by content hash, optionally restricted to `range`.
    ///
    /// Reads the local store first. On a local miss for a branch tracking a
    /// remote upstream, the blob is hydrated from the remote (digest-verified as
    /// it lands locally) and then served from the now-local copy.
    pub fn read_blob(&self, hash: &Blake3Hash, range: Option<ByteRange>) -> ReadBlob<'_> {
        ReadBlob {
            branch: self,
            hash: hash.clone(),
            range,
        }
    }
}

/// Build the tree store for the branch's blob-index reads.
///
/// A branch tracking a remote upstream may need remote-only tree nodes to read
/// its blob index — after a fast-forward pull only the revision pointer is
/// local, and the index nodes hydrate lazily. Fall back to the remote archive
/// on a local miss (caching what lands), exactly as `commit` and `write_blob`
/// do. With no remote upstream this degrades to a plain local index.
async fn index_store<'e, Env>(branch: &Branch, env: &'e Env) -> NetworkedIndex<'e, Env>
where
    Env: Provider<Resolve> + ConditionalSync + 'static,
{
    let remote = match branch.upstream() {
        Some(Upstream::Remote { remote: name, .. }) => {
            branch.subject().remote(name).load().perform(env).await.ok()
        }
        _ => None,
    };
    NetworkedIndex::new(env, branch.archive().index(), remote)
}

impl BlobSize<'_> {
    /// Execute the lookup, returning the blob's size or `None` if unreferenced.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<u64>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + ConditionalSync
            + 'static,
    {
        let Some(revision) = self.branch.revision() else {
            return Ok(None);
        };
        let store = index_store(self.branch, env).await;
        let tree = Index::from_hash(NodeHash::from(*revision.tree.hash()));
        let index_hash: dialog_storage::Blake3Hash = *self.hash.as_bytes();
        Ok(tree.get_blob(&store, &index_hash).await?.map(|r| r.size))
    }
}

impl ListBlobs<'_> {
    /// Execute the listing, returning `(hash, record)` for each referenced blob
    /// in hash order.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<Vec<(dialog_storage::Blake3Hash, BlobRecord)>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + ConditionalSync
            + 'static,
    {
        let Some(revision) = self.branch.revision() else {
            return Ok(Vec::new());
        };
        let store = index_store(self.branch, env).await;
        let tree = Index::from_hash(NodeHash::from(*revision.tree.hash()));
        let listed = tree.list_blobs(store).try_collect().await?;
        Ok(listed)
    }
}

impl ReadBlob<'_> {
    /// Execute the read, returning a streaming [`BlobReader`].
    ///
    /// Local first; on `BlobError::NotFound` for a branch with a remote
    /// upstream, the full blob is fetched from the remote and written through a
    /// local digest-verified [`Import`] sink (so a lying remote surfaces as
    /// `DigestMismatch` at `finish`), then the requested (possibly ranged) read
    /// is served from the now-local copy.
    pub async fn perform<Env>(self, env: &Env) -> Result<BlobReader, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<BlobRead>
            + Provider<BlobImport>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, BlobRead>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let hash = self.hash;
        let range = self.range;

        let local = branch
            .archive()
            .blob()
            .invoke(BlobRead {
                digest: hash.clone(),
                range,
            })
            .perform(env)
            .await;

        let miss_key = match local {
            Ok(reader) => return Ok(reader),
            Err(BlobError::NotFound(key)) => key,
            Err(other) => return Err(other.into()),
        };

        // Local miss. Hydrate from the remote upstream, if any.
        let Some(Upstream::Remote { remote: name, .. }) = branch.upstream() else {
            return Err(BlobError::NotFound(miss_key).into());
        };

        // The index must already reference the blob for us to import it; without
        // a size we have no import to issue and the miss is genuine.
        let Some(size) = branch.blob_size(&hash).perform(env).await? else {
            return Err(BlobError::NotFound(miss_key).into());
        };

        let remote = branch
            .subject()
            .remote(name)
            .load()
            .perform(env)
            .await
            .map_err(|e| CommitError::Blob(BlobError::ExecutionError(e.to_string())))?;
        let address = remote.address();

        // Full-blob read from the remote, forked to its site (mirrors
        // `RemoteGet` in repository/remote/archive.rs and `NetworkedIndex`'s
        // local-miss -> remote-fallback, substituting the blob `Read` effect).
        let mut source = address
            .subject
            .clone()
            .archive()
            .blob()
            .read(hash.clone())
            .fork(address.site())
            .perform(env)
            .await?;

        // Write the bytes through a local digest-verified import sink.
        let mut sink = branch
            .archive()
            .blob()
            .import(hash.clone(), size)
            .perform(env)
            .await?;
        while let Some(chunk) = source.next().await? {
            sink.write_all(&chunk).await?;
        }
        sink.finish().await?;

        // Serve the requested read from the now-local copy.
        branch
            .archive()
            .blob()
            .invoke(BlobRead {
                digest: hash,
                range,
            })
            .perform(env)
            .await
            .map_err(Into::into)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::helpers::unique_name;
    use crate::RepositoryExt as _;
    use anyhow::Result;
    use dialog_capability::Subject;
    use dialog_effects::blob::ByteRange;
    use dialog_network::Network;
    use dialog_operator::Profile;
    use dialog_storage::provider::storage::Storage;
    use futures_util::stream;

    async fn drain(mut reader: dialog_effects::blob::BlobReader) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            out.extend(chunk);
        }
        out
    }

    // Blob effects stream through a real filesystem, so this test builds the
    // operator over a temp-dir native space (`Storage::temp()`) rather than the
    // volatile (memory) space used elsewhere: the volatile space has no blob
    // provider. Mirrors `write_blob`'s test.
    #[dialog_common::test]
    async fn it_reads_back_a_written_blob() -> Result<()> {
        let storage = Storage::temp();
        let profile = Profile::open(unique_name("read-blob"))
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

        let payload = b"the quick brown blob".to_vec();
        let hash = branch
            .write_blob(stream::iter(vec![payload.clone()]))
            .perform(&operator)
            .await?;

        // Whole-blob read.
        let reader = branch.read_blob(&hash, None).perform(&operator).await?;
        assert_eq!(drain(reader).await, payload);

        // Ranged read: "quick".
        let reader = branch
            .read_blob(
                &hash,
                Some(ByteRange {
                    offset: 4,
                    length: Some(5),
                }),
            )
            .perform(&operator)
            .await?;
        assert_eq!(drain(reader).await, b"quick".to_vec());

        // Index-only queries.
        assert_eq!(
            branch.blob_size(&hash).perform(&operator).await?,
            Some(payload.len() as u64)
        );
        let listed = branch.list_blobs().perform(&operator).await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].1.size, payload.len() as u64);

        // Unknown hash is a clean miss, not a panic.
        let missing = dialog_common::Blake3Hash::from([0u8; 32]);
        assert_eq!(branch.blob_size(&missing).perform(&operator).await?, None);
        assert!(
            branch
                .read_blob(&missing, None)
                .perform(&operator)
                .await
                .is_err()
        );
        Ok(())
    }
}
