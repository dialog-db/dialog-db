//! Entity-addressed blob API.
//!
//! A blob is a whole, hash-addressable binary object that rides the artifact
//! tree via the blob index. It is referenced by its content-derived entity
//! `blob:<hash>` (see [`Entity::from_blob`]), so a blob is a first-class
//! resource other facts can point at — attach a name, a media type, an author
//! as ordinary assertions, then find blobs with a normal datalog query rather
//! than a full-index scan.
//!
//! The surface is [`Blob`] (the noun) plus a [`BlobArchive`] target that a
//! [`Branch`] converts into:
//!
//! ```ignore
//! // read (whole, or a slice); local-first with remote hydration + cache
//! let bytes = Blob::from(entity).read(branch.into()).perform(env).await?;
//! let head  = Blob::from(entity).slice(range).read(branch.into()).perform(env).await?;
//! let size  = Blob::from(entity).size(branch.into()).perform(env).await?;   // index-only
//!
//! // write: stream chunks in, get the blob's entity back (recorded in the
//! // index so `push` replicates it)
//! let entity = Blob::import(chunks).write(branch.into()).perform(env).await?;
//! ```

use crate::RevisionExt as _;
use crate::{
    Branch, CommitError, EMPTY_TREE_HASH, Index, NetworkedIndex, RemoteSite,
    RepositoryArchiveExt as _, RepositoryMemoryExt as _, Revision, TreeReference, Upstream,
};
use dialog_artifacts::history::{Context, TreeHistory, context_of, extend_skips};
use dialog_artifacts::tree::ArtifactTreeExt as _;
use dialog_artifacts::{BlobIndexExt as _, BlobRecord, DialogArtifactsError, Entity};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync};
use dialog_effects::archive::prelude::{ArchiveSubjectExt as _, CatalogExt as _};
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify, OperatorExt as _};
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, BlobExt as _};
use dialog_effects::blob::{
    BlobError, BlobReader, ByteRange, Import as BlobImport, Read as BlobRead, Write as BlobWrite,
};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::Delta;
use futures_util::{Stream, StreamExt};

/// A branch's blob store: the target that blob reads and writes bind to.
///
/// Holds a reference to the [`Branch`], so it carries the subject (for the
/// capability chain), the blob index (for size lookups), and the upstream (for
/// remote hydration). Obtain one with [`Branch::blobs`] or `branch.into()`.
#[derive(Clone, Copy)]
pub struct BlobArchive<'a> {
    branch: &'a Branch,
}

impl<'a> From<&'a Branch> for BlobArchive<'a> {
    fn from(branch: &'a Branch) -> Self {
        Self { branch }
    }
}

impl Branch {
    /// This branch's blob store, the target for [`Blob`] reads and writes.
    pub fn blobs(&self) -> BlobArchive<'_> {
        BlobArchive { branch: self }
    }
}

/// A blob, referenced by entity for reading or ingested from a stream for
/// writing.
///
/// `Blob::from(entity)` builds a read (optionally narrowed with
/// [`slice`](Blob::slice)); `Blob::import(chunks)` builds a write. Neither does
/// any work until bound to a [`BlobArchive`] and `perform`ed.
pub struct Blob {
    entity: Entity,
    range: Option<ByteRange>,
}

impl Blob {
    /// Reference an existing blob by its entity, for reading.
    pub fn from(entity: impl Into<Entity>) -> Self {
        Self {
            entity: entity.into(),
            range: None,
        }
    }

    /// Ingest a blob from a stream of byte chunks. The content hash is
    /// discovered as the bytes are written.
    pub fn import<S>(chunks: S) -> BlobImportBuilder<S> {
        BlobImportBuilder { chunks }
    }

    /// Narrow a read to a byte range (`length` bytes from `offset`, or to the
    /// end when `length` is `None`). Mirrors `Blob.slice`.
    pub fn slice(mut self, range: ByteRange) -> Self {
        self.range = Some(range);
        self
    }

    /// Read the blob's bytes from `archive` (local-first, hydrating from the
    /// remote upstream on a local miss).
    pub fn read<'a>(self, archive: BlobArchive<'a>) -> ReadBlob<'a> {
        ReadBlob {
            archive,
            entity: self.entity,
            range: self.range,
        }
    }

    /// Look up the blob's size from `archive`'s index, without fetching bytes.
    pub fn size<'a>(self, archive: BlobArchive<'a>) -> BlobSize<'a> {
        BlobSize {
            archive,
            entity: self.entity,
        }
    }
}

/// A write builder from [`Blob::import`]; bind it to a target with
/// [`write`](BlobImportBuilder::write).
pub struct BlobImportBuilder<S> {
    chunks: S,
}

impl<S> BlobImportBuilder<S> {
    /// Bind the ingest to a target blob store.
    pub fn write<'a>(self, archive: BlobArchive<'a>) -> WriteBlob<'a, S> {
        WriteBlob {
            archive,
            chunks: self.chunks,
        }
    }
}

/// The `blob:<hash>` hash carried by `entity`, or a `NotFound` error naming it.
fn blob_hash(entity: &Entity) -> Result<Blake3Hash, BlobError> {
    entity
        .blob_hash()
        .map(Blake3Hash::from)
        .ok_or_else(|| BlobError::NotFound(format!("not a blob entity: {entity}")))
}

/// Build the tree store for blob-index reads.
///
/// A branch tracking a remote upstream may need remote-only tree nodes to read
/// its blob index — after a fast-forward pull only the revision pointer is
/// local, and the index nodes hydrate lazily. Fall back to the remote archive
/// on a local miss (caching what lands), as `commit` does. With no remote
/// upstream this degrades to a plain local index.
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

/// The size recorded for `hash` in the branch's blob index, or `None` if the
/// current tree does not reference it.
async fn index_size<Env>(
    branch: &Branch,
    hash: &Blake3Hash,
    env: &Env,
) -> Result<Option<u64>, CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + ConditionalSync
        + 'static,
{
    let Some(revision) = branch.revision() else {
        return Ok(None);
    };
    let store = index_store(branch, env).await;
    let tree = Index::from_hash(NodeHash::from(*revision.tree.hash()));
    Ok(tree
        .get_blob(&store, hash.as_bytes())
        .await?
        .map(|r| r.size))
}

/// Look up a blob's size from the blob index. Created by [`Blob::size`].
pub struct BlobSize<'a> {
    archive: BlobArchive<'a>,
    entity: Entity,
}

impl BlobSize<'_> {
    /// Execute the lookup, returning the size or `None` if unreferenced.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<u64>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + ConditionalSync
            + 'static,
    {
        let hash = blob_hash(&self.entity)?;
        index_size(self.archive.branch, &hash, env).await
    }
}

/// Read a blob's bytes (optionally a range), hydrating from the remote upstream
/// on a local miss. Created by [`Blob::read`].
pub struct ReadBlob<'a> {
    archive: BlobArchive<'a>,
    entity: Entity,
    range: Option<ByteRange>,
}

impl ReadBlob<'_> {
    /// Execute the read, returning a streaming [`BlobReader`].
    ///
    /// Local first; on `BlobError::NotFound` for a branch with a remote
    /// upstream, the full blob is fetched from the remote and written through a
    /// local digest-verified [`Import`](dialog_effects::blob::Import) sink (so a
    /// lying remote surfaces as `DigestMismatch` at `finish`), then the
    /// requested (possibly ranged) read is served from the now-local copy.
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
        let branch = self.archive.branch;
        let hash = blob_hash(&self.entity)?;
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
        let Some(size) = index_size(branch, &hash, env).await? else {
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

        // Full-blob read from the remote, forked to its site.
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

/// Ingest a blob and record it in the blob index as one new revision. Created
/// by [`Blob::import`] then [`write`](BlobImportBuilder::write).
pub struct WriteBlob<'a, S> {
    archive: BlobArchive<'a>,
    chunks: S,
}

impl<S> WriteBlob<'_, S>
where
    S: Stream<Item = Result<Vec<u8>, BlobError>> + ConditionalSend + Unpin,
{
    /// Execute the write, returning the blob's entity (`blob:<hash>`).
    ///
    /// Streams the source into the local blob store (hashing and counting bytes
    /// as it goes), records the resulting `{size}` in the blob index, then
    /// publishes a new revision CAS'd against the head this write was built on —
    /// so the bytes are durable before any revision references them, and a
    /// concurrent write that advanced the head makes this publish fail loudly
    /// rather than clobber it.
    pub async fn perform<Env>(mut self, env: &Env) -> Result<Entity, CommitError>
    where
        Env: Provider<BlobWrite>
            + Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.archive.branch;

        // 1. Stream the bytes into the local blob store. The hash is discovered
        //    as the bytes are written; the size is counted alongside. The bytes
        //    are durable once `finish` returns, before any revision points at
        //    the record below.
        let mut sink = branch.archive().blob().write().perform(env).await?;
        let mut size: u64 = 0;
        while let Some(chunk) = self.chunks.next().await {
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

        // A blob write advances the branch like any commit, so its revision
        // gets the full version-control treatment: a DAG edge and skip table
        // in a signed revision record, and an issuer-signed head — otherwise
        // the published head would fail pull-side verification and leave a
        // hole in the ancestry walk.
        let authority = Identify.perform(env).await?;
        let issuer = authority.did();
        let profile = authority.profile().clone();

        let parent = base_revision.as_ref().map(Revision::version);
        let skips = match &parent {
            Some(parent) => {
                let history = TreeHistory::from_root_with_cache(
                    &base_tree_hash,
                    store.clone(),
                    branch.node_cache(),
                )
                .with_record_cache(branch.records());
                extend_skips(&history, parent).await?
            }
            None => Vec::new(),
        };
        let base_context = base_revision.as_ref().and_then(|base| base.context.clone());
        let mut revision = match base_revision {
            Some(base) => base.advance(
                TreeReference::default(),
                branch.of().clone(),
                branch.name(),
                issuer,
                profile,
            ),
            None => Revision::new(
                TreeReference::default(),
                branch.of().clone(),
                branch.name(),
                issuer,
                profile,
            ),
        };
        let mut record = revision.record(parent.into_iter().collect(), skips);
        record.signature = Attest::new(record.payload()?).perform(env).await?;
        // The record's key carries its value through the tree's own
        // inline-vs-spill threshold, so read it off the tree rather than
        // assuming the default.
        let manifest = tree.format_manifest(store.clone(), &delta).await?;
        tree.record(&mut store, &mut delta, record.entries(&manifest)?)
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

        // The new head's causal context: the parent's plus this write's
        // own version, exactly as `Commit` derives it — a blob write
        // advances the head like any commit and publishes its watermark
        // the same way.
        let contexts = branch.contexts();
        let minted = revision.version();
        let context = {
            let mut context = match (&parent, base_context) {
                (None, _) => Context::new(),
                (Some(_), Some(context)) => context,
                (Some(parent), None) => match contexts.cached(parent).await {
                    Some(context) => context,
                    None => {
                        let history = TreeHistory::from_root_with_cache(
                            &base_tree_hash,
                            store.clone(),
                            branch.node_cache(),
                        )
                        .with_record_cache(branch.records());
                        context_of(parent, &history).await?
                    }
                },
            };
            context.record(minted);
            context
        };

        revision.tree = TreeReference::from(*tree.root().as_bytes());
        revision.context = Some(context.clone());
        revision.signature = Attest::new(revision.payload()).perform(env).await?;

        head.publish(revision, env).await?;

        // Advance the branch memo so later pulls through this handle
        // answer the context from memory.
        contexts.insert(minted, context);

        Ok(Entity::from_blob(&index_hash)?)
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::Blob;
    use crate::RepositoryExt as _;
    use crate::helpers::unique_name;
    use anyhow::Result;
    use dialog_capability::Subject;
    use dialog_effects::blob::{BlobError, BlobReader, ByteRange};
    use dialog_network::Network;
    use dialog_operator::Profile;
    use dialog_storage::provider::storage::Storage;
    use futures_util::stream;

    async fn drain(mut reader: BlobReader) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            out.extend(chunk);
        }
        out
    }

    // The volatile (in-memory) space now has a blob provider, so this runs on
    // both native and wasm — no filesystem, no target gate.
    #[dialog_common::test]
    async fn it_writes_a_blob_and_reads_it_back_by_entity() -> Result<()> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("blob")).perform(&storage).await?;
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

        let payload: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
        let chunks: Vec<Result<Vec<u8>, BlobError>> =
            payload.chunks(8192).map(|c| Ok(c.to_vec())).collect();

        // import -> Entity
        let entity = Blob::import(stream::iter(chunks))
            .write((&branch).into())
            .perform(&operator)
            .await?;
        assert!(entity.as_str().starts_with("blob:"));

        // size from the index, no fetch
        assert_eq!(
            Blob::from(entity.clone())
                .size((&branch).into())
                .perform(&operator)
                .await?,
            Some(payload.len() as u64)
        );

        // whole read
        let reader = Blob::from(entity.clone())
            .read((&branch).into())
            .perform(&operator)
            .await?;
        assert_eq!(drain(reader).await, payload);

        // ranged read (slice): 9 bytes from offset 10
        let reader = Blob::from(entity)
            .slice(ByteRange {
                offset: 10,
                length: Some(9),
            })
            .read((&branch).into())
            .perform(&operator)
            .await?;
        assert_eq!(drain(reader).await, payload[10..19]);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_a_non_blob_entity() -> Result<()> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("blob-reject"))
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

        let entity: dialog_artifacts::Entity = "user:alice".parse()?;
        let result = Blob::from(entity)
            .read((&branch).into())
            .perform(&operator)
            .await;
        assert!(matches!(
            result,
            Err(crate::CommitError::Blob(BlobError::NotFound(_)))
        ));

        Ok(())
    }
}
