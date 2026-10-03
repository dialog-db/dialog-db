//! Entity-addressed blob API.
//!
//! A blob is a whole, hash-addressable binary object the line records as an
//! asset (see [`asset`](super::asset)): its bytes live in the blob store and
//! its `asset:<hash> dialog.asset/size <size>` fact rides the tree. It is
//! referenced by that content-derived entity (see [`Entity::from_blob`]), so
//! a blob is a first-class resource other facts can point at: attach a name,
//! a media type, an author as ordinary assertions, then find blobs with a
//! normal datalog query.
//!
//! Trees written before assets recorded their blobs in the blob index
//! instead. Those entries are still read wherever content is sized,
//! hydrated or shipped, and nothing writes new ones.
//!
//! The surface is [`Blob`] (the noun) plus a [`BlobArchive`] target that a
//! [`Branch`] converts into:
//!
//! ```no_run
//! # use dialog_capability::{Fork, Provider};
//! # use dialog_effects::archive::{Get, Import, Put};
//! # use dialog_effects::authority::{Attest, Identify};
//! # use dialog_effects::blob::{//! #     BlobError, ByteRange, Import as BlobImport, Read as BlobRead, Write as BlobWrite, //! #};
//! # use dialog_effects::memory::{Publish, Resolve};
//! # use dialog_repository::{Blob, Branch, CommitError, RemoteSite};
//! # async fn example<Env>(
//! #     branch: &Branch,
//! #     env: &Env,
//! #     entity: dialog_artifacts::Entity,
//! #     range: ByteRange,
//! #     chunks: futures_util::stream::Iter<std::vec::IntoIter<Result<Vec<u8>, BlobError>>>,
//! # ) -> Result<(), CommitError>
//! # where
//! #     Env: Provider<Get>
//! #         + Provider<Put>
//! #         + Provider<Import>
//! #         + Provider<Resolve>
//! #         + Provider<Publish>
//! #         + Provider<Identify>
//! #         + Provider<Attest>
//! #         + Provider<BlobRead>
//! #         + Provider<BlobWrite>
//! #         + Provider<BlobImport>
//! #         + Provider<crate::Hydrate>
//! #         + Provider<Fork<RemoteSite, Resolve>>
//! #         + Provider<Fork<RemoteSite, BlobRead>>
//! #         + dialog_common::ConditionalSync
//! #         + 'static,
//! # {
//! // read (whole, or a slice); local-first with remote hydration + cache
//! let bytes = Blob::from(entity.clone()).read(branch.into()).perform(env).await?;
//! let head = Blob::from(entity.clone())
//!     .slice(range)
//!     .read(branch.into())
//!     .perform(env)
//!     .await?;
//! let size = Blob::from(entity).size(branch.into()).perform(env).await?; // no fetch
//!
//! // write: stream chunks in, get the blob's entity back (recorded as an
//! // asset so `push` replicates it)
//! let entity = Blob::import(chunks).write(branch.into()).perform(env).await?;
//!
//! // retract: drop the line's reference (the removal replicates like any
//! // commit); the bytes stay in the blob store
//! Blob::from(entity).retract(branch.into()).perform(env).await?;
//! # Ok(())
//! # }
//! ```

use crate::repository::branch::asset::recorded_size;
use crate::repository::remote::Step;
use crate::repository::source::SourceRef;
use crate::{
    Branch, CommitError, Hydrate, Index, NetworkedIndex, RemoteFallback, RemoteSite, Snapshot,
};
use dialog_artifacts::{Asset, BlobIndexExt as _, BlobRecord, Entity, Instruction};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync};
use dialog_effects::MethodExt as _;
use dialog_effects::archive::prelude::ArchiveExt as _;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, ReadBlobExt as _};
use dialog_effects::blob::{
    BlobError, BlobReader, ByteRange, Import as BlobImport, Read as BlobRead, Write as BlobWrite,
};
use dialog_effects::memory::{Publish, Resolve};
use futures_util::{Stream, stream};

/// A line's blob store: the target that blob reads and writes bind to.
///
/// Holds a reference to the [`Branch`] or [`Snapshot`], so it carries the
/// subject (for the capability chain), the tree (for size lookups), and,
/// for a branch, the upstream (for remote hydration). Obtain one
/// with [`Branch::blobs`], [`Snapshot::blobs`], or `branch.into()`.
///
/// Reads bind to either kind. Writes and retractions advance the line,
/// which a reference to a snapshot cannot do (its revision is held by
/// value): they fail with [`CommitError::Detached`], and the snapshot is
/// advanced by consuming it instead.
#[derive(Clone, Copy)]
pub struct BlobArchive<'a> {
    source: SourceRef<'a>,
}

impl<'a> From<&'a Branch> for BlobArchive<'a> {
    fn from(branch: &'a Branch) -> Self {
        Self {
            source: SourceRef::from(branch),
        }
    }
}

impl<'a> From<&'a Snapshot> for BlobArchive<'a> {
    fn from(snapshot: &'a Snapshot) -> Self {
        Self {
            source: SourceRef::from(snapshot),
        }
    }
}

impl<'a> BlobArchive<'a> {
    /// The branch behind this archive, for the operations that advance
    /// it; a snapshot's archive refuses them.
    fn branch(&self) -> Result<&'a Branch, CommitError> {
        match self.source {
            SourceRef::Branch(branch) | SourceRef::Pinned(branch, _) => Ok(branch),
            SourceRef::Snapshot(_) => Err(CommitError::Detached),
        }
    }
}

impl Branch {
    /// This branch's blob store, the target for [`Blob`] reads and writes.
    pub fn blobs(&self) -> BlobArchive<'_> {
        BlobArchive::from(self)
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

    /// Look up the blob's size from `archive`'s tree, without fetching bytes.
    pub fn size<'a>(self, archive: BlobArchive<'a>) -> BlobSize<'a> {
        BlobSize {
            archive,
            entity: self.entity,
        }
    }

    /// Drop `archive`'s reference to the blob, without touching its bytes.
    pub fn retract<'a>(self, archive: BlobArchive<'a>) -> RetractBlob<'a> {
        RetractBlob {
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

/// The hash an `asset:<hash>` entity names, or a `NotFound` error naming it.
fn blob_hash(entity: &Entity) -> Result<Blake3Hash, BlobError> {
    entity
        .blob_hash()
        .map(Blake3Hash::from)
        .ok_or_else(|| BlobError::NotFound(format!("not a blob entity: {entity}")))
}

/// Build the tree store for reading the content a line vouches for.
///
/// A branch tracking a remote upstream may need remote-only tree nodes to read
/// it — after a fast-forward pull only the revision pointer is
/// local, and the index nodes hydrate lazily. Fall back to the remote archive
/// on a local miss (caching what lands), as `commit` does. With no remote
/// upstream (a snapshot never has one) this degrades to a plain local index.
pub(crate) async fn index_store<'s, 'e, Env>(
    source: impl Into<SourceRef<'s>>,
    env: &'e Env,
) -> NetworkedIndex<'e, Env>
where
    Env: Provider<Resolve> + ConditionalSync + 'static,
{
    let source = source.into();
    let remote = source.fallback();
    NetworkedIndex::new(env, source.archive().index(), remote)
}

/// The size of the content the line's current tree vouches for under `hash`,
/// by an asset's `dialog.asset/size` fact or a legacy blob-index entry, or
/// `None` when it vouches for no such content.
async fn index_size<Env>(
    source: SourceRef<'_>,
    hash: &Blake3Hash,
    env: &Env,
) -> Result<Option<u64>, CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<crate::Hydrate>
        + ConditionalSync
        + 'static,
{
    let Some(revision) = source.revision() else {
        return Ok(None);
    };
    let store = index_store(source, env).await;
    let tree = Index::from_hash(NodeHash::from(*revision.tree.hash()));
    Ok(tree.content_size(&store, hash.as_bytes()).await?)
}

/// Whether the line's blob index itself still references `hash`: content a
/// tree recorded before assets, which a retraction tombstones.
async fn index_references<Env>(
    source: SourceRef<'_>,
    hash: &Blake3Hash,
    env: &Env,
) -> Result<bool, CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Hydrate>
        + ConditionalSync
        + 'static,
{
    let Some(revision) = source.revision() else {
        return Ok(false);
    };
    let store = index_store(source, env).await;
    let tree = Index::from_hash(NodeHash::from(*revision.tree.hash()));
    Ok(tree.get_blob(&store, hash.as_bytes()).await?.is_some())
}

/// Look up a blob's size from the line's tree, without fetching its bytes.
/// Created by [`Blob::size`].
pub struct BlobSize<'a> {
    archive: BlobArchive<'a>,
    entity: Entity,
}

impl BlobSize<'_> {
    /// Execute the lookup, returning the size or `None` if unreferenced.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<u64>, CommitError>
    where
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<crate::Hydrate>
            + ConditionalSync
            + 'static,
    {
        let hash = blob_hash(&self.entity)?;
        index_size(self.archive.source, &hash, env).await
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
            + Provider<crate::Hydrate>
            + Provider<Fork<RemoteSite, BlobRead>>
            + ConditionalSync
            + 'static,
    {
        let line = self.archive.source;
        let hash = blob_hash(&self.entity)?;
        let range = self.range;

        let local = line
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

        // Local miss. Hydrate from the upstream peer, if any (a snapshot
        // has none: its reads are local).
        let remote = match line.fallback() {
            RemoteFallback::Remote(remote) => remote,
            RemoteFallback::None => return Err(BlobError::NotFound(miss_key).into()),
            RemoteFallback::Unavailable { remote, reason } => {
                return Err(CommitError::Blob(BlobError::Storage(format!(
                    "upstream {remote} is unreachable: {reason}"
                ))));
            }
        };

        // The tree must already vouch for the blob for us to import it; without
        // a size we have no import to issue and the miss is genuine.
        let Some(size) = index_size(line, &hash, env).await? else {
            return Err(BlobError::NotFound(miss_key).into());
        };

        // Full-blob read from the remote, forked to its site, written
        // through a local digest-verified import sink. An attempt is the
        // whole transfer, since the read can fail at any point.
        let hash = &hash;
        // Only the peer's side fails over: the local import would fail the
        // same at every address.
        remote
            .reach(|address| async move {
                let mut source = address
                    .subject
                    .clone()
                    .reader()
                    .archive()
                    .blob()
                    .read(hash.clone())
                    .fork(address.site())
                    .perform(env)
                    .await
                    .map_err(Step::Remote)?;
                let mut sink = line
                    .archive()
                    .blob()
                    .import(hash.clone(), size)
                    .perform(env)
                    .await
                    .map_err(Step::Local)?;
                while let Some(chunk) = source.next().await.map_err(Step::Remote)? {
                    sink.write_all(&chunk).await.map_err(Step::Local)?;
                }
                sink.finish().await.map_err(Step::Local)?;
                Ok::<_, Step<BlobError>>(())
            })
            .await
            .map_err(Step::into_inner)?;

        // Serve the requested read from the now-local copy.
        line.archive()
            .blob()
            .invoke(BlobRead {
                digest: hash.clone(),
                range,
            })
            .perform(env)
            .await
            .map_err(Into::into)
    }
}

/// Ingest a blob and record it as an asset in one new revision. Created by
/// [`Blob::import`] then [`write`](BlobImportBuilder::write).
pub struct WriteBlob<'a, S> {
    archive: BlobArchive<'a>,
    chunks: S,
}

impl<S> WriteBlob<'_, S>
where
    S: Stream<Item = Result<Vec<u8>, BlobError>> + ConditionalSend + Unpin,
{
    /// Execute the write, returning the blob's entity (`asset:<hash>`).
    ///
    /// Streams the source into the local blob store (hashing and counting bytes
    /// as it goes), then commits the asset's `dialog.asset/size` fact, so the
    /// bytes are durable before any revision references them. Recording an
    /// asset the line already records mints nothing.
    pub async fn perform<Env>(self, env: &Env) -> Result<Entity, CommitError>
    where
        Env: Provider<BlobImport>
            + Provider<BlobRead>
            + Provider<BlobWrite>
            + Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<crate::Hydrate>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.archive.branch()?;
        let asset = branch.asset(self.chunks).import().perform(env).await?;
        let entity = asset.entity()?;
        // A replace, as a transaction's import records it: an asset has one
        // size, and replacing a fact with the value it holds is a no-op.
        Box::pin(
            branch
                .commit(stream::iter(vec![Instruction::Replace(asset.fact()?)]))
                .machinery()
                .perform(env),
        )
        .await?;
        Ok(entity)
    }
}

/// Drop a line's reference to a blob as one new revision. Created by
/// [`Blob::retract`].
///
/// The asset fact the line records for the hash is retracted, as a
/// transaction's `retract(asset)` would, and so is the blob-index entry a
/// tree written before assets holds for it, by a tombstone.
///
/// The bytes stay in the blob store, so a replica that already holds them
/// can still read them locally. Reclaiming bytes nothing references is a
/// separate, local concern. The removal travels with the tree like any
/// commit, so replicas that pull it stop referencing the blob and can no
/// longer hydrate it from a remote.
pub struct RetractBlob<'a> {
    archive: BlobArchive<'a>,
    entity: Entity,
}

impl RetractBlob<'_> {
    /// Execute the retraction.
    ///
    /// Idempotent at the branch level: when the line does not reference the
    /// blob (never written, or already retracted), this is a no-op that mints
    /// no revision.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), CommitError>
    where
        Env: Provider<BlobImport>
            + Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<crate::Hydrate>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.archive.branch()?;
        let hash = blob_hash(&self.entity)?;
        let source = SourceRef::from(branch);
        let mut retractions = Vec::new();
        if let Some(size) = recorded_size(source, hash.as_bytes(), env).await? {
            retractions.push(Instruction::Retract(
                Asset::stored(*hash.as_bytes(), size).fact()?,
            ));
        }
        let mut entries = Vec::new();
        if index_references(source, &hash, env).await? {
            entries.push(BlobRecord::retract_entry(hash.as_bytes()));
        }
        if retractions.is_empty() && entries.is_empty() {
            return Ok(());
        }
        Box::pin(
            branch
                .commit(stream::iter(retractions))
                .machinery()
                .with_entries(entries)
                .perform(env),
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dialog_effects::storage::Location;

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::Blob;
    use crate::RepositoryExt as _;
    use anyhow::Result;
    use dialog_artifacts::{BlobIndexExt as _, BlobRecord, Entity};
    use dialog_capability::Subject;
    use dialog_effects::blob::{BlobError, BlobReader, ByteRange};
    use dialog_peer::helpers::{open_peer, test_storage, unique_name};

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
        let storage = test_storage().await;
        let profile = open_peer(storage.clone(), Location::profile(unique_name("blob"))).await?;
        let operator = profile
            .session(b"test")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        let repo = profile
            .space(unique_name("repo"))
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
        assert!(entity.as_str().starts_with("asset:"));

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
    async fn it_retracts_a_blob_from_the_index_but_not_the_store() -> Result<()> {
        let storage = test_storage().await;
        let profile = open_peer(
            storage.clone(),
            Location::profile(unique_name("blob-retract")),
        )
        .await?;
        let operator = profile
            .session(b"test")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        let repo = profile
            .space(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let payload: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        let chunks: Vec<Result<Vec<u8>, BlobError>> =
            payload.chunks(4096).map(|c| Ok(c.to_vec())).collect();
        let entity = Blob::import(stream::iter(chunks))
            .write((&branch).into())
            .perform(&operator)
            .await?;

        Blob::from(entity.clone())
            .retract((&branch).into())
            .perform(&operator)
            .await?;

        // The index no longer references the blob...
        assert_eq!(
            Blob::from(entity.clone())
                .size((&branch).into())
                .perform(&operator)
                .await?,
            None
        );

        // ...but the bytes were not touched: a local read still serves them.
        let reader = Blob::from(entity.clone())
            .read((&branch).into())
            .perform(&operator)
            .await?;
        assert_eq!(drain(reader).await, payload);

        // Retracting again is a no-op, not an error.
        Blob::from(entity.clone())
            .retract((&branch).into())
            .perform(&operator)
            .await?;

        // Re-importing the same bytes re-references the blob.
        let chunks: Vec<Result<Vec<u8>, BlobError>> =
            payload.chunks(4096).map(|c| Ok(c.to_vec())).collect();
        let again = Blob::import(stream::iter(chunks))
            .write((&branch).into())
            .perform(&operator)
            .await?;
        assert_eq!(again, entity);
        assert_eq!(
            Blob::from(entity)
                .size((&branch).into())
                .perform(&operator)
                .await?,
            Some(payload.len() as u64)
        );

        Ok(())
    }

    /// An import is recorded by the asset's fact; the retired blob index
    /// gets no entry.
    #[dialog_common::test]
    async fn it_records_an_import_as_an_asset() -> Result<()> {
        let storage = test_storage().await;
        let profile = open_peer(
            storage.clone(),
            Location::profile(unique_name("blob-asset")),
        )
        .await?;
        let operator = profile
            .session(b"test")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        let repo = profile
            .space(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let payload = b"recorded as an asset".to_vec();
        let entity = Blob::import(stream::iter(vec![Ok::<_, BlobError>(payload.clone())]))
            .write((&branch).into())
            .perform(&operator)
            .await?;
        let hash = entity.blob_hash().expect("an asset entity");

        let store = super::index_store(&branch, &operator).await;
        let revision = branch.revision().expect("the import minted a revision");
        let tree = crate::Index::from_hash(super::NodeHash::from(*revision.tree.hash()));
        assert_eq!(
            tree.asset_size(&store, &hash).await?,
            Some(payload.len() as u64)
        );
        assert_eq!(tree.get_blob(&store, &hash).await?, None);
        Ok(())
    }

    /// A blob a tree written before assets recorded only in the blob index
    /// is still sized and read through it, and retracting it tombstones the
    /// entry.
    #[dialog_common::test]
    async fn it_reads_and_retracts_a_blob_only_the_legacy_index_records() -> Result<()> {
        let storage = test_storage().await;
        let profile = open_peer(
            storage.clone(),
            Location::profile(unique_name("blob-legacy")),
        )
        .await?;
        let operator = profile
            .session(b"test")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        let repo = profile
            .space(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let payload = b"recorded in the index".to_vec();
        let mut sink = branch.archive().blob().write().perform(&operator).await?;
        sink.write_all(&payload).await?;
        let hash = *sink.finish().await?.as_bytes();
        branch
            .commit(stream::iter(Vec::new()))
            .machinery()
            .with_entries(vec![
                BlobRecord::new(payload.len() as u64).legacy_entry(&hash),
            ])
            .perform(&operator)
            .await?;
        let entity = Entity::from_blob(&hash)?;

        assert_eq!(
            Blob::from(entity.clone())
                .size((&branch).into())
                .perform(&operator)
                .await?,
            Some(payload.len() as u64)
        );
        let reader = Blob::from(entity.clone())
            .read((&branch).into())
            .perform(&operator)
            .await?;
        assert_eq!(drain(reader).await, payload);

        Blob::from(entity.clone())
            .retract((&branch).into())
            .perform(&operator)
            .await?;
        assert_eq!(
            Blob::from(entity)
                .size((&branch).into())
                .perform(&operator)
                .await?,
            None
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_a_non_blob_entity() -> Result<()> {
        let storage = test_storage().await;
        let profile = open_peer(
            storage.clone(),
            Location::profile(unique_name("blob-reject")),
        )
        .await?;
        let operator = profile
            .session(b"test")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        let repo = profile
            .space(unique_name("repo"))
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
