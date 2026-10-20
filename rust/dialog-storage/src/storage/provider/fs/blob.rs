//! Blob capability provider for filesystem.
//!
//! Layout: `{space_root}/blob/{base58(digest)}` — whole, hash-addressable
//! binary objects, parallel to the block archive at `{space_root}/archive/...`.
//! Beside each blob over one group of 16 KiB is its outboard,
//! `{space_root}/blob/{base58(digest)}.outboard`: the hashes of the Bao tree
//! of the blob, which prove any range of it against the digest alone (see
//! [`crate::bao`]).
//!
//! Bytes stream rather than buffer: [`Read`] hands back a [`BlobSource`] over
//! the file (optionally ranged), and [`Write`]/[`Import`] hand back a
//! [`BlobSink`] that stages into a temp file, hashes as it goes, and commits
//! atomically — to the discovered hash for an ingest, or the declared (and
//! verified) digest for an import. The sink builds the outboard in the same
//! pass, into a second temp file, and commits it before the blob, so a blob
//! that the store holds always has its outboard. A sink dropped before its
//! end removes what it staged.
//!
//! A blob that the store holds in part, from [`ImportRange`], is three files
//! beside the blobs: `{base58(digest)}.partial`, the bytes of the groups that
//! it holds at their offsets, with holes between; `.partial.outboard`, the
//! hashes that verified them at their places in the outboard; and `.held`,
//! the record of the groups that it holds (see [`Groups::encode`]). A group
//! counts as held only once the record names it, and the record is written
//! after the bytes. Once the record names each group, the store moves the
//! outboard and the bytes to the places of a whole blob, and removes the
//! record.

use super::{FileReader, FileSystem, FileSystemError, FileSystemHandle, FileWriter};
use crate::bao::{self, Decoder, Encoder, Groups, Item, Need, OutboardBuilder};
use async_trait::async_trait;
use base58::ToBase58;
use dialog_capability::{Capability, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::blob::prelude::{
    BlobHeldExt as _, BlobImportExt as _, BlobImportRangeExt as _, BlobReadExt as _,
    BlobSizeExt as _,
};
use dialog_effects::blob::{
    BlobError, BlobReader, BlobSink, BlobSource, BlobWriter, Encoding, Held, Holding, Import,
    ImportRange, Read, Size, Usage, Write,
};
use futures_util::StreamExt;

const BLOB: &str = "blob";
/// Base name for the staging handle. Only its uniquely-suffixed temp file is
/// ever created; the content commits straight to the hash path.
const STAGING: &str = "_staging";
/// The extension of the outboard of a blob, beside it.
const OUTBOARD: &str = "outboard";
/// The extension of the bytes of a blob that the store holds in part.
const PARTIAL: &str = "partial";
/// The extension of the record of the groups of a partial blob.
const HELD: &str = "held";
/// The bytes of groups that a sink of a range keeps between two writes of the
/// record of a partial blob: 1 MiB. A crash loses at most these, which a
/// later read fetches again.
const RECORD_EVERY: u64 = 1024 * 1024;

impl FileSystem {
    /// The handle for this space's blob directory.
    pub fn blob(&self) -> Result<FileSystemHandle, FileSystemError> {
        self.resolve(BLOB)
    }
}

impl From<FileSystemError> for BlobError {
    fn from(e: FileSystemError) -> Self {
        BlobError::Storage(e.to_string())
    }
}

fn blob_key(digest: &Blake3Hash) -> String {
    digest.as_bytes().to_base58()
}

/// The name of the outboard of the blob `digest`, in the blob directory.
fn outboard_key(digest: &Blake3Hash) -> String {
    format!("{}.{OUTBOARD}", blob_key(digest))
}

/// The names of the bytes, the outboard, and the record of the blob
/// `digest` that the store holds in part, in the blob directory.
fn partial_keys(digest: &Blake3Hash) -> [String; 3] {
    let key = blob_key(digest);
    [
        format!("{key}.{PARTIAL}"),
        format!("{key}.{PARTIAL}.{OUTBOARD}"),
        format!("{key}.{HELD}"),
    ]
}

/// A [`BlobSource`] over a file's streaming reader.
struct FileBlobSource(FileReader);

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSource for FileBlobSource {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        match self.0.next().await {
            Some(Ok(chunk)) => Ok(Some(chunk)),
            Some(Err(e)) => Err(BlobError::Storage(e.to_string())),
            None => Ok(None),
        }
    }
}

/// A [`BlobSink`] that stages bytes into a temp file while hashing, then
/// commits to the content-addressed path on [`finish`](BlobSink::finish). It
/// stages the outboard of the blob beside it, from its first parent on.
struct FileBlobSink {
    writer: FileWriter,
    blob_dir: FileSystemHandle,
    /// The hash and the outboard of the bytes so far.
    builder: OutboardBuilder,
    /// The staged outboard, once the blob has more than one group.
    outboard: Option<FileWriter>,
    /// `Some` for an import (the declared digest, verified at finish); `None`
    /// for an ingest (the hash is discovered).
    expected: Option<Blake3Hash>,
}

impl FileSystem {
    /// Open a staging sink in the blob directory.
    async fn open_blob_sink(
        &self,
        expected: Option<Blake3Hash>,
    ) -> Result<FileBlobSink, BlobError> {
        let blob_dir = self.blob()?;
        blob_dir.ensure_dir().await?;
        let writer = blob_dir.resolve(STAGING)?.writer().await?;
        Ok(FileBlobSink {
            writer,
            blob_dir,
            builder: OutboardBuilder::new(),
            outboard: None,
            expected,
        })
    }
}

impl FileBlobSink {
    /// Appends `pairs`, the next bytes of the outboard, to its staged file.
    async fn stage_outboard(&mut self, pairs: &[u8]) -> Result<(), BlobError> {
        if pairs.is_empty() {
            return Ok(());
        }
        let outboard = match &mut self.outboard {
            Some(outboard) => outboard,
            None => self
                .outboard
                .insert(self.blob_dir.resolve(STAGING)?.writer().await?),
        };
        outboard.write_all(pairs).await?;
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSink for FileBlobSink {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        let pairs = self.builder.update(bytes);
        self.stage_outboard(&pairs).await?;
        self.writer
            .write_all(bytes)
            .await
            .map_err(|e| BlobError::Storage(e.to_string()))
    }

    async fn finish(mut self: Box<Self>) -> Result<Blake3Hash, BlobError> {
        let builder = std::mem::take(&mut self.builder);
        let (hash, pairs) = builder.finish();
        self.stage_outboard(&pairs).await?;
        let FileBlobSink {
            writer,
            blob_dir,
            outboard,
            expected,
            ..
        } = *self;

        if let Some(expected) = expected
            && hash != expected
        {
            let _ = writer.discard().await;
            if let Some(outboard) = outboard {
                let _ = outboard.discard().await;
            }
            return Err(BlobError::DigestMismatch {
                expected: expected.as_bytes().to_base58(),
                actual: hash.as_bytes().to_base58(),
            });
        }

        let dest = blob_dir.resolve(&blob_key(&hash))?;
        // The outboard first: a blob that the store holds has its outboard.
        // A blob that a store held before it kept outboards gets one here.
        if let Some(outboard) = outboard {
            let place = blob_dir.resolve(&outboard_key(&hash))?;
            if place.exists().await {
                let _ = outboard.discard().await;
            } else {
                outboard.finish_to(&place).await?;
            }
        }
        // Content addressing is idempotent: an existing blob has identical
        // bytes, so drop the staged copy rather than rewrite.
        if dest.exists().await {
            let _ = writer.discard().await;
        } else {
            writer.finish_to(&dest).await?;
        }
        Ok(hash)
    }
}

/// A lock of each record of a partial blob in this process, by its path,
/// beside the lock of the file system, which another process may hold.
type Records = std::collections::HashMap<String, std::sync::Arc<futures_util::lock::Mutex<()>>>;
static RECORDS: std::sync::LazyLock<std::sync::Mutex<Records>> =
    std::sync::LazyLock::new(Default::default);

impl FileSystem {
    /// Which bytes of the blob `digest` the store holds.
    async fn holding(&self, digest: &Blake3Hash) -> Result<Option<Holding>, BlobError> {
        let blob_dir = self.blob()?;
        if let Some(size) = blob_dir.resolve(&blob_key(digest))?.size().await? {
            return Ok(Some(Holding {
                size,
                ranges: vec![(0, size)],
            }));
        }
        let [_, _, held] = partial_keys(digest);
        let Some(record) = blob_dir.resolve(&held)?.read_optional().await? else {
            return Ok(None);
        };
        let (size, groups) = Groups::decode(&record)
            .ok_or_else(|| BlobError::Storage(format!("the record {held} is not valid")))?;
        // A sink that opened writes the record of no group yet.
        if groups.is_empty() {
            return Ok(None);
        }
        Ok(Some(Holding {
            size,
            ranges: groups.bytes(size),
        }))
    }
}

/// Runs `then` with the record `record` of a partial blob to itself: in this
/// process, and against another process that holds the store.
async fn locked<T>(
    record: &FileSystemHandle,
    then: impl AsyncFnOnce() -> Result<T, BlobError>,
) -> Result<T, BlobError> {
    let local = {
        let mut records = RECORDS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        records.retain(|_, held| std::sync::Arc::strong_count(held) > 1);
        std::sync::Arc::clone(records.entry(record.path().to_owned()).or_default())
    };
    let _local = local.lock().await;
    let _shared = shared_lock(record).await?;
    then().await
}

/// The lock of the file system of `record`, once no other process holds it.
#[cfg(not(target_arch = "wasm32"))]
async fn shared_lock(record: &FileSystemHandle) -> Result<super::LockGuard, BlobError> {
    let mut waited = std::time::Duration::ZERO;
    loop {
        match super::lock(record).await {
            Ok(guard) => return Ok(guard),
            Err(FileSystemError::Lock(_)) if waited < std::time::Duration::from_secs(10) => {
                let pause = std::time::Duration::from_millis(5);
                tokio::time::sleep(pause).await;
                waited += pause;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// The lock of the file system of `record`: the web locks wait their turn.
#[cfg(target_arch = "wasm32")]
async fn shared_lock(record: &FileSystemHandle) -> Result<super::LockGuard, BlobError> {
    Ok(super::lock(record).await?)
}

/// A [`BlobSink`] of the Bao encoding of a range of a blob: it keeps each
/// group that verifies, and the hashes that verified it, in the partial blob,
/// and records them at most [`RECORD_EVERY`] bytes apart, and at its end.
struct RangeSink {
    blob_dir: FileSystemHandle,
    digest: Blake3Hash,
    size: u64,
    tree: bao_tree::BaoTree,
    decoder: Decoder,
    /// The groups that the sink kept and did not record yet.
    kept: Groups,
    /// The bytes of `kept`.
    unrecorded: u64,
    /// Whether the store held the blob whole at the start: the sink still
    /// verifies each part, and keeps nothing more of it.
    whole: bool,
    /// Whether a part did not verify: the sink takes no more.
    failed: bool,
}

impl FileSystem {
    /// Open a sink of the range `range` of the blob `digest` of `size` bytes.
    async fn open_range_sink(
        &self,
        digest: Blake3Hash,
        size: u64,
        range: dialog_effects::blob::ByteRange,
    ) -> Result<RangeSink, BlobError> {
        let end = bao::group_range(size, range.offset, range.length).map_err(BlobError::Storage)?;
        let blob_dir = self.blob()?;
        blob_dir.ensure_dir().await?;
        let whole = blob_dir.resolve(&blob_key(&digest))?.exists().await;
        // A record of the blob at another size names groups of another
        // tree: the pairs of this import would land at other places of the
        // outboard. The first sink writes its size into the record as it
        // opens, under the lock of the record, so a sink of another size is
        // refused before it writes anything.
        let [_, _, held] = partial_keys(&digest);
        if !whole {
            let record = blob_dir.resolve(&held)?;
            locked(&record, async || match record.read_optional().await? {
                Some(bytes) if Groups::decode(&bytes).is_some_and(|(held, _)| held == size) => {
                    Ok(())
                }
                Some(_) => Err(BlobError::Storage(format!(
                    "the store holds a part of the blob {} of another size than {size} bytes",
                    blob_key(&digest)
                ))),
                None => Ok(record.write_atomic(&Groups::new().encode(size)).await?),
            })
            .await?;
        }
        Ok(RangeSink {
            decoder: Decoder::new(&digest, size, range.offset, end),
            tree: bao::tree(size),
            blob_dir,
            digest,
            size,
            kept: Groups::new(),
            unrecorded: 0,
            whole,
            failed: false,
        })
    }
}

impl RangeSink {
    /// Takes each item of the encoding that the decoder holds, and keeps it,
    /// unless the store holds the blob whole.
    async fn keep(&mut self) -> Result<(), BlobError> {
        let [data, outboard, _] = partial_keys(&self.digest);
        while let Some(item) = self.decoder.next_item() {
            match item {
                Ok(_) if self.whole => {}
                Ok(Item::Parent { node, pair }) => {
                    if let Some(offset) = bao::pair_offset(&self.tree, node) {
                        self.blob_dir
                            .resolve(&outboard)?
                            .write_at(offset, &pair)
                            .await?;
                    }
                }
                Ok(Item::Group { offset, bytes }) => {
                    self.blob_dir
                        .resolve(&data)?
                        .write_at(offset, &bytes)
                        .await?;
                    self.kept.insert(offset / bao::GROUP);
                    self.unrecorded += bytes.len() as u64;
                }
                Err(unverified) => {
                    self.failed = true;
                    self.record().await?;
                    return Err(BlobError::Unverified {
                        digest: blob_key(&self.digest),
                        reason: unverified.to_string(),
                    });
                }
            }
        }
        if self.decoder.excess() > 0 {
            self.failed = true;
            self.record().await?;
            return Err(BlobError::Unverified {
                digest: blob_key(&self.digest),
                reason: "the encoding runs past the end of its range".into(),
            });
        }
        if self.unrecorded >= RECORD_EVERY {
            self.record().await?;
        }
        Ok(())
    }

    /// Adds the groups that the sink kept to the record of the partial blob,
    /// and moves the blob to its place once the record names each group. The
    /// bytes and the hashes reach the disk before the record names them, so a
    /// record never names a group whose bytes a power loss left out.
    async fn record(&mut self) -> Result<(), BlobError> {
        let kept = std::mem::take(&mut self.kept);
        self.unrecorded = 0;
        let [data, outboard, held] = partial_keys(&self.digest);
        let blob_dir = self.blob_dir.clone();
        let (digest, size) = (self.digest.clone(), self.size);
        let record = blob_dir.resolve(&held)?;
        locked(&record, async || {
            let dest = blob_dir.resolve(&blob_key(&digest))?;
            let (data, outboard) = (blob_dir.resolve(&data)?, blob_dir.resolve(&outboard)?);
            if dest.exists().await {
                // Another sink completed the blob: what this one wrote since
                // is a copy of bytes that the blob holds.
                for leftover in [&data, &outboard, &record] {
                    leftover.remove().await?;
                }
                return Ok(());
            }
            data.sync().await?;
            outboard.sync().await?;
            let mut groups = match record.read_optional().await? {
                Some(bytes) => match Groups::decode(&bytes) {
                    Some((held, groups)) if held == size => groups,
                    _ => {
                        return Err(BlobError::Storage(format!(
                            "the record {} is not of a blob of {size} bytes",
                            record.path()
                        )));
                    }
                },
                None => Groups::new(),
            };
            groups.extend(&kept);
            if !groups.covers(bao::groups(size)) {
                if !kept.is_empty() {
                    record.write_atomic(&groups.encode(size)).await?;
                }
                return Ok(());
            }
            // The outboard first: a blob that the store holds has its
            // outboard. An outboard that is there already is whole, as one
            // that a crash left between the two moves is: the partial one,
            // which can hold the pairs of one range alone, does not replace
            // it.
            let whole_outboard = blob_dir.resolve(&outboard_key(&digest))?;
            if whole_outboard.exists().await {
                outboard.remove().await?;
            } else if outboard.exists().await {
                outboard.rename(&whole_outboard).await?;
            }
            if data.exists().await {
                data.rename(&dest).await?;
            } else {
                dest.write(&[]).await?;
            }
            record.remove().await?;
            Ok(())
        })
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSink for RangeSink {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        if self.failed {
            return Err(BlobError::Unverified {
                digest: blob_key(&self.digest),
                reason: "an earlier part of the range did not verify".into(),
            });
        }
        self.decoder.push(bytes);
        self.keep().await
    }

    async fn finish(mut self: Box<Self>) -> Result<Blake3Hash, BlobError> {
        if !self.whole && !self.kept.is_empty() {
            self.record().await?;
        }
        Ok(self.digest.clone())
    }
}

/// A [`BlobSource`] that gives nothing more after `left` bytes of a reader.
struct Limited {
    reader: FileReader,
    left: u64,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSource for Limited {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        if self.left == 0 {
            return Ok(None);
        }
        match self.reader.next().await {
            Some(Ok(mut chunk)) => {
                chunk.truncate(usize::try_from(self.left).unwrap_or(usize::MAX));
                self.left -= chunk.len() as u64;
                Ok(Some(chunk))
            }
            Some(Err(e)) => Err(BlobError::Storage(e.to_string())),
            None => Ok(None),
        }
    }
}

/// The most bytes of the encoding of a verified range that one call of
/// [`VerifiedSource::next`] gives.
const VERIFIED_CHUNK: usize = 1024 * 1024;

/// A [`BlobSource`] of a verified range: its header, and then the encoding of
/// its groups, which the store reads a group and a pair at a time. A part
/// that does not verify, as a group that went bad on the disk, ends the
/// source with an error after the parts before it.
struct VerifiedSource {
    header: Option<[u8; bao::HEADER]>,
    encoder: Encoder,
    digest: Blake3Hash,
    data: FileSystemHandle,
    outboard: FileSystemHandle,
    /// Why the next call fails, after a call that gave the parts before it.
    failure: Option<BlobError>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSource for VerifiedSource {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        if let Some(header) = self.header.take() {
            return Ok(Some(header.to_vec()));
        }
        if let Some(failure) = self.failure.take() {
            return Err(failure);
        }
        let mut encoded = Vec::new();
        while let Some(need) = self.encoder.need() {
            let bytes = match need {
                Need::Pair { offset } => self.outboard.read_at(offset, bao::PAIR).await?,
                Need::Group { offset, length } => self.data.read_at(offset, length).await?,
            };
            match self.encoder.take(&bytes) {
                Ok(item) => encoded.extend(item),
                Err(unverified) => {
                    let failure = BlobError::Unverified {
                        digest: blob_key(&self.digest),
                        reason: format!("the store holds a part that went bad: {unverified}"),
                    };
                    if encoded.is_empty() {
                        return Err(failure);
                    }
                    self.failure = Some(failure);
                    break;
                }
            }
            if encoded.len() >= VERIFIED_CHUNK {
                break;
            }
        }
        Ok((!encoded.is_empty()).then_some(encoded))
    }
}

impl FileSystem {
    /// A verified range of the blob `digest`, from `offset`, for `length`
    /// bytes or to its end: of a whole blob, or of the held part of a partial
    /// one from the start of the range. See [`Encoding::Bao`].
    async fn verified(
        &self,
        digest: &Blake3Hash,
        offset: u64,
        length: Option<u64>,
    ) -> Result<BlobReader, BlobError> {
        let blob_dir = self.blob()?;
        let whole = blob_dir.resolve(&blob_key(digest))?;
        let (size, data, outboard, held_from): (u64, _, _, Box<dyn Fn(u64) -> u64 + Send + Sync>) =
            if let Some(size) = whole.size().await? {
                let outboard = blob_dir.resolve(&outboard_key(digest))?;
                if size > bao::GROUP && !outboard.exists().await {
                    rebuild_outboard(digest, &whole, &blob_dir, &outboard).await?;
                }
                (size, whole, outboard, Box::new(move |_| size))
            } else if let Some(holding) = self.holding(digest).await? {
                let [data, outboard, _] = partial_keys(digest);
                let size = holding.size;
                (
                    size,
                    blob_dir.resolve(&data)?,
                    blob_dir.resolve(&outboard)?,
                    Box::new(move |from| holding.held_from(from)),
                )
            } else {
                return Err(BlobError::NotFound(blob_key(digest)));
            };
        let end = length.map_or(size, |length| offset.saturating_add(length).min(size));
        let (first, last) = if end > offset {
            bao::groups_of(size, offset, end)
        } else {
            let first = bao::groups_of(size, offset, offset).0;
            (first, first)
        };
        let covered = last.min(held_from(first)).max(first);
        Ok(Box::new(VerifiedSource {
            header: Some(bao::header(size, covered)),
            encoder: Encoder::new(digest, size, first, covered),
            digest: digest.clone(),
            data,
            outboard,
            failure: None,
        }))
    }
}

/// Builds the outboard of the whole blob `whole`, which a store wrote before
/// it kept outboards, and keeps it at `place`, if its bytes hash to `digest`.
async fn rebuild_outboard(
    digest: &Blake3Hash,
    whole: &FileSystemHandle,
    blob_dir: &FileSystemHandle,
    place: &FileSystemHandle,
) -> Result<(), BlobError> {
    let mut builder = OutboardBuilder::new();
    let mut staged = blob_dir.resolve(STAGING)?.writer().await?;
    let mut reader = whole.reader().await?;
    while let Some(chunk) = reader.next().await {
        let pairs = builder.update(&chunk?);
        staged.write_all(&pairs).await?;
    }
    let (hash, pairs) = builder.finish();
    staged.write_all(&pairs).await?;
    if &hash != digest {
        let _ = staged.discard().await;
        return Err(BlobError::DigestMismatch {
            expected: blob_key(digest),
            actual: blob_key(&hash),
        });
    }
    staged.finish_to(place).await?;
    Ok(())
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Read> for FileSystem {
    async fn execute(&self, effect: Capability<Read>) -> Result<BlobReader, BlobError> {
        let digest = effect.digest();
        let handle = self.blob()?.resolve(&blob_key(digest))?;
        let (offset, length) = match effect.range() {
            Some(range) => (range.offset, range.length),
            None => (0, None),
        };
        if effect.encoding() == Some(Encoding::Bao) {
            return self.verified(digest, offset, length).await;
        }
        if handle.exists().await {
            let reader = handle.reader_range(offset, length).await?;
            return Ok(Box::new(FileBlobSource(reader)));
        }
        // A blob that the store holds in part reads to where the held part of
        // the range ends.
        let held = match self.holding(digest).await? {
            Some(holding) if holding.held_from(offset) > offset => holding.held_from(offset),
            _ => return Err(BlobError::NotFound(blob_key(digest))),
        };
        let end = length.map_or(held, |length| offset.saturating_add(length).min(held));
        let [data, _, _] = partial_keys(digest);
        let reader = match self
            .blob()?
            .resolve(&data)?
            .reader_range(offset, Some(end - offset))
            .await
        {
            Ok(reader) => reader,
            // A sink completed the blob after the record was read: the bytes
            // moved to the place of a whole blob.
            Err(_) if handle.exists().await => {
                let reader = handle.reader_range(offset, length).await?;
                return Ok(Box::new(FileBlobSource(reader)));
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Box::new(Limited {
            reader,
            left: end - offset,
        }))
    }
}

/// The bytes that the blob store keeps: the length of each whole blob and
/// outboard, and of each partial blob the bytes of the groups that its record
/// names, with its outboard. A staged write, a lock, and a temporary file
/// keep nothing yet. The store reads the record of each partial blob, and no
/// blob.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Usage> for FileSystem {
    async fn execute(&self, _effect: Capability<Usage>) -> Result<u64, BlobError> {
        let blob_dir = self.blob()?;
        let mut kept = 0u64;
        for name in blob_dir.list().await? {
            if name.starts_with(STAGING) || name.ends_with(".lock") || name.ends_with(".tmp") {
                continue;
            }
            let file = blob_dir.resolve(&name)?;
            if let Some(key) = name.strip_suffix(&format!(".{PARTIAL}")) {
                // The bytes of a partial blob have holes: what it keeps is
                // the groups that its record names.
                let record = blob_dir.resolve(&format!("{key}.{HELD}"))?;
                if let Some((size, groups)) = record
                    .read_optional()
                    .await?
                    .and_then(|record| Groups::decode(&record))
                {
                    kept += groups
                        .bytes(size)
                        .iter()
                        .map(|(start, end)| end - start)
                        .sum::<u64>();
                }
                continue;
            }
            kept += file.size().await?.unwrap_or(0);
        }
        Ok(kept)
    }
}

/// Which bytes of a blob the store holds: from the length of a whole blob,
/// or from the record of a partial one.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Held> for FileSystem {
    async fn execute(&self, effect: Capability<Held>) -> Result<Option<Holding>, BlobError> {
        self.holding(effect.digest()).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ImportRange> for FileSystem {
    async fn execute(&self, effect: Capability<ImportRange>) -> Result<BlobWriter, BlobError> {
        let sink = self
            .open_range_sink(effect.digest().clone(), effect.size(), effect.range())
            .await?;
        Ok(Box::new(sink))
    }
}

/// The blob file's length, from its metadata (natively) or its `File`
/// (on the web); its bytes are not read.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Size> for FileSystem {
    async fn execute(&self, effect: Capability<Size>) -> Result<Option<u64>, BlobError> {
        let handle = self.blob()?.resolve(&blob_key(effect.digest()))?;
        Ok(handle.size().await?)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Write> for FileSystem {
    async fn execute(&self, _effect: Capability<Write>) -> Result<BlobWriter, BlobError> {
        Ok(Box::new(self.open_blob_sink(None).await?))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Import> for FileSystem {
    async fn execute(&self, effect: Capability<Import>) -> Result<BlobWriter, BlobError> {
        let expected = effect.digest().clone();
        Ok(Box::new(self.open_blob_sink(Some(expected)).await?))
    }
}

#[cfg(test)]
mod tests {
    // On wasm these run in a worker against OPFS, exercising the true-streaming
    // web writer (createWritable + staging + rename) and the Blob.stream reader
    // — the ranged-read and import-verify paths the WebSpace round-trip test
    // doesn't cover.
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::resource::Resource;
    use dialog_capability::Subject;
    use dialog_effects::prelude::*;
    use dialog_effects::storage::{Directory, Location as StorageLocation};

    async fn test_space(name: &str) -> FileSystem {
        let location = StorageLocation::new(Directory::Temp, crate::helpers::unique_name(name));
        FileSystem::open(&location).await.unwrap()
    }

    fn subject() -> Subject {
        Subject::from(dialog_capability::did!("key:z6MkBlobProvider"))
    }

    async fn drain(mut reader: BlobReader) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            out.extend(chunk);
        }
        out
    }

    #[dialog_common::test]
    async fn it_ingests_then_reads_a_blob() {
        let fs = test_space("blob-ingest").await;
        let payload: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let expected_hash = Blake3Hash::from(*blake3::hash(&payload).as_bytes());

        // Ingest: stream in, get the discovered hash back.
        let mut sink = subject()
            .writer()
            .archive()
            .blob()
            .write()
            .perform(&fs)
            .await
            .unwrap();
        for chunk in payload.chunks(8192) {
            sink.write_all(chunk).await.unwrap();
        }
        let hash = sink.finish().await.unwrap();
        assert_eq!(hash, expected_hash);

        // Read the whole blob back by hash.
        let reader = subject()
            .reader()
            .archive()
            .blob()
            .read(hash.clone())
            .perform(&fs)
            .await
            .unwrap();
        assert_eq!(drain(reader).await, payload);

        // Ranged read: 9 bytes from offset 10.
        let reader = subject()
            .reader()
            .archive()
            .blob()
            .invoke(Read::range(hash, 10, Some(9)))
            .perform(&fs)
            .await
            .unwrap();
        assert_eq!(drain(reader).await, payload[10..19]);
    }

    /// Writes `payload` as a blob through an ingest sink, in pieces of 8 KiB.
    async fn ingest(fs: &FileSystem, payload: &[u8]) -> Blake3Hash {
        let mut sink = subject()
            .writer()
            .archive()
            .blob()
            .write()
            .perform(fs)
            .await
            .unwrap();
        for chunk in payload.chunks(8192) {
            sink.write_all(chunk).await.unwrap();
        }
        sink.finish().await.unwrap()
    }

    /// The names of the files in the blob directory of `fs`.
    async fn blob_files(fs: &FileSystem) -> Vec<String> {
        let mut files = fs.blob().unwrap().list().await.unwrap();
        files.sort();
        files
    }

    #[dialog_common::test]
    async fn it_keeps_an_outboard_for_a_blob_of_more_than_one_group() {
        let fs = test_space("blob-outboard").await;
        let payload: Vec<u8> = (0..16 * 1024 + 1).map(|i| (i % 251) as u8).collect();

        let hash = ingest(&fs, &payload).await;

        let outboard = fs
            .blob()
            .unwrap()
            .resolve(&outboard_key(&hash))
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(outboard.len(), 64);
        assert_eq!(outboard, crate::bao::outboard(&payload).1);
        assert_eq!(
            blob_files(&fs).await,
            [blob_key(&hash), outboard_key(&hash)]
        );
    }

    #[dialog_common::test]
    async fn it_keeps_no_outboard_for_a_blob_of_one_group() {
        let fs = test_space("blob-one-group").await;
        let payload = vec![7u8; 16 * 1024];

        let hash = ingest(&fs, &payload).await;

        assert_eq!(blob_files(&fs).await, [blob_key(&hash)]);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_keeps_nothing_of_a_write_that_does_not_end() {
        let fs = test_space("blob-dropped").await;
        let mut sink = subject()
            .writer()
            .archive()
            .blob()
            .write()
            .perform(&fs)
            .await
            .unwrap();
        sink.write_all(&vec![3u8; 100 * 1024]).await.unwrap();

        drop(sink);

        assert_eq!(blob_files(&fs).await, Vec::<String>::new());
    }

    #[dialog_common::test]
    async fn it_keeps_no_outboard_of_an_import_that_does_not_verify() {
        let fs = test_space("blob-forged-outboard").await;
        let payload = vec![5u8; 100 * 1024];
        let mut sink = subject()
            .writer()
            .archive()
            .blob()
            .import(Blake3Hash::from([0u8; 32]), payload.len() as u64)
            .perform(&fs)
            .await
            .unwrap();
        sink.write_all(&payload).await.unwrap();

        assert!(matches!(
            sink.finish().await,
            Err(BlobError::DigestMismatch { .. })
        ));
        assert_eq!(blob_files(&fs).await, Vec::<String>::new());
    }

    #[dialog_common::test]
    async fn it_keeps_the_groups_of_the_middle_of_a_blob_alone() {
        let fs = test_space("blob-partial-middle").await;
        crate::bao::conformance::it_keeps_the_groups_of_the_middle_alone(&fs, &subject()).await;
    }

    #[dialog_common::test]
    async fn it_holds_a_blob_whole_once_it_keeps_each_group() {
        let fs = test_space("blob-partial-whole").await;
        crate::bao::conformance::it_holds_a_blob_whole_once_it_keeps_each_group(&fs, &subject())
            .await;
        // The partial blob gave its place to the whole blob and its outboard.
        let mut files = blob_files(&fs).await;
        files.retain(|name| !name.ends_with(".lock"));
        assert_eq!(files.len(), 2, "{files:?}");
        assert!(
            files.iter().any(|name| name.ends_with(".outboard")),
            "{files:?}"
        );
    }

    #[dialog_common::test]
    async fn it_refuses_a_group_that_does_not_verify() {
        let fs = test_space("blob-partial-unverified").await;
        crate::bao::conformance::it_refuses_a_group_that_does_not_verify(&fs, &subject()).await;
    }

    #[dialog_common::test]
    async fn it_reports_missing_blobs() {
        let fs = test_space("blob-missing").await;
        let result = subject()
            .reader()
            .archive()
            .blob()
            .read([9u8; 32])
            .perform(&fs)
            .await;
        assert!(matches!(result, Err(BlobError::NotFound(_))));
    }

    #[dialog_common::test]
    async fn it_imports_a_known_blob_and_verifies_the_digest() {
        let fs = test_space("blob-import").await;
        let payload = b"a known blob".to_vec();
        let digest = Blake3Hash::from(*blake3::hash(&payload).as_bytes());

        // Import under the correct digest succeeds.
        let mut sink = subject()
            .writer()
            .archive()
            .blob()
            .import(digest.clone(), payload.len() as u64)
            .perform(&fs)
            .await
            .unwrap();
        sink.write_all(&payload).await.unwrap();
        assert_eq!(sink.finish().await.unwrap(), digest.clone());

        let reader = subject()
            .reader()
            .archive()
            .blob()
            .read(digest)
            .perform(&fs)
            .await
            .unwrap();
        assert_eq!(drain(reader).await, payload);

        // Import claiming a wrong digest is rejected at finish.
        let wrong = Blake3Hash::from([0u8; 32]);
        let mut sink = subject()
            .writer()
            .archive()
            .blob()
            .import(wrong, payload.len() as u64)
            .perform(&fs)
            .await
            .unwrap();
        sink.write_all(&payload).await.unwrap();
        assert!(matches!(
            sink.finish().await,
            Err(BlobError::DigestMismatch { .. })
        ));
    }

    #[dialog_common::test]
    async fn it_keeps_both_of_two_imports_of_one_blob_at_once() {
        let fs = test_space("blob-partial-two").await;
        crate::bao::conformance::it_keeps_both_of_two_imports_at_once(&fs, &subject()).await;
    }

    #[dialog_common::test]
    async fn it_refuses_an_import_at_another_size() {
        let fs = test_space("blob-partial-size").await;
        crate::bao::conformance::it_refuses_an_import_at_another_size(&fs, &subject()).await;
    }

    #[dialog_common::test]
    async fn it_verifies_a_range_of_a_blob_that_it_holds_whole() {
        let fs = test_space("blob-partial-verify-whole").await;
        crate::bao::conformance::it_verifies_a_range_of_a_blob_that_it_holds_whole(&fs, &subject())
            .await;
    }

    /// A crash between the two moves of a completion leaves the whole
    /// outboard in place and the record of the partial blob: the outboard of
    /// a later import, which holds the pairs of its range alone, does not
    /// replace it.
    #[dialog_common::test]
    async fn it_keeps_a_whole_outboard_that_a_crash_left_beside_a_partial_blob() {
        use crate::bao::conformance::{blob, encoding, import};
        let fs = test_space("blob-partial-crash").await;
        let bytes = blob(8 * crate::bao::GROUP as usize);
        let size = bytes.len() as u64;
        let digest = Blake3Hash::from(*blake3::hash(&bytes).as_bytes());
        let half = 4 * crate::bao::GROUP;
        import(&fs, &subject(), &bytes, 0, half, &encoding(&bytes, 0, half))
            .await
            .unwrap();
        // The crash: the whole outboard moved to its place, and the partial
        // outboard with it.
        let blob_dir = fs.blob().unwrap();
        let outboard = crate::bao::outboard(&bytes).1;
        let [_, partial_outboard, _] = partial_keys(&digest);
        blob_dir
            .resolve(&partial_outboard)
            .unwrap()
            .remove()
            .await
            .unwrap();
        blob_dir
            .resolve(&outboard_key(&digest))
            .unwrap()
            .write(&outboard)
            .await
            .unwrap();

        import(
            &fs,
            &subject(),
            &bytes,
            half,
            size,
            &encoding(&bytes, half, size),
        )
        .await
        .unwrap();

        let kept = blob_dir
            .resolve(&outboard_key(&digest))
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(kept, outboard);
        assert_eq!(
            blob_dir
                .resolve(&blob_key(&digest))
                .unwrap()
                .read()
                .await
                .unwrap(),
            bytes
        );
    }

    #[dialog_common::test]
    async fn it_counts_the_bytes_that_it_keeps() {
        let fs = test_space("blob-usage").await;
        crate::bao::conformance::it_counts_the_bytes_that_it_keeps(&fs, &subject()).await;
    }

    #[dialog_common::test]
    async fn it_answers_a_verified_range_of_a_whole_blob() {
        let fs = test_space("blob-verified-whole").await;
        crate::bao::conformance::it_answers_a_verified_range_of_a_whole_blob(&fs, &subject()).await;
    }

    #[dialog_common::test]
    async fn it_answers_the_held_start_of_a_range() {
        let fs = test_space("blob-verified-partial").await;
        crate::bao::conformance::it_answers_the_held_start_of_a_range(&fs, &subject()).await;
    }

    /// A group that went bad on the disk ends a verified range before it:
    /// the store sends the groups before it, and then an error, and never a
    /// group that does not verify.
    #[dialog_common::test]
    async fn it_ends_a_verified_range_before_a_group_that_went_bad() {
        let fs = test_space("blob-verified-bad").await;
        let payload: Vec<u8> = (0..10 * crate::bao::GROUP as usize)
            .map(|i| (i % 249) as u8)
            .collect();
        let digest = ingest(&fs, &payload).await;
        let file = fs.blob().unwrap().resolve(&blob_key(&digest)).unwrap();
        file.write_at(4 * crate::bao::GROUP + 9, &[payload[4 * 16384 + 9] ^ 1])
            .await
            .unwrap();
        let mut reader = subject()
            .reader()
            .archive()
            .blob()
            .invoke(Read::verified(digest.clone(), 0, None))
            .perform(&fs)
            .await
            .unwrap();

        let mut bytes = Vec::new();
        let ended = loop {
            match reader.next().await {
                Ok(Some(chunk)) => bytes.extend(chunk),
                Ok(None) => break None,
                Err(error) => break Some(error),
            }
        };

        assert!(
            matches!(ended, Some(BlobError::Unverified { .. })),
            "{ended:?}"
        );
        let mut decoder =
            crate::bao::Decoder::new(&digest, payload.len() as u64, 0, payload.len() as u64);
        decoder.push(&bytes[crate::bao::HEADER..]);
        let mut groups = 0;
        while let Some(item) = decoder.next_item() {
            if let crate::bao::Item::Group { .. } = item.unwrap() {
                groups += 1;
            }
        }
        assert_eq!(groups, 4, "the groups before the bad one");
    }

    /// A blob that a store wrote before it kept outboards gets one at its
    /// first verified read.
    #[dialog_common::test]
    async fn it_builds_the_outboard_of_an_older_blob_at_its_first_verified_read() {
        let fs = test_space("blob-verified-older").await;
        let payload = crate::bao::conformance::blob(3 * crate::bao::GROUP as usize);
        let digest = ingest(&fs, &payload).await;
        let outboard = fs.blob().unwrap().resolve(&outboard_key(&digest)).unwrap();
        outboard.remove().await.unwrap();

        let read = crate::bao::conformance::verified(&fs, &subject(), &digest, 0, None).await;

        assert!(read.is_ok(), "{:?}", read.err());
        assert_eq!(
            outboard.read().await.unwrap(),
            crate::bao::outboard(&payload).1
        );
    }
}
