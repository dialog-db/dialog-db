//! Blob capability provider for volatile storage.
//!
//! In-memory mirror of the filesystem blob provider
//! (`storage/provider/fs/blob.rs`): whole, hash-addressable binary objects
//! stored per subject session, keyed by base58 digest. [`Write`] discovers the
//! hash as bytes are written; [`Import`] verifies the declared digest at
//! [`finish`](BlobSink::finish) (a lying writer surfaces as
//! [`BlobError::DigestMismatch`] and nothing is stored); [`Read`] serves the
//! whole blob or a byte range. "Streaming" degrades to buffering — the store
//! is a `HashMap`, so a sink accumulates bytes and commits on finish, and a
//! reader yields the (ranged) copy as a single chunk. A sink builds the
//! outboard of the blob as it takes the bytes, as the filesystem sink does,
//! and keeps it beside a blob of more than one group (see [`crate::bao`]).
//! [`ImportRange`] keeps each group of a verified range in a partial blob, as
//! it verifies, and the session holds the blob whole once it holds each
//! group.

use super::{Session, Volatile};
use crate::bao::{self, Decoder, Encoder, Groups, Item, Need, OutboardBuilder};
use async_trait::async_trait;
use base58::ToBase58;
use dialog_capability::{Capability, Did, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::blob::prelude::{
    BlobHeldExt as _, BlobImportExt as _, BlobImportRangeExt as _, BlobProofExt as _,
    BlobReadExt as _, BlobSizeExt as _,
};
use dialog_effects::blob::{
    BlobError, BlobReader, BlobSink, BlobSource, BlobWriter, Encoding, Held, Holding, Import,
    ImportRange, Proof, Read, Size, Usage, Write,
};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

fn blob_key(digest: &Blake3Hash) -> String {
    digest.as_bytes().to_base58()
}

/// A [`BlobSource`] over an owned copy of the (already ranged) bytes, yielded
/// as one chunk.
struct MemoryBlobSource(Option<Vec<u8>>);

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSource for MemoryBlobSource {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        Ok(self.0.take())
    }
}

/// A [`BlobSink`] that buffers bytes while hashing, then commits them to the
/// subject's session on [`finish`](BlobSink::finish) — to the discovered hash
/// for an ingest, or the declared (and verified) digest for an import.
struct MemoryBlobSink {
    sessions: Arc<RwLock<HashMap<Did, Session>>>,
    subject: Did,
    buffer: Vec<u8>,
    /// The hash and the outboard of the bytes so far.
    builder: OutboardBuilder,
    outboard: Vec<u8>,
    /// `Some` for an import (the declared digest, verified at finish); `None`
    /// for an ingest (the hash is discovered).
    expected: Option<Blake3Hash>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSink for MemoryBlobSink {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        let pairs = self.builder.update(bytes);
        self.outboard.extend(pairs);
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<Blake3Hash, BlobError> {
        let MemoryBlobSink {
            sessions,
            subject,
            buffer,
            builder,
            mut outboard,
            expected,
        } = *self;

        let (hash, pairs) = builder.finish();
        outboard.extend(pairs);

        if let Some(expected) = expected
            && hash != expected
        {
            return Err(BlobError::DigestMismatch {
                expected: expected.as_bytes().to_base58(),
                actual: hash.as_bytes().to_base58(),
            });
        }

        // Content addressing is idempotent: an existing blob has identical
        // bytes, so keep the first copy.
        let mut sessions = sessions.write();
        let session = sessions.entry(subject).or_default();
        if !outboard.is_empty() {
            session.outboards.entry(blob_key(&hash)).or_insert(outboard);
        }
        session.blobs.entry(blob_key(&hash)).or_insert(buffer);

        Ok(hash)
    }
}

impl Volatile {
    fn open_blob_sink(&self, subject: Did, expected: Option<Blake3Hash>) -> MemoryBlobSink {
        MemoryBlobSink {
            sessions: self.sessions.clone(),
            subject,
            buffer: Vec::new(),
            builder: OutboardBuilder::new(),
            outboard: Vec::new(),
            expected,
        }
    }
}

/// A blob that a session holds in part: the groups that it holds, by their
/// offsets, and the hashes that verified them, by their offsets in the
/// outboard.
#[derive(Debug, Default)]
pub(super) struct Partial {
    size: u64,
    groups: BTreeMap<u64, Vec<u8>>,
    pairs: BTreeMap<u64, [u8; bao::PAIR]>,
    held: Groups,
}

impl Partial {
    /// Which bytes of the blob the session holds.
    fn holding(&self) -> Holding {
        Holding {
            size: self.size,
            ranges: self.held.bytes(self.size),
        }
    }
}

/// A [`BlobSink`] of the Bao encoding of a range of a blob, which keeps each
/// group that verifies in the partial blob of its session at once.
struct MemoryRangeSink {
    sessions: Arc<RwLock<HashMap<Did, Session>>>,
    subject: Did,
    digest: Blake3Hash,
    size: u64,
    tree: bao_tree::BaoTree,
    decoder: Decoder,
    /// Whether the partial blob is of the size of this sink: see the claim
    /// of the file system store.
    claimed: bool,
    /// The items that verified before the first group.
    pending: Vec<Item>,
    failed: bool,
}

impl MemoryRangeSink {
    /// Keeps `item`, and moves the blob to its place once the session holds
    /// each group of it. The items before the first group wait for it. Then
    /// the sink claims the partial blob, as the file system store claims its
    /// record: a part of another size goes, since a group of the start
    /// verifies at more than one size.
    fn keep(&mut self, item: Item) -> Result<(), BlobError> {
        let key = blob_key(&self.digest);
        let mut sessions = self.sessions.write();
        let session = sessions.entry(self.subject.clone()).or_default();
        if session.blobs.contains_key(&key) {
            return Ok(());
        }
        let items = if self.claimed {
            vec![item]
        } else {
            let group = matches!(item, Item::Group { .. });
            self.pending.push(item);
            if !group {
                return Ok(());
            }
            if session
                .partials
                .get(&key)
                .is_some_and(|partial| partial.size != self.size)
            {
                session.partials.remove(&key);
            }
            self.claimed = true;
            std::mem::take(&mut self.pending)
        };
        let partial = session
            .partials
            .entry(key.clone())
            .or_insert_with(|| Partial {
                size: self.size,
                ..Partial::default()
            });
        if partial.size != self.size {
            return Err(BlobError::Storage(format!(
                "an import of the blob {key} at another size than {} bytes took the place of \
                 this one",
                self.size
            )));
        }
        for item in items {
            match item {
                Item::Parent { node, pair } => {
                    if let Some(offset) = bao::pair_offset(&self.tree, node) {
                        partial.pairs.insert(offset, pair);
                    }
                }
                Item::Group { offset, bytes } => {
                    partial.groups.insert(offset, bytes);
                    partial.held.insert(offset / bao::GROUP);
                }
            }
        }
        if !partial.held.covers(bao::groups(self.size)) {
            return Ok(());
        }
        let partial = session
            .partials
            .remove(&key)
            .expect("the partial blob is there");
        let blob: Vec<u8> = partial.groups.into_values().flatten().collect();
        let outboard: Vec<u8> = partial.pairs.into_values().flatten().collect();
        if !outboard.is_empty() {
            session.outboards.insert(key.clone(), outboard);
        }
        session.blobs.insert(key, blob);
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSink for MemoryRangeSink {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        let digest = blob_key(&self.digest);
        let unverified = |reason: String| BlobError::Unverified {
            digest: digest.clone(),
            reason,
        };
        if self.failed {
            return Err(unverified(
                "an earlier part of the range did not verify".into(),
            ));
        }
        self.decoder.push(bytes);
        while let Some(item) = self.decoder.next_item() {
            match item {
                Ok(item) => {
                    if let Err(error) = self.keep(item) {
                        self.failed = true;
                        return Err(error);
                    }
                }
                Err(error) => {
                    self.failed = true;
                    return Err(unverified(error.to_string()));
                }
            }
        }
        if self.decoder.excess() > 0 {
            self.failed = true;
            return Err(unverified(
                "the encoding runs past the end of its range".into(),
            ));
        }
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<Blake3Hash, BlobError> {
        Ok(self.digest.clone())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Read> for Volatile {
    async fn execute(&self, effect: Capability<Read>) -> Result<BlobReader, BlobError> {
        let subject: Did = effect.subject().into();
        let digest = effect.digest();
        let key = blob_key(digest);
        let (offset, length) = effect
            .range()
            .map_or((0, None), |range| (range.offset, range.length));

        let sessions = self.sessions.read();
        let session = sessions.get(&subject);
        if effect.encoding() == Some(Encoding::Bao) {
            return verified(session, digest, offset, length);
        }
        if let Some(bytes) = session.and_then(|session| session.blobs.get(&key)) {
            let start = (offset as usize).min(bytes.len());
            let end = match length {
                Some(length) => start.saturating_add(length as usize).min(bytes.len()),
                None => bytes.len(),
            };
            return Ok(Box::new(MemoryBlobSource(Some(bytes[start..end].to_vec()))));
        }
        // A blob that the session holds in part reads to where the held part
        // of the range ends.
        let partial = session
            .and_then(|session| session.partials.get(&key))
            .filter(|partial| partial.holding().held_from(offset) > offset)
            .ok_or(BlobError::NotFound(key))?;
        let held = partial.holding().held_from(offset);
        let end = length.map_or(held, |length| offset.saturating_add(length).min(held));
        let mut ranged = Vec::new();
        for (start, bytes) in partial.groups.range(offset / bao::GROUP * bao::GROUP..end) {
            let from = offset.saturating_sub(*start) as usize;
            let to = (end - start).min(bytes.len() as u64) as usize;
            ranged.extend_from_slice(&bytes[from..to]);
        }
        Ok(Box::new(MemoryBlobSource(Some(ranged))))
    }
}

/// The bytes that the session keeps: each blob and outboard, and the groups
/// and the pairs of each partial blob.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Usage> for Volatile {
    async fn execute(&self, effect: Capability<Usage>) -> Result<u64, BlobError> {
        let subject: Did = effect.subject().into();
        let sessions = self.sessions.read();
        let Some(session) = sessions.get(&subject) else {
            return Ok(0);
        };
        let lengths = |map: &HashMap<String, Vec<u8>>| map.values().map(Vec::len).sum::<usize>();
        let partials: usize = session
            .partials
            .values()
            .map(|partial| {
                partial.groups.values().map(Vec::len).sum::<usize>()
                    + partial.pairs.len() * bao::PAIR
            })
            .sum();
        Ok((lengths(&session.blobs) + lengths(&session.outboards) + partials) as u64)
    }
}

/// A verified range of the blob `digest` that `session` holds, from
/// `offset`, for `length` bytes or to its end, encoded whole: the session
/// holds the blob in memory already. See [`Encoding::Bao`].
fn verified(
    session: Option<&Session>,
    digest: &Blake3Hash,
    offset: u64,
    length: Option<u64>,
) -> Result<BlobReader, BlobError> {
    let key = blob_key(digest);
    let whole = session.and_then(|session| session.blobs.get(&key));
    let partial = session.and_then(|session| session.partials.get(&key));
    let size = match (whole, partial) {
        (Some(bytes), _) => bytes.len() as u64,
        (None, Some(partial)) if !partial.held.is_empty() => partial.size,
        _ => return Err(BlobError::NotFound(key)),
    };
    let end = length.map_or(size, |length| offset.saturating_add(length).min(size));
    let (first, last) = if end > offset {
        bao::groups_of(size, offset, end)
    } else {
        let first = bao::groups_of(size, offset, offset).0;
        (first, first)
    };
    let held = match (whole, partial) {
        (Some(_), _) => size,
        (None, Some(partial)) => partial.holding().held_from(first),
        (None, None) => first,
    };
    let covered = last.min(held).max(first);
    let outboard = session.and_then(|session| session.outboards.get(&key));
    let mut encoder = Encoder::new(digest, size, first, covered);
    let mut encoded = bao::header(size, covered).to_vec();
    while let Some(need) = encoder.need() {
        let bytes: Vec<u8> = match (need, whole, partial) {
            (Need::Pair { offset }, Some(_), _) => outboard
                .and_then(|outboard| outboard.get(offset as usize..offset as usize + bao::PAIR))
                .map(<[u8]>::to_vec)
                .unwrap_or_default(),
            (Need::Pair { offset }, None, Some(partial)) => partial
                .pairs
                .get(&offset)
                .map(|pair| pair.to_vec())
                .unwrap_or_default(),
            (Need::Group { offset, length }, Some(bytes), _) => bytes
                .get(offset as usize..offset as usize + length)
                .map(<[u8]>::to_vec)
                .unwrap_or_default(),
            (Need::Group { offset, .. }, None, Some(partial)) => {
                partial.groups.get(&offset).cloned().unwrap_or_default()
            }
            _ => Vec::new(),
        };
        encoded.extend(
            encoder
                .take(&bytes)
                .map_err(|unverified| BlobError::Unverified {
                    digest: key.clone(),
                    reason: format!("the store holds a part that went bad: {unverified}"),
                })?,
        );
    }
    Ok(Box::new(MemoryBlobSource(Some(encoded))))
}

/// Which bytes of a blob the session holds.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Held> for Volatile {
    async fn execute(&self, effect: Capability<Held>) -> Result<Option<Holding>, BlobError> {
        let subject: Did = effect.subject().into();
        let key = blob_key(effect.digest());
        let sessions = self.sessions.read();
        let Some(session) = sessions.get(&subject) else {
            return Ok(None);
        };
        if let Some(bytes) = session.blobs.get(&key) {
            let size = bytes.len() as u64;
            return Ok(Some(Holding {
                size,
                ranges: vec![(0, size)],
            }));
        }
        Ok(session.partials.get(&key).map(Partial::holding))
    }
}

/// The proof of a range of a blob, from the pairs of its outboard, or of the
/// hashes that verified the groups of a partial blob that holds each group
/// whose path the proof reads.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Proof> for Volatile {
    async fn execute(
        &self,
        effect: Capability<Proof>,
    ) -> Result<Option<Vec<Blake3Hash>>, BlobError> {
        let subject: Did = effect.subject().into();
        let (digest, size, range) = (effect.digest(), effect.size(), effect.range());
        let end = range
            .length
            .map_or(size, |length| range.offset.saturating_add(length).min(size));
        if range.offset >= end {
            return Err(BlobError::Storage(format!(
                "a proof covers at least one byte of the blob, of {size} bytes"
            )));
        }
        let proof = bao::Proof::new(size, range.offset, end);
        let key = blob_key(digest);
        let sessions = self.sessions.read();
        let Some(session) = sessions.get(&subject) else {
            return Ok(None);
        };
        let hashes = if let Some(bytes) = session.blobs.get(&key) {
            if bytes.len() as u64 != size {
                return Ok(None);
            }
            let outboard = session.outboards.get(&key);
            proof.hashes(digest, |offset| {
                outboard
                    .and_then(|outboard| outboard.get(offset as usize..offset as usize + bao::PAIR))
                    .and_then(|pair| pair.try_into().ok())
            })
        } else {
            let Some(partial) = session.partials.get(&key) else {
                return Ok(None);
            };
            let holding = partial.holding();
            if partial.size != size || !proof.groups().all(|group| holding.held_from(group) > group)
            {
                return Ok(None);
            }
            proof.hashes(digest, |offset| partial.pairs.get(&offset).copied())
        };
        hashes
            .map(|hashes| hashes.map(|hashes| hashes.into_iter().map(Blake3Hash::from).collect()))
            .map_err(|unverified| BlobError::Unverified {
                digest: key,
                reason: format!("the store holds a part that went bad: {unverified}"),
            })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ImportRange> for Volatile {
    async fn execute(&self, effect: Capability<ImportRange>) -> Result<BlobWriter, BlobError> {
        let (size, range) = (effect.size(), effect.range());
        let end = bao::group_range(size, range.offset, range.length).map_err(BlobError::Storage)?;
        Ok(Box::new(MemoryRangeSink {
            sessions: self.sessions.clone(),
            subject: effect.subject().into(),
            digest: effect.digest().clone(),
            size,
            tree: bao::tree(size),
            decoder: Decoder::new(effect.digest(), size, range.offset, end),
            claimed: false,
            pending: Vec::new(),
            failed: false,
        }))
    }
}

/// The length of the buffer held under the digest; nothing is copied.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Size> for Volatile {
    async fn execute(&self, effect: Capability<Size>) -> Result<Option<u64>, BlobError> {
        let subject: Did = effect.subject().into();
        let key = blob_key(effect.digest());
        Ok(self
            .sessions
            .read()
            .get(&subject)
            .and_then(|session| session.blobs.get(&key))
            .map(|bytes| bytes.len() as u64))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Write> for Volatile {
    async fn execute(&self, effect: Capability<Write>) -> Result<BlobWriter, BlobError> {
        Ok(Box::new(self.open_blob_sink(effect.subject().into(), None)))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Import> for Volatile {
    async fn execute(&self, effect: Capability<Import>) -> Result<BlobWriter, BlobError> {
        let expected = effect.digest().clone();
        Ok(Box::new(
            self.open_blob_sink(effect.subject().into(), Some(expected)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::unique_subject;
    use dialog_effects::prelude::*;

    async fn drain(mut reader: BlobReader) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            out.extend(chunk);
        }
        out
    }

    #[dialog_common::test]
    async fn it_ingests_then_reads_a_blob() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("blob-ingest");
        let payload: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let expected_hash = Blake3Hash::from(*blake3::hash(&payload).as_bytes());

        // Ingest: stream in, get the discovered hash back.
        let mut sink = subject
            .clone()
            .writer()
            .archive()
            .blob()
            .write()
            .perform(&provider)
            .await?;
        for chunk in payload.chunks(8192) {
            sink.write_all(chunk).await?;
        }
        let hash = sink.finish().await?;
        assert_eq!(hash, expected_hash);

        // Read the whole blob back by hash.
        let reader = subject
            .clone()
            .reader()
            .archive()
            .blob()
            .read(hash.clone())
            .perform(&provider)
            .await?;
        assert_eq!(drain(reader).await, payload);

        // Ranged read: 9 bytes from offset 10.
        let reader = subject
            .reader()
            .archive()
            .blob()
            .invoke(Read::range(hash, 10, Some(9)))
            .perform(&provider)
            .await?;
        assert_eq!(drain(reader).await, payload[10..19]);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_reports_missing_blobs() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("blob-missing");
        let result = subject
            .reader()
            .archive()
            .blob()
            .read([9u8; 32])
            .perform(&provider)
            .await;
        assert!(matches!(result, Err(BlobError::NotFound(_))));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_imports_a_known_blob_and_verifies_the_digest() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("blob-import");
        let payload = b"a known blob".to_vec();
        let digest = Blake3Hash::from(*blake3::hash(&payload).as_bytes());

        // Import under the correct digest succeeds.
        let mut sink = subject
            .clone()
            .writer()
            .archive()
            .blob()
            .import(digest.clone(), payload.len() as u64)
            .perform(&provider)
            .await?;
        sink.write_all(&payload).await?;
        assert_eq!(sink.finish().await?, digest.clone());

        let reader = subject
            .clone()
            .reader()
            .archive()
            .blob()
            .read(digest)
            .perform(&provider)
            .await?;
        assert_eq!(drain(reader).await, payload);

        // Import claiming a wrong digest is rejected at finish and stores
        // nothing under the claimed hash.
        let wrong = Blake3Hash::from([0u8; 32]);
        let mut sink = subject
            .clone()
            .writer()
            .archive()
            .blob()
            .import(wrong.clone(), payload.len() as u64)
            .perform(&provider)
            .await?;
        sink.write_all(&payload).await?;
        assert!(matches!(
            sink.finish().await,
            Err(BlobError::DigestMismatch { .. })
        ));
        let missing = subject
            .reader()
            .archive()
            .blob()
            .read(wrong)
            .perform(&provider)
            .await;
        assert!(matches!(missing, Err(BlobError::NotFound(_))));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_blobs_by_subject() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let alice = unique_subject("blob-alice");
        let bob = unique_subject("blob-bob");
        let payload = b"alice's bytes".to_vec();

        let mut sink = alice
            .writer()
            .archive()
            .blob()
            .write()
            .perform(&provider)
            .await?;
        sink.write_all(&payload).await?;
        let hash = sink.finish().await?;

        let missing = bob
            .reader()
            .archive()
            .blob()
            .read(hash)
            .perform(&provider)
            .await;
        assert!(matches!(missing, Err(BlobError::NotFound(_))));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_keeps_an_outboard_for_a_blob_of_more_than_one_group() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let subject = unique_subject("blob-outboard");
        let write = async |payload: &[u8]| -> anyhow::Result<Blake3Hash> {
            let mut sink = subject
                .clone()
                .writer()
                .archive()
                .blob()
                .write()
                .perform(&provider)
                .await?;
            sink.write_all(payload).await?;
            Ok(sink.finish().await?)
        };
        let long: Vec<u8> = (0..16 * 1024 + 1).map(|i| (i % 251) as u8).collect();
        let short = vec![1u8; 16 * 1024];

        let (long_hash, short_hash) = (write(&long).await?, write(&short).await?);

        let sessions = provider.sessions.read();
        let outboards = &sessions[subject.did()].outboards;
        assert_eq!(
            outboards.get(&blob_key(&long_hash)),
            Some(&crate::bao::outboard(&long).1)
        );
        assert_eq!(outboards.get(&blob_key(&long_hash)).map(Vec::len), Some(64));
        assert_eq!(outboards.get(&blob_key(&short_hash)), None);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_keeps_the_groups_of_the_middle_of_a_blob_alone() {
        let subject = unique_subject("blob-partial-middle");
        crate::bao::conformance::it_keeps_the_groups_of_the_middle_alone(
            &Volatile::new(),
            &subject,
        )
        .await;
    }

    #[dialog_common::test]
    async fn it_holds_a_blob_whole_once_it_keeps_each_group() {
        let subject = unique_subject("blob-partial-whole");
        let provider = Volatile::new();
        crate::bao::conformance::it_holds_a_blob_whole_once_it_keeps_each_group(
            &provider, &subject,
        )
        .await;
        let sessions = provider.sessions.read();
        let session = &sessions[subject.did()];
        assert!(session.partials.is_empty());
        assert_eq!(session.outboards.len(), 1);
    }

    #[dialog_common::test]
    async fn it_refuses_a_group_that_does_not_verify() {
        let subject = unique_subject("blob-partial-unverified");
        crate::bao::conformance::it_refuses_a_group_that_does_not_verify(
            &Volatile::new(),
            &subject,
        )
        .await;
    }

    #[dialog_common::test]
    async fn it_keeps_both_of_two_imports_of_one_blob_at_once() {
        let subject = unique_subject("blob-partial-two");
        crate::bao::conformance::it_keeps_both_of_two_imports_at_once(&Volatile::new(), &subject)
            .await;
    }

    #[dialog_common::test]
    async fn it_imports_a_blob_after_a_sink_of_another_size_kept_nothing() {
        let subject = unique_subject("blob-partial-nothing");
        crate::bao::conformance::it_imports_a_blob_after_a_sink_of_another_size_kept_nothing(
            &Volatile::new(),
            &subject,
        )
        .await;
    }

    #[dialog_common::test]
    async fn it_holds_the_part_of_the_size_whose_group_verified_last() {
        let subject = unique_subject("blob-partial-size");
        crate::bao::conformance::it_holds_the_part_of_the_size_whose_group_verified_last(
            &Volatile::new(),
            &subject,
        )
        .await;
    }

    #[dialog_common::test]
    async fn it_proves_a_range_of_a_blob_that_it_holds_whole() {
        let subject = unique_subject("blob-proof-whole");
        crate::bao::conformance::it_proves_a_range_of_a_blob_that_it_holds_whole(
            &Volatile::new(),
            &subject,
        )
        .await;
    }

    #[dialog_common::test]
    async fn it_proves_a_range_of_a_blob_that_it_holds_in_part() {
        let subject = unique_subject("blob-proof-partial");
        crate::bao::conformance::it_proves_a_range_of_a_blob_that_it_holds_in_part(
            &Volatile::new(),
            &subject,
        )
        .await;
    }

    #[dialog_common::test]
    async fn it_verifies_a_range_of_a_blob_that_it_holds_whole() {
        let subject = unique_subject("blob-partial-verify-whole");
        crate::bao::conformance::it_verifies_a_range_of_a_blob_that_it_holds_whole(
            &Volatile::new(),
            &subject,
        )
        .await;
    }

    #[dialog_common::test]
    async fn it_counts_the_bytes_that_it_keeps() {
        let subject = unique_subject("blob-usage");
        crate::bao::conformance::it_counts_the_bytes_that_it_keeps(&Volatile::new(), &subject)
            .await;
    }

    #[dialog_common::test]
    async fn it_answers_a_verified_range_of_a_whole_blob() {
        let subject = unique_subject("blob-verified-whole");
        crate::bao::conformance::it_answers_a_verified_range_of_a_whole_blob(
            &Volatile::new(),
            &subject,
        )
        .await;
    }

    #[dialog_common::test]
    async fn it_answers_the_held_start_of_a_range() {
        let subject = unique_subject("blob-verified-partial");
        crate::bao::conformance::it_answers_the_held_start_of_a_range(&Volatile::new(), &subject)
            .await;
    }
}
