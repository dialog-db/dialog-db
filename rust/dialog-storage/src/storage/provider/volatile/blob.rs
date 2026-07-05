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
//! reader yields the (ranged) copy as a single chunk.

use super::{Session, Volatile};
use async_trait::async_trait;
use base58::ToBase58;
use blake3::Hasher;
use dialog_capability::{Capability, Did, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::blob::prelude::{BlobImportExt as _, BlobReadExt as _};
use dialog_effects::blob::{
    BlobError, BlobReader, BlobSink, BlobSource, BlobWriter, Import, Read, Write,
};
use parking_lot::RwLock;
use std::collections::HashMap;
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
    hasher: Hasher,
    /// `Some` for an import (the declared digest, verified at finish); `None`
    /// for an ingest (the hash is discovered).
    expected: Option<Blake3Hash>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSink for MemoryBlobSink {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        self.hasher.update(bytes);
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<Blake3Hash, BlobError> {
        let MemoryBlobSink {
            sessions,
            subject,
            buffer,
            hasher,
            expected,
        } = *self;

        let hash = Blake3Hash::from(*hasher.finalize().as_bytes());

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
        sessions
            .write()
            .entry(subject)
            .or_default()
            .blobs
            .entry(blob_key(&hash))
            .or_insert(buffer);

        Ok(hash)
    }
}

impl Volatile {
    fn open_blob_sink(&self, subject: Did, expected: Option<Blake3Hash>) -> MemoryBlobSink {
        MemoryBlobSink {
            sessions: self.sessions.clone(),
            subject,
            buffer: Vec::new(),
            hasher: Hasher::new(),
            expected,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Read> for Volatile {
    async fn execute(&self, effect: Capability<Read>) -> Result<BlobReader, BlobError> {
        let subject: Did = effect.subject().into();
        let digest = effect.digest();
        let key = blob_key(digest);

        let sessions = self.sessions.read();
        let bytes = sessions
            .get(&subject)
            .and_then(|session| session.blobs.get(&key))
            .ok_or_else(|| BlobError::NotFound(key))?;

        let ranged = match effect.range() {
            None => bytes.clone(),
            Some(range) => {
                let start = (range.offset as usize).min(bytes.len());
                let end = match range.length {
                    Some(length) => start.saturating_add(length as usize).min(bytes.len()),
                    None => bytes.len(),
                };
                bytes[start..end].to_vec()
            }
        };

        Ok(Box::new(MemoryBlobSource(Some(ranged))))
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
            .archive()
            .blob()
            .read(hash.clone())
            .perform(&provider)
            .await?;
        assert_eq!(drain(reader).await, payload);

        // Ranged read: 9 bytes from offset 10.
        let reader = subject
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
            .archive()
            .blob()
            .import(digest.clone(), payload.len() as u64)
            .perform(&provider)
            .await?;
        sink.write_all(&payload).await?;
        assert_eq!(sink.finish().await?, digest.clone());

        let reader = subject
            .clone()
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
        let missing = subject.archive().blob().read(wrong).perform(&provider).await;
        assert!(matches!(missing, Err(BlobError::NotFound(_))));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_blobs_by_subject() -> anyhow::Result<()> {
        let provider = Volatile::new();
        let alice = unique_subject("blob-alice");
        let bob = unique_subject("blob-bob");
        let payload = b"alice's bytes".to_vec();

        let mut sink = alice.archive().blob().write().perform(&provider).await?;
        sink.write_all(&payload).await?;
        let hash = sink.finish().await?;

        let missing = bob.archive().blob().read(hash).perform(&provider).await;
        assert!(matches!(missing, Err(BlobError::NotFound(_))));
        Ok(())
    }
}
