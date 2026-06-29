//! Blob capability provider for filesystem.
//!
//! Layout: `{space_root}/blob/{base58(digest)}` — whole, hash-addressable
//! binary objects, parallel to the block archive at `{space_root}/archive/...`.
//!
//! Bytes stream rather than buffer: [`Read`] hands back a [`BlobSource`] over
//! the file (optionally ranged), and [`Write`]/[`Import`] hand back a
//! [`BlobSink`] that stages into a temp file, hashes as it goes, and commits
//! atomically — to the discovered hash for an ingest, or the declared (and
//! verified) digest for an import.

use super::{FileReader, FileSystem, FileSystemError, FileSystemHandle, FileWriter};
use async_trait::async_trait;
use base58::ToBase58;
use blake3::Hasher;
use dialog_capability::{Capability, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::blob::prelude::{BlobImportExt as _, BlobReadExt as _};
use dialog_effects::blob::{
    BlobError, BlobReader, BlobSink, BlobSource, BlobWriter, Import, Read, Write,
};
use futures_util::StreamExt;

const BLOB: &str = "blob";
/// Base name for the staging handle. Only its uniquely-suffixed temp file is
/// ever created; the content commits straight to the hash path.
const STAGING: &str = "_staging";

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

/// A [`BlobSource`] over a file's streaming reader.
struct FileBlobSource(FileReader);

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSource for FileBlobSource {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        match self.0.next().await {
            Some(Ok(chunk)) => Ok(Some(chunk)),
            Some(Err(e)) => Err(BlobError::Io(e.to_string())),
            None => Ok(None),
        }
    }
}

/// A [`BlobSink`] that stages bytes into a temp file while hashing, then
/// commits to the content-addressed path on [`finish`](BlobSink::finish).
struct FileBlobSink {
    writer: FileWriter,
    blob_dir: FileSystemHandle,
    hasher: Hasher,
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
            hasher: Hasher::new(),
            expected,
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSink for FileBlobSink {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        self.hasher.update(bytes);
        self.writer
            .write_all(bytes)
            .await
            .map_err(|e| BlobError::Io(e.to_string()))
    }

    async fn finish(self: Box<Self>) -> Result<Blake3Hash, BlobError> {
        let FileBlobSink {
            writer,
            blob_dir,
            hasher,
            expected,
        } = *self;

        let hash = Blake3Hash::from(*hasher.finalize().as_bytes());

        if let Some(expected) = expected
            && hash != expected
        {
            let _ = writer.discard().await;
            return Err(BlobError::DigestMismatch {
                expected: expected.as_bytes().to_base58(),
                actual: hash.as_bytes().to_base58(),
            });
        }

        let dest = blob_dir.resolve(&blob_key(&hash))?;
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Read> for FileSystem {
    async fn execute(&self, effect: Capability<Read>) -> Result<BlobReader, BlobError> {
        let digest = effect.digest();
        let handle = self.blob()?.resolve(&blob_key(digest))?;
        if !handle.exists().await {
            return Err(BlobError::NotFound(blob_key(digest)));
        }
        let (offset, length) = match effect.range() {
            Some(range) => (range.offset, range.length),
            None => (0, None),
        };
        let reader = handle.reader_range(offset, length).await?;
        Ok(Box::new(FileBlobSource(reader)))
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::resource::Resource;
    use dialog_capability::Subject;
    use dialog_effects::prelude::*;
    use dialog_effects::storage::{Directory, Location as StorageLocation};

    async fn test_space(name: &str) -> FileSystem {
        let location = StorageLocation::new(Directory::Temp, &crate::helpers::unique_name(name));
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
        let mut sink = subject().archive().blob().write().perform(&fs).await.unwrap();
        for chunk in payload.chunks(8192) {
            sink.write_all(chunk).await.unwrap();
        }
        let hash = sink.finish().await.unwrap();
        assert_eq!(hash, expected_hash);

        // Read the whole blob back by hash.
        let reader = subject()
            .archive()
            .blob()
            .read(hash.clone())
            .perform(&fs)
            .await
            .unwrap();
        assert_eq!(drain(reader).await, payload);

        // Ranged read: 9 bytes from offset 10.
        let reader = subject()
            .archive()
            .blob()
            .invoke(Read::range(hash, 10, Some(9)))
            .perform(&fs)
            .await
            .unwrap();
        assert_eq!(drain(reader).await, payload[10..19]);
    }

    #[dialog_common::test]
    async fn it_reports_missing_blobs() {
        let fs = test_space("blob-missing").await;
        let result = subject()
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
            .archive()
            .blob()
            .import(digest.clone(), payload.len() as u64)
            .perform(&fs)
            .await
            .unwrap();
        sink.write_all(&payload).await.unwrap();
        assert_eq!(sink.finish().await.unwrap(), digest.clone());

        let reader = subject().archive().blob().read(digest).perform(&fs).await.unwrap();
        assert_eq!(drain(reader).await, payload);

        // Import claiming a wrong digest is rejected at finish.
        let wrong = Blake3Hash::from([0u8; 32]);
        let mut sink = subject()
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
}
