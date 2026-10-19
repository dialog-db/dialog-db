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

use super::{FileReader, FileSystem, FileSystemError, FileSystemHandle, FileWriter};
use crate::bao::OutboardBuilder;
use async_trait::async_trait;
use base58::ToBase58;
use dialog_capability::{Capability, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::blob::prelude::{BlobImportExt as _, BlobReadExt as _, BlobSizeExt as _};
use dialog_effects::blob::{
    BlobError, BlobReader, BlobSink, BlobSource, BlobWriter, Import, Read, Size, Write,
};
use futures_util::StreamExt;

const BLOB: &str = "blob";
/// Base name for the staging handle. Only its uniquely-suffixed temp file is
/// ever created; the content commits straight to the hash path.
const STAGING: &str = "_staging";
/// The extension of the outboard of a blob, beside it.
const OUTBOARD: &str = "outboard";

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
}
