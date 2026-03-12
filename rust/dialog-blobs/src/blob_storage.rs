//! Content-addressed blob storage.
//!
//! Stores blobs in a sharded directory structure based on their Blake3 hash.
//! Supports concurrent writes and atomic moves across platforms.

use base58::ToBase58;
use dialog_common::Blake3Hash;
use futures_core::Stream;
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;

use crate::{DialogBlobsError, FileReader, Vfs};

/// Derives the content-addressed path for a blob from its Blake3 hash.
fn hash_to_path(hash: &Blake3Hash) -> String {
    let b58 = hash.as_bytes().to_base58();
    format!("{}/{}/{}/{}", &b58[0..2], &b58[2..4], &b58[4..6], b58)
}

/// High-level abstraction over platform-specific content-addressed blob
/// storage.
///
/// Blobs are stored in a sharded directory tree derived from the base58
/// encoding of their hash. Given a hash that encodes to `AbCdEfGh...`:
///
/// ```text
/// <vfs root>/
///   Ab/
///     Cd/
///       Ef/
///         AbCdEfGh...   <- full base58 hash as filename
/// ```
///
/// On native targets, native file system APIs are used. On the web, the
/// origin-private file system is used.
pub struct BlobStorage {
    fs: Vfs,
}

impl BlobStorage {
    /// Creates a new [`BlobStorage`] backed by the given [`Vfs`].
    pub fn new(fs: Vfs) -> Self {
        Self { fs }
    }

    /// Stores a blob from a stream of bytes.
    /// Returns the Blake3 hash of the stored blob.
    pub async fn put<S>(&mut self, mut bytes: S) -> Result<Blake3Hash, DialogBlobsError>
    where
        S: Stream<Item = Vec<u8>> + Unpin,
    {
        let mut hasher = blake3::Hasher::new();

        // Each writer gets a unique temp file (ULID), so concurrent writes
        // never interfere during the streaming phase.
        let temp_name = format!(".tmp-{}", ulid::Ulid::new());
        let mut writer = self.fs.write_to(&temp_name).await?;

        while let Some(chunk) = bytes.next().await {
            hasher.update(&chunk);
            writer
                .write_all(&chunk)
                .await
                .map_err(|error| DialogBlobsError::Put(format!("Write failed: {error}")))?;
        }

        writer
            .shutdown()
            .await
            .map_err(|error| DialogBlobsError::Put(format!("Shutdown failed: {error}")))?;

        let hash = Blake3Hash::from(*hasher.finalize().as_bytes());
        let path = hash_to_path(&hash);

        // Concurrent moves to the same content-addressed path are safe:
        //
        // - Native: rename(2) is atomic, so the last writer wins
        //   harmlessly (content is identical by definition).
        // - Web: FileSystemFileHandle.move(parent, name) is supported
        //   by all major browsers (Chromium, Firefox, Safari) and is
        //   atomic, same as native rename. A stream-copy fallback with
        //   Web Lock serialization exists for edge cases where the
        //   native move is unavailable.
        //
        // In all cases the worst outcome is redundant I/O, never
        // corruption or partial reads.
        self.fs.move_file(&temp_name, &path).await?;

        Ok(hash)
    }

    /// Retrieves a blob by its Blake3 hash.
    ///
    /// Returns `Ok(None)` when the blob does not exist, or propagates
    /// storage errors (I/O failures, permission issues, etc.) as `Err`.
    pub async fn get(&self, hash: Blake3Hash) -> Result<Option<FileReader>, DialogBlobsError> {
        let path = hash_to_path(&hash);

        match self.fs.read_from(&path).await {
            Ok(reader) => Ok(Some(reader)),
            Err(DialogBlobsError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use wasm_bindgen_test::wasm_bindgen_test_configure;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test_configure!(run_in_service_worker);

    #[cfg(not(target_arch = "wasm32"))]
    struct TestHarness {
        storage: BlobStorage,
        _dir: tempfile::TempDir,
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl TestHarness {
        fn make_storage(&self) -> BlobStorage {
            BlobStorage::new(Vfs::new(self._dir.path().to_path_buf()))
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn harness() -> TestHarness {
        let dir = tempfile::tempdir().unwrap();
        let vfs = Vfs::new(dir.path().to_path_buf());
        TestHarness {
            storage: BlobStorage::new(vfs),
            _dir: dir,
        }
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    struct TestHarness {
        storage: BlobStorage,
        root: String,
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    impl TestHarness {
        fn make_storage(&self) -> BlobStorage {
            BlobStorage::new(Vfs::new(self.root.clone()))
        }
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    fn harness() -> TestHarness {
        let root = format!("test/{}", ulid::Ulid::new());
        let vfs = Vfs::new(root.clone());
        TestHarness {
            storage: BlobStorage::new(vfs),
            root,
        }
    }

    async fn collect_reader(mut reader: FileReader) -> Vec<u8> {
        let mut buf = Vec::new();
        while let Some(chunk) = reader.next().await {
            buf.extend_from_slice(&chunk.unwrap());
        }
        buf
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_a_blob() {
        let mut h = harness();
        let data = b"hello world";

        let hash = h
            .storage
            .put(stream::iter(vec![data.to_vec()]))
            .await
            .unwrap();
        let reader = h
            .storage
            .get(hash)
            .await
            .unwrap()
            .expect("blob should exist");
        assert_eq!(collect_reader(reader).await, data);
    }

    #[dialog_common::test]
    async fn it_returns_the_correct_hash() {
        let mut h = harness();
        let data = b"test content";

        let hash = h
            .storage
            .put(stream::iter(vec![data.to_vec()]))
            .await
            .unwrap();
        let expected = Blake3Hash::hash(data);
        assert_eq!(hash, expected);
    }

    #[dialog_common::test]
    async fn it_handles_multiple_chunks() {
        let mut h = harness();
        let chunks = vec![b"chunk1".to_vec(), b"chunk2".to_vec(), b"chunk3".to_vec()];
        let full_data: Vec<u8> = chunks.iter().flatten().copied().collect();

        let hash = h.storage.put(stream::iter(chunks)).await.unwrap();
        let reader = h
            .storage
            .get(hash)
            .await
            .unwrap()
            .expect("blob should exist");
        assert_eq!(collect_reader(reader).await, full_data);
    }

    #[dialog_common::test]
    async fn it_returns_none_for_nonexistent_blob() {
        let h = harness();
        let fake_hash = Blake3Hash::from([0u8; 32]);
        assert!(h.storage.get(fake_hash).await.unwrap().is_none());
    }

    #[dialog_common::test]
    async fn it_produces_consistent_hashes_for_identical_content() {
        let mut h = harness();
        let data = b"deterministic";

        let hash1 = h
            .storage
            .put(stream::iter(vec![data.to_vec()]))
            .await
            .unwrap();
        let hash2 = h
            .storage
            .put(stream::iter(vec![data.to_vec()]))
            .await
            .unwrap();
        assert_eq!(hash1, hash2);
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_many_blobs() {
        let mut h = harness();
        let mut hashes = Vec::new();

        for i in 0..20 {
            let data = format!("blob number {i}");
            let hash = h
                .storage
                .put(stream::iter(vec![data.into_bytes()]))
                .await
                .unwrap();
            hashes.push(hash);
        }

        for (i, hash) in hashes.into_iter().enumerate() {
            let expected = format!("blob number {i}");
            let reader = h
                .storage
                .get(hash)
                .await
                .unwrap()
                .expect("blob should exist");
            assert_eq!(collect_reader(reader).await, expected.into_bytes());
        }
    }

    #[dialog_common::test]
    async fn it_reads_multiple_blobs_concurrently() {
        let mut h = harness();
        let mut hashes = Vec::new();

        for i in 0..10 {
            let data = format!("concurrent read {i}");
            let hash = h
                .storage
                .put(stream::iter(vec![data.into_bytes()]))
                .await
                .unwrap();
            hashes.push(hash);
        }

        let reads: Vec<_> = hashes
            .iter()
            .map(|hash| h.storage.get(hash.clone()))
            .collect();

        let results = futures_util::future::join_all(reads).await;

        for (i, result) in results.into_iter().enumerate() {
            let expected = format!("concurrent read {i}");
            let reader = result.unwrap().expect("blob should exist");
            assert_eq!(collect_reader(reader).await, expected.into_bytes());
        }
    }

    /// Returns a stream that yields each chunk with an explicit
    /// yield between items, forcing the async runtime to interleave
    /// concurrent tasks at every chunk boundary.
    fn yielding_stream(chunks: Vec<Vec<u8>>) -> impl Stream<Item = Vec<u8>> + Unpin {
        Box::pin(stream::iter(chunks).then(|chunk| async {
            // tokio::task::yield_now requires the `rt` feature, which is
            // unavailable on wasm32. A resolved JS Promise serves the same
            // purpose: it defers to the next microtask, letting other
            // futures make progress.
            #[cfg(not(target_arch = "wasm32"))]
            tokio::task::yield_now().await;

            #[cfg(target_arch = "wasm32")]
            {
                let _ = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(
                    &wasm_bindgen::JsValue::UNDEFINED,
                ))
                .await;
            }

            chunk
        }))
    }

    #[dialog_common::test]
    async fn it_writes_identical_blobs_concurrently() {
        let h = harness();

        // Two writers produce identical multi-chunk content. The
        // yielding stream forces the runtime to interleave execution
        // at every chunk boundary. Each put() streams to a unique temp
        // file (safe), then moves it to the same content-addressed
        // destination. On the web slow path, without the Web Lock,
        // both moves would stream-copy to the destination concurrently,
        // interleaving their writes and corrupting the blob.
        let chunks: Vec<Vec<u8>> = (0..8).map(|i| format!("chunk-{i}-").into_bytes()).collect();
        let full_data: Vec<u8> = chunks.iter().flatten().copied().collect();

        let mut writer_a = h.make_storage();
        let mut writer_b = h.make_storage();

        let (result_a, result_b) = futures_util::future::join(
            writer_a.put(yielding_stream(chunks.clone())),
            writer_b.put(yielding_stream(chunks.clone())),
        )
        .await;

        let hash_a = result_a.unwrap();
        let hash_b = result_b.unwrap();

        assert_eq!(
            hash_a, hash_b,
            "identical content must produce identical hashes"
        );

        let reader = h
            .storage
            .get(hash_a)
            .await
            .unwrap()
            .expect("blob should exist");
        assert_eq!(collect_reader(reader).await, full_data);
    }

    #[dialog_common::test]
    async fn it_writes_blobs_concurrently_across_instances() {
        let h = harness();

        let futs: Vec<_> = (0..10)
            .map(|i| {
                let mut storage = h.make_storage();
                async move {
                    let data = format!("parallel write {i}");
                    let hash = storage
                        .put(stream::iter(vec![data.into_bytes()]))
                        .await
                        .unwrap();
                    (i, hash)
                }
            })
            .collect();

        let results = futures_util::future::join_all(futs).await;

        for (i, hash) in results {
            let expected = format!("parallel write {i}");
            let reader = h
                .storage
                .get(hash)
                .await
                .unwrap()
                .expect("blob should exist");
            assert_eq!(collect_reader(reader).await, expected.into_bytes());
        }
    }
}
