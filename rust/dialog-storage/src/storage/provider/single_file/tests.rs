use super::*;

#[tokio::test]
async fn roundtrip_reopen_catalogs_and_exclusive_lock() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("archive");
    let archive = SingleFile::open(&path, RawFrameCodec).await?;
    assert!(SingleFile::open(&path, RawFrameCodec).await.is_err());
    let hash = archive.put("tree", b"hello".to_vec()).await?;
    assert_eq!(archive.get("tree", hash).await?, Some(b"hello".to_vec()));
    assert_eq!(archive.get("other", hash).await?, None);
    let size = std::fs::metadata(&path)?.len();
    archive.put("tree", b"hello".to_vec()).await?;
    assert_eq!(std::fs::metadata(&path)?.len(), size);
    archive.put("other", b"hello".to_vec()).await?;
    archive.put("tree", vec![]).await?;
    archive.sync().await?;
    drop(archive);
    let archive = SingleFile::open(&path, RawFrameCodec).await?;
    assert_eq!(archive.get("other", hash).await?, Some(b"hello".to_vec()));
    assert_eq!(
        archive.get("tree", *blake3::hash(&[]).as_bytes()).await?,
        Some(vec![])
    );
    Ok(())
}

#[tokio::test]
async fn every_torn_append_preserves_previous_block() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("source");
    let archive = SingleFile::open(&path, RawFrameCodec).await?;
    let old = archive.put("tree", b"previous".to_vec()).await?;
    let before = std::fs::metadata(&path)?.len() as usize;
    let new = archive.put("tree", b"next block".to_vec()).await?;
    drop(archive);
    let bytes = std::fs::read(&path)?;
    for end in before..=bytes.len() {
        let cut = dir.path().join("cut");
        std::fs::write(&cut, &bytes[..end])?;
        let archive = SingleFile::open(&cut, RawFrameCodec).await?;
        assert_eq!(archive.get("tree", old).await?, Some(b"previous".to_vec()));
        assert_eq!(
            archive.get("tree", new).await?.is_some(),
            end == bytes.len()
        );
        archive.put("tree", b"after recovery".to_vec()).await?;
    }
    Ok(())
}

#[tokio::test]
async fn rejects_corruption_and_codec_mismatch() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("archive");
    let archive = SingleFile::open(&path, RawFrameCodec).await?;
    archive.put("tree", b"hello".to_vec()).await?;
    drop(archive);
    let original = std::fs::read(&path)?;
    for at in [0, original.len() - 1, MAGIC.len() + 8 + "raw-v1".len() + 9] {
        let mut bytes = original.clone();
        bytes[at] ^= 1;
        std::fs::write(&path, bytes)?;
        assert!(SingleFile::open(&path, RawFrameCodec).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn cloned_handles_serialize_writes() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let archive = SingleFile::open(dir.path().join("archive"), RawFrameCodec).await?;
    let jobs = (0..32).map(|i| {
        let archive = archive.clone();
        async move { archive.put("tree", vec![i; 100]).await }
    });
    let hashes = futures_util::future::try_join_all(jobs).await?;
    for (i, hash) in hashes.into_iter().enumerate() {
        assert_eq!(archive.get("tree", hash).await?, Some(vec![i as u8; 100]));
    }
    Ok(())
}

#[tokio::test]
async fn corrupt_middle_length_never_truncates_history() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("archive");
    let archive = SingleFile::open(&path, RawFrameCodec).await?;
    archive.put("tree", b"first".to_vec()).await?;
    archive.put("tree", b"second".to_vec()).await?;
    drop(archive);
    let mut bytes = std::fs::read(&path)?;
    let first = MAGIC.len() + 8 + "raw-v1".len();
    bytes[first + 2] ^= 1;
    std::fs::write(&path, &bytes)?;
    assert!(SingleFile::open(&path, RawFrameCodec).await.is_err());
    assert_eq!(std::fs::read(&path)?, bytes);
    Ok(())
}

#[tokio::test]
async fn profiling_entrypoints_run_codec_on_calling_thread() -> io::Result<()> {
    struct ThreadCodec(std::thread::ThreadId);
    impl FrameCodec for ThreadCodec {
        fn identifier(&self) -> &'static str {
            "thread-probe-v1"
        }
        fn encode(&self, bytes: &[u8]) -> io::Result<BlockFrame> {
            assert_eq!(std::thread::current().id(), self.0);
            RawFrameCodec.encode(bytes)
        }
        fn decode(&self, metadata: &[u8], tokens: &[&[u8]]) -> io::Result<Vec<u8>> {
            assert_eq!(std::thread::current().id(), self.0);
            RawFrameCodec.decode(metadata, tokens)
        }
    }
    let dir = tempfile::tempdir()?;
    let archive = SingleFile::open(
        dir.path().join("archive"),
        ThreadCodec(std::thread::current().id()),
    )
    .await?;
    let hash = archive.put_blocking("tree", b"profiled bytes")?;
    assert_eq!(
        archive.get_blocking("tree", hash)?,
        Some(b"profiled bytes".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn compressed_frames_reopen_and_recover_every_torn_tail() -> io::Result<()> {
    struct MetadataCodec;
    impl FrameCodec for MetadataCodec {
        fn identifier(&self) -> &'static str {
            "metadata-v1"
        }
        fn encode(&self, bytes: &[u8]) -> io::Result<BlockFrame> {
            Ok(BlockFrame {
                metadata: bytes.to_vec(),
                tokens: vec![],
            })
        }
        fn decode(&self, metadata: &[u8], tokens: &[&[u8]]) -> io::Result<Vec<u8>> {
            assert!(tokens.is_empty());
            Ok(metadata.to_vec())
        }
    }
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("archive");
    let archive = SingleFile::open(&path, MetadataCodec).await?;
    let old = archive.put("tree", b"old".to_vec()).await?;
    let before = std::fs::metadata(&path)?.len() as usize;
    let content = vec![7; 100_000];
    let hash = archive.put("tree", content.clone()).await?;
    drop(archive);
    let bytes = std::fs::read(&path)?;
    assert!(bytes.len() - before < content.len() / 10);
    assert_eq!(bytes[before + 16], 2, "compressed frame discriminator");
    for end in before..=bytes.len() {
        let cut = dir.path().join("cut");
        std::fs::write(&cut, &bytes[..end])?;
        let archive = SingleFile::open(&cut, MetadataCodec).await?;
        assert_eq!(archive.get("tree", old).await?, Some(b"old".to_vec()));
        let result = archive.get("tree", hash).await?;
        if end == bytes.len() {
            assert_eq!(result, Some(content.clone()));
        } else {
            assert!(result.is_none());
        }
    }
    Ok(())
}

#[test]
fn compressed_frame_expansion_is_bounded_and_validated() -> io::Result<()> {
    let mut payload = vec![1];
    payload.extend_from_slice(&vec![0; 100_000]);
    let packed = pack_record(&payload)?;
    assert_eq!(unpack_record(packed.clone())?, payload);
    let mut oversized = packed.clone();
    oversized[1..9].copy_from_slice(&((MAX_RECORD as u64) + 1).to_le_bytes());
    assert!(unpack_record(oversized).is_err());
    let mut short = packed.clone();
    short[1..9].copy_from_slice(&10u64.to_le_bytes());
    assert!(unpack_record(short).is_err());
    let mut corrupt = packed;
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(unpack_record(corrupt).is_err());
    Ok(())
}
