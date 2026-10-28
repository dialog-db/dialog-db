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
