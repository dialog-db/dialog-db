use super::*;
use std::io::Cursor;

/// An authenticated immutable prefix of a single-file archive.
///
/// Construction streams the file once and builds a BAO outboard in memory
/// (roughly 6.25% of the file). Extracting subsequent proofs reads only the
/// requested range and its boundary chunks. Later appends do not change this
/// snapshot's root. A snapshot keeps the archive's exclusive lock alive.
#[derive(Clone)]
pub struct RangeSnapshot {
    inner: Arc<Mutex<Log>>,
    root: [u8; 32],
    length: u64,
    outboard: Arc<[u8]>,
}
impl<C: FrameCodec> SingleFile<C> {
    /// Authenticate the current completed prefix for verified range reads.
    ///
    /// This is deliberately explicit: ordinary archive operations do not pay
    /// to rebuild a BAO tree after every append. Keep the snapshot to serve many
    /// ranges from one root. It does not fsync; see [`SingleFile::sync`].
    pub async fn snapshot(&self) -> io::Result<RangeSnapshot> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut log = inner.lock().map_err(|_| invalid("archive lock poisoned"))?;
            log.healthy()?;
            let length = log.end;
            log.file.seek(SeekFrom::Start(0))?;
            let mut encoder = bao::encode::Encoder::new_outboard(Cursor::new(Vec::new()));
            let copied = io::copy(&mut (&mut log.file).take(length), &mut encoder)?;
            if copied != length {
                return Err(invalid("archive truncated during snapshot"));
            }
            let root = *encoder.finalize()?.as_bytes();
            let outboard = Arc::from(encoder.into_inner().into_inner());
            drop(log);
            Ok(RangeSnapshot {
                inner,
                root,
                length,
                outboard,
            })
        })
        .await
        .map_err(io::Error::other)?
    }
}
impl RangeSnapshot {
    /// BLAKE3 hash authenticating every byte in this snapshot prefix.
    pub fn root(&self) -> [u8; 32] {
        self.root
    }
    /// Number of archive bytes authenticated by the root.
    pub fn len(&self) -> u64 {
        self.length
    }
    /// Whether the snapshot contains no bytes (archives include a header).
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
    /// Resident BAO outboard bytes; excludes the archive's shared indexes.
    pub fn outboard_bytes(&self) -> usize {
        self.outboard.len()
    }
    /// Produce a BAO slice covering `offset..offset+length` in this snapshot.
    /// The proof includes boundary chunks and authentication hashes. A verifier
    /// must obtain the expected root and range from its trusted request context.
    pub async fn prove(&self, offset: u64, length: u64) -> io::Result<Vec<u8>> {
        if offset
            .checked_add(length)
            .is_none_or(|end| end > self.length)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "range outside snapshot",
            ));
        }
        let snapshot = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut log = snapshot
                .inner
                .lock()
                .map_err(|_| invalid("archive lock poisoned"))?;
            log.healthy()?;
            log.file.seek(SeekFrom::Start(0))?;
            let mut extractor = bao::encode::SliceExtractor::new_outboard(
                &mut log.file,
                Cursor::new(snapshot.outboard.as_ref()),
                offset,
                length,
            );
            let mut proof = Vec::new();
            extractor.read_to_end(&mut proof)?;
            Ok(proof)
        })
        .await
        .map_err(io::Error::other)?
    }
    /// Verify an untrusted BAO slice against an independently trusted root and
    /// requested range, returning exactly those bytes or an error. Limit the
    /// requested length at the application boundary for untrusted requests.
    pub fn verify(root: [u8; 32], offset: u64, length: u64, proof: &[u8]) -> io::Result<Vec<u8>> {
        if offset.checked_add(length).is_none() {
            return Err(invalid("range overflow"));
        }
        let hash = blake3::Hash::from(root);
        let mut decoder = bao::decode::SliceDecoder::new(proof, &hash, offset, length);
        let mut bytes = Vec::new();
        decoder.read_to_end(&mut bytes)?;
        if bytes.len() as u64 != length {
            return Err(invalid("proof did not contain requested range"));
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn verifies_ranges_and_rejects_tampering_after_append() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("archive");
        let archive = SingleFile::open(&path, RawFrameCodec).await?;
        archive
            .put("tree", (0..20_000).map(|i| (i % 251) as u8).collect())
            .await?;
        let snapshot = archive.snapshot().await?;
        let original = std::fs::read(&path)?;
        assert_eq!(snapshot.root(), *blake3::hash(&original).as_bytes());
        archive.put("tree", b"a later commit".to_vec()).await?;
        for (offset, len) in [
            (0, 1),
            (17, 2000),
            (1024, 2048),
            (snapshot.len() - 7, 7),
            (0, 0),
        ] {
            let proof = snapshot.prove(offset, len).await?;
            assert_eq!(
                RangeSnapshot::verify(snapshot.root(), offset, len, &proof)?,
                original[offset as usize..(offset + len) as usize]
            );
            let mut tampered = proof.clone();
            if let Some(last) = tampered.last_mut() {
                *last ^= 1;
            }
            assert!(RangeSnapshot::verify(snapshot.root(), offset, len, &tampered).is_err());
            assert!(RangeSnapshot::verify([7; 32], offset, len, &proof).is_err());
            assert!(
                RangeSnapshot::verify(snapshot.root(), offset, len, &proof[..proof.len() / 2])
                    .is_err()
            );
        }
        assert!(snapshot.prove(snapshot.len(), 1).await.is_err());
        assert!(snapshot.prove(u64::MAX, 2).await.is_err());
        Ok(())
    }
}
