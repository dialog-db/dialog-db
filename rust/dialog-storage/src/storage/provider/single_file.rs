//! Opt-in native append-only archive with file-wide token sharing.
//!
//! A codec separates a wire block into metadata and reusable tokens. Only new
//! tokens are appended; block frames refer to their earlier file offsets. The
//! archive always returns the original wire bytes and verifies their hash.
//! Memory cells and assets can remain on another provider through [`super::Space`].
//!
//! One open handle owns an exclusive OS file lock; clone it to share access.
//! Each completed record has a checksum. An incomplete final record is removed
//! on reopen, while a complete corrupt record is rejected. Like the filesystem
//! archive, writes do not fsync: this protects against process interruption,
//! not loss of acknowledged writes after power failure. Call [`SingleFile::sync`]
//! before publishing a durable root when power-loss durability is required.

use dialog_capability::{Capability, Provider};
use dialog_effects::archive::prelude::{GetExt, ImportExt, PutExt};
use dialog_effects::archive::{ArchiveError, Get, Import, Put};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

const MAGIC: &[u8; 8] = b"DLGAR001";
const MAX_RECORD: usize = 256 * 1024 * 1024;

/// A block split into private metadata and file-wide reusable byte tokens.
#[derive(Debug)]
pub struct BlockFrame {
    /// Codec-specific reconstruction instructions, without shared token bytes.
    pub metadata: Vec<u8>,
    /// Tokens referenced by the metadata, in codec-defined order.
    pub tokens: Vec<Vec<u8>>,
}

/// Lossless transformation between wire blocks and dictionary frames.
///
/// The identifier is persisted and checked when opening the file. A codec must
/// change it whenever its metadata format changes. `encode` must accept opaque
/// blocks as well as any specialized node format it recognizes.
pub trait FrameCodec: Send + Sync + 'static {
    /// Stable on-disk codec identifier.
    fn identifier(&self) -> &'static str;
    /// Split a wire block into metadata and tokens.
    fn encode(&self, bytes: &[u8]) -> io::Result<BlockFrame>;
    /// Reconstruct the exact wire bytes.
    fn decode(&self, metadata: &[u8], tokens: &[&[u8]]) -> io::Result<Vec<u8>>;
}

/// Opaque-block codec, useful for testing or archives without tree nodes.
#[derive(Debug, Clone, Copy, Default)]
pub struct RawFrameCodec;
impl FrameCodec for RawFrameCodec {
    fn identifier(&self) -> &'static str {
        "raw-v1"
    }
    fn encode(&self, bytes: &[u8]) -> io::Result<BlockFrame> {
        Ok(BlockFrame {
            metadata: Vec::new(),
            tokens: vec![bytes.to_vec()],
        })
    }
    fn decode(&self, metadata: &[u8], tokens: &[&[u8]]) -> io::Result<Vec<u8>> {
        if !metadata.is_empty() || tokens.len() != 1 {
            return Err(invalid("invalid raw frame"));
        }
        Ok(tokens[0].to_vec())
    }
}

/// A cloneable native archive backed by one exclusively locked file.
#[derive(Clone)]
pub struct SingleFile<C = RawFrameCodec> {
    inner: Arc<Mutex<Log>>,
    codec: Arc<C>,
}
struct Log {
    file: File,
    end: u64,
    tokens: HashMap<Arc<[u8]>, u64>,
    offsets: HashMap<u64, Arc<[u8]>>,
    blocks: HashMap<(String, [u8; 32]), u64>,
    poisoned: bool,
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn storage(error: impl std::fmt::Display) -> ArchiveError {
    ArchiveError::Storage(error.to_string())
}
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn take<'a>(bytes: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
    if n > bytes.len() {
        return Err(invalid("truncated frame"));
    }
    let (head, tail) = bytes.split_at(n);
    *bytes = tail;
    Ok(head)
}
fn number(bytes: &mut &[u8]) -> io::Result<u64> {
    Ok(u64::from_le_bytes(take(bytes, 8)?.try_into().unwrap()))
}
fn sized<'a>(bytes: &mut &'a [u8]) -> io::Result<&'a [u8]> {
    let n = usize::try_from(number(bytes)?).map_err(|_| invalid("length overflow"))?;
    take(bytes, n)
}
fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// Resident-index accounting, excluding allocator/hash-table overhead.
#[derive(Debug, Clone, Copy)]
pub struct ArchiveStats {
    /// Distinct shared tokens.
    pub tokens: usize,
    /// Token payload bytes (shared by both indexes).
    pub token_bytes: usize,
    /// Lower bound including index entries and catalog strings, not allocator overhead.
    pub index_bytes_lower_bound: usize,
}

impl<C: FrameCodec> SingleFile<C> {
    /// Report resident token/index lower bounds for workload measurements.
    pub fn stats(&self) -> io::Result<ArchiveStats> {
        let log = self
            .inner
            .lock()
            .map_err(|_| invalid("archive lock poisoned"))?;
        let token_bytes = log.offsets.values().map(|v| v.len()).sum::<usize>();
        let index_bytes_lower_bound = token_bytes
            + log.offsets.len()
                * (2 * std::mem::size_of::<(u64, Arc<[u8]>)>() + 2 * std::mem::size_of::<usize>())
            + log
                .blocks
                .iter()
                .map(|((catalog, _), _)| {
                    std::mem::size_of::<((String, [u8; 32]), u64)>() + catalog.len()
                })
                .sum::<usize>();
        Ok(ArchiveStats {
            tokens: log.offsets.len(),
            token_bytes,
            index_bytes_lower_bound,
        })
    }
    /// Snapshot catalog/hash keys, for archive verification and migration tools.
    pub fn entries(&self) -> io::Result<Vec<(String, [u8; 32])>> {
        let log = self
            .inner
            .lock()
            .map_err(|_| invalid("archive lock poisoned"))?;
        Ok(log.blocks.keys().cloned().collect())
    }

    /// Open or create an archive. The parent directory must already exist.
    /// A second independently opened handle fails while this handle is alive.
    pub async fn open(path: impl AsRef<Path>, codec: C) -> io::Result<Self> {
        let path = path.as_ref().to_owned();
        let codec = Arc::new(codec);
        let id = codec.identifier().to_owned();
        let log = tokio::task::spawn_blocking(move || Log::open(&path, &id))
            .await
            .map_err(io::Error::other)??;
        Ok(Self {
            inner: Arc::new(Mutex::new(log)),
            codec,
        })
    }
    /// Flush completed records to durable storage before publishing a root.
    pub async fn sync(&self) -> io::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let log = inner.lock().map_err(|_| invalid("archive lock poisoned"))?;
            log.healthy()?;
            log.file.sync_all()
        })
        .await
        .map_err(io::Error::other)?
    }
    /// Read a block synchronously on the calling thread.
    ///
    /// This uses the same codec, verification and log as [`Self::get`], but
    /// deliberately excludes async worker scheduling. It is useful for CPU
    /// profiling under a thread-local collection marker. Do not call it on an
    /// async executor in an application; use [`Self::get`] there.
    pub fn get_blocking(&self, catalog: &str, hash: [u8; 32]) -> io::Result<Option<Vec<u8>>> {
        self.inner
            .lock()
            .map_err(|_| invalid("archive lock poisoned"))?
            .get(catalog, hash, self.codec.as_ref())
    }
    /// Store a block synchronously on the calling thread.
    ///
    /// This executes the same encoding, hashing, dictionary lookup and I/O as
    /// [`Self::put`], without worker scheduling. See [`Self::get_blocking`].
    pub fn put_blocking(&self, catalog: &str, bytes: &[u8]) -> io::Result<[u8; 32]> {
        self.inner
            .lock()
            .map_err(|_| invalid("archive lock poisoned"))?
            .put(catalog, bytes, self.codec.as_ref())
    }

    /// Read a catalog's block by its existing BLAKE3 wire hash.
    pub async fn get(&self, catalog: &str, hash: [u8; 32]) -> io::Result<Option<Vec<u8>>> {
        let inner = self.inner.clone();
        let codec = self.codec.clone();
        let catalog = catalog.to_owned();
        tokio::task::spawn_blocking(move || {
            inner
                .lock()
                .map_err(|_| invalid("archive lock poisoned"))?
                .get(&catalog, hash, codec.as_ref())
        })
        .await
        .map_err(io::Error::other)?
    }
    /// Store a wire block, returning its unchanged BLAKE3 hash.
    pub async fn put(&self, catalog: &str, bytes: Vec<u8>) -> io::Result<[u8; 32]> {
        let inner = self.inner.clone();
        let codec = self.codec.clone();
        let catalog = catalog.to_owned();
        tokio::task::spawn_blocking(move || {
            inner
                .lock()
                .map_err(|_| invalid("archive lock poisoned"))?
                .put(&catalog, &bytes, codec.as_ref())
        })
        .await
        .map_err(io::Error::other)?
    }
}

impl Log {
    fn healthy(&self) -> io::Result<()> {
        if self.poisoned {
            Err(invalid(
                "archive write failed; close and reopen before reuse",
            ))
        } else {
            Ok(())
        }
    }
    fn open(path: &Path, codec: &str) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.try_lock().map_err(io::Error::other)?;
        let mut header = MAGIC.to_vec();
        put_bytes(&mut header, codec.as_bytes());
        let len = file.metadata()?.len();
        if len == 0 {
            file.write_all(&header)?;
        } else {
            let mut actual = vec![0; header.len()];
            file.read_exact(&mut actual)?;
            if actual != header {
                return Err(invalid("archive format or codec mismatch"));
            }
        }
        let mut log = Self {
            file,
            end: header.len() as u64,
            tokens: HashMap::new(),
            offsets: HashMap::new(),
            blocks: HashMap::new(),
            poisoned: false,
        };
        let len = log.file.metadata()?.len();
        while log.end < len {
            let start = log.end;
            if len - start < 16 {
                log.file.set_len(start)?;
                break;
            }
            log.file.seek(SeekFrom::Start(start))?;
            let mut size = [0; 8];
            log.file.read_exact(&mut size)?;
            let size = u64::from_le_bytes(size);
            let mut complement = [0; 8];
            log.file.read_exact(&mut complement)?;
            if u64::from_le_bytes(complement) != !size {
                return Err(invalid("archive record length checksum mismatch"));
            }
            // An impossible length is corruption, not evidence of a torn tail.
            if size > MAX_RECORD as u64 {
                return Err(invalid("archive record exceeds size limit"));
            }
            if len - start - 16 < size + 32 {
                log.file.set_len(start)?;
                break;
            }
            let payload = log.read_record(start)?;
            log.index(start, &payload)?;
            log.end += 16 + size + 32;
        }
        Ok(log)
    }
    fn read_record(&mut self, at: u64) -> io::Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(at))?;
        let mut size = [0; 8];
        self.file.read_exact(&mut size)?;
        let size = u64::from_le_bytes(size);
        let mut complement = [0; 8];
        self.file.read_exact(&mut complement)?;
        if u64::from_le_bytes(complement) != !size {
            return Err(invalid("archive record length checksum mismatch"));
        }
        if size > MAX_RECORD as u64 {
            return Err(invalid("archive record exceeds size limit"));
        }
        let mut payload = vec![0; size as usize];
        self.file.read_exact(&mut payload)?;
        let mut hash = [0; 32];
        self.file.read_exact(&mut hash)?;
        if blake3::hash(&payload).as_bytes() != &hash {
            return Err(invalid("archive checksum mismatch"));
        }
        Ok(payload)
    }
    fn index(&mut self, at: u64, payload: &[u8]) -> io::Result<()> {
        let mut data = payload;
        match take(&mut data, 1)?[0] {
            0 => {
                let token: Arc<[u8]> = Arc::from(data);
                self.tokens.insert(token.clone(), at);
                self.offsets.insert(at, token);
            }
            1 => {
                let catalog = std::str::from_utf8(sized(&mut data)?)
                    .map_err(io::Error::other)?
                    .to_owned();
                let hash = take(&mut data, 32)?.try_into().unwrap();
                let _metadata = sized(&mut data)?;
                let count = number(&mut data)?;
                if count > (data.len() / 8) as u64 {
                    return Err(invalid("invalid token count"));
                }
                for _ in 0..count {
                    let offset = number(&mut data)?;
                    if offset >= at || !self.offsets.contains_key(&offset) {
                        return Err(invalid("invalid token offset"));
                    }
                }
                if !data.is_empty() {
                    return Err(invalid("trailing frame bytes"));
                }
                self.blocks.insert((catalog, hash), at);
            }
            _ => return Err(invalid("unknown record kind")),
        }
        Ok(())
    }
    fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        self.healthy()?;
        if payload.len() > MAX_RECORD {
            return Err(invalid("archive record exceeds size limit"));
        }
        let at = self.end;
        let result = (|| {
            self.file.seek(SeekFrom::Start(at))?;
            self.file.write_all(&(payload.len() as u64).to_le_bytes())?;
            self.file
                .write_all(&(!(payload.len() as u64)).to_le_bytes())?;
            self.file.write_all(payload)?;
            self.file.write_all(blake3::hash(payload).as_bytes())?;
            Ok(())
        })();
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }
        self.end += 16 + payload.len() as u64 + 32;
        self.index(at, payload)?;
        Ok(at)
    }
    fn put(
        &mut self,
        catalog: &str,
        bytes: &[u8],
        codec: &impl FrameCodec,
    ) -> io::Result<[u8; 32]> {
        self.healthy()?;
        let hash = *blake3::hash(bytes).as_bytes();
        if self.blocks.contains_key(&(catalog.to_owned(), hash)) {
            return Ok(hash);
        }
        let frame = codec.encode(bytes)?;
        // A codec cannot silently alter the archive's content-addressing contract.
        let refs: Vec<_> = frame.tokens.iter().map(Vec::as_slice).collect();
        if codec.decode(&frame.metadata, &refs)? != bytes {
            return Err(invalid("codec changed wire bytes"));
        }
        let mut payload = vec![1];
        put_bytes(&mut payload, catalog.as_bytes());
        payload.extend_from_slice(&hash);
        put_bytes(&mut payload, &frame.metadata);
        put_u64(&mut payload, frame.tokens.len() as u64);
        for token in frame.tokens {
            let offset = match self.tokens.get(token.as_slice()) {
                Some(offset) => *offset,
                None => {
                    let mut record = vec![0];
                    record.extend_from_slice(&token);
                    self.append(&record)?
                }
            };
            put_u64(&mut payload, offset);
        }
        self.append(&payload)?;
        Ok(hash)
    }
    fn get(
        &mut self,
        catalog: &str,
        hash: [u8; 32],
        codec: &impl FrameCodec,
    ) -> io::Result<Option<Vec<u8>>> {
        self.healthy()?;
        let Some(at) = self.blocks.get(&(catalog.to_owned(), hash)).copied() else {
            return Ok(None);
        };
        let payload = self.read_record(at)?;
        let mut data = &payload[1..];
        let _catalog = sized(&mut data)?;
        take(&mut data, 32)?;
        let metadata = sized(&mut data)?;
        let count = number(&mut data)?;
        if count > (data.len() / 8) as u64 {
            return Err(invalid("invalid token count"));
        }
        let mut tokens = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let offset = number(&mut data)?;
            tokens.push(
                self.offsets
                    .get(&offset)
                    .ok_or_else(|| invalid("missing token"))?
                    .as_ref(),
            );
        }
        let bytes = codec.decode(metadata, &tokens)?;
        if blake3::hash(&bytes).as_bytes() != &hash {
            return Err(invalid("wire block hash mismatch"));
        }
        Ok(Some(bytes))
    }
}

#[async_trait::async_trait]
impl<C: FrameCodec> Provider<Get> for SingleFile<C> {
    async fn execute(&self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        self.get(effect.catalog(), *effect.digest().as_bytes())
            .await
            .map_err(storage)
    }
}
#[async_trait::async_trait]
impl<C: FrameCodec> Provider<Put> for SingleFile<C> {
    async fn execute(&self, effect: Capability<Put>) -> Result<(), ArchiveError> {
        self.put(effect.catalog(), effect.content().to_vec())
            .await
            .map_err(storage)?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl<C: FrameCodec> Provider<Import> for SingleFile<C> {
    async fn execute(&self, effect: Capability<Import>) -> Result<(), ArchiveError> {
        for block in effect.blocks() {
            self.put(effect.catalog(), block.as_ref().to_vec())
                .await
                .map_err(storage)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

mod proof;
pub use proof::RangeSnapshot;
