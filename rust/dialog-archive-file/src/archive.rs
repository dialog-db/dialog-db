//! [`FileArchive`]: the archive effects over one file.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Weak};

use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_common::{Blake3Hash, Buffer};
use dialog_effects::archive::prelude::{GetExt, ImportExt, PutExt};
use dialog_effects::archive::{ArchiveError, Get, Import, Put};
use parking_lot::{Mutex, RwLock};

use crate::file::{
    FOOTER, HEADER, Kind, MAGIC, Record, put_record, read_at, read_record, write_at,
};

/// The files this process has open, so a second open of one shares it.
static OPEN: LazyLock<Mutex<HashMap<PathBuf, Weak<Inner>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Bytes appended between checkpoints, by default.
pub const CHECKPOINT_EVERY: u64 = 16 * 1024 * 1024;

/// How a [`FileArchive`] writes its file.
#[derive(Clone, Debug)]
pub struct Options {
    /// Bytes appended between checkpoints. Opening a file reads its latest
    /// checkpoint, then every record after it, so this bounds what an open
    /// reads beyond the checkpoint itself.
    pub checkpoint_every: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            checkpoint_every: CHECKPOINT_EVERY,
        }
    }
}

/// Where a block's bytes sit in the file.
#[derive(Clone, Copy, Debug)]
struct Extent {
    offset: u64,
    len: u32,
}

/// Every block in the file, by catalog and hash.
type Index = HashMap<String, HashMap<Blake3Hash, Extent>>;

/// Where the next append goes, and what the last checkpoint covered.
struct Writer {
    end: u64,
    /// The latest checkpoint record, or 0 for none.
    checkpoint: u64,
    /// Bytes appended since that checkpoint.
    since: u64,
}

struct Inner {
    file: File,
    path: PathBuf,
    options: Options,
    index: RwLock<Index>,
    writer: Mutex<Writer>,
}

/// A dialog archive kept in one append-only file.
///
/// It answers [`Get`], [`Put`] and [`Import`] the way a file-per-block
/// archive does, so the tree, queries and sync read and write it as they do
/// any archive. Every block is stored whole, as its own record, and found
/// through an index of every block's catalog, hash and place in the file,
/// held in memory.
///
/// The file is a magic number, then records: blocks, checkpoints of the
/// index, and a footer that ends every append. Opening a file reads the
/// checkpoint its footer names and then the records after it. A file that
/// does not end in a footer was cut short; opening it reads every record
/// from the start and drops the torn tail.
///
/// One file is one space, as one directory is for the `fs` provider: the
/// subject an effect names is not part of where a block is kept. One process
/// writes a file at a time. Durability matches the `fs` provider: appends
/// are not synced. Call [`FileArchive::sync`] for a point that is.
///
/// Clones share the file.
#[derive(Clone)]
pub struct FileArchive {
    inner: Arc<Inner>,
}

fn storage_error(path: &Path, error: io::Error) -> ArchiveError {
    ArchiveError::Storage(format!("{}: {error}", path.display()))
}

/// The body of a block record: its catalog, then its bytes.
fn block_body(catalog: &str, bytes: &[u8]) -> io::Result<Vec<u8>> {
    let name = catalog.as_bytes();
    let length = u8::try_from(name.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("catalog name over 255 bytes: {catalog}"),
        )
    })?;
    let mut body = Vec::with_capacity(1 + name.len() + bytes.len());
    body.push(length);
    body.extend_from_slice(name);
    body.extend_from_slice(bytes);
    Ok(body)
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// A catalog name at the start of `bytes`, and the bytes after it.
fn take_catalog(bytes: &[u8]) -> io::Result<(String, &[u8])> {
    let (&length, rest) = bytes.split_first().ok_or_else(|| invalid("no catalog"))?;
    let length = length as usize;
    if rest.len() < length {
        return Err(invalid("a catalog name runs past its record"));
    }
    let name =
        std::str::from_utf8(&rest[..length]).map_err(|_| invalid("a catalog name is not UTF-8"))?;
    Ok((name.to_string(), &rest[length..]))
}

/// Adds the block a block record holds to `index`.
fn index_block(index: &mut Index, record: &Record) -> io::Result<()> {
    let (catalog, bytes) = take_catalog(&record.body)?;
    let offset = record.body_offset() + (record.body.len() - bytes.len()) as u64;
    let hash = Blake3Hash::hash(bytes);
    index.entry(catalog).or_default().insert(
        hash,
        Extent {
            offset,
            len: bytes.len() as u32,
        },
    );
    Ok(())
}

/// The body of a checkpoint record: every block's catalog, hash and extent.
fn checkpoint_body(index: &Index) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    for (catalog, blocks) in index {
        for (hash, extent) in blocks {
            let name = catalog.as_bytes();
            body.push(u8::try_from(name.len()).map_err(|_| invalid("catalog over 255 bytes"))?);
            body.extend_from_slice(name);
            body.extend_from_slice(hash.as_bytes());
            body.extend_from_slice(&extent.offset.to_le_bytes());
            body.extend_from_slice(&extent.len.to_le_bytes());
        }
    }
    Ok(body)
}

fn parse_checkpoint(body: &[u8]) -> io::Result<Index> {
    let mut index = Index::new();
    let mut rest = body;
    while !rest.is_empty() {
        let (catalog, after) = take_catalog(rest)?;
        if after.len() < 32 + 8 + 4 {
            return Err(invalid("a checkpoint entry runs past its record"));
        }
        let hash = Blake3Hash::from(<[u8; 32]>::try_from(&after[..32]).expect("32 bytes"));
        let offset = u64::from_le_bytes(after[32..40].try_into().expect("8 bytes"));
        let len = u32::from_le_bytes(after[40..44].try_into().expect("4 bytes"));
        index
            .entry(catalog)
            .or_default()
            .insert(hash, Extent { offset, len });
        rest = &after[44..];
    }
    Ok(index)
}

/// The checkpoint the file's footer names, if the file ends in a whole
/// footer: `Some(0)` for a footer that names none.
fn footer(file: &File, len: u64) -> io::Result<Option<u64>> {
    if len < MAGIC.len() as u64 + FOOTER {
        return Ok(None);
    }
    match read_record(file, len - FOOTER, len)? {
        Some(record) if record.kind == Kind::Footer && record.body.len() == 8 => Ok(Some(
            u64::from_le_bytes(record.body[..].try_into().expect("8 bytes")),
        )),
        _ => Ok(None),
    }
}

impl FileArchive {
    /// Opens the archive at `path`, creating it when there is no file there.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ArchiveError> {
        Self::open_with(path, Options::default())
    }

    /// Opens the archive at `path` with `options`.
    ///
    /// A file this process already has open is shared, not opened twice, so
    /// every handle appends after the same end; the shared handle keeps the
    /// options it was first opened with.
    pub fn open_with(path: impl AsRef<Path>, options: Options) -> Result<Self, ArchiveError> {
        let path = path.as_ref().to_path_buf();
        let key = std::path::absolute(&path).map_err(|error| storage_error(&path, error))?;
        let mut open = OPEN.lock();
        open.retain(|_, inner| inner.strong_count() > 0);
        if let Some(inner) = open.get(&key).and_then(Weak::upgrade) {
            return Ok(Self { inner });
        }
        let (file, index, writer) =
            Self::recover(&path).map_err(|error| storage_error(&path, error))?;
        let inner = Arc::new(Inner {
            file,
            path,
            options,
            index: RwLock::new(index),
            writer: Mutex::new(writer),
        });
        open.insert(key, Arc::downgrade(&inner));
        Ok(Self { inner })
    }

    fn recover(path: &Path) -> io::Result<(File, Index, Writer)> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            write_at(&file, 0, MAGIC)?;
            let start = MAGIC.len() as u64;
            let writer = Writer {
                end: start,
                checkpoint: 0,
                since: 0,
            };
            return Ok((file, Index::new(), writer));
        }
        let mut magic = [0u8; 8];
        if len < MAGIC.len() as u64 || {
            read_at(&file, 0, &mut magic)?;
            &magic != MAGIC
        } {
            return Err(invalid("not a dialog archive file"));
        }

        // Start from the checkpoint the footer names; with no footer, or a
        // checkpoint that does not read back, from the first record.
        let start = MAGIC.len() as u64;
        let mut index = Index::new();
        let mut checkpoint = 0;
        let mut at = start;
        if let Some(named) = footer(&file, len)?
            && named != 0
            && let Some(record) = read_record(&file, named, len)?
            && record.kind == Kind::Checkpoint
        {
            index = parse_checkpoint(&record.body)?;
            checkpoint = named;
            at = record.end();
        }
        let mut checkpoint_end = at;
        while at < len {
            let Some(record) = read_record(&file, at, len)? else {
                break;
            };
            match record.kind {
                Kind::Block => index_block(&mut index, &record)?,
                Kind::Checkpoint => {
                    checkpoint = record.offset;
                    checkpoint_end = record.end();
                }
                Kind::Footer => {}
            }
            at = record.end();
        }
        // A tail that is not whole records was being written when the file
        // was last closed: drop it, so the next append follows whole ones.
        if at < len {
            file.set_len(at)?;
        }
        let writer = Writer {
            end: at,
            checkpoint,
            since: at - checkpoint_end,
        };
        Ok((file, index, writer))
    }

    /// Where the file is.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// How many blocks the archive holds, in every catalog.
    pub fn len(&self) -> usize {
        self.inner.index.read().values().map(HashMap::len).sum()
    }

    /// Whether the archive holds no block.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Makes everything appended so far durable.
    pub fn sync(&self) -> Result<(), ArchiveError> {
        let _writer = self.inner.writer.lock();
        self.inner
            .file
            .sync_data()
            .map_err(|error| storage_error(&self.inner.path, error))
    }

    /// The block `hash` names in `catalog`, if the archive holds it.
    pub fn get(&self, catalog: &str, hash: &Blake3Hash) -> Result<Option<Vec<u8>>, ArchiveError> {
        let Some(extent) = self
            .inner
            .index
            .read()
            .get(catalog)
            .and_then(|blocks| blocks.get(hash))
            .copied()
        else {
            return Ok(None);
        };
        let mut bytes = vec![0u8; extent.len as usize];
        read_at(&self.inner.file, extent.offset, &mut bytes)
            .map_err(|error| storage_error(&self.inner.path, error))?;
        Ok(Some(bytes))
    }

    /// Appends the `blocks` the archive does not hold yet to `catalog`, then
    /// a footer, and a checkpoint first when one is due.
    pub fn append(&self, catalog: &str, blocks: &[Buffer]) -> Result<(), ArchiveError> {
        self.write(catalog, blocks)
            .map_err(|error| storage_error(&self.inner.path, error))
    }

    fn write(&self, catalog: &str, blocks: &[Buffer]) -> io::Result<()> {
        let mut writer = self.inner.writer.lock();
        let fresh: Vec<&Buffer> = {
            let index = self.inner.index.read();
            let held = index.get(catalog);
            let mut seen = HashSet::new();
            blocks
                .iter()
                .filter(|block| {
                    let hash = block.blake3_hash();
                    !held.is_some_and(|held| held.contains_key(hash)) && seen.insert(hash.clone())
                })
                .collect()
        };
        if fresh.is_empty() {
            return Ok(());
        }

        let mut bytes = Vec::new();
        let mut extents = Vec::with_capacity(fresh.len());
        for block in &fresh {
            let content = block.as_ref();
            let record = writer.end + bytes.len() as u64;
            put_record(&mut bytes, Kind::Block, &block_body(catalog, content)?)?;
            extents.push((
                block.blake3_hash().clone(),
                Extent {
                    offset: record + HEADER + 1 + catalog.len() as u64,
                    len: content.len() as u32,
                },
            ));
        }
        write_at(&self.inner.file, writer.end, &bytes)?;
        writer.end += bytes.len() as u64;
        writer.since += bytes.len() as u64;
        {
            let mut index = self.inner.index.write();
            index
                .entry(catalog.to_string())
                .or_default()
                .extend(extents);
        }

        let mut tail = Vec::new();
        if writer.since >= self.inner.options.checkpoint_every {
            let body = checkpoint_body(&self.inner.index.read())?;
            writer.checkpoint = writer.end;
            put_record(&mut tail, Kind::Checkpoint, &body)?;
            writer.since = 0;
        }
        put_record(&mut tail, Kind::Footer, &writer.checkpoint.to_le_bytes())?;
        write_at(&self.inner.file, writer.end, &tail)?;
        writer.end += tail.len() as u64;
        Ok(())
    }
}

#[async_trait]
impl Provider<Get> for FileArchive {
    async fn execute(&self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        self.get(effect.catalog(), effect.digest())
    }
}

#[async_trait]
impl Provider<Put> for FileArchive {
    async fn execute(&self, effect: Capability<Put>) -> Result<(), ArchiveError> {
        let block = Buffer::from(effect.content().to_vec());
        self.append(effect.catalog(), std::slice::from_ref(&block))
    }
}

#[async_trait]
impl Provider<Import> for FileArchive {
    async fn execute(&self, effect: Capability<Import>) -> Result<(), ArchiveError> {
        self.append(effect.catalog(), effect.blocks())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Did, Subject};
    use dialog_effects::prelude::*;

    fn subject() -> Subject {
        let did: Did = "did:test:file-archive".parse().expect("a test DID");
        Subject::from(did)
    }

    fn block(text: &str) -> Buffer {
        Buffer::from(text.as_bytes().to_vec())
    }

    #[tokio::test]
    async fn it_returns_none_for_missing_content() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let archive = FileArchive::open(directory.path().join("archive"))?;
        let result = subject()
            .reader()
            .archive()
            .catalog("index")
            .get(Blake3Hash::hash(b"nonexistent"))
            .perform(&archive)
            .await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn it_stores_and_retrieves_content() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let archive = FileArchive::open(directory.path().join("archive"))?;
        let content = block("hello world");
        subject()
            .writer()
            .archive()
            .catalog("index")
            .put(content.clone())
            .perform(&archive)
            .await?;
        let result = subject()
            .reader()
            .archive()
            .catalog("index")
            .get(content.blake3_hash().clone())
            .perform(&archive)
            .await?;
        assert_eq!(result.as_deref(), Some(content.as_ref()));
        Ok(())
    }

    #[tokio::test]
    async fn it_keeps_catalogs_apart() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let archive = FileArchive::open(directory.path().join("archive"))?;
        let content = block("in one catalog");
        subject()
            .writer()
            .archive()
            .catalog("one")
            .put(content.clone())
            .perform(&archive)
            .await?;
        let other = subject()
            .reader()
            .archive()
            .catalog("other")
            .get(content.blake3_hash().clone())
            .perform(&archive)
            .await?;
        assert!(other.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn it_imports_a_batch() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let archive = FileArchive::open(directory.path().join("archive"))?;
        let blocks: Vec<Buffer> = (0..32).map(|n| block(&format!("block {n}"))).collect();
        subject()
            .writer()
            .archive()
            .catalog("index")
            .import(blocks.clone())
            .perform(&archive)
            .await?;
        for content in &blocks {
            let read = archive.get("index", content.blake3_hash())?;
            assert_eq!(read.as_deref(), Some(content.as_ref()));
        }
        assert_eq!(archive.len(), 32);
        Ok(())
    }

    #[tokio::test]
    async fn it_writes_a_block_it_holds_only_once() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let archive = FileArchive::open(&path)?;
        let content = block("written twice");
        archive.append("index", &[content.clone(), content.clone()])?;
        let size = std::fs::metadata(&path)?.len();
        archive.append("index", std::slice::from_ref(&content))?;
        assert_eq!(std::fs::metadata(&path)?.len(), size);
        assert_eq!(archive.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn it_reads_what_it_wrote_after_reopening() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let blocks: Vec<Buffer> = (0..100).map(|n| block(&format!("block {n}"))).collect();
        {
            // A checkpoint every few blocks, so the reopen starts from one
            // and reads the records after it.
            let archive = FileArchive::open_with(
                &path,
                Options {
                    checkpoint_every: 256,
                },
            )?;
            for chunk in blocks.chunks(7) {
                archive.append("index", chunk)?;
            }
        }
        let archive = FileArchive::open(&path)?;
        assert_eq!(archive.len(), 100);
        for content in &blocks {
            let read = archive.get("index", content.blake3_hash())?;
            assert_eq!(read.as_deref(), Some(content.as_ref()));
        }
        Ok(())
    }

    #[tokio::test]
    async fn it_drops_a_torn_tail_and_keeps_the_whole_records() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let kept: Vec<Buffer> = (0..10).map(|n| block(&format!("kept {n}"))).collect();
        let torn = block("torn");
        let whole = {
            let archive = FileArchive::open(&path)?;
            archive.append("index", &kept)?;
            let whole = std::fs::metadata(&path)?.len();
            archive.append("index", std::slice::from_ref(&torn))?;
            whole
        };
        // Cut the last append off in the middle of its block record.
        let file = OpenOptions::new().write(true).open(&path)?;
        file.set_len(whole + HEADER + 2)?;
        drop(file);

        let archive = FileArchive::open(&path)?;
        assert_eq!(archive.len(), 10);
        assert!(archive.get("index", torn.blake3_hash())?.is_none());
        assert_eq!(std::fs::metadata(&path)?.len(), whole);

        // The next append follows the whole records, and reads back.
        archive.append("index", std::slice::from_ref(&torn))?;
        drop(archive);
        let reopened = FileArchive::open(&path)?;
        assert_eq!(reopened.len(), 11);
        assert_eq!(
            reopened.get("index", torn.blake3_hash())?.as_deref(),
            Some(torn.as_ref())
        );
        Ok(())
    }

    #[tokio::test]
    async fn it_stops_at_a_record_whose_bytes_changed() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let first = block("first");
        let second = block("second");
        let boundary = {
            let archive = FileArchive::open(&path)?;
            archive.append("index", std::slice::from_ref(&first))?;
            let boundary = std::fs::metadata(&path)?.len();
            archive.append("index", std::slice::from_ref(&second))?;
            boundary
        };
        // Flip a byte inside the second block's record: it and everything
        // after it no longer read back as whole records.
        let mut bytes = std::fs::read(&path)?;
        bytes[boundary as usize + HEADER as usize + 3] ^= 0xFF;
        std::fs::write(&path, &bytes)?;

        let archive = FileArchive::open(&path)?;
        assert_eq!(archive.len(), 1);
        assert!(archive.get("index", first.blake3_hash())?.is_some());
        assert!(archive.get("index", second.blake3_hash())?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn it_refuses_a_file_that_is_not_an_archive() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        std::fs::write(&path, b"something else entirely")?;
        assert!(matches!(
            FileArchive::open(&path),
            Err(ArchiveError::Storage(_))
        ));
        Ok(())
    }
}
