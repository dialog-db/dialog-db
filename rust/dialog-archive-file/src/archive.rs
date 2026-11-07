//! [`FileArchive`]: the archive effects over one file.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_common::{Blake3Hash, Buffer};
use dialog_effects::archive::prelude::{GetExt, ImportExt, PutExt};
use dialog_effects::archive::{ArchiveError, Get, Import, Put};
use dialog_search_tree::{Manifest, NodeBody};
use parking_lot::Mutex;

use crate::file::{FOOTER, HEADER, Kind, MAGIC, put_record, read_at, read_record, write_at};
use crate::node::{TreeNode, realize, rows_of, tree_node};
use crate::rows::{Reader, Rows, get_rows, invalid, put_rows, put_varint, rename};
use crate::tokens::{Draft, TokenRead, read_token_record};

mod index;
mod served;

pub(crate) use index::{Ids, Index, Map, View};

/// The files this process has open, so a second open of one shares it.
static OPEN: LazyLock<Mutex<HashMap<PathBuf, Weak<Inner>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Bytes appended between checkpoints, by default.
pub const CHECKPOINT_EVERY: u64 = 16 * 1024 * 1024;

/// The longest chain of frames, each written against the one before, a
/// read decodes, by default: every this many versions of a node, a frame is
/// written whole.
pub const CHAIN: usize = 32;

/// How a [`FileArchive`] writes its file.
#[derive(Clone, Debug)]
pub struct Options {
    /// Bytes appended between checkpoints. Opening a file reads its latest
    /// checkpoint, then every record after it, so this bounds what an open
    /// reads beyond the checkpoint itself.
    pub checkpoint_every: u64,
    /// Whether tree nodes are written as frames. Without, every block is
    /// written whole.
    pub frames: bool,
    /// The longest chain of frames a read decodes (see [`CHAIN`]).
    pub chain: usize,
    /// How many nodes' decoded rows are kept, so a read of a node written
    /// against another does not decode that one again.
    pub rows_cached: usize,
    /// Whether every frame is checked by building its block again from the
    /// rows it decodes to and comparing the bytes, keeping the block whole
    /// if they differ. The tree's encoding is canonical (the same entries
    /// under the same manifest make the same bytes, which replicas already
    /// rely on to agree on hashes), so a frame of a node in the current
    /// layout rebuilds its block by construction, and this re-tests that on
    /// every block. On in debug builds, so the tests check it everywhere,
    /// and when `DIALOG_ARCHIVE_VERIFY` is set; off otherwise. Every build
    /// checks that a frame decodes back to its rows, and keeps a node of
    /// another layout whole without framing it.
    pub verify: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            checkpoint_every: CHECKPOINT_EVERY,
            frames: true,
            chain: CHAIN,
            rows_cached: 4096,
            verify: cfg!(debug_assertions) || std::env::var_os("DIALOG_ARCHIVE_VERIFY").is_some(),
        }
    }
}

/// Where a block's bytes sit in the file.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Extent {
    offset: u64,
    len: u32,
}

/// How the file holds a block: whole, or as a node's frame.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Place {
    Block(Extent),
    Node(u32),
}

/// What a node is known by when the file looks for the node it supersedes:
/// its catalog, its kind and its first key's token ids.
type Lineage = (u16, u8, [u32; 4]);

/// A node the file holds as a frame.
pub(crate) struct Node {
    hash: Blake3Hash,
    /// The frame record's body.
    extent: Extent,
    /// How many frames its rows are written against, one after another.
    depth: u8,
    /// Its catalog, by its place in [`State::catalogs`].
    catalog: u16,
    kind: u8,
    lineage: Option<[u32; 4]>,
}

/// What a frame record says ahead of its rows.
struct FrameHeader {
    catalog: String,
    hash: Blake3Hash,
    kind: u8,
    base: Option<u32>,
    manifest: u32,
    lineage: Option<[u32; 4]>,
}

impl FrameHeader {
    fn put(&self, out: &mut Vec<u8>) -> io::Result<()> {
        put_catalog(out, &self.catalog)?;
        out.extend_from_slice(self.hash.as_bytes());
        out.push(self.kind);
        put_varint(out, self.base.map_or(0, |base| base as u64 + 1));
        put_varint(out, self.manifest as u64);
        match self.lineage {
            None => out.push(0),
            Some(first) => {
                out.push(1);
                for id in first {
                    put_varint(out, id as u64);
                }
            }
        }
        Ok(())
    }

    fn get(reader: &mut Reader) -> io::Result<Self> {
        let catalog = get_catalog(reader)?;
        let hash = Blake3Hash::from(<[u8; 32]>::try_from(reader.take(32)?).expect("32 bytes"));
        let kind = reader.byte()?;
        let base = match reader.varint()? {
            0 => None,
            base => Some(u32::try_from(base - 1).map_err(|_| invalid("a node id over 32 bits"))?),
        };
        let manifest = reader.u32()?;
        let lineage = match reader.byte()? {
            0 => None,
            _ => Some([reader.u32()?, reader.u32()?, reader.u32()?, reader.u32()?]),
        };
        Ok(Self {
            catalog,
            hash,
            kind,
            base,
            manifest,
            lineage,
        })
    }
}

/// Where the next append goes, and what the last checkpoint covered.
struct Writer {
    end: u64,
    /// The latest checkpoint record, or 0 for none.
    checkpoint: u64,
    /// Bytes appended since that checkpoint.
    since: u64,
}

/// The decoded rows of the nodes read most recently, by node id. A read
/// takes no lock; keeping one takes a short one, for the order they go in.
struct RowsCache {
    capacity: usize,
    rows: Map<u32, (Arc<Rows>, u32)>,
    order: Mutex<VecDeque<u32>>,
}

impl RowsCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            rows: Map::default(),
            order: Mutex::new(VecDeque::new()),
        }
    }

    /// A node's rows and its manifest token.
    fn get(&self, id: u32) -> Option<(Arc<Rows>, u32)> {
        self.rows.pin().get(&id).cloned()
    }

    fn put(&self, id: u32, rows: (Arc<Rows>, u32)) {
        if self.capacity == 0 || self.rows.pin().try_insert(id, rows).is_err() {
            return;
        }
        let mut order = self.order.lock();
        order.push_back(id);
        while order.len() > self.capacity {
            if let Some(evicted) = order.pop_front() {
                self.rows.pin().remove(&evicted);
            }
        }
    }

    fn clear(&self) {
        let mut order = self.order.lock();
        self.rows.pin().clear();
        order.clear();
    }

    /// The memory the kept rows take.
    fn footprint(&self) -> usize {
        self.rows
            .pin()
            .iter()
            .map(|(_, (rows, _))| rows.footprint())
            .sum()
    }
}

/// Where time goes, summed over every append and read since open.
#[derive(Default)]
struct Timings {
    split: AtomicU64,
    encode: AtomicU64,
    verify: AtomicU64,
    write: AtomicU64,
    decode: AtomicU64,
    realize: AtomicU64,
    /// Nodes served to trees in place, and the time spent finding where
    /// their parts are.
    served: AtomicU64,
    serve: AtomicU64,
    /// Blocks built back from rows for a `Get`, and blocks offered to an
    /// append (before those already held are skipped).
    realized: AtomicU64,
    offered: AtomicU64,
    /// Of `split`, the part spent parsing a block into a node.
    parse: AtomicU64,
    /// Whole appends, lock and catching up included, and how many.
    append: AtomicU64,
    appends: AtomicU64,
    /// Nodes kept whole for being in another layout, and for a frame that
    /// did not rebuild the block when verified.
    other_layout: AtomicU64,
    unverified: AtomicU64,
}

impl Timings {
    fn add(counter: &AtomicU64, since: Instant) {
        counter.fetch_add(since.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    fn get(counter: &AtomicU64) -> Duration {
        Duration::from_nanos(counter.load(Ordering::Relaxed))
    }
}

/// With `DIALOG_ARCHIVE_TIMINGS` set, an archive reports where its time
/// went when the last handle to it closes: for profiling what an
/// application's appends and reads spend in the file.
impl Drop for Inner {
    fn drop(&mut self) {
        if std::env::var_os("DIALOG_ARCHIVE_TIMINGS").is_none() {
            return;
        }
        let ms = |counter: &AtomicU64| Timings::get(counter).as_secs_f64() * 1e3;
        let timings = &self.timings;
        eprintln!(
            "archive {}: split {:.1} ms (parse {:.1} ms), encode {:.1} ms, verify {:.1} ms, write {:.1} ms, \
             decode {:.1} ms, realize {:.1} ms ({} blocks built for gets), serve {:.1} ms \
             ({} nodes served), {} blocks offered to appends, {} appends taking {:.1} ms, \
             kept whole: {} of another layout, {} not rebuilt when verified",
            self.path.display(),
            ms(&timings.split),
            ms(&timings.parse),
            ms(&timings.encode),
            ms(&timings.verify),
            ms(&timings.write),
            ms(&timings.decode),
            ms(&timings.realize),
            timings.realized.load(Ordering::Relaxed),
            ms(&timings.serve),
            timings.served.load(Ordering::Relaxed),
            timings.offered.load(Ordering::Relaxed),
            timings.appends.load(Ordering::Relaxed),
            ms(&timings.append),
            timings.other_layout.load(Ordering::Relaxed),
            timings.unverified.load(Ordering::Relaxed),
        );
    }
}

struct Inner {
    timings: Timings,
    file: File,
    path: PathBuf,
    options: Options,
    /// Where everything the file holds is, read without a lock.
    index: Index,
    rows: RowsCache,
    /// Each manifest token, decoded, for the nodes served to trees.
    manifests: Map<u32, Arc<Manifest>>,
    /// The writer's turn in this process: where the next append goes.
    writer: Mutex<Writer>,
    /// The end of what this process has read of the file or is writing to
    /// it, so a read that misses asks for another process's appends only
    /// when the file grew past it, and takes no lock otherwise.
    known: AtomicU64,
    /// Called by an append once its bytes are in the file, before it
    /// publishes them: for tests that read while an append is under way.
    #[cfg(test)]
    before_publish: Mutex<Option<Box<dyn FnMut() + Send>>>,
    /// Called by an append after each step of publishing: for tests that
    /// check every step leaves the index whole.
    #[cfg(test)]
    published: Mutex<Option<PublishedHook>>,
}

/// What a test runs after each step of publishing an append.
#[cfg(test)]
type PublishedHook = Box<dyn FnMut(&FileArchive) + Send>;

/// What a [`FileArchive`] holds, and what holding it costs.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    /// The file's length.
    pub file_bytes: u64,
    /// Blocks held whole.
    pub blocks: usize,
    /// Their bytes.
    pub block_bytes: u64,
    /// Nodes held as frames.
    pub frames: usize,
    /// Frames written against another node's rows.
    pub frames_against: usize,
    /// Their bytes, headers included.
    pub frame_bytes: u64,
    /// Distinct tokens.
    pub tokens: usize,
    /// Their bytes in the file.
    pub token_bytes: u64,
    /// The memory the index of the file takes: tokens, nodes, places.
    pub index_memory: usize,
    /// The memory the decoded rows kept for reads take.
    pub rows_memory: usize,
    /// Time appends spent reading blocks as nodes and splitting them into
    /// rows and tokens.
    pub split: Duration,
    /// Time appends spent finding each node's predecessor and writing its
    /// frame.
    pub encode: Duration,
    /// Time appends spent checking each frame reads back and realizes to
    /// its block.
    pub verify: Duration,
    /// Time appends spent writing to the file.
    pub write: Duration,
    /// Time reads spent decoding frames into rows.
    pub decode: Duration,
    /// Time reads spent realizing rows into blocks.
    pub realize: Duration,
    /// Nodes served to trees in place ([`NodeSource`](dialog_search_tree::NodeSource)).
    pub served: u64,
    /// Time spent serving them: finding where each part of each row is.
    pub serve: Duration,
}

/// A dialog archive kept in one append-only file.
///
/// It answers [`Get`], [`Put`] and [`Import`] the way a file-per-block
/// archive does, so the tree, queries and sync read and write it as they do
/// any archive.
///
/// A node of the artifact tree is kept as a **frame**: its entries, links
/// and buffered operations as rows of ids into the file's **tokens**, each
/// distinct entity, attribute, value and datum kept once. A frame is
/// written against the rows of the node it supersedes (the latest node of
/// its kind in its catalog that starts at the same key) as runs copied
/// from them around what is new, and whole every [`Options::chain`]
/// versions. A frame is only written once its rows read back and realize
/// to the exact block it stands for; any block that does not, and anything
/// that is not a tree node, is kept whole. A read of a frame realizes the
/// block through the tree's own encoder.
///
/// The file is a magic number, then records: blocks, tokens, frames,
/// checkpoints of the index, and a footer that ends every append. Opening a
/// file reads the checkpoint its footer names and then the records after
/// it. A file that does not end in a footer was cut short; opening it
/// reads every record from the start and drops the torn tail.
///
/// One file is one space, as one directory is for the `fs` provider: the
/// subject an effect names is not part of where a block is kept.
///
/// Any number of processes open one file. Writers take turns holding an
/// exclusive lock on it: a writer that takes the lock first reads what
/// other processes appended since its last turn, then appends. Readers take
/// no lock: everything up to a footer never changes, and a read that does
/// not find a block reads the appends finished since ([`refresh`]).
/// Durability matches the `fs` provider: appends are not synced. Call
/// [`FileArchive::sync`] for a point that is.
///
/// [`refresh`]: FileArchive::refresh
///
/// Clones share the file.
#[derive(Clone)]
pub struct FileArchive {
    inner: Arc<Inner>,
}

fn storage_error(path: &Path, error: io::Error) -> ArchiveError {
    ArchiveError::Storage(format!("{}: {error}", path.display()))
}

fn put_catalog(out: &mut Vec<u8>, catalog: &str) -> io::Result<()> {
    let name = catalog.as_bytes();
    let length = u8::try_from(name.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("catalog name over 255 bytes: {catalog}"),
        )
    })?;
    out.push(length);
    out.extend_from_slice(name);
    Ok(())
}

fn get_catalog(reader: &mut Reader) -> io::Result<String> {
    let length = reader.byte()? as usize;
    let name = reader.take(length)?;
    Ok(std::str::from_utf8(name)
        .map_err(|_| invalid("a catalog name is not UTF-8"))?
        .to_string())
}

/// The body of a block record: its catalog, then its bytes.
fn block_body(catalog: &str, bytes: &[u8]) -> io::Result<Vec<u8>> {
    let mut body = Vec::with_capacity(1 + catalog.len() + bytes.len());
    put_catalog(&mut body, catalog)?;
    body.extend_from_slice(bytes);
    Ok(body)
}

/// What a record adds to the index, read but not published yet.
enum Indexed {
    Tokens(Vec<(u64, u32)>),
    Block {
        catalog: String,
        hash: Blake3Hash,
        extent: Extent,
    },
    Frame {
        header: FrameHeader,
        extent: Extent,
    },
    Nothing,
}

/// What a record adds to the index. A checkpoint adds nothing: its index
/// is already what the records before it added.
fn read_indexed(kind: Kind, offset: u64, body: &[u8]) -> io::Result<Indexed> {
    let body_offset = offset + HEADER;
    Ok(match kind {
        Kind::Block => {
            let mut reader = Reader::new(body);
            let catalog = get_catalog(&mut reader)?;
            let start = 1 + catalog.len();
            let bytes = &body[start..];
            Indexed::Block {
                hash: Blake3Hash::hash(bytes),
                extent: Extent {
                    offset: body_offset + start as u64,
                    len: bytes.len() as u32,
                },
                catalog,
            }
        }
        Kind::Tokens => Indexed::Tokens(read_token_record(body, body_offset)?),
        Kind::Frame => Indexed::Frame {
            header: FrameHeader::get(&mut Reader::new(body))?,
            extent: Extent {
                offset: body_offset,
                len: body.len() as u32,
            },
        },
        Kind::Checkpoint | Kind::Footer => Indexed::Nothing,
    })
}

/// Publishes what a run of records adds, in the order the file holds it,
/// once the run checks: every frame is written against a node the file
/// holds or one the run adds before it. A run is published whole or not
/// at all. The mapping must already reach the run's end.
fn publish(index: &Index, run: Vec<Indexed>) -> io::Result<()> {
    let mut nodes = index.nodes().len();
    for item in &run {
        if let Indexed::Frame { header, .. } = item {
            if header.base.is_some_and(|base| base as usize >= nodes) {
                return Err(invalid("a frame against a node the file lacks"));
            }
            nodes += 1;
        }
    }
    for item in run {
        match item {
            Indexed::Tokens(tokens) => {
                for (offset, len) in tokens {
                    index.add_token(offset, len, None);
                }
            }
            Indexed::Block {
                catalog,
                hash,
                extent,
            } => {
                let catalog = index.add_catalog(&catalog);
                index.add_block(catalog, hash, extent);
            }
            Indexed::Frame { header, extent } => {
                let depth = header
                    .base
                    .and_then(|base| index.node(base))
                    .map_or(0, |base| base.depth + 1);
                let catalog = index.add_catalog(&header.catalog);
                index.add_node(Node {
                    hash: header.hash,
                    extent,
                    depth,
                    catalog,
                    kind: header.kind,
                    lineage: header.lineage,
                });
            }
            Indexed::Nothing => {}
        }
    }
    Ok(())
}

/// The body of a checkpoint record: where every token is, every node, and
/// every whole block.
fn checkpoint_body(index: &Index) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    let catalogs: Vec<&str> = index.catalogs().collect();
    put_varint(&mut body, catalogs.len() as u64);
    for catalog in &catalogs {
        put_catalog(&mut body, catalog)?;
    }
    // Where every token is: tokens are written in id order, so each starts
    // after the one before.
    let tokens = index.tokens();
    put_varint(&mut body, tokens.len() as u64);
    let mut previous = 0;
    for token in tokens.iter() {
        put_varint(&mut body, token.offset - previous);
        put_varint(&mut body, token.len as u64);
        previous = token.offset;
    }
    let nodes = index.nodes();
    put_varint(&mut body, nodes.len() as u64);
    for node in nodes.iter() {
        body.extend_from_slice(node.hash.as_bytes());
        body.extend_from_slice(&node.extent.offset.to_le_bytes());
        body.extend_from_slice(&node.extent.len.to_le_bytes());
        body.push(node.depth);
        put_varint(&mut body, node.catalog as u64);
        body.push(node.kind);
        match node.lineage {
            None => body.push(0),
            Some(first) => {
                body.push(1);
                for id in first {
                    put_varint(&mut body, id as u64);
                }
            }
        }
    }
    for (catalog, hash, place) in index.blocks() {
        let Place::Block(extent) = place else {
            continue;
        };
        let catalog = index
            .catalog(catalog)
            .ok_or_else(|| invalid("a block in a catalog the index lacks"))?;
        put_catalog(&mut body, catalog)?;
        body.extend_from_slice(hash.as_bytes());
        body.extend_from_slice(&extent.offset.to_le_bytes());
        body.extend_from_slice(&extent.len.to_le_bytes());
    }
    Ok(body)
}

/// Reads a checkpoint into `index`, which holds nothing yet.
fn read_checkpoint(index: &Index, len: u64, body: &[u8]) -> io::Result<()> {
    let mut reader = Reader::new(body);
    let fixed =
        |reader: &mut Reader, n: usize| -> io::Result<Vec<u8>> { Ok(reader.take(n)?.to_vec()) };
    let u64_of = |bytes: Vec<u8>| u64::from_le_bytes(bytes.try_into().expect("8 bytes"));
    let u32_of = |bytes: Vec<u8>| u32::from_le_bytes(bytes.try_into().expect("4 bytes"));
    for _ in 0..reader.varint()? {
        let catalog = get_catalog(&mut reader)?;
        index.add_catalog(&catalog);
    }
    let mut offset = 0u64;
    for _ in 0..reader.varint()? {
        offset += reader.varint()?;
        let size = reader.u32()?;
        if offset + size as u64 > len {
            return Err(invalid("a checkpoint names a token past the file's end"));
        }
        index.add_token(offset, size, None);
    }
    for _ in 0..reader.varint()? {
        let hash = Blake3Hash::from(<[u8; 32]>::try_from(reader.take(32)?).expect("32 bytes"));
        let offset = u64_of(fixed(&mut reader, 8)?);
        let size = u32_of(fixed(&mut reader, 4)?);
        let depth = reader.byte()?;
        let catalog = u16::try_from(reader.varint()?).map_err(|_| invalid("a catalog index"))?;
        let kind = reader.byte()?;
        let lineage = match reader.byte()? {
            0 => None,
            _ => Some([reader.u32()?, reader.u32()?, reader.u32()?, reader.u32()?]),
        };
        if index.catalog(catalog).is_none() {
            return Err(invalid("a node in a catalog the file lacks"));
        }
        index.add_node(Node {
            hash,
            extent: Extent { offset, len: size },
            depth,
            catalog,
            kind,
            lineage,
        });
    }
    while !reader.is_empty() {
        let catalog = get_catalog(&mut reader)?;
        let hash = Blake3Hash::from(<[u8; 32]>::try_from(reader.take(32)?).expect("32 bytes"));
        let offset = u64_of(fixed(&mut reader, 8)?);
        let size = u32_of(fixed(&mut reader, 4)?);
        let catalog = index.add_catalog(&catalog);
        index.add_block(catalog, hash, Extent { offset, len: size });
    }
    Ok(())
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

/// The exclusive lock on the file, which writers take turns holding: one
/// append at a time, from any process. Released when dropped.
struct Exclusive<'a>(&'a File);

impl<'a> Exclusive<'a> {
    fn take(file: &'a File) -> io::Result<Self> {
        file.lock()?;
        Ok(Self(file))
    }
}

impl Drop for Exclusive<'_> {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

/// Publishes the records from `writer.end` up to `len` in `index`, and
/// moves `writer.end` past them.
///
/// With `finished` set (another process may be appending), only appends
/// that are whole are read: the records up to each footer, a run at a time.
/// Holding the file's lock (`finished` unset), no append is under way, so
/// every whole record is read: records past the last footer were left by
/// an append that did not finish, and they hold whole blocks and frames
/// whose tokens precede them.
fn follow(
    file: &File,
    index: &Index,
    writer: &mut Writer,
    len: u64,
    finished: bool,
) -> io::Result<()> {
    let mut at = writer.end;
    let (mut checkpoint, mut since) = (writer.checkpoint, writer.since);
    let mut run = Vec::new();
    while at < len {
        let Some(record) = read_record(file, at, len)? else {
            break;
        };
        at = record.end();
        if record.kind == Kind::Checkpoint {
            checkpoint = record.offset;
            since = 0;
        } else {
            since += record.end() - record.offset;
        }
        let footer = record.kind == Kind::Footer;
        run.push(read_indexed(record.kind, record.offset, &record.body)?);
        if footer || !finished {
            index.map(file, at)?;
            publish(index, std::mem::take(&mut run))?;
            writer.end = at;
            writer.checkpoint = checkpoint;
            writer.since = since;
        }
    }
    Ok(())
}

/// Brings `index` up to what the file holds, holding its lock: reads what
/// other processes appended, and drops a tail an append left unfinished,
/// so the next append follows whole records.
fn catch_up(file: &File, index: &Index, writer: &mut Writer) -> io::Result<()> {
    let len = file.metadata()?.len();
    if len > writer.end {
        follow(file, index, writer, len, false)?;
        if len > writer.end {
            file.set_len(writer.end)?;
        }
    }
    Ok(())
}

/// Reads the file into its index, creating it when it is empty: from the
/// checkpoint its footer names, then every record after it. Holds the
/// file's lock, so it never reads, or drops, an append under way.
fn recover(file: &File) -> io::Result<(Index, Writer)> {
    let _exclusive = Exclusive::take(file)?;
    let index = Index::default();
    let len = file.metadata()?.len();
    if len == 0 {
        write_at(file, 0, MAGIC)?;
        let writer = Writer {
            end: MAGIC.len() as u64,
            checkpoint: 0,
            since: 0,
        };
        return Ok((index, writer));
    }
    let mut magic = [0u8; 8];
    if len < MAGIC.len() as u64 || {
        read_at(file, 0, &mut magic)?;
        &magic != MAGIC
    } {
        return Err(invalid("not a dialog archive file"));
    }

    // Start from the checkpoint the footer names; with no footer, or a
    // checkpoint that does not read back, from the first record.
    let mut writer = Writer {
        end: MAGIC.len() as u64,
        checkpoint: 0,
        since: 0,
    };
    if let Some(named) = footer(file, len)?
        && named != 0
        && let Some(record) = read_record(file, named, len)?
        && record.kind == Kind::Checkpoint
    {
        read_checkpoint(&index, len, &record.body)?;
        writer.checkpoint = named;
        writer.end = record.end();
    }
    index.map(file, writer.end)?;
    catch_up(file, &index, &mut writer)?;
    index.map(file, writer.end)?;
    Ok((index, writer))
}

/// `nodes` ordered so every node comes after the children it links to
/// among them, so a parent can name a child by the node id it was given.
fn children_first(nodes: Vec<(Buffer, TreeNode)>) -> Vec<(Buffer, TreeNode)> {
    let at: HashMap<Blake3Hash, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, (block, _))| (block.blake3_hash().clone(), i))
        .collect();
    let mut placed = vec![false; nodes.len()];
    let mut order = Vec::with_capacity(nodes.len());
    for root in 0..nodes.len() {
        let mut stack = vec![(root, false)];
        while let Some((i, expanded)) = stack.pop() {
            if placed[i] {
                continue;
            }
            if expanded {
                placed[i] = true;
                order.push(i);
                continue;
            }
            stack.push((i, true));
            if let NodeBody::Index(index) = nodes[i].1.body() {
                for link in 0..index.len() {
                    if let Ok(hash) = index.hash_at(link)
                        && let Some(&child) = at.get(hash)
                        && !placed[child]
                    {
                        stack.push((child, false));
                    }
                }
            }
        }
    }
    let mut slots: Vec<Option<(Buffer, TreeNode)>> = nodes.into_iter().map(Some).collect();
    order
        .into_iter()
        .map(|i| slots[i].take().expect("each node placed once"))
        .collect()
}

/// A node an append writes as a frame, before it is in the index.
struct Staged {
    hash: Blake3Hash,
    rows: Arc<Rows>,
    manifest: u32,
}

/// A node an append writes as a frame: its rows and manifest token, the
/// node it is written against, its lineage and depth, and the frame
/// record's body.
struct Frame {
    rows: Arc<Rows>,
    manifest: u32,
    base: Option<u32>,
    lineage: Option<[u32; 4]>,
    depth: u8,
    body: Vec<u8>,
}

/// What an append writes for one block: the block whole, or the frame of
/// the node it is, by node id.
enum Item {
    Block(Buffer),
    Frame(u32, Buffer, Frame),
}

/// An append worked out before its turn: the tokens it adds, and its
/// blocks and frames, children before their parents.
struct Prepared<'a> {
    catalog: &'a str,
    draft: Draft<'a>,
    /// The id its first frame was drafted with.
    first_node: u32,
    items: Vec<Item>,
}

/// An append as its turn writes it: the drafted tokens it adds (by the ids
/// they were drafted with, in the order they take their ids), and its
/// blocks and frames, named as they are written.
struct Reconciled<'a> {
    catalog_id: u16,
    draft: Draft<'a>,
    new: Vec<u32>,
    items: Vec<Item>,
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
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| storage_error(&path, error))?;
        let (index, writer) = recover(&file).map_err(|error| storage_error(&path, error))?;
        let known = AtomicU64::new(writer.end);
        let inner = Arc::new(Inner {
            known,
            timings: Timings::default(),
            file,
            path,
            rows: RowsCache::new(options.rows_cached),
            manifests: Map::default(),
            options,
            index,
            writer: Mutex::new(writer),
            #[cfg(test)]
            before_publish: Mutex::new(None),
            #[cfg(test)]
            published: Mutex::new(None),
        });
        open.insert(key, Arc::downgrade(&inner));
        Ok(Self { inner })
    }

    /// The index, for tests that measure what it costs.
    #[cfg(test)]
    pub(crate) fn index(&self) -> &Index {
        &self.inner.index
    }

    /// Where the file is.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// How many blocks the archive holds, in every catalog.
    pub fn len(&self) -> usize {
        self.inner.index.block_count()
    }

    /// Whether the archive keeps the block `hash` names in `catalog` as a
    /// frame (rather than whole, or not at all).
    pub fn is_frame(&self, catalog: &str, hash: &Blake3Hash) -> bool {
        matches!(self.inner.index.place(catalog, hash), Some(Place::Node(_)))
    }

    /// Every block the archive holds, by catalog and hash.
    pub fn hashes(&self) -> Vec<(String, Blake3Hash)> {
        let index = &self.inner.index;
        index
            .blocks()
            .into_iter()
            .filter_map(|(catalog, hash, _)| Some((index.catalog(catalog)?.to_string(), hash)))
            .collect()
    }

    /// Whether the archive holds no block.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// What the archive holds, and what holding it costs.
    pub fn stats(&self) -> Stats {
        let index = &self.inner.index;
        let places = index.blocks();
        let whole = places
            .iter()
            .filter(|(_, _, place)| matches!(place, Place::Block(_)))
            .count();
        let nodes = index.nodes();
        let tokens = index.tokens();
        Stats {
            file_bytes: self.inner.known.load(Ordering::Acquire),
            blocks: whole,
            block_bytes: index.block_bytes(),
            frames: nodes.len(),
            frames_against: nodes.iter().filter(|node| node.depth > 0).count(),
            frame_bytes: index.frame_bytes(),
            tokens: tokens.len(),
            token_bytes: tokens.iter().map(|token| token.len as u64).sum(),
            index_memory: tokens.footprint()
                + nodes.footprint()
                + places.len() * (2 + 32 + size_of::<Place>() + 8)
                + nodes.len() * (32 + 4 + 8)
                + index.lineage_count() * (size_of::<Lineage>() + 4 + 8)
                + index.id_table_len() * (8 + 4 + 8),
            split: Timings::get(&self.inner.timings.split),
            encode: Timings::get(&self.inner.timings.encode),
            verify: Timings::get(&self.inner.timings.verify),
            write: Timings::get(&self.inner.timings.write),
            decode: Timings::get(&self.inner.timings.decode),
            realize: Timings::get(&self.inner.timings.realize),
            served: self.inner.timings.served.load(Ordering::Relaxed),
            serve: Timings::get(&self.inner.timings.serve),
            rows_memory: self.inner.rows.footprint(),
        }
    }

    /// Builds what a writer looks tokens up by (each token's id, by its
    /// bytes), if it is not built yet, by reading every token through the
    /// mapping: the first append of a process does this, so a writer can call
    /// it ahead of time. Returns how long it took.
    pub fn prepare_writer(&self) -> Duration {
        let started = Instant::now();
        self.inner.index.build_ids();
        started.elapsed()
    }

    /// Forgets every node's decoded rows, so the next read of each decodes
    /// its frames from the file.
    pub fn forget_rows(&self) {
        self.inner.rows.clear();
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
        self.read(catalog, hash)
            .map_err(|error| storage_error(&self.inner.path, error))
    }

    /// How the file holds the block `hash` names in `catalog`, looking
    /// again after reading what other processes appended when it is not
    /// found.
    fn place(&self, catalog: &str, hash: &Blake3Hash) -> io::Result<Option<Place>> {
        if let Some(place) = self.inner.index.place(catalog, hash) {
            return Ok(Some(place));
        }
        self.reread()?;
        Ok(self.inner.index.place(catalog, hash))
    }

    fn read(&self, catalog: &str, hash: &Blake3Hash) -> io::Result<Option<Vec<u8>>> {
        let Some(place) = self.place(catalog, hash)? else {
            return Ok(None);
        };
        match place {
            Place::Block(extent) => {
                let mut bytes = vec![0u8; extent.len as usize];
                read_at(&self.inner.file, extent.offset, &mut bytes)?;
                Ok(Some(bytes))
            }
            Place::Node(id) => {
                let (rows, manifest) = self.rows(&HashMap::new(), id)?;
                let started = Instant::now();
                let block = self.realize(&rows, manifest)?;
                Timings::add(&self.inner.timings.realize, started);
                self.inner.timings.realized.fetch_add(1, Ordering::Relaxed);
                Ok(Some(block))
            }
        }
    }

    /// Reads the appends other processes finished since this one last
    /// looked, if the file grew.
    pub fn refresh(&self) -> Result<(), ArchiveError> {
        self.reread()
            .map_err(|error| storage_error(&self.inner.path, error))
    }

    fn reread(&self) -> io::Result<()> {
        // Within what this process read or is writing, nothing is new to
        // read. An append of its own under way is not waited for: until it
        // finishes, the block is not there, and a miss says so.
        let len = self.inner.file.metadata()?.len();
        if len <= self.inner.known.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut writer = self.inner.writer.lock();
        if len <= writer.end {
            return Ok(());
        }
        follow(&self.inner.file, &self.inner.index, &mut writer, len, true)?;
        self.know(writer.end);
        Ok(())
    }

    /// Records that this process has read, or is writing, the file up to
    /// `end`.
    fn know(&self, end: u64) {
        self.inner.known.fetch_max(end, Ordering::AcqRel);
    }

    /// The block a node's rows stand for.
    fn realize(&self, rows: &Rows, manifest: u32) -> io::Result<Vec<u8>> {
        let index = &self.inner.index;
        let view = index.view();
        let manifest = Manifest::decode(
            view.get(manifest)
                .ok_or_else(|| invalid("a manifest token the file lacks"))?,
        )
        .map_err(|error| invalid(&error.to_string()))?;
        realize(rows, &view, &manifest, |id| {
            index.node(id).map(|node| node.hash.clone())
        })
    }

    /// The rows of node `id` and its manifest token, decoded through the
    /// frames it is written against. `staged` holds the rows of nodes an
    /// append has not written yet.
    fn rows(&self, staged: &HashMap<u32, Staged>, id: u32) -> io::Result<(Arc<Rows>, u32)> {
        if let Some(staged) = staged.get(&id) {
            return Ok((staged.rows.clone(), staged.manifest));
        }
        if let Some(found) = self.inner.rows.get(id) {
            return Ok(found);
        }
        let node = self
            .inner
            .index
            .node(id)
            .ok_or_else(|| invalid("a node the file lacks"))?;
        let mut body = vec![0u8; node.extent.len as usize];
        read_at(&self.inner.file, node.extent.offset, &mut body)?;
        let mut reader = Reader::new(&body);
        let header = FrameHeader::get(&mut reader)?;
        let base = match header.base {
            None => None,
            Some(base) => Some(self.rows(staged, base)?.0),
        };
        let started = Instant::now();
        let rows = Arc::new(get_rows(&mut reader, header.kind, base.as_deref())?);
        Timings::add(&self.inner.timings.decode, started);
        self.inner.rows.put(id, (rows.clone(), header.manifest));
        Ok((rows, header.manifest))
    }

    /// Appends the `blocks` the archive does not hold yet to `catalog`, then
    /// a footer, and a checkpoint after when one is due.
    pub fn append(&self, catalog: &str, blocks: &[Buffer]) -> Result<(), ArchiveError> {
        let started = Instant::now();
        let appended = self.append_locked(catalog, blocks);
        Timings::add(&self.inner.timings.append, started);
        self.inner.timings.appends.fetch_add(1, Ordering::Relaxed);
        appended
    }

    fn append_locked(&self, catalog: &str, blocks: &[Buffer]) -> Result<(), ArchiveError> {
        // The work an append can do without its turn, done before taking
        // it: other writers append meanwhile.
        match self.prepare(catalog, blocks) {
            Ok(Some(prepared)) => self.flush(prepared, catalog, blocks),
            Ok(None) => Ok(()),
            Err(error) => Err(storage_error(&self.inner.path, error)),
        }
    }

    /// Takes the writer's turn and writes what `prepared` (of `blocks`)
    /// reconciles to, preparing again in the turn if it does not.
    fn flush(
        &self,
        prepared: Prepared<'_>,
        catalog: &str,
        blocks: &[Buffer],
    ) -> Result<(), ArchiveError> {
        let error = |error| storage_error(&self.inner.path, error);
        let mut writer = self.inner.writer.lock();
        let file = &self.inner.file;
        let written = Exclusive::take(file).and_then(|_exclusive| {
            // This process's turn: read what others appended since its
            // last, then append after it.
            catch_up(file, &self.inner.index, &mut writer)?;
            self.know(writer.end);
            let flushed = match self.reconcile(prepared)? {
                Some(reconciled) => self.write(&mut writer, reconciled),
                // What was prepared cannot be named as it was: prepare
                // again, in the turn, where nothing moves.
                None => match self.prepare(catalog, blocks)? {
                    Some(prepared) => match self.reconcile(prepared)? {
                        Some(reconciled) => self.write(&mut writer, reconciled),
                        None => Err(invalid("an append prepared in its turn did not reconcile")),
                    },
                    None => Ok(()),
                },
            };
            flushed.inspect_err(|_| {
                // Drop what of the append reached the file past what was
                // published: nothing reads it.
                let _ = file.set_len(writer.end);
            })
        });
        written.map_err(|failed| {
            self.inner.known.store(writer.end, Ordering::Release);
            error(failed)
        })
    }

    /// What an append of `blocks` to `catalog` writes, worked out without
    /// the writer's turn, against the index as it is: the new tokens, each
    /// tree node split into rows and encoded as a frame, and the blocks kept
    /// whole. New tokens and nodes take the ids after those published now;
    /// [`reconcile`](Self::reconcile) renames them if others appended
    /// first. `None` when the file holds every block already.
    fn prepare<'a>(
        &'a self,
        catalog: &'a str,
        blocks: &[Buffer],
    ) -> io::Result<Option<Prepared<'a>>> {
        let index = &self.inner.index;
        let mut seen = HashSet::new();
        self.inner
            .timings
            .offered
            .fetch_add(blocks.len() as u64, Ordering::Relaxed);
        let fresh: Vec<&Buffer> = blocks
            .iter()
            .filter(|block| {
                let hash = block.blake3_hash();
                !index.held(catalog, hash) && seen.insert(hash.clone())
            })
            .collect();
        if fresh.is_empty() {
            return Ok(None);
        }

        let mut items = Vec::with_capacity(fresh.len());
        let mut nodes = Vec::new();
        for block in fresh {
            let started = Instant::now();
            let node = self
                .inner
                .options
                .frames
                .then(|| tree_node(block))
                .flatten();
            Timings::add(&self.inner.timings.split, started);
            Timings::add(&self.inner.timings.parse, started);
            match node {
                Some(node) => nodes.push((block.clone(), node)),
                None => items.push(Item::Block(block.clone())),
            }
        }

        index.build_ids();
        // A catalog the file holds nothing in yet has no nodes to be
        // written against: no lineage is found under an id no catalog has.
        let catalog_id = index.catalog_id(catalog).unwrap_or(u16::MAX);
        let mut draft = Draft::new(index);
        let first_node = index.nodes().len() as u32;
        // Nodes this append writes as frames: their ids, rows and bodies,
        // and what they are known by, ahead of joining the index.
        let mut staged: HashMap<u32, Staged> = HashMap::new();
        let mut staged_ids: HashMap<Blake3Hash, u32> = HashMap::new();
        let mut staged_lineages: HashMap<Lineage, u32> = HashMap::new();
        let mut staged_depths: HashMap<u32, u8> = HashMap::new();
        for (block, node) in children_first(nodes) {
            let framed = self.frame(
                &mut draft,
                &Staging {
                    rows: &staged,
                    ids: &staged_ids,
                    lineages: &staged_lineages,
                    depths: &staged_depths,
                },
                catalog,
                catalog_id,
                &block,
                &node,
            )?;
            let Some(frame) = framed else {
                items.push(Item::Block(block));
                continue;
            };
            let id = first_node + staged.len() as u32;
            staged_ids.insert(block.blake3_hash().clone(), id);
            if let Some(first) = frame.lineage {
                staged_lineages.insert((catalog_id, frame.rows.kind, first), id);
            }
            staged_depths.insert(id, frame.depth);
            staged.insert(
                id,
                Staged {
                    hash: block.blake3_hash().clone(),
                    rows: frame.rows.clone(),
                    manifest: frame.manifest,
                },
            );
            items.push(Item::Frame(id, block, frame));
        }
        draft.release();
        Ok(Some(Prepared {
            catalog,
            draft,
            first_node,
            items,
        }))
    }

    /// `prepared` as this turn writes it, holding the turn: blocks others
    /// appended since are dropped, and new tokens and nodes take the ids
    /// that are next now. A drafted token another writer wrote first takes
    /// its id. When no id moved, the frames are written as they were
    /// encoded; when one did, their rows are renamed and encoded again.
    /// `None` when a node this append framed is now held whole, so its
    /// parents would have to name it by hash: prepare again.
    fn reconcile<'a>(&'a self, prepared: Prepared<'a>) -> io::Result<Option<Reconciled<'a>>> {
        let index = &self.inner.index;
        let Prepared {
            catalog,
            draft,
            first_node,
            items,
        } = prepared;
        index.build_ids();
        let catalog_id = index.add_catalog(catalog);
        let view = index.view();
        let ids = index.ids();

        // Tokens: a drafted one the file holds now takes its id; the rest
        // follow every published token, in the order they were drafted.
        let first_token = draft.first();
        let next_token = view.len();
        let mut tokens = Vec::with_capacity(draft.pending());
        let mut new = Vec::new();
        for id in first_token..first_token + draft.pending() as u32 {
            let bytes = draft.get(id).expect("a drafted token");
            let hash = draft.hash(id).expect("a drafted token's hash");
            match Index::find(&ids, &view, hash, bytes) {
                Some(held) => tokens.push(held),
                None => {
                    tokens.push(next_token + new.len() as u32);
                    new.push(id);
                }
            }
        }

        // Nodes: a framed node someone appended since keeps their frame;
        // the rest follow every published node.
        let next_node = index.nodes().len() as u32;
        let mut nodes: HashMap<u32, u32> = HashMap::new();
        let mut kept = Vec::with_capacity(items.len());
        let mut kept_frames = 0;
        for item in items {
            let hash = match &item {
                Item::Block(block) | Item::Frame(_, block, _) => block.blake3_hash().clone(),
            };
            match (index.place(catalog, &hash), item) {
                (None, Item::Frame(id, block, frame)) => {
                    nodes.insert(id, next_node + kept_frames);
                    kept_frames += 1;
                    kept.push(Item::Frame(id, block, frame));
                }
                (None, block) => kept.push(block),
                (Some(Place::Node(held)), Item::Frame(id, ..)) => {
                    nodes.insert(id, held);
                }
                (Some(Place::Block(_)), Item::Frame(..)) => return Ok(None),
                (Some(_), Item::Block(_)) => {}
            }
        }

        let token = |id: u32| {
            id.checked_sub(first_token)
                .and_then(|at| tokens.get(at as usize).copied())
                .unwrap_or(id)
        };
        let node = |id: u32| {
            if id < first_node {
                id
            } else {
                nodes.get(&id).copied().unwrap_or(id)
            }
        };
        let moved = new.len() != draft.pending()
            || next_token != first_token
            || next_node != first_node
            || nodes.iter().any(|(drafted, renamed)| drafted != renamed);

        // Frames as written: encoded again when an id moved, against their
        // base's renamed rows.
        let mut renamed: HashMap<u32, (Arc<Rows>, u8)> = HashMap::new();
        let mut items = Vec::with_capacity(kept.len());
        for item in kept {
            let Item::Frame(drafted, block, mut frame) = item else {
                items.push(item);
                continue;
            };
            let id = node(drafted);
            if moved {
                let rows = Arc::new(rename(&frame.rows, token, node));
                let base = frame.base.map(node);
                // The base's rows and depth: a frame of this append's, renamed
                // already (bases come before what is written against them),
                // or one the file holds.
                let (base_rows, depth) = match base {
                    Some(base) => match renamed.get(&base) {
                        Some((rows, depth)) => (Some(rows.clone()), depth + 1),
                        None => (
                            Some(self.rows(&HashMap::new(), base)?.0),
                            index.node(base).map_or(0, |base| base.depth + 1),
                        ),
                    },
                    None => (None, 0),
                };
                let header = FrameHeader {
                    catalog: catalog.to_string(),
                    hash: block.blake3_hash().clone(),
                    kind: rows.kind,
                    base,
                    manifest: token(frame.manifest),
                    lineage: rows.lineage(),
                };
                let mut body = Vec::new();
                header.put(&mut body)?;
                put_rows(&mut body, &rows, base_rows.as_deref());
                let mut reader = Reader::new(&body);
                FrameHeader::get(&mut reader)?;
                if get_rows(&mut reader, rows.kind, base_rows.as_deref())? != *rows {
                    return Err(invalid("a frame did not read back once its ids moved"));
                }
                frame = Frame {
                    rows: rows.clone(),
                    manifest: header.manifest,
                    base,
                    lineage: header.lineage,
                    depth,
                    body,
                };
                renamed.insert(id, (rows, depth));
            }
            items.push(Item::Frame(id, block, frame));
        }
        Ok(Some(Reconciled {
            catalog_id,
            draft,
            new,
            items,
        }))
    }

    /// Writes what an append reconciled in this turn, then publishes it.
    fn write(&self, writer: &mut Writer, reconciled: Reconciled<'_>) -> io::Result<()> {
        let index = &self.inner.index;
        let Reconciled {
            catalog_id,
            draft,
            new,
            items,
        } = reconciled;
        let catalog = index
            .catalog(catalog_id)
            .ok_or_else(|| invalid("a catalog the index lacks"))?
            .to_string();

        // Lay the records out: the new tokens first, so every frame follows
        // the tokens it names, then blocks and frames in order, then a
        // footer.
        let mut bytes = Vec::new();
        let token_body = writer.end + HEADER;
        let mut token_places = Vec::new();
        if !new.is_empty() {
            let (body, places) = draft.record(token_body, &new);
            token_places = places;
            put_record(&mut bytes, Kind::Tokens, &body)?;
        }
        let mut placed = Vec::with_capacity(items.len());
        let mut kept_rows = Vec::new();
        for item in items {
            let record = writer.end + bytes.len() as u64;
            match item {
                Item::Block(block) => {
                    let body = block_body(&catalog, block.as_ref())?;
                    let start = 1 + catalog.len() as u64;
                    let extent = Extent {
                        offset: record + HEADER + start,
                        len: (body.len() as u64 - start) as u32,
                    };
                    put_record(&mut bytes, Kind::Block, &body)?;
                    placed.push(Placed::Block(block.blake3_hash().clone(), extent));
                }
                Item::Frame(id, block, frame) => {
                    let extent = Extent {
                        offset: record + HEADER,
                        len: frame.body.len() as u32,
                    };
                    put_record(&mut bytes, Kind::Frame, &frame.body)?;
                    kept_rows.push((id, frame.rows.clone(), frame.manifest));
                    placed.push(Placed::Frame(
                        id,
                        Node {
                            hash: block.blake3_hash().clone(),
                            extent,
                            depth: frame.depth,
                            catalog: catalog_id,
                            kind: frame.rows.kind,
                            lineage: frame.lineage,
                        },
                    ));
                }
            }
        }
        put_record(&mut bytes, Kind::Footer, &writer.checkpoint.to_le_bytes())?;
        let end = writer.end + bytes.len() as u64;
        self.know(end);
        let started = Instant::now();
        write_at(&self.inner.file, writer.end, &bytes)?;
        Timings::add(&self.inner.timings.write, started);
        #[cfg(test)]
        if let Some(hook) = self.inner.before_publish.lock().as_mut() {
            hook();
        }

        // Publish what is now in the file: the mapping first, then the
        // tokens, then blocks and frames, children before their parents, so
        // a reader that finds anything finds everything it names.
        index.map(&self.inner.file, end)?;
        let next_token = index.tokens().len() as u32;
        for (at, ((offset, len), drafted)) in token_places.into_iter().zip(&new).enumerate() {
            let published = index.add_token(token_body + offset as u64, len, draft.hash(*drafted));
            debug_assert_eq!(
                published,
                next_token + at as u32,
                "tokens publish in the ids they were reconciled to"
            );
            self.published();
        }
        for item in placed {
            match item {
                Placed::Block(hash, extent) => index.add_block(catalog_id, hash, extent),
                Placed::Frame(id, node) => {
                    let published = index.add_node(node);
                    debug_assert_eq!(
                        published, id,
                        "frames publish in the ids they were reconciled to"
                    );
                }
            }
            self.published();
        }
        for (id, rows, manifest) in kept_rows {
            self.inner.rows.put(id, (rows, manifest));
        }
        writer.since += bytes.len() as u64;
        writer.end = end;

        // A checkpoint, when one is due, is an append of its own: what was
        // published stays in the file if it does not land.
        if writer.since >= self.inner.options.checkpoint_every {
            let at = writer.end;
            let mut tail = Vec::new();
            put_record(&mut tail, Kind::Checkpoint, &checkpoint_body(index)?)?;
            put_record(&mut tail, Kind::Footer, &at.to_le_bytes())?;
            self.know(at + tail.len() as u64);
            write_at(&self.inner.file, at, &tail)?;
            writer.checkpoint = at;
            writer.since = 0;
            writer.end = at + tail.len() as u64;
        }
        Ok(())
    }

    /// Calls the test hook for a step of publishing, if one is set.
    fn published(&self) {
        #[cfg(test)]
        {
            let hook = self.inner.published.lock().take();
            if let Some(mut hook) = hook {
                hook(self);
                *self.inner.published.lock() = Some(hook);
            }
        }
    }

    /// The frame `node` is written as, if its rows read back and realize to
    /// its exact `block`: its rows, the frame record's body, its depth and
    /// lineage. `None` keeps the block whole.
    fn frame(
        &self,
        draft: &mut Draft<'_>,
        staging: &Staging<'_>,
        catalog: &str,
        catalog_id: u16,
        block: &Buffer,
        node: &TreeNode,
    ) -> io::Result<Option<Frame>> {
        let index = &self.inner.index;
        // A node in another layout (an older release's) is not what the
        // current encoder makes, so its frame would not rebuild it: keep it
        // whole without splitting it.
        if node.layout_version() != dialog_search_tree::TAGGED_LAYOUT {
            self.inner
                .timings
                .other_layout
                .fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let Ok(manifest) = node.manifest() else {
            return Ok(None);
        };
        let mut manifest_bytes = Vec::new();
        manifest.encode(&mut manifest_bytes);
        let child = |hash: &Blake3Hash| {
            staging
                .ids
                .get(hash)
                .copied()
                .or_else(|| index.node_id(hash))
        };
        // A node that does not split into rows (another tree's, say), or
        // whose frame does not rebuild its block, is kept whole, and the
        // tokens interned for it are dropped again: nothing names them.
        let mark = draft.pending();
        let started = Instant::now();
        let rows = rows_of(node, draft, child);
        Timings::add(&self.inner.timings.split, started);
        let Ok(rows) = rows else {
            draft.roll_back(mark);
            return Ok(None);
        };
        let started = Instant::now();
        let manifest_id = draft.intern(&manifest_bytes);
        let lineage = rows.lineage();
        let depth_of = |id: u32| {
            staging
                .depths
                .get(&id)
                .copied()
                .or_else(|| index.node(id).map(|node| node.depth))
        };
        let base = lineage
            .and_then(|first| {
                let key = (catalog_id, rows.kind, first);
                staging
                    .lineages
                    .get(&key)
                    .copied()
                    .or_else(|| index.lineage(&key))
            })
            .filter(|base| {
                depth_of(*base).is_some_and(|depth| (depth as usize) + 1 < self.inner.options.chain)
            });
        let base_rows = match base {
            Some(base) => Some(self.rows(staging.rows, base)?.0),
            None => None,
        };
        let header = FrameHeader {
            catalog: catalog.to_string(),
            hash: block.blake3_hash().clone(),
            kind: rows.kind,
            base,
            manifest: manifest_id,
            lineage,
        };
        let mut body = Vec::new();
        header.put(&mut body)?;
        put_rows(&mut body, &rows, base_rows.as_deref());
        Timings::add(&self.inner.timings.encode, started);

        let started = Instant::now();
        // The frame reads back to the same rows.
        let mut reader = Reader::new(&body);
        FrameHeader::get(&mut reader)?;
        let decoded = get_rows(&mut reader, rows.kind, base_rows.as_deref())?;
        if decoded != rows {
            Timings::add(&self.inner.timings.verify, started);
            draft.roll_back(mark);
            return Ok(None);
        }
        // And, when verifying, the rows it reads back to rebuild the very
        // block (see `Options::verify`).
        if self.inner.options.verify {
            let realized = realize(&decoded, &*draft, &manifest, |id| {
                staging
                    .rows
                    .get(&id)
                    .map(|staged| staged.hash.clone())
                    .or_else(|| index.node(id).map(|node| node.hash.clone()))
            });
            if realized.ok().as_deref() != Some(block.as_ref()) {
                Timings::add(&self.inner.timings.verify, started);
                self.inner
                    .timings
                    .unverified
                    .fetch_add(1, Ordering::Relaxed);
                draft.roll_back(mark);
                return Ok(None);
            }
        }
        Timings::add(&self.inner.timings.verify, started);
        let depth = base.and_then(depth_of).map_or(0, |depth| depth + 1);
        Ok(Some(Frame {
            rows: Arc::new(rows),
            manifest: manifest_id,
            base,
            lineage,
            depth,
            body,
        }))
    }
}

/// The nodes an append frames before any is in the index: their rows, ids,
/// lineages and depths, for the next node to name or be written against.
struct Staging<'a> {
    rows: &'a HashMap<u32, Staged>,
    ids: &'a HashMap<Blake3Hash, u32>,
    lineages: &'a HashMap<Lineage, u32>,
    depths: &'a HashMap<u32, u8>,
}

/// A block or frame an append wrote, where it is, before it is published.
enum Placed {
    Block(Blake3Hash, Extent),
    Frame(u32, Node),
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
                    ..Options::default()
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

    /// While an append is under way (its bytes in the file, not published
    /// yet), a reader on another thread reads what was there before, frames
    /// in place included, and misses the block being appended rather than
    /// waiting for it; once the append publishes, the block reads.
    #[test]
    fn it_reads_while_an_append_is_under_way() -> anyhow::Result<()> {
        use crate::node::tests::{entity_segment, segment_manifest};
        use std::sync::mpsc::channel;

        let directory = tempfile::tempdir()?;
        let archive = FileArchive::open(directory.path().join("archive"))?;
        let manifest = segment_manifest();
        let (earlier, later) = (
            entity_segment("earlier", &manifest)?,
            entity_segment("later", &manifest)?,
        );
        let (whole, appended) = (block("whole"), block("appended"));
        archive.append("index", &[earlier.clone(), whole.clone()])?;
        assert!(
            archive.is_frame("index", earlier.blake3_hash()),
            "a tree node is kept as a frame"
        );

        let (paused, on_pause) = channel();
        let (resume, on_resume) = channel::<()>();
        let on_resume = Mutex::new(on_resume);
        *archive.inner.before_publish.lock() = Some(Box::new(move || {
            let _ = paused.send(());
            let _ = on_resume.lock().recv();
        }));
        let writer = archive.clone();
        let (later_block, appended_block) = (later.clone(), appended.clone());
        let appending =
            std::thread::spawn(move || writer.append("index", &[later_block, appended_block]));
        on_pause.recv_timeout(Duration::from_secs(5))?;

        let (answer, answered) = channel();
        let reader = archive.clone();
        let (earlier_hash, whole_hash) =
            (earlier.blake3_hash().clone(), whole.blake3_hash().clone());
        let (later_hash, appended_hash) =
            (later.blake3_hash().clone(), appended.blake3_hash().clone());
        std::thread::spawn(move || {
            let read = || -> Result<_, ArchiveError> {
                Ok((
                    reader.get("index", &earlier_hash)?,
                    reader.served("index", &earlier_hash).is_some(),
                    reader.get("index", &whole_hash)?,
                    reader.get("index", &later_hash)?,
                    reader.get("index", &appended_hash)?,
                ))
            };
            let _ = answer.send(read());
        });
        let read = answered.recv_timeout(Duration::from_secs(5));
        let _ = resume.send(());
        appending.join().expect("the append finishes")?;
        let (read_earlier, served, read_whole, read_later, read_appended) =
            read.expect("the reader answered while the append was under way")?;
        assert_eq!(read_earlier.as_deref(), Some(earlier.as_ref()));
        assert!(served, "the earlier frame is served in place");
        assert_eq!(read_whole.as_deref(), Some(whole.as_ref()));
        assert!(
            read_later.is_none(),
            "a frame not published yet is not there"
        );
        assert!(
            read_appended.is_none(),
            "a block not published yet is not there"
        );

        assert_eq!(
            archive.get("index", later.blake3_hash())?.as_deref(),
            Some(later.as_ref())
        );
        assert_eq!(
            archive.get("index", appended.blake3_hash())?.as_deref(),
            Some(appended.as_ref())
        );
        Ok(())
    }

    /// After every step of publishing an append (each token, each block,
    /// each frame), every block the index lists reads whole and every frame
    /// is served in place: nothing is published before what it names.
    #[test]
    fn it_publishes_nothing_before_what_it_names() -> anyhow::Result<()> {
        use crate::node::tests::{entity_segment, segment_manifest};

        let directory = tempfile::tempdir()?;
        let archive = FileArchive::open(directory.path().join("archive"))?;
        let manifest = segment_manifest();
        archive.append("index", &[entity_segment("first", &manifest)?])?;
        let steps = Arc::new(AtomicU64::new(0));
        let counted = steps.clone();
        *archive.inner.published.lock() = Some(Box::new(move |archive: &FileArchive| {
            counted.fetch_add(1, Ordering::Relaxed);
            for (catalog, hash) in archive.hashes() {
                let block = archive
                    .get(&catalog, &hash)
                    .expect("a listed block reads")
                    .expect("a listed block is there");
                assert_eq!(&Blake3Hash::hash(&block), &hash);
                if archive.is_frame(&catalog, &hash) {
                    assert!(
                        archive.served(&catalog, &hash).is_some(),
                        "a listed frame is served"
                    );
                }
            }
        }));
        let batch = [
            entity_segment("second", &manifest)?,
            entity_segment("third", &manifest)?,
            block("whole"),
        ];
        archive.append("index", &batch)?;
        assert!(
            steps.load(Ordering::Relaxed) > batch.len() as u64,
            "every token and block is a step"
        );
        for content in &batch {
            assert!(archive.get("index", content.blake3_hash())?.is_some());
        }
        Ok(())
    }

    /// An append prepared before another one lands takes the ids that are
    /// next when its turn comes: a node the other appended is not written
    /// again, tokens the other wrote first are not either, and every
    /// block reads back, in this process and after reopening.
    #[test]
    fn it_reconciles_an_append_prepared_before_another() -> anyhow::Result<()> {
        use crate::node::tests::{entity_segment, segment_manifest};

        let manifest = segment_manifest();
        let base = [entity_segment("base", &manifest)?];
        let first = [
            entity_segment("shared", &manifest)?,
            entity_segment("only-first", &manifest)?,
            block("whole"),
        ];
        let second = [
            entity_segment("shared", &manifest)?,
            entity_segment("only-second", &manifest)?,
        ];

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let archive = FileArchive::open(&path)?;
        archive.append("index", &base)?;
        let prepared = archive
            .prepare("index", &first)?
            .expect("blocks the file lacks");
        archive.append("index", &second)?;
        archive.flush(prepared, "index", &first)?;

        // The same appends one after another.
        let control = FileArchive::open(directory.path().join("control"))?;
        for batch in [&base[..], &second[..], &first[..]] {
            control.append("index", batch)?;
        }
        assert_eq!(
            archive.stats().tokens,
            control.stats().tokens,
            "no token written twice"
        );
        assert_eq!(
            archive.stats().frames,
            control.stats().frames,
            "no node written twice"
        );

        let every: Vec<&Buffer> = base.iter().chain(&first).chain(&second).collect();
        for content in &every {
            let hash = content.blake3_hash();
            assert_eq!(
                archive.get("index", hash)?.as_deref(),
                Some(content.as_ref())
            );
            if archive.is_frame("index", hash) {
                assert!(archive.served("index", hash).is_some());
            }
        }
        drop(archive);
        let reopened = FileArchive::open(&path)?;
        for content in &every {
            assert_eq!(
                reopened.get("index", content.blake3_hash())?.as_deref(),
                Some(content.as_ref())
            );
        }
        Ok(())
    }

    /// A node an append framed before its turn, which another writer
    /// appended whole meanwhile, cannot be named by a frame of its own:
    /// the append prepares again in its turn, and the node stays whole.
    #[test]
    fn it_prepares_again_when_a_framed_node_was_appended_whole() -> anyhow::Result<()> {
        use crate::node::tests::{entity_segment, index_over, segment_manifest};

        let manifest = segment_manifest();
        let shared = entity_segment("shared", &manifest)?;
        let only_first = entity_segment("only-first", &manifest)?;
        // An index over both, so a frame of it names the shared node.
        let parent = index_over(&[only_first.clone(), shared.clone()], &manifest)?;
        let first = [shared.clone(), only_first, parent.clone()];

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let archive = FileArchive::open(&path)?;
        archive.append("index", &[entity_segment("base", &manifest)?])?;
        // Another writer: the same file under another name, so it keeps an
        // index of its own, as another process would, and keeps every block
        // whole.
        let other_path = directory.path().join("other");
        std::fs::hard_link(&path, &other_path)?;
        let other = FileArchive::open_with(
            &other_path,
            Options {
                frames: false,
                ..Options::default()
            },
        )?;

        let prepared = archive
            .prepare("index", &first)?
            .expect("blocks the file lacks");
        other.append("index", std::slice::from_ref(&shared))?;
        archive.flush(prepared, "index", &first)?;

        assert!(
            !archive.is_frame("index", shared.blake3_hash()),
            "the shared node stays whole"
        );
        assert!(archive.is_frame("index", first[1].blake3_hash()));
        assert!(
            archive.is_frame("index", parent.blake3_hash()),
            "the index is a frame"
        );
        assert!(archive.served("index", parent.blake3_hash()).is_some());
        for content in &first {
            assert_eq!(
                archive.get("index", content.blake3_hash())?.as_deref(),
                Some(content.as_ref())
            );
        }
        drop((archive, other));
        let reopened = FileArchive::open(&path)?;
        for content in &first {
            assert_eq!(
                reopened.get("index", content.blake3_hash())?.as_deref(),
                Some(content.as_ref())
            );
        }
        Ok(())
    }

    /// Readers on other threads read every block they find while one writer
    /// keeps appending frames and whole blocks: whatever a reader finds, it
    /// reads whole, and its bytes hash to its name.
    #[test]
    fn it_reads_whole_whatever_it_finds_while_appending() -> anyhow::Result<()> {
        use crate::node::tests::{entity_segment, segment_manifest};
        use std::sync::atomic::AtomicBool;

        let directory = tempfile::tempdir()?;
        let archive = FileArchive::open_with(
            directory.path().join("archive"),
            Options {
                checkpoint_every: 4096,
                ..Options::default()
            },
        )?;
        let manifest = segment_manifest();
        let done = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let (archive, done) = (archive.clone(), done.clone());
                std::thread::spawn(move || -> Result<usize, ArchiveError> {
                    let mut reads = 0;
                    while !done.load(Ordering::Acquire) {
                        for (catalog, hash) in archive.hashes() {
                            let block =
                                archive.get(&catalog, &hash)?.expect("a listed block reads");
                            assert_eq!(&Blake3Hash::hash(&block), &hash);
                            if archive.is_frame(&catalog, &hash) {
                                assert!(archive.served(&catalog, &hash).is_some());
                            }
                            reads += 1;
                        }
                    }
                    Ok(reads)
                })
            })
            .collect();
        for batch in 0..40 {
            archive.append(
                "index",
                &[
                    entity_segment(&format!("entity-{batch}"), &manifest)?,
                    block(&format!("whole {batch}")),
                ],
            )?;
        }
        done.store(true, Ordering::Release);
        for reader in readers {
            let reads = reader.join().expect("a reader read every block whole")?;
            assert!(reads > 0);
        }
        assert_eq!(archive.len(), 80);
        Ok(())
    }

    /// A read of a block the file does not hold, while an append holds the
    /// writer's turn, answers without waiting for it: the file has not
    /// grown past what this process knows, so there is nothing to read.
    #[test]
    fn it_answers_a_miss_while_an_append_holds_the_turn() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let archive = FileArchive::open(directory.path().join("archive"))?;
        archive.append("index", &[block("held")])?;
        let turn = archive.inner.writer.lock();
        let (answer, answered) = std::sync::mpsc::channel();
        let reader = archive.clone();
        std::thread::spawn(move || {
            let _ = answer.send(
                reader
                    .get("index", &Blake3Hash::hash(b"absent"))
                    .map(|found| found.is_none()),
            );
        });
        let missed = answered.recv_timeout(Duration::from_secs(5));
        drop(turn);
        assert!(
            matches!(missed, Ok(Ok(true))),
            "the miss answered while the turn was held"
        );
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

#[cfg(test)]
mod index_breakdown {
    use super::*;

    /// Prints what the in-memory index of the file named by
    /// `DIALOG_ARCHIVE_INDEX` holds, structure by structure. Run by hand.
    #[test]
    #[ignore = "breaks down the index of a file named by DIALOG_ARCHIVE_INDEX"]
    fn index_breakdown() -> anyhow::Result<()> {
        let Ok(path) = std::env::var("DIALOG_ARCHIVE_INDEX") else {
            return Ok(());
        };
        let archive = FileArchive::open(&path)?;
        let index = &archive.inner.index;
        let stats = archive.stats();
        let kib = |bytes: usize| format!("{:.0} KiB", bytes as f64 / 1024.0);
        println!("file {}", kib(stats.file_bytes as usize));
        println!(
            "blocks by catalog and hash: {} entries × ~{} B",
            index.block_count(),
            2 + 32 + size_of::<Place>() + 8
        );
        println!(
            "nodes by id: {} × {} B, {} allocated",
            index.nodes().len(),
            size_of::<Node>(),
            kib(index.nodes().footprint())
        );
        println!(
            "latest node per lineage: {} entries × ~{} B",
            index.lineage_count(),
            size_of::<Lineage>() + 4 + 8
        );
        println!(
            "tokens: {} tokens, where each is and whether it was checked: {}",
            index.tokens().len(),
            kib(index.tokens().footprint())
        );
        println!("the index in all: {}", kib(stats.index_memory));
        Ok(())
    }
}

#[cfg(test)]
mod offsets_instead_of_ids {
    use super::*;
    use crate::rows::{NONE, Row, Section, sections_of};

    /// What frames would take if rows named tokens and children by file
    /// offset rather than dense id, written against the same predecessors;
    /// and what checking every datum once costs. Run by hand on the file
    /// named by `DIALOG_ARCHIVE_INDEX`.
    #[test]
    #[ignore = "measures the file named by DIALOG_ARCHIVE_INDEX"]
    fn offsets_instead_of_ids() -> anyhow::Result<()> {
        let Ok(path) = std::env::var("DIALOG_ARCHIVE_INDEX") else {
            return Ok(());
        };
        let archive = FileArchive::open(&path)?;
        let index = &archive.inner.index;
        // A token's offset, divided by `unit`: every token starts on an
        // `ALIGN` boundary, so dividing by it loses nothing.
        let by_offset = |rows: &Rows, unit: u64| -> Vec<Vec<[u64; 6]>> {
            let token = |id: u32| index.extent(id).map(|(offset, _)| offset / unit).unwrap();
            sections_of(rows.kind)
                .iter()
                .zip(&rows.sections)
                .map(|(section, rows)| {
                    rows.iter()
                        .map(|row| {
                            let mut out = [0u64; 6];
                            for field in 0..6 {
                                out[field] = row[field] as u64;
                            }
                            let id = |value: u32| {
                                if value == NONE {
                                    u64::MAX
                                } else {
                                    token(value)
                                }
                            };
                            match section {
                                Section::Entries | Section::Ops => {
                                    for slot in 1..=crate::rows::SLOTS {
                                        if row[slot] != 0 || slot == 1 {
                                            out[slot] = token(row[slot]);
                                        }
                                    }
                                    out[crate::rows::DATUM] = id(row[crate::rows::DATUM]);
                                }
                                Section::Links => {
                                    out[0] = token(row[0]);
                                    out[1] = if row[1] == NONE {
                                        u64::MAX
                                    } else {
                                        index.node(row[1]).unwrap().extent.offset
                                    };
                                    out[3] = id(row[3]);
                                }
                                Section::Buffers => {}
                            }
                            out
                        })
                        .collect()
                })
                .collect()
        };
        // Zigzag-delta varint bytes of every column of `rows`, written as
        // literals: the width of the ids against the width of the offsets.
        // Each column of `rows` as zigzag deltas, one varint a delta.
        let leb = |value: u128| (128 - value.leading_zeros() as usize).max(1).div_ceil(7);
        let deltas = |rows: &[Vec<[u64; 6]>]| -> Vec<Vec<u128>> {
            let mut columns = Vec::new();
            for section in rows {
                for field in 0..6 {
                    let mut previous = 0i128;
                    let mut column = Vec::new();
                    for row in section {
                        if row[field] == u64::MAX {
                            continue;
                        }
                        let delta = row[field] as i128 - previous;
                        previous = row[field] as i128;
                        column.push(((delta << 1) ^ (delta >> 127)) as u128);
                    }
                    columns.push(column);
                }
            }
            columns
        };
        let width = |rows: &[Vec<[u64; 6]>]| -> usize {
            deltas(rows).iter().flatten().map(|delta| leb(*delta)).sum()
        };
        // The same deltas run-length encoded as Automerge's columns are
        // (hexane): a signed count, positive for a value repeated that
        // many times, negative for that many values written one by one.
        let run_width = |rows: &[Vec<[u64; 6]>]| -> usize {
            let mut bytes = 0;
            for column in deltas(rows) {
                let mut literals = 0usize;
                let mut at = 0;
                let close = |literals: &mut usize, bytes: &mut usize| {
                    if *literals > 0 {
                        *bytes += leb((*literals as u128) << 1);
                        *literals = 0;
                    }
                };
                while at < column.len() {
                    let mut end = at + 1;
                    while end < column.len() && column[end] == column[at] {
                        end += 1;
                    }
                    if end - at >= 2 {
                        close(&mut literals, &mut bytes);
                        bytes += leb(((end - at) as u128) << 1) + leb(column[at]);
                    } else {
                        literals += 1;
                        bytes += leb(column[at]);
                    }
                    at = end;
                }
                close(&mut literals, &mut bytes);
            }
            bytes
        };
        let (mut ids, mut offsets, mut aligned, mut literal_rows, mut frames) =
            (0, 0, 0, 0usize, 0usize);
        let (mut ids_in_runs, mut aligned_in_runs) = (0, 0);
        for id in 0..index.nodes().len() as u32 {
            let (rows, _) = archive.rows(&HashMap::new(), id)?;
            let node = index.node(id).unwrap();
            frames += node.extent.len as usize;
            // Only literal rows carry ids: a frame against a predecessor
            // copies the rest. Count every row of whole frames, and the rows
            // a frame against a predecessor does not share with it.
            let literal: Rows = if node.depth == 0 {
                (*rows).clone()
            } else {
                let mut body = vec![0u8; node.extent.len as usize];
                read_at(&archive.inner.file, node.extent.offset, &mut body)?;
                let header = FrameHeader::get(&mut Reader::new(&body))?;
                let base = archive.rows(&HashMap::new(), header.base.unwrap())?.0;
                Rows {
                    kind: rows.kind,
                    sections: rows
                        .sections
                        .iter()
                        .zip(&base.sections)
                        .map(|(ours, theirs)| {
                            let known: HashSet<&Row> = theirs.iter().collect();
                            ours.iter()
                                .filter(|row| !known.contains(row))
                                .copied()
                                .collect()
                        })
                        .collect(),
                }
            };
            literal_rows += literal.sections.iter().map(Vec::len).sum::<usize>();
            let as_ids: Vec<Vec<[u64; 6]>> = literal
                .sections
                .iter()
                .map(|section| {
                    section
                        .iter()
                        .map(|row| {
                            row.map(|value| {
                                if value == NONE {
                                    u64::MAX
                                } else {
                                    value as u64
                                }
                            })
                        })
                        .collect()
                })
                .collect();
            // Only the fields a frame writes: a key's unused slots, and a
            // buffer's or a link's spare ones, are not written at all.
            let written = |columns: Vec<Vec<[u64; 6]>>| -> Vec<Vec<[u64; 6]>> {
                sections_of(literal.kind)
                    .iter()
                    .zip(&literal.sections)
                    .zip(columns)
                    .map(|((section, rows), mut columns)| {
                        for (row, column) in rows.iter().zip(&mut columns) {
                            for (field, value) in column.iter_mut().enumerate() {
                                let kept = match section {
                                    Section::Entries | Section::Ops => {
                                        field == 0
                                            || field == crate::rows::DATUM
                                            || field <= crate::rows::slots_of(row[0] as u8)
                                    }
                                    Section::Links => field < 4,
                                    Section::Buffers => field < 2,
                                };
                                if !kept {
                                    *value = u64::MAX;
                                }
                            }
                        }
                        columns
                    })
                    .collect()
            };
            let as_ids = written(as_ids);
            let as_offsets = written(by_offset(&literal, 1));
            let as_aligned = written(by_offset(&literal, crate::tokens::ALIGN));
            ids += width(&as_ids);
            ids_in_runs += run_width(&as_ids);
            aligned_in_runs += run_width(&as_aligned);
            offsets += width(&as_offsets);
            aligned += width(&as_aligned);
        }
        println!(
            "frames {} KiB; literal rows {literal_rows}: id columns {} KiB, offset columns {} KiB, \
             offsets in units of the token alignment {} KiB",
            frames / 1024,
            ids / 1024,
            offsets / 1024,
            aligned / 1024
        );
        println!(
            "run-length encoded as Automerge's columns: id columns {} KiB, aligned offset columns {} KiB",
            ids_in_runs / 1024,
            aligned_in_runs / 1024
        );

        let started = std::time::Instant::now();
        let mut datums = 0;
        let view = index.view();
        for id in 0..index.tokens().len() as u32 {
            let bytes = view.get(id).unwrap();
            if rkyv::access::<
                rkyv::Archived<dialog_artifacts::State<dialog_artifacts::Datum>>,
                rkyv::rancor::Error,
            >(bytes)
            .is_ok()
            {
                datums += 1;
            }
        }
        println!(
            "checking every token as a datum: {datums} datums of {} tokens in {:?}",
            index.tokens().len(),
            started.elapsed()
        );
        Ok(())
    }
}

#[cfg(test)]
mod record_breakdown {
    use super::*;

    /// Prints the bytes of the file named by `DIALOG_ARCHIVE_INDEX`, record
    /// kind by record kind, headers included, with the padding and lengths
    /// inside token records apart from the tokens. Run by hand.
    #[test]
    #[ignore = "breaks down the records of a file named by DIALOG_ARCHIVE_INDEX"]
    fn record_breakdown() -> anyhow::Result<()> {
        let Ok(path) = std::env::var("DIALOG_ARCHIVE_INDEX") else {
            return Ok(());
        };
        let file = File::open(&path)?;
        let len = file.metadata()?.len();
        let mut at = MAGIC.len() as u64;
        let mut totals: std::collections::BTreeMap<String, (u64, u64)> = Default::default();
        let (mut token_bytes, mut token_count) = (0u64, 0u64);
        while let Some(record) = read_record(&file, at, len)? {
            let entry = totals.entry(format!("{:?}", record.kind)).or_default();
            entry.0 += 1;
            entry.1 += record.end() - record.offset;
            if record.kind == Kind::Tokens {
                let mut reader = Reader::new(&record.body);
                for _ in 0..reader.varint()? {
                    let size = reader.varint()? as usize;
                    let pad = reader.byte()? as usize;
                    reader.take(pad)?;
                    reader.take(size)?;
                    token_bytes += size as u64;
                    token_count += 1;
                }
            }
            at = record.end();
        }
        let mib = |bytes: u64| format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0));
        println!("file {} ({len} bytes)", mib(len));
        for (kind, (count, bytes)) in &totals {
            println!("  {kind}: {count} records, {}", mib(*bytes));
        }
        let tokens = totals.get("Tokens").map_or(0, |(_, bytes)| *bytes);
        println!(
            "  of the token records: {} of token bytes ({token_count} tokens), {} of lengths, padding and headers",
            mib(token_bytes),
            mib(tokens - token_bytes)
        );
        Ok(())
    }
}
