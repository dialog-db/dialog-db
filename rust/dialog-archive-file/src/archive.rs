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
use parking_lot::{Mutex, RwLock};

use crate::file::{FOOTER, HEADER, Kind, MAGIC, put_record, read_at, read_record, write_at};
use crate::node::{TreeNode, realize, rows_of, tree_node};
use crate::rows::{Reader, Rows, get_rows, invalid, put_rows, put_varint};
use crate::tokens::Tokens;

mod served;

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
}

impl Default for Options {
    fn default() -> Self {
        Self {
            checkpoint_every: CHECKPOINT_EVERY,
            frames: true,
            chain: CHAIN,
            rows_cached: 4096,
        }
    }
}

/// Where a block's bytes sit in the file.
#[derive(Clone, Copy, Debug)]
struct Extent {
    offset: u64,
    len: u32,
}

/// How the file holds a block: whole, or as a node's frame.
#[derive(Clone, Copy, Debug)]
enum Place {
    Block(Extent),
    Node(u32),
}

/// What a node is known by when the file looks for the node it supersedes:
/// its catalog, its kind and its first key's token ids.
type Lineage = (u16, u8, [u32; 4]);

/// A node the file holds as a frame.
struct Node {
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

/// Everything the file holds, as an index of where it is.
#[derive(Default)]
struct State {
    catalogs: Vec<String>,
    /// Every block, by catalog and hash.
    blocks: HashMap<String, HashMap<Blake3Hash, Place>>,
    /// Every frame, by node id.
    nodes: Vec<Node>,
    /// The latest node of each hash, for naming children.
    node_ids: HashMap<Blake3Hash, u32>,
    /// The latest node of each lineage, for the next to be written against.
    lineages: HashMap<Lineage, u32>,
    tokens: Tokens,
    /// Bytes of whole blocks and frames, for [`Stats`].
    block_bytes: u64,
    frame_bytes: u64,
}

impl State {
    fn catalog(&mut self, name: &str) -> u16 {
        if let Some(at) = self.catalogs.iter().position(|catalog| catalog == name) {
            return at as u16;
        }
        self.catalogs.push(name.to_string());
        (self.catalogs.len() - 1) as u16
    }

    fn add_node(&mut self, node: Node) -> io::Result<u32> {
        let id = self.nodes.len() as u32;
        let catalog = self
            .catalogs
            .get(node.catalog as usize)
            .ok_or_else(|| invalid("a node in a catalog the file lacks"))?
            .clone();
        self.blocks
            .entry(catalog)
            .or_default()
            .insert(node.hash.clone(), Place::Node(id));
        self.node_ids.insert(node.hash.clone(), id);
        if let Some(first) = node.lineage {
            self.lineages.insert((node.catalog, node.kind, first), id);
        }
        self.frame_bytes += node.extent.len as u64;
        self.nodes.push(node);
        Ok(id)
    }

    fn add_block(&mut self, catalog: String, hash: Blake3Hash, extent: Extent) {
        self.block_bytes += extent.len as u64;
        self.blocks
            .entry(catalog)
            .or_default()
            .insert(hash, Place::Block(extent));
    }

    fn held(&self, catalog: &str, hash: &Blake3Hash) -> bool {
        self.blocks
            .get(catalog)
            .is_some_and(|blocks| blocks.contains_key(hash))
    }
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

/// The decoded rows of the nodes read most recently, by node id.
struct RowsCache {
    capacity: usize,
    rows: HashMap<u32, (Arc<Rows>, u32)>,
    order: VecDeque<u32>,
}

impl RowsCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            rows: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// A node's rows and its manifest token.
    fn get(&self, id: u32) -> Option<(Arc<Rows>, u32)> {
        self.rows.get(&id).cloned()
    }

    fn put(&mut self, id: u32, rows: (Arc<Rows>, u32)) {
        if self.capacity == 0 || self.rows.insert(id, rows).is_some() {
            return;
        }
        self.order.push_back(id);
        while self.order.len() > self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.rows.remove(&evicted);
            }
        }
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.order.clear();
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
}

impl Timings {
    fn add(counter: &AtomicU64, since: Instant) {
        counter.fetch_add(since.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    fn get(counter: &AtomicU64) -> Duration {
        Duration::from_nanos(counter.load(Ordering::Relaxed))
    }
}

struct Inner {
    timings: Timings,
    file: File,
    path: PathBuf,
    options: Options,
    state: RwLock<State>,
    rows: Mutex<RowsCache>,
    /// Each manifest token, decoded, for the nodes served to trees.
    manifests: Mutex<HashMap<u32, Arc<Manifest>>>,
    writer: Mutex<Writer>,
}

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

/// Adds what a record holds to `state`. A checkpoint adds nothing: its
/// index is already what the records before it added.
fn index_record(state: &mut State, kind: Kind, offset: u64, body: &[u8]) -> io::Result<()> {
    let body_offset = offset + HEADER;
    match kind {
        Kind::Block => {
            let mut reader = Reader::new(body);
            let catalog = get_catalog(&mut reader)?;
            let start = 1 + catalog.len();
            let bytes = &body[start..];
            state.add_block(
                catalog,
                Blake3Hash::hash(bytes),
                Extent {
                    offset: body_offset + start as u64,
                    len: bytes.len() as u32,
                },
            );
        }
        Kind::Tokens => {
            state.tokens.read_record(body, body_offset)?;
        }
        Kind::Frame => {
            let header = FrameHeader::get(&mut Reader::new(body))?;
            let depth = match header.base {
                None => 0,
                Some(base) => {
                    state
                        .nodes
                        .get(base as usize)
                        .ok_or_else(|| invalid("a frame against a node the file lacks"))?
                        .depth
                        + 1
                }
            };
            let catalog = state.catalog(&header.catalog);
            state.add_node(Node {
                hash: header.hash,
                extent: Extent {
                    offset: body_offset,
                    len: body.len() as u32,
                },
                depth,
                catalog,
                kind: header.kind,
                lineage: header.lineage,
            })?;
        }
        Kind::Checkpoint | Kind::Footer => {}
    }
    Ok(())
}

/// The body of a checkpoint record: where every token is, every node, and
/// every whole block.
fn checkpoint_body(state: &State) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    put_varint(&mut body, state.catalogs.len() as u64);
    for catalog in &state.catalogs {
        put_catalog(&mut body, catalog)?;
    }
    // Where every token is: tokens are written in id order, so each starts
    // after the one before.
    let extents = state.tokens.extents();
    put_varint(&mut body, extents.len() as u64);
    let mut previous = 0;
    for (offset, len) in extents {
        put_varint(&mut body, offset - previous);
        put_varint(&mut body, *len as u64);
        previous = *offset;
    }
    put_varint(&mut body, state.nodes.len() as u64);
    for node in &state.nodes {
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
    for (catalog, blocks) in &state.blocks {
        for (hash, place) in blocks {
            let Place::Block(extent) = place else {
                continue;
            };
            put_catalog(&mut body, catalog)?;
            body.extend_from_slice(hash.as_bytes());
            body.extend_from_slice(&extent.offset.to_le_bytes());
            body.extend_from_slice(&extent.len.to_le_bytes());
        }
    }
    Ok(body)
}

fn read_checkpoint(len: u64, body: &[u8]) -> io::Result<State> {
    let mut state = State::default();
    let mut reader = Reader::new(body);
    let fixed =
        |reader: &mut Reader, n: usize| -> io::Result<Vec<u8>> { Ok(reader.take(n)?.to_vec()) };
    let u64_of = |bytes: Vec<u8>| u64::from_le_bytes(bytes.try_into().expect("8 bytes"));
    let u32_of = |bytes: Vec<u8>| u32::from_le_bytes(bytes.try_into().expect("4 bytes"));
    for _ in 0..reader.varint()? {
        let catalog = get_catalog(&mut reader)?;
        state.catalogs.push(catalog);
    }
    let mut offset = 0u64;
    for _ in 0..reader.varint()? {
        offset += reader.varint()?;
        let size = reader.u32()?;
        if offset + size as u64 > len {
            return Err(invalid("a checkpoint names a token past the file's end"));
        }
        state.tokens.push(offset, size);
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
        state.add_node(Node {
            hash,
            extent: Extent { offset, len: size },
            depth,
            catalog,
            kind,
            lineage,
        })?;
    }
    while !reader.is_empty() {
        let catalog = get_catalog(&mut reader)?;
        let hash = Blake3Hash::from(<[u8; 32]>::try_from(reader.take(32)?).expect("32 bytes"));
        let offset = u64_of(fixed(&mut reader, 8)?);
        let size = u32_of(fixed(&mut reader, 4)?);
        state.add_block(catalog, hash, Extent { offset, len: size });
    }
    Ok(state)
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

/// Adds the records from `writer.end` up to `len` to `state`, and moves
/// `writer.end` past them.
///
/// Without the file's lock (`finished` set), only appends that are whole
/// are read: the records up to each footer. Another process may be writing
/// the ones after the last. With the lock, no append is under way, so
/// every whole record is read: records past the last footer were left by
/// an append that did not finish, and they hold whole blocks and frames
/// whose tokens precede them.
fn follow(
    file: &File,
    state: &mut State,
    writer: &mut Writer,
    len: u64,
    finished: bool,
) -> io::Result<()> {
    let mut at = writer.end;
    let mut pending = Vec::new();
    while at < len {
        let Some(record) = read_record(file, at, len)? else {
            break;
        };
        at = record.end();
        let footer = record.kind == Kind::Footer;
        pending.push(record);
        if footer || !finished {
            for record in pending.drain(..) {
                index_record(state, record.kind, record.offset, &record.body)?;
                if record.kind == Kind::Checkpoint {
                    writer.checkpoint = record.offset;
                    writer.since = 0;
                } else {
                    writer.since += record.end() - record.offset;
                }
            }
            writer.end = at;
        }
    }
    Ok(())
}

/// Brings `state` up to what the file holds, holding its lock: reads what
/// other processes appended, and drops a tail an append left unfinished,
/// so the next append follows whole records.
fn catch_up(file: &File, state: &mut State, writer: &mut Writer) -> io::Result<()> {
    let len = file.metadata()?.len();
    if len > writer.end {
        follow(file, state, writer, len, false)?;
        if len > writer.end {
            file.set_len(writer.end)?;
        }
    }
    Ok(())
}

/// Reads the file into its index, creating it when it is empty: from the
/// checkpoint its footer names, then every record after it. Holds the
/// file's lock, so it never reads, or drops, an append under way.
fn recover(file: &File) -> io::Result<(State, Writer)> {
    let _exclusive = Exclusive::take(file)?;
    let len = file.metadata()?.len();
    if len == 0 {
        write_at(file, 0, MAGIC)?;
        let writer = Writer {
            end: MAGIC.len() as u64,
            checkpoint: 0,
            since: 0,
        };
        return Ok((State::default(), writer));
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
    let mut state = State::default();
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
        state = read_checkpoint(len, &record.body)?;
        writer.checkpoint = named;
        writer.end = record.end();
    }
    catch_up(file, &mut state, &mut writer)?;
    state.tokens.map(file, writer.end)?;
    Ok((state, writer))
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

/// What an append writes for one block, before it knows where.
enum Pending {
    Block { hash: Blake3Hash, body: Vec<u8> },
    Frame { node: Node, body: Vec<u8> },
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
        let (state, writer) = recover(&file).map_err(|error| storage_error(&path, error))?;
        let inner = Arc::new(Inner {
            timings: Timings::default(),
            file,
            path,
            rows: Mutex::new(RowsCache::new(options.rows_cached)),
            manifests: Mutex::new(HashMap::new()),
            options,
            state: RwLock::new(state),
            writer: Mutex::new(writer),
        });
        open.insert(key, Arc::downgrade(&inner));
        Ok(Self { inner })
    }

    /// Where the file is.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// How many blocks the archive holds, in every catalog.
    pub fn len(&self) -> usize {
        self.inner
            .state
            .read()
            .blocks
            .values()
            .map(HashMap::len)
            .sum()
    }

    /// Whether the archive keeps the block `hash` names in `catalog` as a
    /// frame (rather than whole, or not at all).
    pub fn is_frame(&self, catalog: &str, hash: &Blake3Hash) -> bool {
        matches!(
            self.inner
                .state
                .read()
                .blocks
                .get(catalog)
                .and_then(|blocks| blocks.get(hash)),
            Some(Place::Node(_))
        )
    }

    /// Every block the archive holds, by catalog and hash.
    pub fn hashes(&self) -> Vec<(String, Blake3Hash)> {
        let state = self.inner.state.read();
        state
            .blocks
            .iter()
            .flat_map(|(catalog, blocks)| {
                blocks
                    .keys()
                    .map(move |hash| (catalog.clone(), hash.clone()))
            })
            .collect()
    }

    /// Whether the archive holds no block.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// What the archive holds, and what holding it costs.
    pub fn stats(&self) -> Stats {
        let state = self.inner.state.read();
        let writer = self.inner.writer.lock();
        let places: usize = state.blocks.values().map(HashMap::len).sum();
        Stats {
            file_bytes: writer.end,
            blocks: places - state.nodes.len(),
            block_bytes: state.block_bytes,
            frames: state.nodes.len(),
            frames_against: state.nodes.iter().filter(|node| node.depth > 0).count(),
            frame_bytes: state.frame_bytes,
            tokens: state.tokens.len(),
            token_bytes: state
                .tokens
                .extents()
                .iter()
                .map(|(_, len)| *len as u64)
                .sum(),
            index_memory: state.tokens.footprint()
                + state.nodes.capacity() * size_of::<Node>()
                + state.node_ids.capacity() * (32 + 4 + 1)
                + state.lineages.capacity() * (size_of::<Lineage>() + 4 + 1)
                + places * (32 + size_of::<Place>() + 1),
            split: Timings::get(&self.inner.timings.split),
            encode: Timings::get(&self.inner.timings.encode),
            verify: Timings::get(&self.inner.timings.verify),
            write: Timings::get(&self.inner.timings.write),
            decode: Timings::get(&self.inner.timings.decode),
            realize: Timings::get(&self.inner.timings.realize),
            served: self.inner.timings.served.load(Ordering::Relaxed),
            serve: Timings::get(&self.inner.timings.serve),
            rows_memory: self
                .inner
                .rows
                .lock()
                .rows
                .values()
                .map(|(rows, _)| rows.footprint())
                .sum(),
        }
    }

    /// Forgets every node's decoded rows, so the next read of each decodes
    /// its frames from the file.
    pub fn forget_rows(&self) {
        self.inner.rows.lock().clear();
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

    fn read(&self, catalog: &str, hash: &Blake3Hash) -> io::Result<Option<Vec<u8>>> {
        let place = |state: &State| {
            state
                .blocks
                .get(catalog)
                .and_then(|blocks| blocks.get(hash))
                .copied()
        };
        let mut state = self.inner.state.read();
        if place(&state).is_none() {
            // Another process may have appended it since: read what it
            // finished appending, and look again.
            drop(state);
            self.reread()?;
            state = self.inner.state.read();
        }
        let Some(place) = place(&state) else {
            return Ok(None);
        };
        match place {
            Place::Block(extent) => {
                drop(state);
                let mut bytes = vec![0u8; extent.len as usize];
                read_at(&self.inner.file, extent.offset, &mut bytes)?;
                Ok(Some(bytes))
            }
            Place::Node(id) => {
                let (rows, manifest) = self.rows(&state, &HashMap::new(), id)?;
                let started = Instant::now();
                let block = self.realize(&state, &rows, manifest)?;
                Timings::add(&self.inner.timings.realize, started);
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
        let len = self.inner.file.metadata()?.len();
        let mut writer = self.inner.writer.lock();
        if len <= writer.end {
            return Ok(());
        }
        let mut state = self.inner.state.write();
        follow(&self.inner.file, &mut state, &mut writer, len, true)?;
        state.tokens.map(&self.inner.file, writer.end)
    }

    /// The block a node's rows stand for.
    fn realize(&self, state: &State, rows: &Rows, manifest: u32) -> io::Result<Vec<u8>> {
        let manifest = Manifest::decode(
            state
                .tokens
                .get(manifest)
                .ok_or_else(|| invalid("a manifest token the file lacks"))?,
        )
        .map_err(|error| invalid(&error.to_string()))?;
        realize(rows, &state.tokens, &manifest, |id| {
            state.nodes.get(id as usize).map(|node| node.hash.clone())
        })
    }

    /// The rows of node `id` and its manifest token, decoded through the
    /// frames it is written against. `staged` holds the rows of nodes an
    /// append has not written yet.
    fn rows(
        &self,
        state: &State,
        staged: &HashMap<u32, Staged>,
        id: u32,
    ) -> io::Result<(Arc<Rows>, u32)> {
        if let Some(staged) = staged.get(&id) {
            return Ok((staged.rows.clone(), staged.manifest));
        }
        if let Some(found) = self.inner.rows.lock().get(id) {
            return Ok(found);
        }
        let node = state
            .nodes
            .get(id as usize)
            .ok_or_else(|| invalid("a node the file lacks"))?;
        let mut body = vec![0u8; node.extent.len as usize];
        read_at(&self.inner.file, node.extent.offset, &mut body)?;
        let mut reader = Reader::new(&body);
        let header = FrameHeader::get(&mut reader)?;
        let base = match header.base {
            None => None,
            Some(base) => Some(self.rows(state, staged, base)?.0),
        };
        let started = Instant::now();
        let rows = Arc::new(get_rows(&mut reader, header.kind, base.as_deref())?);
        Timings::add(&self.inner.timings.decode, started);
        self.inner
            .rows
            .lock()
            .put(id, (rows.clone(), header.manifest));
        Ok((rows, header.manifest))
    }

    /// Appends the `blocks` the archive does not hold yet to `catalog`, then
    /// a footer, and a checkpoint first when one is due.
    pub fn append(&self, catalog: &str, blocks: &[Buffer]) -> Result<(), ArchiveError> {
        let mut writer = self.inner.writer.lock();
        let mut state = self.inner.state.write();
        let file = &self.inner.file;
        let written = Exclusive::take(file).and_then(|_exclusive| {
            // This process's turn: read what others appended since its
            // last, then append after it.
            catch_up(file, &mut state, &mut writer)?;
            state.tokens.map(file, writer.end)?;
            let end = writer.end;
            self.write(&mut writer, &mut state, catalog, blocks)
                .inspect_err(|_| {
                    // Drop whatever of the append reached the file.
                    let _ = file.set_len(end);
                })
        });
        written.map_err(|error| {
            // The index may hold some of what failed: read the file again.
            self.inner.rows.lock().clear();
            if let Ok((recovered, end)) = recover(file) {
                *state = recovered;
                *writer = end;
            }
            storage_error(&self.inner.path, error)
        })
    }

    fn write(
        &self,
        writer: &mut Writer,
        state: &mut State,
        catalog: &str,
        blocks: &[Buffer],
    ) -> io::Result<()> {
        let mut seen = HashSet::new();
        let fresh: Vec<&Buffer> = blocks
            .iter()
            .filter(|block| {
                let hash = block.blake3_hash();
                !state.held(catalog, hash) && seen.insert(hash.clone())
            })
            .collect();
        if fresh.is_empty() {
            return Ok(());
        }

        let mut pending = Vec::with_capacity(fresh.len());
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
            match node {
                Some(node) => nodes.push((block.clone(), node)),
                None => pending.push(Pending::Block {
                    hash: block.blake3_hash().clone(),
                    body: block_body(catalog, block.as_ref())?,
                }),
            }
        }

        let catalog_id = state.catalog(catalog);
        let first_id = state.nodes.len() as u32;
        // Nodes this append writes as frames: their ids, rows and bodies,
        // and what they are known by, ahead of joining the index.
        let mut staged: HashMap<u32, Staged> = HashMap::new();
        let mut staged_ids: HashMap<Blake3Hash, u32> = HashMap::new();
        let mut staged_lineages: HashMap<Lineage, u32> = HashMap::new();
        let mut staged_depths: HashMap<u32, u8> = HashMap::new();
        for (block, node) in children_first(nodes) {
            match self.frame(
                state,
                &staged,
                &staged_ids,
                &staged_lineages,
                &staged_depths,
                catalog,
                catalog_id,
                &block,
                &node,
            )? {
                Some((rows, manifest, body, depth, lineage)) => {
                    let id = first_id + staged.len() as u32;
                    staged_ids.insert(block.blake3_hash().clone(), id);
                    if let Some(first) = lineage {
                        staged_lineages.insert((catalog_id, rows.kind, first), id);
                    }
                    staged_depths.insert(id, depth);
                    pending.push(Pending::Frame {
                        node: Node {
                            hash: block.blake3_hash().clone(),
                            extent: Extent { offset: 0, len: 0 },
                            depth,
                            catalog: catalog_id,
                            kind: rows.kind,
                            lineage,
                        },
                        body,
                    });
                    staged.insert(
                        id,
                        Staged {
                            hash: block.blake3_hash().clone(),
                            rows,
                            manifest,
                        },
                    );
                }
                None => pending.push(Pending::Block {
                    hash: block.blake3_hash().clone(),
                    body: block_body(catalog, block.as_ref())?,
                }),
            }
        }

        // Lay the records out: the new tokens first, so every frame follows
        // the tokens it names, then blocks and frames in order.
        let mut bytes = Vec::new();
        let mut token_record = None;
        if state.tokens.pending() > 0 {
            let (body, places) = state.tokens.take_pending(writer.end + HEADER);
            token_record = Some((writer.end + HEADER, places));
            put_record(&mut bytes, Kind::Tokens, &body)?;
        }
        for item in pending {
            let record = writer.end + bytes.len() as u64;
            match item {
                Pending::Block { hash, body } => {
                    let start = 1 + catalog.len() as u64;
                    let extent = Extent {
                        offset: record + HEADER + start,
                        len: (body.len() as u64 - start) as u32,
                    };
                    put_record(&mut bytes, Kind::Block, &body)?;
                    state.add_block(catalog.to_string(), hash, extent);
                }
                Pending::Frame { mut node, body } => {
                    node.extent = Extent {
                        offset: record + HEADER,
                        len: body.len() as u32,
                    };
                    put_record(&mut bytes, Kind::Frame, &body)?;
                    state.add_node(node)?;
                }
            }
        }
        let started = Instant::now();
        write_at(&self.inner.file, writer.end, &bytes)?;
        Timings::add(&self.inner.timings.write, started);
        if let Some((body_offset, places)) = token_record {
            state.tokens.settle(body_offset, &places);
        }
        writer.end += bytes.len() as u64;
        writer.since += bytes.len() as u64;
        {
            let mut cache = self.inner.rows.lock();
            for (id, staged) in staged {
                cache.put(id, (staged.rows, staged.manifest));
            }
        }

        let mut tail = Vec::new();
        if writer.since >= self.inner.options.checkpoint_every {
            let body = checkpoint_body(state)?;
            writer.checkpoint = writer.end;
            put_record(&mut tail, Kind::Checkpoint, &body)?;
            writer.since = 0;
        }
        put_record(&mut tail, Kind::Footer, &writer.checkpoint.to_le_bytes())?;
        write_at(&self.inner.file, writer.end, &tail)?;
        writer.end += tail.len() as u64;
        state.tokens.map(&self.inner.file, writer.end)?;
        Ok(())
    }

    /// The frame `node` is written as, if its rows read back and realize to
    /// its exact `block`: its rows, the frame record's body, its depth and
    /// lineage. `None` keeps the block whole.
    #[allow(clippy::too_many_arguments)]
    fn frame(
        &self,
        state: &mut State,
        staged: &HashMap<u32, Staged>,
        staged_ids: &HashMap<Blake3Hash, u32>,
        staged_lineages: &HashMap<Lineage, u32>,
        staged_depths: &HashMap<u32, u8>,
        catalog: &str,
        catalog_id: u16,
        block: &Buffer,
        node: &TreeNode,
    ) -> io::Result<Option<FrameOf>> {
        let Ok(manifest) = node.manifest() else {
            return Ok(None);
        };
        let mut manifest_bytes = Vec::new();
        manifest.encode(&mut manifest_bytes);
        let node_ids = &state.node_ids;
        let child =
            |hash: &Blake3Hash| staged_ids.get(hash).or_else(|| node_ids.get(hash)).copied();
        // A node that does not split into rows (another tree's, say) is
        // kept whole; its tokens stay, unnamed.
        let started = Instant::now();
        let rows = rows_of(node, &mut state.tokens, child);
        Timings::add(&self.inner.timings.split, started);
        let Ok(rows) = rows else {
            return Ok(None);
        };
        let started = Instant::now();
        let manifest_id = state.tokens.intern(&manifest_bytes);
        let lineage = rows.lineage();
        let depth_of = |id: u32| {
            staged_depths
                .get(&id)
                .copied()
                .or_else(|| state.nodes.get(id as usize).map(|node| node.depth))
        };
        let base = lineage
            .and_then(|first| {
                let key = (catalog_id, rows.kind, first);
                staged_lineages
                    .get(&key)
                    .or_else(|| state.lineages.get(&key))
                    .copied()
            })
            .filter(|base| {
                depth_of(*base).is_some_and(|depth| (depth as usize) + 1 < self.inner.options.chain)
            });
        let base_rows = match base {
            Some(base) => Some(self.rows(state, staged, base)?.0),
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
        // The checked conversion: the frame reads back to the same rows,
        // and the rows realize to the very block.
        let mut reader = Reader::new(&body);
        FrameHeader::get(&mut reader)?;
        let decoded = get_rows(&mut reader, rows.kind, base_rows.as_deref())?;
        if decoded != rows {
            return Ok(None);
        }
        let realized = realize(&rows, &state.tokens, &manifest, |id| {
            staged
                .get(&id)
                .map(|staged| staged.hash.clone())
                .or_else(|| state.nodes.get(id as usize).map(|node| node.hash.clone()))
        });
        Timings::add(&self.inner.timings.verify, started);
        if realized.ok().as_deref() != Some(block.as_ref()) {
            return Ok(None);
        }
        let depth = base.and_then(depth_of).map_or(0, |depth| depth + 1);
        Ok(Some((Arc::new(rows), manifest_id, body, depth, lineage)))
    }
}

/// A node's frame: its rows, its manifest token, the frame record's body,
/// its depth and its lineage.
type FrameOf = (Arc<Rows>, u32, Vec<u8>, u8, Option<[u32; 4]>);

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
        let state = archive.inner.state.read();
        let places: usize = state.blocks.values().map(HashMap::len).sum();
        let capacity: usize = state.blocks.values().map(HashMap::capacity).sum();
        let kib = |bytes: usize| format!("{:.0} KiB", bytes as f64 / 1024.0);
        let file = archive.stats().file_bytes as usize;
        println!("file {}", kib(file));
        println!(
            "blocks by catalog and hash: {places} entries, {} slots × {} B = {}",
            capacity,
            32 + size_of::<Place>() + 1,
            kib(capacity * (32 + size_of::<Place>() + 1))
        );
        println!(
            "nodes by id: {} × {} B = {}",
            state.nodes.capacity(),
            size_of::<Node>(),
            kib(state.nodes.capacity() * size_of::<Node>())
        );
        println!(
            "node ids by hash: {} slots × {} B = {}",
            state.node_ids.capacity(),
            32 + 4 + 1,
            kib(state.node_ids.capacity() * 37)
        );
        println!(
            "latest node per lineage: {} entries, {} slots × {} B = {}",
            state.lineages.len(),
            state.lineages.capacity(),
            size_of::<Lineage>() + 4 + 1,
            kib(state.lineages.capacity() * (size_of::<Lineage>() + 5))
        );
        println!(
            "tokens: {} tokens, where each is and whether it was checked: {}",
            state.tokens.len(),
            kib(state.tokens.footprint())
        );
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
        let state = archive.inner.state.read();
        let token = |id: u32| state.tokens.extent(id).map(|(offset, _)| offset).unwrap();
        let by_offset = |rows: &Rows| -> Vec<Vec<[u64; 5]>> {
            sections_of(rows.kind)
                .iter()
                .zip(&rows.sections)
                .map(|(section, rows)| {
                    rows.iter()
                        .map(|row| {
                            let mut out = [0u64; 5];
                            for field in 0..5 {
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
                                    for slot in 1..4 {
                                        if row[slot] != 0 || slot == 1 {
                                            out[slot] = token(row[slot]);
                                        }
                                    }
                                    out[4] = id(row[4]);
                                }
                                Section::Links => {
                                    out[0] = token(row[0]);
                                    out[1] = if row[1] == NONE {
                                        u64::MAX
                                    } else {
                                        state.nodes[row[1] as usize].extent.offset
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
        let width = |rows: &[Vec<[u64; 5]>]| -> usize {
            let mut bytes = 0;
            for section in rows {
                for field in 0..5 {
                    let mut previous = 0i128;
                    for row in section {
                        if row[field] == u64::MAX {
                            continue;
                        }
                        let delta = row[field] as i128 - previous;
                        previous = row[field] as i128;
                        let zigzag = ((delta << 1) ^ (delta >> 127)) as u128;
                        bytes += (128 - zigzag.leading_zeros() as usize).max(1).div_ceil(7);
                    }
                }
            }
            bytes
        };
        let (mut ids, mut offsets, mut literal_rows, mut frames) = (0, 0, 0usize, 0usize);
        for id in 0..state.nodes.len() as u32 {
            let (rows, _) = archive.rows(&state, &HashMap::new(), id)?;
            let node = &state.nodes[id as usize];
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
                let base = archive
                    .rows(&state, &HashMap::new(), header.base.unwrap())?
                    .0;
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
            let as_ids: Vec<Vec<[u64; 5]>> = literal
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
            ids += width(&as_ids);
            offsets += width(&by_offset(&literal));
        }
        println!(
            "frames {} KiB; literal rows {literal_rows}: id columns {} KiB, offset columns {} KiB",
            frames / 1024,
            ids / 1024,
            offsets / 1024
        );

        let started = std::time::Instant::now();
        let mut datums = 0;
        for id in 0..state.tokens.len() as u32 {
            let bytes = state.tokens.get(id).unwrap();
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
            state.tokens.len(),
            started.elapsed()
        );
        Ok(())
    }
}
