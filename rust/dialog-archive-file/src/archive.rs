//! [`FileArchive`]: the archive effects over one file.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use dialog_capability::{Capability, Provider, Resume};
use dialog_common::{Blake3Hash, Buffer};
use dialog_effects::archive::prelude::{GetExt, ImportExt, PutExt};
use dialog_effects::archive::{ArchiveError, Get, Import, Put};
use dialog_search_tree::{Manifest, NodeBody};
use parking_lot::Mutex;

use crate::file::{HEADER, Kind, MAGIC, put_record, read_at, record_in, write_at};
use crate::locks;
use crate::node::{TreeNode, realize, rows_after, tree_node};
use crate::rows::{Reader, Rows, get_rows, invalid, put_rows, put_varint, rename};
use crate::tokens::{ALIGN, Draft, TokenRead};

mod cells;
mod extent;
mod index;
#[cfg(test)]
mod model;
mod served;
mod slot;
mod tree;

use extent::Owned;
use index::{
    FrameHeader, Place, Snapshot, WHOLE, block_key, catalog_in, lineage_key, next, token_id,
};
use slot::{DATA, SLOT, Slot, boot};
use tree::{Child, Nodes, PAGE, Pages, check, is_stale};

/// The files this process has open, so a second open of one shares it.
static OPEN: LazyLock<Mutex<HashMap<PathBuf, Weak<Inner>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The longest chain of frames, each written against the one before, a
/// read decodes, by default: every this many versions of a node, a frame is
/// written whole.
pub const CHAIN: usize = 32;

/// How many tokens a writer remembers where it found in each of two
/// generations, so it looks each up in the index only once while it stays
/// in use.
const TOKENS_REMEMBERED: usize = 1 << 20;

/// How many datum tokens a reader remembers having checked.
const DATUMS_CHECKED: usize = 1 << 20;

/// A sync point follows an append once this many appends came since the
/// last, once this many index pages wait on one to be written over, or
/// once this long has passed: a power loss loses at most what came after
/// the last sync point, and the index's freed pages wait at most this long.
const SYNC_APPENDS: u64 = 64;
const SYNC_PAGES: usize = 2048;
const SYNC_AFTER: Duration = Duration::from_secs(2);

/// Keys that are hashes already: hashed by taking them as they are.
#[derive(Clone, Copy, Default)]
struct Hashed;

impl std::hash::BuildHasher for Hashed {
    type Hasher = Passed;

    fn build_hasher(&self) -> Passed {
        Passed(0)
    }
}

/// The hasher of [`Hashed`]: the last `u64` written is the hash.
#[derive(Default)]
struct Passed(u64);

impl std::hash::Hasher for Passed {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = self.0.rotate_left(8) ^ *byte as u64;
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

/// How a [`FileArchive`] writes its file.
#[derive(Clone, Debug)]
pub struct Options {
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
    /// The bytes of blocks an extent holds: what a writer that finds the
    /// writer's turn taken claims once, to append to without the turn.
    pub extent: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            frames: true,
            chain: CHAIN,
            rows_cached: 4096,
            extent: 1 << 20,
            verify: cfg!(debug_assertions) || std::env::var_os("DIALOG_ARCHIVE_VERIFY").is_some(),
        }
    }
}

/// The decoded rows of the frames read most recently, by where each
/// starts. A read takes no lock; keeping one takes a short one, for the
/// order they go in.
struct RowsCache {
    capacity: usize,
    rows: papaya::HashMap<u32, (Arc<Rows>, u32)>,
    order: Mutex<VecDeque<u32>>,
}

impl RowsCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            rows: papaya::HashMap::new(),
            order: Mutex::new(VecDeque::new()),
        }
    }

    /// A frame's rows and its manifest token.
    fn get(&self, at: u32) -> Option<(Arc<Rows>, u32)> {
        self.rows.pin().get(&at).cloned()
    }

    fn put(&self, at: u32, rows: (Arc<Rows>, u32)) {
        if self.capacity == 0 || self.rows.pin().try_insert(at, rows).is_err() {
            return;
        }
        let mut order = self.order.lock();
        order.push_back(at);
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
    /// Rows a frame took from the node it supersedes rather than interning.
    rows_taken: AtomicU64,
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
    /// Nodes kept whole because their frame did not read back to their
    /// rows: an encoding bug, never expected.
    unread: AtomicU64,
    /// Sync points, and the time they took.
    sync: AtomicU64,
    syncs: AtomicU64,
    /// Index pages written over where they were.
    reused: AtomicU64,
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
             kept whole: {} of another layout, {} not rebuilt when verified, {} not read back; \
             {} sync points taking {:.1} ms",
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
            timings.unread.load(Ordering::Relaxed),
            timings.syncs.load(Ordering::Relaxed),
            ms(&timings.sync),
        );
    }
}

struct Inner {
    timings: Timings,
    file: File,
    path: PathBuf,
    options: Options,
    /// The file as of its latest append this process has read, read
    /// without a lock.
    current: ArcSwap<Snapshot>,
    rows: RowsCache,
    /// Each manifest token, decoded, for the nodes served to trees.
    manifests: papaya::HashMap<u32, Arc<Manifest>>,
    /// The datum tokens checked as archives, so each is checked once.
    checked: papaya::HashSet<u32>,
    /// Where a writer found tokens, by the hash of their bytes: the index
    /// is asked once for each while it stays here.
    tokens: Mutex<Remembered>,
    /// The writer's turn in this process, and when this process last made
    /// a sync point.
    writer: Mutex<()>,
    synced: Mutex<Instant>,
    /// The extents this process writes.
    extents: Mutex<Extents>,
    /// The blocks extents hold, as far as this process has read them.
    loose: Mutex<Loose>,
    /// Each cell's lock in this process, by where its data starts.
    cell_locks: Mutex<HashMap<u64, Arc<Mutex<()>>>>,
    /// The end of what this process has read of the file or is writing to
    /// it, so a read that misses looks for another process's appends only
    /// when the file grew past it, and takes no lock otherwise.
    known: AtomicU64,
    /// Called by an append once its bytes are in the file, before it
    /// publishes them: for tests that read while an append is under way.
    #[cfg(test)]
    before_publish: Mutex<Option<Box<dyn FnMut() + Send>>>,
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
    /// Nodes served to trees in place, answering their reads (`Read`).
    pub served: u64,
    /// Rows framed nodes took from the nodes they supersede, unchanged,
    /// rather than interning their entries again.
    pub rows_taken: u64,
    /// Nodes kept whole because their rows did not rebuild their block,
    /// when frames are verified ([`Options::verify`]).
    pub unverified: u64,
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
/// and buffered operations as rows naming the file's **tokens** (each
/// distinct entity, attribute, value and datum, kept once) and its
/// children by where they are in the file. A frame is written against the
/// rows of the node it supersedes (the latest node of its kind in its
/// catalog that starts at the same key) as runs copied from them around
/// what is new, and whole every [`Options::chain`] versions. Anything that
/// is not a tree node, or does not rebuild from its frame, is kept whole. A
/// read of a frame realizes the block through the tree's own encoder.
///
/// The file is a magic number, three slots, then records: tokens, frames
/// (blocks kept whole among them), and pages of the file's index, a tree
/// whose root is in the slot. Every append writes its records past the end
/// and its index pages, then the older of two rolling slots; opening reads
/// the newer one that is whole, and nothing else. An append cut short
/// leaves the other slot whole and records past its end, which the next
/// writer drops.
///
/// Only the index's pages are written over, once no root reaches them,
/// and only after a **sync point** has made that safe: the file synced,
/// the third, durable slot written and synced. A sync point follows an
/// append every 64 appends, every 2,048 freed pages or every 2 seconds,
/// and [`FileArchive::sync`] makes one. A power loss loses at most what
/// came after the last: opening in a later boot checks what was written
/// since the durable slot and goes back to the newest state that is whole.
///
/// One file is one space, as one directory is for the `fs` provider: the
/// subject an effect names is not part of where a block is kept.
///
/// Any number of processes open one file. Writers take turns holding an
/// exclusive lock on it: a writer prepares its append before its turn,
/// then, holding the lock, reads the newest slot and writes. Readers take
/// no lock: a slot names frames and tokens that never change, an index page
/// written over since a reader's slot reads as stale (its append and
/// checksum say so) and the reader looks again from a newer slot, and a
/// read that does not find a block reads the slot again if the file grew
/// ([`refresh`]).
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

/// The least a mapping of the file reserves.
const MAPPED: u64 = 64 << 20;

/// Pairs `slot` with a mapping of `file` that reaches its end: the
/// `previous` snapshot's, when it does, else a new one reserving twice
/// that. A mapping lasts while the file grows into it, so an append does
/// not map the file again, and pages a read faulted in stay mapped for the
/// next one; the file is mapped again only each time it doubles. The index
/// pages checked so far carry over too.
fn snapshot(file: &File, slot: Slot, previous: Option<&Snapshot>) -> io::Result<Snapshot> {
    let pages = previous.map_or_else(Default::default, |previous| previous.pages.clone());
    if let Some(previous) = previous
        && previous.map.len() as u64 >= slot.end
    {
        return Ok(Snapshot {
            slot,
            map: previous.map.clone(),
            pages,
        });
    }
    // Past a file's end, a mapping on Unix reads nothing until the file
    // grows there; on Windows it would grow the file, so there the mapping
    // is the file as it is.
    let reserve = if cfg!(unix) {
        slot.end.saturating_mul(2).max(MAPPED)
    } else {
        file.metadata()?.len()
    };
    let len = usize::try_from(reserve).unwrap_or(usize::MAX);
    // SAFETY: frames and tokens are never written again once a slot names
    // them, and a snapshot reads only up to its slot's end, never past the
    // file's, so the mapping's reach past the end of the file, reserved
    // for appends to come, is never read while nothing is there. The slots
    // are read with plain reads, never through the mapping. The index's
    // pages are the exception: a writer, in this process or another, may
    // write one over while a reader holding an older slot reads it. Such a
    // reader copies the page out of the mapping before anything else and
    // checks the copy (`tree::check`), so a page changed under it is found
    // stale, never trusted; the bytes it copied are plain bytes, whatever
    // they turn out to be. Like any mapped file, changing it from outside
    // dialog is not supported.
    let map = unsafe { memmap2::MmapOptions::new().len(len).map(file)? };
    Ok(Snapshot {
        slot,
        map: Arc::new(map),
        pages,
    })
}

/// The writer's turn across processes: the turn's byte of the file (see
/// the `locks` module), held exclusive. Released when dropped.
struct Exclusive<'a>(&'a File);

impl<'a> Exclusive<'a> {
    /// Waits for the turn.
    fn take(file: &'a File) -> io::Result<Self> {
        locks::lock(file, locks::TURN, true, true)?;
        Ok(Self(file))
    }

    /// The turn, if no other process holds it.
    fn try_take(file: &'a File) -> io::Result<Option<Self>> {
        Ok(locks::lock(file, locks::TURN, true, false)?.then_some(Self(file)))
    }
}

impl Drop for Exclusive<'_> {
    fn drop(&mut self) {
        let _ = locks::unlock(self.0, locks::TURN);
    }
}

/// The writer's turn, in this process and across processes.
struct Turn<'a> {
    _process: parking_lot::MutexGuard<'a, ()>,
    _file: Exclusive<'a>,
}

/// The extents this process writes: those idle, free for a writer to take,
/// and the records of those a writer has taken.
#[derive(Default)]
struct Extents {
    idle: Vec<Owned>,
    busy: HashSet<u64>,
}

/// What this process knows of the blocks extents hold, its own and other
/// processes': how far it has read each extent, and where each block it
/// found is, by its catalog's number and hash, with the extent it is in.
#[derive(Default)]
struct Loose {
    read: HashMap<u64, u64>,
    blocks: HashMap<(u16, Blake3Hash), (u64, u32)>,
    /// The slot the extents were last read as of.
    sequence: u64,
}

/// The newest slot of `file` whose records are all in it. Holding the
/// file's lock, a tail past it is an append that did not finish: it is
/// dropped.
fn newest(file: &File, locked: bool) -> io::Result<Slot> {
    let len = file.metadata()?.len();
    let slot = Slot::read(file)?
        .into_iter()
        .find(|slot| slot.end <= len)
        .ok_or_else(|| invalid("no whole slot"))?;
    if locked && len > slot.end {
        file.set_len(slot.end)?;
    }
    Ok(slot)
}

/// Opens the file's state, creating it when it is empty. Holds the file's
/// lock, so it never reads, or drops, an append under way.
///
/// A rolling slot written in this boot is taken as it is: a process that
/// died left what it wrote with the operating system, in order. One
/// written in another boot (or where the platform does not say) may have
/// been cut short by a power loss that kept some of what came before it
/// and lost the rest, in any order. Then everything written since the
/// durable slot is checked: every record whole, every index page the slot
/// reaches and was written since. The newest rolling slot that passes is
/// taken; if none does, the file goes back to the durable slot, which
/// nothing since was allowed to write over.
fn recover(file: &File) -> io::Result<Slot> {
    let _exclusive = Exclusive::take(file)?;
    let len = file.metadata()?.len();
    if len == 0 {
        write_at(file, 0, MAGIC)?;
        file.set_len(DATA)?;
        let slot = Slot {
            boot: boot(),
            ..Slot::default()
        };
        slot.write(file)?;
        slot.write_durable(file)?;
        file.sync_data()?;
        return Ok(slot);
    }
    let mut magic = [0u8; 8];
    if len < MAGIC.len() as u64 || {
        read_at(file, 0, &mut magic)?;
        &magic != MAGIC
    } {
        return Err(invalid("not a dialog archive file"));
    }
    let durable = Slot::read_durable(file)?.ok_or_else(|| invalid("no whole durable slot"))?;
    let candidates: Vec<Slot> = Slot::read(file)?
        .into_iter()
        .filter(|slot| slot.end <= len && slot.sequence >= durable.sequence)
        .collect();
    let this_boot = boot();
    let chosen = match candidates.first() {
        Some(newest) if this_boot != 0 && newest.boot == this_boot => Some(newest.clone()),
        _ => {
            let mut chosen = None;
            for candidate in candidates {
                if whole_since(file, &durable, &candidate)? {
                    chosen = Some(candidate);
                    break;
                }
            }
            chosen
        }
    };
    let slot = match chosen {
        Some(slot) => slot,
        None => {
            // Back to the durable slot: the rolling slots past it are
            // dropped, so no later open takes one of them.
            for at in 0..2u64 {
                write_at(file, MAGIC.len() as u64 + at * SLOT, &[0; 8])?;
            }
            durable.write(file)?;
            file.set_len(durable.end)?;
            file.sync_data()?;
            durable
        }
    };
    if len > slot.end {
        file.set_len(slot.end)?;
    }
    Ok(slot)
}

/// Whether everything `slot` names that was written since the `durable`
/// slot is whole: its records, each by its checksum, and the index pages it
/// reaches that were written since, each by its own.
fn whole_since(file: &File, durable: &Slot, slot: &Slot) -> io::Result<bool> {
    let view = snapshot(file, slot.clone(), None)?;
    let bytes = view.bytes();
    let mut at = durable.end as usize;
    while at < slot.end as usize {
        match record_in(bytes, at) {
            Some((_, _, end)) => at = end,
            None => return Ok(false),
        }
    }
    if at != slot.end as usize {
        return Ok(false);
    }
    // A page that fails its check, or does not read as a node, is not
    // whole.
    let mut pages = Vec::new();
    Ok(slot
        .root
        .pages_since(&view, Some(durable.sequence as u32), &mut pages)
        .is_ok()
        && slot
            .cells
            .pages_since(&view, Some(durable.sequence as u32), &mut pages)
            .is_ok())
}

/// `nodes` ordered so every node comes after the children it links to
/// among them, so a parent can name a child by where it was put.
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

/// A node an append frames, before its turn: its rows, manifest token,
/// lineage and base as drafted (new tokens and frames by provisional
/// ids), and its depth.
struct Frame {
    rows: Arc<Rows>,
    manifest: u32,
    base: Option<u32>,
    depth: u8,
}

/// What an append writes for one block: the block whole, or the frame of
/// the node it is, by provisional id.
enum Item {
    Block(Buffer),
    Frame(u32, Buffer, Frame),
}

/// An append worked out before its turn: the tokens it adds, and its
/// blocks and frames, children before their parents. New tokens and frames
/// are named from `first` up.
struct Prepared<'a> {
    catalog: &'a str,
    first: u32,
    tokens: Draft<'a>,
    items: Vec<Item>,
}

/// An append laid out in its turn: its bytes, from `start`, the slot that
/// names them, and what it adds to the caches once published.
struct Laid {
    start: u64,
    bytes: Vec<u8>,
    /// Index pages written over where they are, by page.
    pages: Vec<(u32, Vec<u8>)>,
    slot: Slot,
    rows: Vec<(u32, Arc<Rows>, u32)>,
    tokens: Vec<(u64, u32)>,
}

/// The first free key from `key` on, in `snapshot`'s index and among the
/// keys an append took already.
fn free(snapshot: &Snapshot, mut key: u32, taken: &mut HashSet<u32>) -> io::Result<u32> {
    loop {
        if !taken.contains(&key) && snapshot.slot.root.get(snapshot, key)?.is_none() {
            taken.insert(key);
            return Ok(key);
        }
        key = next(key);
    }
}

/// Where an append's index pages go: pages free to write over first, then
/// new pages after the append's records. It reads the pages it wrote as
/// well as the snapshot's.
struct Writer<'a> {
    snapshot: &'a Snapshot,
    sequence: u32,
    reusable: Vec<u32>,
    pending: Vec<u32>,
    /// The pages this append wrote, and their bytes.
    written: HashMap<u32, Vec<u8>>,
    /// Pages this append wrote and then freed again: never reached by a
    /// root, so free at once.
    unused: Vec<u32>,
    /// The first page past the append's records, and the next new one.
    first: u32,
    next: u32,
}

impl Nodes for Writer<'_> {
    fn read(&self, child: Child) -> io::Result<tree::Page<'_>> {
        match self.written.get(&child.page) {
            Some(bytes) => {
                check(bytes, child)?;
                Ok(tree::Page::Borrowed(bytes))
            }
            None => self.snapshot.read(child),
        }
    }
}

impl Pages for Writer<'_> {
    fn write(&mut self, node: &tree::Node) -> io::Result<Child> {
        let bytes = node.page(self.sequence)?;
        let page = match self.unused.pop().or_else(|| self.reusable.pop()) {
            Some(page) => page,
            None => {
                self.next += 1;
                self.next - 1
            }
        };
        self.written.insert(page, bytes);
        Ok(Child {
            page,
            sequence: self.sequence,
        })
    }

    fn free(&mut self, child: Child) {
        if self.written.remove(&child.page).is_some() {
            self.unused.push(child.page);
        } else {
            self.pending.push(child.page);
        }
    }
}

/// Appends to `bytes` (the append's, from `start` in the file) a frame
/// record holding `data`, which starts on the alignment after a byte
/// saying how much padding comes first. Returns where the data starts,
/// divided by [`ALIGN`].
fn put_frame(bytes: &mut Vec<u8>, start: u64, data: &[u8]) -> io::Result<u32> {
    let body_offset = start + bytes.len() as u64 + HEADER;
    let pad = (ALIGN - (body_offset + 1) % ALIGN) % ALIGN;
    let mut body = Vec::with_capacity(1 + pad as usize + data.len());
    body.push(pad as u8);
    body.resize(1 + pad as usize, 0);
    body.extend_from_slice(data);
    put_record(bytes, Kind::Frame, &body)?;
    token_id(body_offset + 1 + pad)
}

/// Where a writer found tokens, by the hash of their bytes, in two
/// generations: once the newer holds [`TOKENS_REMEMBERED`], it becomes the
/// older and the older is dropped, and a token found in the older moves to
/// the newer. So what a writer keeps using stays, in bounded memory.
#[derive(Default)]
struct Remembered {
    newer: HashMap<u64, u32, Hashed>,
    older: HashMap<u64, u32, Hashed>,
}

impl Remembered {
    fn get(&mut self, hash: u64) -> Option<u32> {
        if let Some(id) = self.newer.get(&hash) {
            return Some(*id);
        }
        let id = self.older.remove(&hash)?;
        self.insert(hash, id);
        Some(id)
    }

    fn insert(&mut self, hash: u64, id: u32) {
        if self.newer.len() >= TOKENS_REMEMBERED {
            self.older = std::mem::take(&mut self.newer);
        }
        self.newer.insert(hash, id);
    }

    /// The memory the two generations take.
    fn footprint(&self) -> usize {
        (self.newer.capacity() + self.older.capacity()) * (8 + 4 + 1)
    }
}

/// The token equal to `token` the file holds as of `snapshot`, by the
/// value rows name it by: in `remembered`, or found in the index and
/// remembered there.
fn find_in(
    remembered: &mut Remembered,
    snapshot: &Snapshot,
    hash: u64,
    token: &[u8],
) -> Option<u32> {
    if let Some(id) = remembered.get(hash)
        && snapshot.get(id) == Some(token)
    {
        return Some(id);
    }
    let (found, _) = snapshot.probe_token(hash, token).ok()?;
    if let Some(id) = found {
        remembered.insert(hash, id);
    }
    found
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
        let current = recover(&file)
            .and_then(|slot| snapshot(&file, slot, None))
            .map_err(|error| storage_error(&path, error))?;
        let inner = Arc::new(Inner {
            known: AtomicU64::new(current.slot.end),
            current: ArcSwap::from_pointee(current),
            timings: Timings::default(),
            file,
            path,
            rows: RowsCache::new(options.rows_cached),
            manifests: papaya::HashMap::new(),
            checked: papaya::HashSet::new(),
            tokens: Mutex::new(Remembered::default()),
            options,
            writer: Mutex::new(()),
            synced: Mutex::new(Instant::now()),
            extents: Mutex::new(Extents::default()),
            loose: Mutex::new(Loose::default()),
            cell_locks: Mutex::new(HashMap::new()),
            #[cfg(test)]
            before_publish: Mutex::new(None),
        });
        open.insert(key, Arc::downgrade(&inner));
        Ok(Self { inner })
    }

    /// Where the file is.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    fn current(&self) -> Arc<Snapshot> {
        self.inner.current.load_full()
    }

    /// Makes `snapshot` current, unless a newer one already is.
    fn publish(&self, snapshot: Snapshot) {
        let snapshot = Arc::new(snapshot);
        self.inner.current.rcu(|current| {
            if snapshot.slot.sequence > current.slot.sequence {
                snapshot.clone()
            } else {
                current.clone()
            }
        });
    }

    /// How many blocks the archive holds, in every catalog.
    pub fn len(&self) -> usize {
        let counts = self.current().slot.counts;
        (counts.blocks + counts.frames) as usize
    }

    /// Whether the archive holds no block.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the archive keeps the block `hash` names in `catalog` as a
    /// frame (rather than whole, or not at all).
    pub fn is_frame(&self, catalog: &str, hash: &Blake3Hash) -> bool {
        matches!(self.place(catalog, hash), Ok((_, Some(Place::Frame(_)))))
    }

    /// Every block the archive holds, by catalog and hash.
    pub fn hashes(&self) -> Vec<(String, Blake3Hash)> {
        let Ok((snapshot, blocks)) = self.look(|snapshot| snapshot.blocks()) else {
            return Vec::new();
        };
        blocks
            .into_iter()
            .filter_map(|(catalog, hash, _)| Some((snapshot.catalog(catalog)?.to_string(), hash)))
            .collect()
    }

    /// What the archive holds, and what holding it costs.
    pub fn stats(&self) -> Stats {
        let snapshot = self.current();
        let counts = snapshot.slot.counts;
        Stats {
            file_bytes: snapshot.slot.end,
            blocks: counts.blocks as usize,
            block_bytes: counts.block_bytes,
            frames: counts.frames as usize,
            frames_against: counts.frames_against as usize,
            frame_bytes: counts.frame_bytes,
            tokens: counts.tokens as usize,
            token_bytes: counts.token_bytes,
            index_memory: snapshot.slot.root.encode().len()
                + self.inner.tokens.lock().footprint()
                + self.inner.checked.len() * (4 + 8),
            split: Timings::get(&self.inner.timings.split),
            encode: Timings::get(&self.inner.timings.encode),
            verify: Timings::get(&self.inner.timings.verify),
            write: Timings::get(&self.inner.timings.write),
            decode: Timings::get(&self.inner.timings.decode),
            realize: Timings::get(&self.inner.timings.realize),
            served: self.inner.timings.served.load(Ordering::Relaxed),
            rows_taken: self.inner.timings.rows_taken.load(Ordering::Relaxed),
            unverified: self.inner.timings.unverified.load(Ordering::Relaxed),
            serve: Timings::get(&self.inner.timings.serve),
            rows_memory: self.inner.rows.footprint(),
        }
    }

    /// A draft over the tokens the file holds now, as a writer's split
    /// finds them: for tests that measure it.
    #[cfg(test)]
    pub(crate) fn draft(&self) -> Draft<'_> {
        let snapshot = self.current();
        let finding = snapshot.clone();
        let first = token_id(snapshot.slot.end).expect("a file under 64 GiB") + 1;
        Draft::new(
            snapshot,
            Box::new(move |hash, token| self.find_token(&finding, hash, token)),
            first,
        )
    }

    /// Prepares this process to write. Nothing is loaded ahead of time any
    /// more: a writer finds tokens through the file's index as it needs
    /// them. Returns how long it took.
    pub fn prepare_writer(&self) -> Duration {
        Duration::ZERO
    }

    /// Forgets every node's decoded rows, so the next read of each decodes
    /// its frames from the file.
    pub fn forget_rows(&self) {
        self.inner.rows.clear();
    }

    /// Makes everything appended so far durable: a sync point (see
    /// [`FileArchive`]).
    pub fn sync(&self) -> Result<(), ArchiveError> {
        let _turn = self.inner.writer.lock();
        let file = &self.inner.file;
        Exclusive::take(file)
            .and_then(|_exclusive| {
                let slot = newest(file, true)?;
                let slot = if slot.sequence > self.current().slot.sequence {
                    slot
                } else {
                    self.current().slot.clone()
                };
                let slot = self.sync_point(slot)?;
                self.publish(snapshot(file, slot, Some(&self.current()))?);
                Ok(())
            })
            .map_err(|error| storage_error(&self.inner.path, error))
    }

    /// The block `hash` names in `catalog`, if the archive holds it.
    pub fn get(&self, catalog: &str, hash: &Blake3Hash) -> Result<Option<Vec<u8>>, ArchiveError> {
        self.read(catalog, hash)
            .map_err(|error| storage_error(&self.inner.path, error))
    }

    /// How the file holds the block `hash` names in `catalog`, and the
    /// snapshot that says so, looking again after reading the slot when it
    /// is not found and another process grew the file.
    fn place(
        &self,
        catalog: &str,
        hash: &Blake3Hash,
    ) -> io::Result<(Arc<Snapshot>, Option<Place>)> {
        let (snapshot, place) = self.look(|snapshot| snapshot.place(catalog, hash))?;
        if place.is_some() {
            return Ok((snapshot, place));
        }
        let (snapshot, place) = if self.reread()? {
            self.look(|snapshot| snapshot.place(catalog, hash))?
        } else {
            (snapshot, None)
        };
        if place.is_some() {
            return Ok((snapshot, place));
        }
        let loose = self.loose(&snapshot, catalog, hash)?;
        Ok((snapshot, loose))
    }

    /// `find` over the current snapshot, again over a newer one while an
    /// index page it reads was written over since its root was read.
    fn look<T>(&self, find: impl Fn(&Snapshot) -> io::Result<T>) -> io::Result<(Arc<Snapshot>, T)> {
        loop {
            let snapshot = self.current();
            match find(&snapshot) {
                Err(error) if is_stale(&error) => self.newer(&snapshot)?,
                found => return found.map(|found| (snapshot, found)),
            }
        }
    }

    /// Makes a snapshot newer than `than` current, reading the slots if no
    /// newer one is: an index page written over since `than`'s root was
    /// read means another append came since. A page the newest root names
    /// that fails its check is damage, not a race.
    fn newer(&self, than: &Snapshot) -> io::Result<()> {
        if self.current().slot.sequence <= than.slot.sequence {
            let slot = newest(&self.inner.file, false)?;
            if slot.sequence > self.current().slot.sequence {
                self.know(slot.end);
                self.publish(snapshot(&self.inner.file, slot, Some(&self.current()))?);
            }
        }
        if self.current().slot.sequence <= than.slot.sequence {
            return Err(invalid(
                "an index page the newest slot names fails its check",
            ));
        }
        Ok(())
    }

    fn read(&self, catalog: &str, hash: &Blake3Hash) -> io::Result<Option<Vec<u8>>> {
        let (snapshot, Some(place)) = self.place(catalog, hash)? else {
            return Ok(None);
        };
        match place {
            Place::Block(at) => Ok(Some(snapshot.block(at)?.to_vec())),
            Place::Frame(at) => {
                let (rows, manifest) = self.rows(&snapshot, &HashMap::new(), at)?;
                let started = Instant::now();
                let block = self.realize(&snapshot, &rows, manifest)?;
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
            .map(|_| ())
            .map_err(|error| storage_error(&self.inner.path, error))
    }

    /// Reads the slots again if another process grew the file, and
    /// publishes what they say. Whether a newer snapshot is current.
    fn reread(&self) -> io::Result<bool> {
        // Within what this process read or is writing, nothing is new to
        // read. An append of its own under way is not waited for: until it
        // finishes, the block is not there, and a miss says so.
        let len = self.inner.file.metadata()?.len();
        if len <= self.inner.known.load(Ordering::Acquire) {
            return Ok(false);
        }
        let slot = newest(&self.inner.file, false)?;
        if slot.sequence <= self.current().slot.sequence {
            return Ok(false);
        }
        self.know(slot.end);
        self.publish(snapshot(&self.inner.file, slot, Some(&self.current()))?);
        Ok(true)
    }

    /// Records that this process has read, or is writing, the file up to
    /// `end`.
    fn know(&self, end: u64) {
        self.inner.known.fetch_max(end, Ordering::AcqRel);
    }

    /// The block a node's rows stand for.
    fn realize(&self, snapshot: &Snapshot, rows: &Rows, manifest: u32) -> io::Result<Vec<u8>> {
        let manifest = Manifest::decode(
            snapshot
                .get(manifest)
                .ok_or_else(|| invalid("a manifest token the file lacks"))?,
        )
        .map_err(|error| invalid(&error.to_string()))?;
        realize(rows, snapshot, &manifest, |at| {
            snapshot
                .frame_hash(at)
                .map(|hash| Blake3Hash::from(<[u8; 32]>::try_from(hash).expect("32 bytes")))
        })
    }

    /// The rows of the frame starting at `at` (divided by [`ALIGN`]) and
    /// its manifest token, decoded through the frames it is written
    /// against. `staged` holds the rows of frames an append is laying out.
    fn rows(
        &self,
        snapshot: &Snapshot,
        staged: &HashMap<u32, (Arc<Rows>, u32)>,
        at: u32,
    ) -> io::Result<(Arc<Rows>, u32)> {
        if let Some(found) = staged.get(&at) {
            return Ok(found.clone());
        }
        if let Some(found) = self.inner.rows.get(at) {
            return Ok(found);
        }
        let mut reader = Reader::new(snapshot.frame_bytes(at)?);
        let header = FrameHeader::get(&mut reader)?;
        let base = match header.base {
            None => None,
            Some(base) => Some(self.rows(snapshot, staged, base)?.0),
        };
        let started = Instant::now();
        let rows = Arc::new(get_rows(&mut reader, header.kind, base.as_deref())?);
        Timings::add(&self.inner.timings.decode, started);
        self.inner.rows.put(at, (rows.clone(), header.manifest));
        Ok((rows, header.manifest))
    }

    /// The token equal to `token` the file holds as of `snapshot`, by the
    /// value rows name it by: remembered, or found in the index.
    fn find_token(&self, snapshot: &Snapshot, hash: u64, token: &[u8]) -> Option<u32> {
        find_in(&mut self.inner.tokens.lock(), snapshot, hash, token)
    }

    fn remember(&self, tokens: &[(u64, u32)]) {
        let mut remembered = self.inner.tokens.lock();
        for (hash, id) in tokens {
            remembered.insert(*hash, *id);
        }
    }

    /// Appends the `blocks` the archive does not hold yet to `catalog`, as
    /// one append.
    pub fn append(&self, catalog: &str, blocks: &[Buffer]) -> Result<(), ArchiveError> {
        let started = Instant::now();
        let appended = self
            .append_somewhere(catalog, blocks)
            .map_err(|error| storage_error(&self.inner.path, error));
        Timings::add(&self.inner.timings.append, started);
        self.inner.timings.appends.fetch_add(1, Ordering::Relaxed);
        appended
    }

    /// Appends with the writer's turn if it is free, folding in what other
    /// writers left in extents; without it into an extent of this
    /// process's, waiting for the turn only to claim one, and then folding
    /// in if the turn is free by then.
    fn append_somewhere(&self, catalog: &str, blocks: &[Buffer]) -> io::Result<()> {
        if let Some(turn) = self.try_turn()? {
            self.append_in_turn(&turn, catalog, blocks)?;
            // Nothing else writes in the turn: the append's own snapshot is
            // the newest.
            self.integrate(&turn, self.current())?;
            return self.keep_spare(&turn);
        }
        if self.append_loose(catalog, blocks)? {
            if let Some(turn) = self.try_turn()? {
                let snapshot = self.newest_in_turn(&turn)?;
                self.integrate(&turn, snapshot)?;
            }
            return Ok(());
        }
        let turn = self.turn()?;
        self.append_in_turn(&turn, catalog, blocks)?;
        self.integrate(&turn, self.current())?;
        self.keep_spare(&turn)
    }

    /// The writer's turn, waiting for it.
    fn turn(&self) -> io::Result<Turn<'_>> {
        let process = self.inner.writer.lock();
        let file = Exclusive::take(&self.inner.file)?;
        Ok(Turn {
            _process: process,
            _file: file,
        })
    }

    /// The writer's turn, if nobody in this process or another holds it.
    fn try_turn(&self) -> io::Result<Option<Turn<'_>>> {
        let Some(process) = self.inner.writer.try_lock() else {
            return Ok(None);
        };
        Ok(Exclusive::try_take(&self.inner.file)?.map(|file| Turn {
            _process: process,
            _file: file,
        }))
    }

    /// The file's newest state, read in the writer's turn, which another
    /// process may have written since this one looked; an append cut short
    /// is dropped.
    fn newest_in_turn(&self, _turn: &Turn<'_>) -> io::Result<Arc<Snapshot>> {
        let file = &self.inner.file;
        let slot = newest(file, true)?;
        if slot.sequence > self.current().slot.sequence {
            self.publish(snapshot(file, slot, Some(&self.current()))?);
        }
        let current = self.current();
        self.know(current.slot.end);
        Ok(current)
    }

    /// Appends `blocks` in the writer's turn: tokens, frames and the
    /// index's entries, laid out against the file as it is in the turn.
    fn append_in_turn(&self, turn: &Turn<'_>, catalog: &str, blocks: &[Buffer]) -> io::Result<()> {
        let snapshot = self.newest_in_turn(turn)?;
        let Some(prepared) = self.prepare(snapshot.clone(), catalog, blocks)? else {
            return Ok(());
        };
        let laid = self
            .lay_out(&snapshot, prepared)?
            .ok_or_else(|| invalid("an append prepared in its turn did not lay out"))?;
        self.commit(laid).inspect_err(|_| {
            // Drop what of the append reached the file: no slot names it.
            let _ = self.inner.file.set_len(snapshot.slot.end);
            self.inner
                .known
                .store(self.current().slot.end, Ordering::Release);
        })
    }

    /// Appends `blocks` whole into an extent of this process's, without the
    /// writer's turn. `false` when it cannot: a catalog the file does not
    /// number yet, or blocks that would not fit an extent; the caller
    /// appends in the turn then.
    fn append_loose(&self, catalog: &str, blocks: &[Buffer]) -> io::Result<bool> {
        let snapshot = self.current();
        let Some(catalog_id) = snapshot.catalog_id(catalog) else {
            return Ok(false);
        };
        let mut seen = HashSet::new();
        let mut fresh = Vec::new();
        for block in blocks {
            let hash = block.blake3_hash();
            if seen.insert(hash.clone())
                && snapshot.place(catalog, hash)?.is_none()
                && self.loose(&snapshot, catalog, hash)?.is_none()
            {
                fresh.push(block);
            }
        }
        if fresh.is_empty() {
            return Ok(true);
        }
        let mut owned = self.take_extent()?;
        let laid = match owned.lay_out(catalog_id, &fresh)? {
            Some(laid) => laid,
            None => {
                // Full: keep it for integration, and start another.
                self.give_back(owned);
                owned = self.claim()?;
                match owned.lay_out(catalog_id, &fresh)? {
                    Some(laid) => laid,
                    None => {
                        self.give_back(owned);
                        return Ok(false);
                    }
                }
            }
        };
        let (bytes, places) = laid;
        let written = owned.write(&self.inner.file, &bytes);
        if written.is_ok() {
            let mut loose = self.inner.loose.lock();
            for (block, at) in fresh.iter().zip(places) {
                loose.blocks.insert(
                    (catalog_id, block.blake3_hash().clone()),
                    (owned.record, at),
                );
            }
            loose.read.insert(owned.record, owned.used);
        }
        self.give_back(owned);
        written.map(|()| true)
    }

    /// An extent of this process's for a writer to append to: an idle one,
    /// else a new one claimed.
    fn take_extent(&self) -> io::Result<Owned> {
        let taken = {
            let mut extents = self.inner.extents.lock();
            let taken = extents.idle.pop();
            if let Some(owned) = &taken {
                extents.busy.insert(owned.record);
            }
            taken
        };
        match taken {
            Some(owned) => Ok(owned),
            None => self.claim(),
        }
    }

    /// Returns an extent a writer took, for the next to take.
    fn give_back(&self, owned: Owned) {
        let mut extents = self.inner.extents.lock();
        extents.busy.remove(&owned.record);
        extents.idle.push(owned);
    }

    /// Claims an extent for this process, waiting for the writer's turn:
    /// the one wait a writer without the turn has, and only when its
    /// process had no extent ready.
    fn claim(&self) -> io::Result<Owned> {
        let turn = self.turn()?;
        let claimed = self.claim_in_turn(&turn)?;
        self.inner.extents.lock().busy.insert(claimed.record);
        Ok(claimed)
    }

    /// Keeps an extent ready for this process's writers, claimed while it
    /// holds the turn anyway, so a writer that later finds the turn taken
    /// writes at once instead of waiting to claim one.
    fn keep_spare(&self, turn: &Turn<'_>) -> io::Result<()> {
        {
            let extents = self.inner.extents.lock();
            if !extents.idle.is_empty() || !extents.busy.is_empty() {
                return Ok(());
            }
        }
        let spare = self.claim_in_turn(turn)?;
        self.inner.extents.lock().idle.push(spare);
        Ok(())
    }

    /// Claims an extent in the writer's turn: one freed by an integration a
    /// sync point has made durable, else a new one at the file's end.
    fn claim_in_turn(&self, turn: &Turn<'_>) -> io::Result<Owned> {
        let snapshot = self.newest_in_turn(turn)?;
        let file = &self.inner.file;
        let mut slot = snapshot.slot.clone();
        slot.sequence += 1;
        let (record, reused) = match slot.extents_free.pop() {
            Some(record) => (record, true),
            None => {
                let record = slot.end;
                let bytes = extent::record(record, self.inner.options.extent)?;
                write_at(file, record, &bytes)?;
                slot.end += bytes.len() as u64;
                (record, false)
            }
        };
        slot.extents.push(record);
        slot.boot = boot();
        self.know(slot.end);
        snapshot_after_claim(self, file, slot, reused, record).inspect_err(|_| {
            let _ = file.set_len(snapshot.slot.end);
        })
    }

    /// Where `catalog`'s block `hash` is in an extent, as far as this
    /// process has read the extents `snapshot` names: read again past where
    /// it stopped, and checked against the hash before it is trusted.
    fn loose(
        &self,
        snapshot: &Snapshot,
        catalog: &str,
        hash: &Blake3Hash,
    ) -> io::Result<Option<Place>> {
        if snapshot.slot.extents.is_empty() {
            return Ok(None);
        }
        let Some(catalog_id) = snapshot.catalog_id(catalog) else {
            return Ok(None);
        };
        let bytes = snapshot.bytes();
        let mut loose = self.inner.loose.lock();
        if loose.sequence != snapshot.slot.sequence {
            // Extents integrated since are dropped: their blocks are in
            // the index now.
            let live: HashSet<u64> = snapshot.slot.extents.iter().copied().collect();
            loose.read.retain(|record, _| live.contains(record));
            loose.blocks.retain(|_, (record, _)| live.contains(record));
            loose.sequence = snapshot.slot.sequence;
        }
        for &record in &snapshot.slot.extents {
            let Some(used) = extent::used(bytes, record) else {
                continue;
            };
            let read = loose.read.get(&record).copied().unwrap_or(0);
            // An extent emptied to be claimed again starts over.
            let read = if used < read { 0 } else { read };
            if used > read {
                for held in extent::held(bytes, record, read, used)? {
                    loose
                        .blocks
                        .insert((held.catalog, held.hash), (record, held.at));
                }
            }
            loose.read.insert(record, used);
        }
        let Some(&(_, at)) = loose.blocks.get(&(catalog_id, hash.clone())) else {
            return Ok(None);
        };
        drop(loose);
        // A block whose extent was emptied and written over since is not
        // there any more: its bytes no longer hash to it.
        match snapshot.block(at) {
            Ok(block) if Blake3Hash::hash(block) == *hash => Ok(Some(Place::Block(at))),
            _ => Ok(None),
        }
    }

    /// Folds what extents hold into the file as frames, in the writer's
    /// turn: this process's idle extents, and any whose owner is gone. Once
    /// an extent's blocks are appended, the extent is retired; a sync point
    /// frees it to be claimed again.
    fn integrate(&self, turn: &Turn<'_>, snapshot: Arc<Snapshot>) -> io::Result<()> {
        if snapshot.slot.extents.is_empty() {
            return Ok(());
        }
        let file = &self.inner.file;
        let mut ours = Vec::new();
        let mut abandoned = Vec::new();
        {
            let mut extents = self.inner.extents.lock();
            for &record in &snapshot.slot.extents {
                if let Some(at) = extents.idle.iter().position(|owned| owned.record == record) {
                    // An empty one stays ready for the next writer.
                    if extents.idle[at].used > 0 {
                        ours.push(extents.idle.swap_remove(at));
                    }
                } else if !extents.busy.contains(&record)
                    && locks::lock(file, locks::owner(record), true, false)?
                {
                    abandoned.push(record);
                }
            }
        }
        let records: Vec<u64> = ours
            .iter()
            .map(|owned| owned.record)
            .chain(abandoned.iter().copied())
            .collect();
        if records.is_empty() {
            return Ok(());
        }
        let integrated = self.fold(turn, &snapshot, &records);
        for record in &records {
            let _ = locks::unlock(file, locks::owner(*record));
        }
        if let Err(error) = integrated {
            // Keep this process's extents for the next try.
            let mut extents = self.inner.extents.lock();
            extents.idle.extend(ours);
            return Err(error);
        }
        Ok(())
    }

    /// Appends the blocks the extents at `records` hold, then retires the
    /// extents.
    fn fold(&self, turn: &Turn<'_>, snapshot: &Snapshot, records: &[u64]) -> io::Result<()> {
        let bytes = snapshot.bytes();
        let mut by_catalog: HashMap<u16, Vec<Buffer>> = HashMap::new();
        for &record in records {
            let used = extent::used(bytes, record).unwrap_or(0);
            for held in extent::held(bytes, record, 0, used)? {
                let block = snapshot.block(held.at)?;
                by_catalog
                    .entry(held.catalog)
                    .or_default()
                    .push(Buffer::from(block.to_vec()));
            }
        }
        for (catalog, blocks) in by_catalog {
            let name = snapshot
                .catalog(catalog)
                .ok_or_else(|| invalid("an extent's block of a catalog the file lacks"))?
                .to_string();
            self.append_in_turn(turn, &name, &blocks)?;
        }
        // Retire the extents: their blocks are frames now.
        let snapshot = self.newest_in_turn(turn)?;
        let mut slot = snapshot.slot.clone();
        slot.sequence += 1;
        slot.extents.retain(|record| !records.contains(record));
        slot.extents_pending.extend_from_slice(records);
        slot.boot = boot();
        slot.write(&self.inner.file)?;
        self.publish(self::snapshot(
            &self.inner.file,
            slot,
            Some(&self.current()),
        )?);
        Ok(())
    }

    /// What an append of `blocks` to `catalog` writes, worked out without
    /// the writer's turn, against `snapshot`: the new tokens, each tree
    /// node split into rows, and the blocks kept whole. `None` when the
    /// file holds every block already.
    fn prepare<'a>(
        &'a self,
        snapshot: Arc<Snapshot>,
        catalog: &'a str,
        blocks: &[Buffer],
    ) -> io::Result<Option<Prepared<'a>>> {
        let mut seen = HashSet::new();
        self.inner
            .timings
            .offered
            .fetch_add(blocks.len() as u64, Ordering::Relaxed);
        let mut fresh = Vec::new();
        for block in blocks {
            let hash = block.blake3_hash();
            if seen.insert(hash.clone()) && snapshot.place(catalog, hash)?.is_none() {
                fresh.push(block);
            }
        }
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

        // New tokens and frames are named above everything the snapshot
        // holds, until the turn gives them their places.
        let first = token_id(snapshot.slot.end)? + 1;
        let finding = snapshot.clone();
        let mut draft = Draft::new(
            snapshot.clone(),
            Box::new(move |hash, token| self.find_token(&finding, hash, token)),
            first,
        );
        let catalog_id = snapshot.catalog_id(catalog);
        let mut staged: HashMap<u32, (Arc<Rows>, u32, Blake3Hash)> = HashMap::new();
        let mut staged_ids: HashMap<Blake3Hash, u32> = HashMap::new();
        let mut staged_lineages: HashMap<(u8, [u32; 4]), u32> = HashMap::new();
        let mut staged_depths: HashMap<u32, u8> = HashMap::new();
        for (block, node) in children_first(nodes) {
            let framed = self.frame(
                &snapshot,
                &mut draft,
                &Staging {
                    first,
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
            let id = first + staged.len() as u32;
            staged_ids.insert(block.blake3_hash().clone(), id);
            if let Some(lineage) = frame.rows.lineage() {
                staged_lineages.insert((frame.rows.kind, lineage), id);
            }
            staged_depths.insert(id, frame.depth);
            staged.insert(
                id,
                (
                    frame.rows.clone(),
                    frame.manifest,
                    block.blake3_hash().clone(),
                ),
            );
            items.push(Item::Frame(id, block, frame));
        }
        Ok(Some(Prepared {
            catalog,
            first,
            tokens: draft,
            items,
        }))
    }

    /// Writes `prepared` (of `blocks`, worked out before the turn) in the
    /// writer's turn, laid out against the file as it is then; prepares
    /// again in the turn if it cannot be. For tests of an append another
    /// lands ahead of.
    #[cfg(test)]
    fn flush(
        &self,
        prepared: Prepared<'_>,
        catalog: &str,
        blocks: &[Buffer],
    ) -> Result<(), ArchiveError> {
        let error = |error| storage_error(&self.inner.path, error);
        let turn = self.turn().map_err(error)?;
        let snapshot = self.newest_in_turn(&turn).map_err(error)?;
        let laid = match self.lay_out(&snapshot, prepared).map_err(error)? {
            Some(laid) => laid,
            None => match self
                .prepare(snapshot.clone(), catalog, blocks)
                .map_err(error)?
            {
                Some(prepared) => self
                    .lay_out(&snapshot, prepared)
                    .map_err(error)?
                    .ok_or_else(|| {
                        error(invalid("an append prepared in its turn did not lay out"))
                    })?,
                None => return Ok(()),
            },
        };
        self.commit(laid).map_err(error)
    }

    /// `prepared` laid out after `snapshot`'s end: blocks the file holds by
    /// now dropped, new tokens and frames given their places, frames
    /// renamed and encoded, and the index's new entries inserted. `None`
    /// when a node the append framed is held whole by now.
    fn lay_out(&self, snapshot: &Snapshot, prepared: Prepared<'_>) -> io::Result<Option<Laid>> {
        let Prepared {
            catalog,
            first,
            tokens: draft,
            items,
        } = prepared;
        let mut slot = snapshot.slot.clone();
        slot.sequence += 1;
        let catalog_id = catalog_in(&mut slot, catalog)?;
        let start = slot.end;

        // Blocks the file holds by now are not written again; a node it
        // holds as a frame is named by that frame.
        let mut nodes: HashMap<u32, u32> = HashMap::new();
        let mut kept = Vec::with_capacity(items.len());
        for item in items {
            let hash = match &item {
                Item::Block(block) | Item::Frame(_, block, _) => block.blake3_hash().clone(),
            };
            match (snapshot.probe_block(catalog_id, &hash)?.0, item) {
                (None, item) => kept.push(item),
                (Some(Place::Frame(at)), Item::Frame(id, ..)) => {
                    nodes.insert(id, at);
                }
                (Some(Place::Block(_)), Item::Frame(..)) => return Ok(None),
                (Some(_), Item::Block(_)) => {}
            }
        }

        // Tokens: a drafted one the file holds by now is named as it is;
        // the rest go into one record, first.
        let mut bytes = Vec::new();
        let mut tokens: Vec<u32> = Vec::with_capacity(draft.pending());
        let mut new = Vec::new();
        for id in first..first + draft.pending() as u32 {
            let token = draft.get(id).expect("a drafted token");
            let hash = draft.hash(id).expect("a drafted token's hash");
            match self.find_token(snapshot, hash, token) {
                Some(held) => tokens.push(held),
                None => {
                    tokens.push(0);
                    new.push(id);
                }
            }
        }
        let mut taken = HashSet::new();
        let mut entries = Vec::new();
        let mut remembered = Vec::new();
        if !new.is_empty() {
            let body_offset = start + bytes.len() as u64 + HEADER;
            let (body, places) = draft.record(body_offset, &new);
            put_record(&mut bytes, Kind::Tokens, &body)?;
            for (id, place) in new.iter().zip(places) {
                let named = token_id(place)?;
                tokens[(*id - first) as usize] = named;
                let token = draft.get(*id).expect("a drafted token");
                let hash = draft.hash(*id).expect("a drafted token's hash");
                let (_, key) = snapshot.probe_token(hash, token)?;
                entries.push((free(snapshot, key, &mut taken)?, named));
                remembered.push((hash, named));
                slot.counts.tokens += 1;
                slot.counts.token_bytes += token.len() as u64;
            }
        }
        let token = |id: u32| {
            if id < first {
                id
            } else {
                tokens[(id - first) as usize]
            }
        };

        // Blocks and frames, children first, each frame renamed to where
        // its tokens and children are, then encoded against its base.
        let mut staged: HashMap<u32, (Arc<Rows>, u32)> = HashMap::new();
        let mut depths: HashMap<u32, u8> = HashMap::new();
        let mut rows_laid = Vec::new();
        for item in kept {
            match item {
                Item::Block(block) => {
                    // A block kept whole: a frame of the whole kind, its
                    // length and bytes after the header.
                    let mut data = Vec::with_capacity(block.as_ref().len() + 48);
                    FrameHeader {
                        hash: block.blake3_hash().clone(),
                        kind: WHOLE,
                        depth: 0,
                        catalog: catalog_id,
                        base: None,
                        manifest: 0,
                    }
                    .put(&mut data);
                    put_varint(&mut data, block.as_ref().len() as u64);
                    data.extend_from_slice(block.as_ref());
                    let at = put_frame(&mut bytes, start, &data)?;
                    let key = block_key(catalog_id, block.blake3_hash());
                    entries.push((free(snapshot, key, &mut taken)?, Place::Block(at).value()));
                    slot.counts.blocks += 1;
                    slot.counts.block_bytes += block.as_ref().len() as u64;
                }
                Item::Frame(id, block, frame) => {
                    let started = Instant::now();
                    let node = |id: u32| {
                        if id < first {
                            id
                        } else {
                            nodes.get(&id).copied().unwrap_or(id)
                        }
                    };
                    let rows = Arc::new(rename(&frame.rows, token, node));
                    let base = frame.base.map(node);
                    let (base_rows, depth) = match base {
                        Some(base) => {
                            let depth = match depths.get(&base) {
                                Some(depth) => depth + 1,
                                None => snapshot.frame(base)?.depth + 1,
                            };
                            (Some(self.rows(snapshot, &staged, base)?.0), depth)
                        }
                        None => (None, 0),
                    };
                    let header = FrameHeader {
                        hash: block.blake3_hash().clone(),
                        kind: rows.kind,
                        depth,
                        catalog: catalog_id,
                        base,
                        manifest: token(frame.manifest),
                    };
                    let mut data = Vec::new();
                    header.put(&mut data);
                    put_rows(&mut data, &rows, base_rows.as_deref());
                    Timings::add(&self.inner.timings.encode, started);
                    // The frame reads back to the same rows.
                    let started = Instant::now();
                    let mut reader = Reader::new(&data);
                    FrameHeader::get(&mut reader)?;
                    let decoded = get_rows(&mut reader, rows.kind, base_rows.as_deref())?;
                    Timings::add(&self.inner.timings.verify, started);
                    if decoded != *rows {
                        self.inner.timings.unread.fetch_add(1, Ordering::Relaxed);
                        return Err(invalid("a frame did not read back to its rows"));
                    }
                    let record = bytes.len();
                    let at = put_frame(&mut bytes, start, &data)?;
                    nodes.insert(id, at);
                    depths.insert(at, depth);
                    staged.insert(at, (rows.clone(), header.manifest));
                    rows_laid.push((at, rows.clone(), header.manifest));
                    let key = block_key(catalog_id, block.blake3_hash());
                    entries.push((free(snapshot, key, &mut taken)?, Place::Frame(at).value()));
                    if let Some(lineage) = rows.lineage() {
                        entries.push((lineage_key(catalog_id, rows.kind, lineage), at));
                    }
                    slot.counts.frames += 1;
                    slot.counts.frames_against += base.is_some() as u64;
                    slot.counts.frame_bytes += (bytes.len() - record) as u64;
                }
            }
        }

        let pages = write_index(snapshot, &mut slot, start, &mut bytes, entries, Vec::new())?;
        Ok(Some(Laid {
            start,
            bytes,
            pages,
            slot,
            rows: rows_laid,
            tokens: remembered,
        }))
    }

    /// Writes an append laid out in this turn, then its slot, then
    /// publishes it.
    fn commit(&self, laid: Laid) -> io::Result<()> {
        let file = &self.inner.file;
        self.know(laid.slot.end);
        let started = Instant::now();
        write_at(file, laid.start, &laid.bytes)?;
        for (page, bytes) in &laid.pages {
            write_at(file, *page as u64 * PAGE as u64, bytes)?;
        }
        self.inner
            .timings
            .reused
            .fetch_add(laid.pages.len() as u64, Ordering::Relaxed);
        #[cfg(test)]
        if let Some(hook) = self.inner.before_publish.lock().as_mut() {
            hook();
        }
        let mut slot = laid.slot;
        slot.boot = boot();
        slot.write(file)?;
        Timings::add(&self.inner.timings.write, started);
        if slot.pending.len() >= SYNC_PAGES
            || slot.sequence - slot.durable >= SYNC_APPENDS
            || self.inner.synced.lock().elapsed() >= SYNC_AFTER
        {
            slot = self.sync_point(slot)?;
        }
        for (at, rows, manifest) in laid.rows {
            self.inner.rows.put(at, (rows, manifest));
        }
        self.remember(&laid.tokens);
        self.publish(snapshot(file, slot, Some(&self.current()))?);
        Ok(())
    }

    /// Makes everything `slot` names durable, in the writer's turn: the
    /// file synced, then the durable slot written as `slot` and synced,
    /// then the next rolling slot written with the pages freed until now
    /// free to write over, since the durable slot reaches none of them.
    /// Returns that next slot.
    fn sync_point(&self, slot: Slot) -> io::Result<Slot> {
        let file = &self.inner.file;
        let started = Instant::now();
        file.sync_data()?;
        let mut durable = slot;
        durable.reusable.append(&mut durable.pending);
        durable.extents_free.append(&mut durable.extents_pending);
        durable.durable = durable.sequence;
        durable.write_durable(file)?;
        file.sync_data()?;
        let mut next = durable;
        next.sequence += 1;
        next.write(file)?;
        *self.inner.synced.lock() = Instant::now();
        Timings::add(&self.inner.timings.sync, started);
        self.inner.timings.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(next)
    }

    /// The frame `node` is written as, if it splits into rows (and, when
    /// verifying, they realize to its exact `block`): its rows, manifest
    /// token, base and depth, as drafted. `None` keeps the block whole.
    #[allow(clippy::too_many_arguments)]
    fn frame(
        &self,
        snapshot: &Snapshot,
        draft: &mut Draft<'_>,
        staging: &Staging<'_>,
        catalog: &str,
        catalog_id: Option<u16>,
        block: &Buffer,
        node: &TreeNode,
    ) -> io::Result<Option<Frame>> {
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
                .or_else(|| match snapshot.place(catalog, hash) {
                    Ok(Some(Place::Frame(at))) => Some(at),
                    _ => None,
                })
        };
        // A node that does not split into rows (another tree's, say), or
        // whose rows do not rebuild its block, is kept whole, and the
        // tokens interned for it are dropped again: nothing names them.
        let mark = draft.pending();
        let started = Instant::now();
        // The node it supersedes, by its first row: entries it keeps
        // unchanged take that node's rows rather than being interned again.
        let no_staging = HashMap::new();
        let predecessor = |kind: u8, first: [u32; 4]| -> Option<Arc<Rows>> {
            let id = staging.lineages.get(&(kind, first)).copied().or_else(|| {
                let catalog = catalog_id?;
                let found = snapshot.lineage(lineage_key(catalog, kind, first)).ok()??;
                (snapshot.frame(found).ok()?.kind == kind).then_some(found)
            })?;
            if id >= staging.first {
                staging.rows.get(&id).map(|(rows, _, _)| rows.clone())
            } else {
                self.rows(snapshot, &no_staging, id)
                    .ok()
                    .map(|(rows, _)| rows)
            }
        };
        let rows = rows_after(node, draft, child, predecessor);
        Timings::add(&self.inner.timings.split, started);
        let Ok((rows, taken)) = rows else {
            draft.roll_back(mark);
            return Ok(None);
        };
        self.inner
            .timings
            .rows_taken
            .fetch_add(taken as u64, Ordering::Relaxed);
        let manifest_id = draft.intern(&manifest_bytes);
        // The node it supersedes: the latest of its lineage, among this
        // append's or the file's, while the chain stays short.
        let depth_of = |id: u32| -> Option<u8> {
            if id >= staging.first {
                staging.depths.get(&id).copied()
            } else {
                snapshot.frame(id).ok().map(|header| header.depth)
            }
        };
        let base = rows
            .lineage()
            .and_then(|first| {
                staging
                    .lineages
                    .get(&(rows.kind, first))
                    .copied()
                    .or_else(|| {
                        let catalog = catalog_id?;
                        let found = snapshot
                            .lineage(lineage_key(catalog, rows.kind, first))
                            .ok()??;
                        // A key two lineages share names another kind's
                        // frame now and then: only a frame of this kind is
                        // a base.
                        (snapshot.frame(found).ok()?.kind == rows.kind).then_some(found)
                    })
            })
            .filter(|base| {
                depth_of(*base).is_some_and(|depth| (depth as usize) + 1 < self.inner.options.chain)
            });
        let depth = base.and_then(depth_of).map_or(0, |depth| depth + 1);
        // When verifying, the rows rebuild the very block (see
        // `Options::verify`).
        if self.inner.options.verify {
            let started = Instant::now();
            let realized = realize(&rows, &*draft, &manifest, |id| {
                if id >= staging.first {
                    staging.rows.get(&id).map(|(_, _, hash)| hash.clone())
                } else {
                    snapshot
                        .frame_hash(id)
                        .map(|hash| Blake3Hash::from(<[u8; 32]>::try_from(hash).expect("32 bytes")))
                }
            });
            Timings::add(&self.inner.timings.verify, started);
            if realized.ok().as_deref() != Some(block.as_ref()) {
                self.inner
                    .timings
                    .unverified
                    .fetch_add(1, Ordering::Relaxed);
                draft.roll_back(mark);
                return Ok(None);
            }
        }
        Ok(Some(Frame {
            rows: Arc::new(rows),
            manifest: manifest_id,
            base,
            depth,
        }))
    }
}

/// Inserts `blocks` into the slot's index and `cells` into its cells'
/// index, for an append laid out as `bytes` from `start`: the pages the
/// inserts rewrite go to pages free to write over first, then to new ones
/// in a record of their own after `bytes`, each on a page boundary. Sets
/// the slot's end past them; returns the pages written over, and their
/// bytes.
fn write_index(
    snapshot: &Snapshot,
    slot: &mut Slot,
    start: u64,
    bytes: &mut Vec<u8>,
    blocks: Vec<tree::Entry>,
    cells: Vec<tree::Entry>,
) -> io::Result<Vec<(u32, Vec<u8>)>> {
    let body = start + bytes.len() as u64 + HEADER;
    let first = (body + 2).div_ceil(PAGE as u64);
    let mut writer = Writer {
        snapshot,
        sequence: slot.sequence as u32,
        reusable: std::mem::take(&mut slot.reusable),
        pending: std::mem::take(&mut slot.pending),
        written: HashMap::new(),
        unused: Vec::new(),
        first: u32::try_from(first).map_err(|_| invalid("an index page past 16 TiB"))?,
        next: first as u32,
    };
    if !blocks.is_empty() {
        let root = std::mem::take(&mut slot.root);
        slot.root = root.insert(&mut writer, blocks)?;
    }
    if !cells.is_empty() {
        let root = std::mem::take(&mut slot.cells);
        slot.cells = root.insert(&mut writer, cells)?;
    }
    let mut pages = Vec::new();
    let mut written = std::mem::take(&mut writer.written);
    if writer.next > writer.first {
        let pad = (first * PAGE as u64 - body - 2) as usize;
        let mut record = Vec::with_capacity(2 + pad + (writer.next - writer.first) as usize * PAGE);
        record.extend_from_slice(&(pad as u16).to_le_bytes());
        record.resize(2 + pad, 0);
        for page in writer.first..writer.next {
            match written.remove(&page) {
                Some(page) => record.extend_from_slice(&page),
                None => {
                    // Written and freed again in this append: free.
                    record.resize(record.len() + PAGE, 0);
                }
            }
        }
        put_record(bytes, Kind::Pages, &record)?;
    }
    pages.extend(written);
    slot.reusable = writer.reusable;
    slot.reusable.append(&mut writer.unused);
    slot.pending = writer.pending;
    slot.end = start + bytes.len() as u64;
    Ok(pages)
}

/// Writes the slot that claims the extent at `record`, publishes it and
/// takes the extent's owner's byte; an extent claimed again is emptied
/// first.
fn snapshot_after_claim(
    archive: &FileArchive,
    file: &File,
    slot: Slot,
    reused: bool,
    record: u64,
) -> io::Result<Owned> {
    slot.write(file)?;
    archive.publish(snapshot(file, slot, Some(&archive.current()))?);
    if !locks::lock(file, locks::owner(record), true, false)? {
        return Err(invalid("an extent just claimed is held by another process"));
    }
    let mut owned = Owned::of(archive.current().bytes(), record)?;
    if reused {
        owned.clear(file)?;
    }
    Ok(owned)
}

/// The last handle of a file in this process folds what its extents hold
/// into the file before it goes, waiting for the turn: closing a space
/// waits for it. What it cannot fold, the next writer to take the turn
/// does, as the extent's owner is gone.
impl Drop for FileArchive {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) != 1 || self.inner.extents.lock().idle.is_empty() {
            return;
        }
        if let Ok(turn) = self.turn()
            && let Ok(snapshot) = self.newest_in_turn(&turn)
        {
            let _ = self.integrate(&turn, snapshot);
        }
    }
}

/// The nodes an append frames before any is in the file: their rows,
/// ids, lineages and depths, for the next node to name or be written
/// against. Ids from `first` up are this append's.
struct Staging<'a> {
    first: u32,
    rows: &'a HashMap<u32, (Arc<Rows>, u32, Blake3Hash)>,
    ids: &'a HashMap<Blake3Hash, u32>,
    lineages: &'a HashMap<(u8, [u32; 4]), u32>,
    depths: &'a HashMap<u32, u8>,
}

// The file answers from its mapping and appends in place: nothing to wait
// on, so every answer is at hand.
impl Provider<Get> for FileArchive {
    fn execute(
        &self,
        effect: Capability<Get>,
    ) -> Resume<'_, Result<Option<Vec<u8>>, ArchiveError>> {
        Resume::now(self.get(effect.catalog(), effect.digest()))
    }
}

impl Provider<Put> for FileArchive {
    fn execute(&self, effect: Capability<Put>) -> Resume<'_, Result<(), ArchiveError>> {
        let block = Buffer::from(effect.content().to_vec());
        Resume::now(self.append(effect.catalog(), std::slice::from_ref(&block)))
    }
}

impl Provider<Import> for FileArchive {
    fn execute(&self, effect: Capability<Import>) -> Resume<'_, Result<(), ArchiveError>> {
        Resume::now(self.append(effect.catalog(), effect.blocks()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Did, Subject};
    use dialog_effects::prelude::*;

    /// A writer that finds the writer's turn taken by another process does
    /// not wait for it: its blocks go into an extent of its own and read
    /// back at once, here and through another handle. Once the turn is
    /// free, the next append folds them into the file and retires the
    /// extent; everything reads back after reopening.
    #[test]
    fn it_appends_without_waiting_while_another_process_holds_the_turn() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let archive = FileArchive::open(&path)?;
        let first = block("first");
        archive.append("index", std::slice::from_ref(&first))?;
        // Holding the turn, the writer kept an extent ready.
        let extents = archive.current().slot.extents.clone();
        assert_eq!(extents.len(), 1, "a spare extent claimed in the turn");
        let extent = extents[0];
        // Another handle on the file, as another process reads it. Opened
        // first: opening waits for an append under way to finish.
        let other_path = directory.path().join("other");
        std::fs::hard_link(&path, &other_path)?;
        let other = FileArchive::open(&other_path)?;

        let mut holder = crate::locks::tests::hold_turn(&path)?;
        let loose = [block("loose one"), block("loose two")];
        let started = Instant::now();
        archive.append("index", &loose)?;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the append did not wait for the turn"
        );
        assert_eq!(
            archive.current().slot.extents,
            vec![extent],
            "it wrote to the spare extent, claiming none"
        );
        for block in &loose {
            assert_eq!(
                archive.get("index", block.blake3_hash())?.as_deref(),
                Some(block.as_ref()),
                "read back from the extent"
            );
        }
        for block in &loose {
            assert_eq!(
                other.get("index", block.blake3_hash())?.as_deref(),
                Some(block.as_ref()),
                "another handle reads the extent too"
            );
        }

        drop(holder.stdin.take());
        holder.wait()?;
        let last = block("last");
        archive.append("index", std::slice::from_ref(&last))?;
        let slot = archive.current().slot.clone();
        assert!(!slot.extents.contains(&extent), "the extent is folded in");
        assert_eq!(
            slot.extents_pending,
            vec![extent],
            "and retired until a sync point"
        );
        for block in loose.iter().chain([&first, &last]) {
            assert_eq!(
                archive.get("index", block.blake3_hash())?.as_deref(),
                Some(block.as_ref())
            );
            assert!(
                archive
                    .current()
                    .place("index", block.blake3_hash())?
                    .is_some(),
                "the index holds it"
            );
        }

        drop((archive, other));
        let reopened = FileArchive::open(&path)?;
        for block in loose.iter().chain([&first, &last]) {
            assert_eq!(
                reopened.get("index", block.blake3_hash())?.as_deref(),
                Some(block.as_ref())
            );
        }
        Ok(())
    }

    /// An extent a writer left before folding it in (its process gone) is
    /// read from where it is after reopening, and folded in by the next
    /// writer to take the turn.
    #[test]
    fn it_reads_and_folds_in_an_extent_its_writer_left() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let archive = FileArchive::open(&path)?;
        archive.append("index", &[block("first")])?;
        let mut holder = crate::locks::tests::hold_turn(&path)?;
        let loose = block("left in an extent");
        archive.append("index", std::slice::from_ref(&loose))?;
        // The process goes without folding it in: its extents are forgotten
        // as if it had died.
        archive.inner.extents.lock().idle.clear();
        drop(archive);
        drop(holder.stdin.take());
        holder.wait()?;

        let reopened = FileArchive::open(&path)?;
        let left = reopened.current().slot.extents.clone();
        assert_eq!(left.len(), 1);
        assert_eq!(
            reopened.get("index", loose.blake3_hash())?.as_deref(),
            Some(loose.as_ref()),
            "read from the extent it was left in"
        );
        reopened.append("index", &[block("next")])?;
        assert!(
            !reopened.current().slot.extents.contains(&left[0]),
            "folded in"
        );
        assert!(
            reopened
                .current()
                .place("index", loose.blake3_hash())?
                .is_some()
        );
        Ok(())
    }

    fn subject() -> Subject {
        let did: Did = "did:test:file-archive".parse().expect("a test DID");
        Subject::from(did)
    }

    fn block(text: &str) -> Buffer {
        Buffer::from(text.as_bytes().to_vec())
    }

    /// A snapshot keeps the mapping it is given while the file grows into
    /// it, and reads what was appended after the mapping was made; past
    /// it, the file is mapped again, reserving room to grow.
    #[cfg(unix)]
    #[test]
    fn it_maps_the_file_again_only_when_it_outgrows_the_mapping() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(directory.path().join("mapped"))?;
        write_at(&file, 0, b"first")?;
        let first = snapshot(
            &file,
            Slot {
                end: 5,
                ..Slot::default()
            },
            None,
        )?;
        assert_eq!(first.map.len() as u64, MAPPED);

        write_at(&file, 5, b" and then")?;
        let second = snapshot(
            &file,
            Slot {
                sequence: 1,
                end: 14,
                ..Slot::default()
            },
            Some(&first),
        )?;
        assert!(Arc::ptr_eq(&first.map, &second.map));
        assert_eq!(second.bytes(), b"first and then");

        let past = MAPPED + 1;
        file.set_len(past)?;
        let third = snapshot(
            &file,
            Slot {
                sequence: 2,
                end: past,
                ..Slot::default()
            },
            Some(&second),
        )?;
        assert!(!Arc::ptr_eq(&second.map, &third.map));
        assert_eq!(third.map.len() as u64, 2 * past);
        assert_eq!(&third.bytes()[..14], b"first and then");
        Ok(())
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
            let archive = FileArchive::open(&path)?;
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

    /// A slot whose bytes changed is not whole: opening takes the other,
    /// and drops the append only the changed one named.
    #[tokio::test]
    async fn it_opens_from_the_other_slot_when_one_changed() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let first = block("first");
        let second = block("second");
        let (boundary, sequence) = {
            let archive = FileArchive::open(&path)?;
            archive.append("index", std::slice::from_ref(&first))?;
            let boundary = std::fs::metadata(&path)?.len();
            archive.append("index", std::slice::from_ref(&second))?;
            (boundary, archive.current().slot.sequence)
        };
        // The second append's sequence picks the slot it wrote: flip a byte
        // inside it.
        let mut bytes = std::fs::read(&path)?;
        let newest = MAGIC.len() + (sequence % 2) as usize * SLOT as usize;
        bytes[newest + 20] ^= 0xFF;
        std::fs::write(&path, &bytes)?;

        let archive = FileArchive::open(&path)?;
        assert_eq!(archive.len(), 1);
        assert!(archive.get("index", first.blake3_hash())?.is_some());
        assert!(archive.get("index", second.blake3_hash())?.is_none());
        assert_eq!(std::fs::metadata(&path)?.len(), boundary);
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

    /// An append prepared before another one lands takes its places when
    /// its turn comes: a node the other appended is not written again,
    /// tokens the other wrote first are not either, and every block reads
    /// back, in this process and after reopening.
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
            .prepare(archive.current(), "index", &first)?
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
            .prepare(archive.current(), "index", &first)?
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

    /// Appends `appends` batches of 40 whole blocks to `archive`, named
    /// from `from` on.
    fn appended(archive: &FileArchive, from: usize, appends: usize) -> anyhow::Result<Vec<Buffer>> {
        let blocks: Vec<Buffer> = (from..from + appends * 40)
            .map(|n| block(&format!("paged {n}")))
            .collect();
        for batch in blocks.chunks(40) {
            archive.append("index", batch)?;
        }
        Ok(blocks)
    }

    /// The pages in `archive`'s pages records.
    fn pages_in_file(archive: &FileArchive) -> usize {
        let snapshot = archive.current();
        let bytes = snapshot.bytes();
        let (mut at, mut pages) = (DATA as usize, 0);
        while let Some((kind, body, end)) = record_in(bytes, at) {
            if kind == Kind::Pages {
                let pad = u16::from_le_bytes([bytes[body], bytes[body + 1]]) as usize;
                pages += (end - body - 2 - pad) / PAGE;
            }
            at = end;
        }
        pages
    }

    /// Once a sync point has made it safe, an append writes index pages
    /// over where they were: every page in the file is one the root
    /// reaches or one free to write over, and the file holds fewer pages
    /// than were written.
    #[test]
    fn it_writes_index_pages_over_once_a_sync_point_frees_them() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let archive = FileArchive::open(directory.path().join("archive"))?;
        let blocks = appended(&archive, 0, 1500)?;
        let reused = archive.inner.timings.reused.load(Ordering::Relaxed);
        assert!(reused > 0, "pages were written over");
        let snapshot = archive.current();
        let mut reached = Vec::new();
        snapshot.slot.root.pages(&*snapshot, &mut reached)?;
        let in_file = pages_in_file(&archive);
        assert_eq!(
            reached.len() + snapshot.slot.reusable.len() + snapshot.slot.pending.len(),
            in_file,
            "every page is reached or free"
        );
        assert!(
            in_file < reached.len() * 2,
            "{in_file} pages for {} reached",
            reached.len()
        );
        for block in &blocks {
            assert_eq!(
                archive.get("index", block.blake3_hash())?.as_deref(),
                Some(block.as_ref())
            );
        }
        Ok(())
    }

    /// A snapshot held across appends that wrote over the index's pages
    /// finds them stale rather than reading another node's entries (or
    /// reads them as they were, from the pages this process kept), and a
    /// read through the archive looks again from a newer one.
    #[test]
    fn it_finds_a_page_written_over_stale_and_reads_on() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let archive = FileArchive::open(directory.path().join("archive"))?;
        let first = appended(&archive, 0, 1500)?;
        let old = archive.current();
        let later = appended(&archive, 60_000, 1500)?;
        assert!(archive.inner.timings.reused.load(Ordering::Relaxed) > 0);
        // A page this process checked is kept, by the child that names it,
        // and reads as it was for the old snapshot; one it reads from the
        // file afresh is the one written over.
        old.pages.pin().clear();
        let mut stale = 0;
        for block in &first {
            match old.place("index", block.blake3_hash()) {
                Ok(found) => assert!(found.is_some(), "an old snapshot finds what it held"),
                Err(error) if is_stale(&error) => stale += 1,
                Err(error) => return Err(error.into()),
            }
        }
        assert!(stale > 0, "the old snapshot reaches pages written over");
        for block in first.iter().chain(&later) {
            assert_eq!(
                archive.get("index", block.blake3_hash())?.as_deref(),
                Some(block.as_ref())
            );
        }
        Ok(())
    }

    /// Readers on other threads read every block they hold while a writer
    /// writes the index's pages over: each read is the block, whether its
    /// snapshot's pages were still there or it looked again.
    #[test]
    fn it_reads_every_block_while_pages_are_written_over() -> anyhow::Result<()> {
        use std::sync::atomic::AtomicBool;

        let directory = tempfile::tempdir()?;
        let archive = FileArchive::open(directory.path().join("archive"))?;
        let held = Arc::new(appended(&archive, 0, 100)?);
        let done = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let (archive, held, done) = (archive.clone(), held.clone(), done.clone());
                std::thread::spawn(move || -> Result<usize, ArchiveError> {
                    let mut reads = 0;
                    loop {
                        let last = done.load(Ordering::Acquire);
                        for block in held.iter() {
                            let read = archive.get("index", block.blake3_hash())?;
                            assert_eq!(read.as_deref(), Some(block.as_ref()));
                            reads += 1;
                        }
                        if last {
                            return Ok(reads);
                        }
                    }
                })
            })
            .collect();
        appended(&archive, 100_000, 1500)?;
        done.store(true, Ordering::Release);
        assert!(archive.inner.timings.reused.load(Ordering::Relaxed) > 0);
        for reader in readers {
            let reads = reader.join().expect("every read is the block")?;
            assert!(reads >= held.len());
        }
        Ok(())
    }

    /// `sync` makes a sync point: the durable slot is the file as it was,
    /// and every page freed until then is free to write over.
    #[test]
    fn it_makes_a_sync_point() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let archive = FileArchive::open(directory.path().join("archive"))?;
        appended(&archive, 0, 200)?;
        archive.sync()?;
        let current = archive.current();
        let durable = Slot::read_durable(&archive.inner.file)?.expect("a durable slot");
        assert_eq!(durable.sequence + 1, current.slot.sequence);
        assert_eq!(current.slot.durable, durable.sequence);
        assert_eq!(durable.end, current.slot.end);
        assert!(current.slot.pending.is_empty());
        Ok(())
    }

    /// Rewrites `path`'s rolling slots as written in another boot, as a
    /// power loss leaves them.
    fn as_if_rebooted(path: &Path) -> anyhow::Result<()> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        for mut slot in Slot::read(&file)? {
            slot.boot = boot().wrapping_add(1);
            slot.write(&file)?;
        }
        Ok(())
    }

    /// After a power loss that kept a slot but lost a record it names,
    /// opening checks what was written since the durable slot, finds the
    /// record damaged, and goes back to the newest state that is whole: no
    /// block it holds is damaged, and every block synced before is there.
    #[test]
    fn it_goes_back_to_what_is_whole_after_a_power_loss() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let archive = FileArchive::open(&path)?;
        let synced = appended(&archive, 0, 20)?;
        archive.sync()?;
        let durable = Slot::read_durable(&archive.inner.file)?.expect("a durable slot");
        let unsynced: Vec<Buffer> = (0..10).map(|n| block(&format!("unsynced {n}"))).collect();
        for block in &unsynced {
            archive.append("index", std::slice::from_ref(block))?;
        }
        let end = archive.current().slot.end;
        assert!(end > durable.end, "appends came after the sync point");
        drop(archive);

        // The power loss: the last record's bytes never reached the disk.
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        write_at(&file, end - 4, &[0; 4])?;
        drop(file);
        as_if_rebooted(&path)?;

        let reopened = FileArchive::open(&path)?;
        let slot = reopened.current().slot.clone();
        assert!(slot.sequence >= durable.sequence && slot.end < end);
        assert_eq!(
            std::fs::metadata(&path)?.len(),
            slot.end,
            "the rest is dropped"
        );
        for block in &synced {
            assert_eq!(
                reopened.get("index", block.blake3_hash())?.as_deref(),
                Some(block.as_ref())
            );
        }
        for block in &unsynced {
            if let Some(read) = reopened.get("index", block.blake3_hash())? {
                assert_eq!(read, block.as_ref(), "nothing it holds is damaged");
            }
        }
        // And it takes appends again.
        reopened.append("index", &[block("after")])?;
        assert!(
            reopened
                .get("index", block("after").blake3_hash())?
                .is_some()
        );
        Ok(())
    }

    /// An index page written since the durable slot that a power loss
    /// lost (the page there is still the one it wrote over) fails its
    /// check when the file is opened in the next boot, and the file goes
    /// back to the durable slot, whose pages nothing wrote over.
    #[test]
    fn it_goes_back_to_the_durable_slot_when_a_page_did_not_reach_the_disk() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let archive = FileArchive::open(&path)?;
        let synced = appended(&archive, 0, 1500)?;
        archive.sync()?;
        let durable = Slot::read_durable(&archive.inner.file)?.expect("a durable slot");
        appended(&archive, 60_000, 40)?;
        let current = archive.current();
        let mut since = Vec::new();
        current
            .slot
            .root
            .pages_since(&*current, Some(durable.sequence as u32), &mut since)?;
        let lost = *since
            .first()
            .expect("an index page written since the sync point");
        drop(current);
        drop(archive);

        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        write_at(&file, lost.page as u64 * PAGE as u64, &[0; 8])?;
        drop(file);
        as_if_rebooted(&path)?;

        let reopened = FileArchive::open(&path)?;
        assert_eq!(reopened.current().slot.sequence, durable.sequence);
        assert_eq!(reopened.current().slot.end, durable.end);
        for block in &synced {
            assert_eq!(
                reopened.get("index", block.blake3_hash())?.as_deref(),
                Some(block.as_ref())
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
        let archive = FileArchive::open(directory.path().join("archive"))?;
        let manifest = segment_manifest();
        let done = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let (archive, done) = (archive.clone(), done.clone());
                std::thread::spawn(move || -> Result<usize, ArchiveError> {
                    let mut reads = 0;
                    // A pass at least once the writer is done, however late
                    // this thread starts: it reads every block there is.
                    loop {
                        let last = done.load(Ordering::Acquire);
                        for (catalog, hash) in archive.hashes() {
                            let block =
                                archive.get(&catalog, &hash)?.expect("a listed block reads");
                            assert_eq!(&Blake3Hash::hash(&block), &hash);
                            if archive.is_frame(&catalog, &hash) {
                                assert!(archive.served(&catalog, &hash).is_some());
                            }
                            reads += 1;
                        }
                        if last {
                            return Ok(reads);
                        }
                    }
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
            assert!(reads >= 80, "the last pass reads all 80 blocks");
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
mod breakdown {
    use super::*;
    use crate::file::read_record;
    use crate::tokens::read_token_record;

    /// What building the index again from the records costs, on the file
    /// named by `DIALOG_ARCHIVE_INDEX`: a pass over every record through
    /// the mapping, checking each one's checksum, the keys of every block,
    /// frame and token, sorting them, and writing the tree in one batch
    /// (in memory). Run by hand.
    #[test]
    #[ignore = "rebuilds the index of the file named by DIALOG_ARCHIVE_INDEX"]
    fn rebuild() -> anyhow::Result<()> {
        use crate::archive::tree::{Node, tests::Memory};

        let Ok(path) = std::env::var("DIALOG_ARCHIVE_INDEX") else {
            return Ok(());
        };
        let archive = FileArchive::open(&path)?;
        let snapshot = archive.current();
        let bytes = snapshot.bytes();
        let started = Instant::now();
        let (mut entries, mut records, mut read) = (Vec::new(), 0u64, 0u64);
        let mut at = DATA as usize;
        while at < bytes.len() {
            let (kind, body_at, end) =
                record_in(bytes, at).ok_or_else(|| invalid("a record that is not whole"))?;
            let body = &bytes[body_at..end];
            records += 1;
            read += (end - at) as u64;
            match kind {
                Kind::Frame => {
                    let pad = body[0] as usize;
                    let frame = ((body_at + 1 + pad) as u64 / ALIGN) as u32;
                    let header = snapshot.frame(frame)?;
                    entries.push((block_key(header.catalog, &header.hash), frame));
                }
                Kind::Tokens => {
                    for start in read_token_record(body, body_at as u64)? {
                        let id = token_id(start)?;
                        let token = snapshot.get(id).ok_or_else(|| invalid("a token"))?;
                        entries.push((index::token_key(crate::hash::hash(token)), id));
                    }
                }
                Kind::Pages | Kind::Extent | Kind::Cell => {}
            }
            at = end;
        }
        let scanned = started.elapsed();
        let started = Instant::now();
        entries.sort_unstable_by_key(|(key, _)| *key);
        let sorted = started.elapsed();

        let mut memory = Memory::default();
        let started = Instant::now();
        let count = entries.len();
        let root = Node::default().insert(&mut memory, entries)?;
        let built = started.elapsed();
        println!(
            "{records} records, {:.0} MiB, read and checked with their keys in {:.1} s; \
             {count} entries sorted in {:.1} s, the tree written ({:.0} MiB, root {} B) in {:.1} s",
            read as f64 / 1048576.0,
            scanned.as_secs_f64(),
            sorted.as_secs_f64(),
            memory.written as f64 / 1048576.0,
            root.encode().len(),
            built.as_secs_f64(),
        );
        Ok(())
    }

    /// What the tree `DIALOG_ARCHIVE_ROOT` names in the file
    /// `DIALOG_ARCHIVE_INDEX` takes alone, as a file holding no history
    /// would: every frame the root reaches written whole (against nothing)
    /// and every token those frames name. Run by hand.
    #[test]
    #[ignore = "sizes the tree DIALOG_ARCHIVE_ROOT names in DIALOG_ARCHIVE_INDEX"]
    fn live() -> anyhow::Result<()> {
        use crate::rows::{INDEX, NONE, put_rows};

        let (Ok(path), Ok(root)) = (
            std::env::var("DIALOG_ARCHIVE_INDEX"),
            std::env::var("DIALOG_ARCHIVE_ROOT"),
        ) else {
            return Ok(());
        };
        let archive = FileArchive::open(&path)?;
        let snapshot = archive.current();
        let mut start = None;
        for (catalog, hash) in archive.hashes() {
            if hash.to_string() == root
                && let Some(Place::Frame(at)) = snapshot.place(&catalog, &hash)?
            {
                start = Some(at);
            }
        }
        let start = start.ok_or_else(|| invalid("the root is not a frame in the file"))?;
        let (mut frames, mut frame_bytes, mut entries) = (0u64, 0u64, 0u64);
        let mut tokens = HashSet::new();
        let mut seen = HashSet::new();
        // By level from the root: nodes, their children, their entries
        // (a leaf's, or an index node's buffered ops).
        let mut levels: Vec<(u64, u64, u64)> = Vec::new();
        let mut stack = vec![(start, 0usize)];
        let token = |id: u32, tokens: &mut HashSet<u32>| {
            if id != NONE && id != 0 {
                tokens.insert(id);
            }
        };
        while let Some((at, level)) = stack.pop() {
            if !seen.insert(at) {
                continue;
            }
            if levels.len() <= level {
                levels.resize(level + 1, (0, 0, 0));
            }
            levels[level].0 += 1;
            let (rows, manifest) = archive.rows(&snapshot, &HashMap::new(), at)?;
            token(manifest, &mut tokens);
            let mut whole = Vec::new();
            put_rows(&mut whole, &rows, None);
            frames += 1;
            // The header: the hash, kind, depth, catalog, base, manifest.
            frame_bytes += whole.len() as u64 + 32 + 8;
            if rows.kind == INDEX {
                for link in &rows.sections[0] {
                    token(link[0], &mut tokens);
                    token(link[3], &mut tokens);
                    levels[level].1 += 1;
                    if link[1] != NONE {
                        stack.push((link[1], level + 1));
                    }
                }
                for op in &rows.sections[2] {
                    entries += 1;
                    levels[level].2 += 1;
                    for id in &op[1..] {
                        token(*id, &mut tokens);
                    }
                }
            } else {
                for entry in &rows.sections[0] {
                    entries += 1;
                    levels[level].2 += 1;
                    for id in &entry[1..] {
                        token(*id, &mut tokens);
                    }
                }
            }
        }
        let token_bytes: u64 = tokens
            .iter()
            .map(|id| snapshot.get(*id).map_or(0, |token| token.len() as u64))
            .sum();
        let mib = |bytes: u64| bytes as f64 / 1048576.0;
        println!(
            "the tree alone: {frames} nodes, {entries} entries, {:.1} MiB of frames written whole; \
             {} tokens, {:.1} MiB of bytes ({:.1} MiB with lengths and alignment); {:.1} MiB in all",
            mib(frame_bytes),
            tokens.len(),
            mib(token_bytes),
            mib(token_bytes + tokens.len() as u64 * 12),
            mib(frame_bytes + token_bytes + tokens.len() as u64 * 12),
        );
        for (level, (nodes, children, entries)) in levels.iter().enumerate() {
            println!(
                "  level {level}: {nodes} nodes, {:.1} children and {:.1} entries a node",
                *children as f64 / *nodes as f64,
                *entries as f64 / *nodes as f64
            );
        }
        Ok(())
    }

    /// Prints what the file named by `DIALOG_ARCHIVE_INDEX` holds, record
    /// kind by kind, what its tokens' records spend on lengths and
    /// padding, and what the archive keeps in memory to read it. Run by
    /// hand.
    #[test]
    #[ignore = "breaks down the file named by DIALOG_ARCHIVE_INDEX"]
    fn breakdown() -> anyhow::Result<()> {
        let Ok(path) = std::env::var("DIALOG_ARCHIVE_INDEX") else {
            return Ok(());
        };
        let archive = FileArchive::open(&path)?;
        let snapshot = archive.current();
        let file = &archive.inner.file;
        let mib = |bytes: u64| format!("{:.2} MiB", bytes as f64 / 1048576.0);
        let mut totals: std::collections::BTreeMap<String, (u64, u64)> = Default::default();
        let (mut token_bytes, mut tokens) = (0u64, 0u64);
        let mut at = DATA;
        while at < snapshot.slot.end {
            let Some(record) = read_record(file, at, snapshot.slot.end)? else {
                break;
            };
            let total = totals.entry(format!("{:?}", record.kind)).or_default();
            total.0 += 1;
            total.1 += record.end() - record.offset;
            if record.kind == Kind::Tokens {
                for start in read_token_record(&record.body, record.body_offset())? {
                    token_bytes += snapshot
                        .get(token_id(start)?)
                        .map_or(0, |token| token.len()) as u64;
                    tokens += 1;
                }
            }
            at = record.end();
        }
        println!(
            "file {} ({} bytes), slots and magic {}",
            mib(snapshot.slot.end),
            snapshot.slot.end,
            mib(DATA)
        );
        for (kind, (count, bytes)) in &totals {
            println!("  {kind}: {count} records, {}", mib(*bytes));
        }
        let records = totals.get("Tokens").map_or(0, |(_, bytes)| *bytes);
        println!(
            "  of the token records: {} of token bytes ({tokens} tokens), {} of lengths, padding and headers",
            mib(token_bytes),
            mib(records - token_bytes)
        );
        let mut reached = Vec::new();
        snapshot.slot.root.pages(&*snapshot, &mut reached)?;
        let mut spaces = [0usize; 4];
        for (key, _) in snapshot.slot.root.entries(&*snapshot)? {
            spaces[(key >> 30) as usize] += 1;
        }
        println!(
            "  the index: {} pages the root reaches ({}), {} free to write over, {} freed since \
             the last sync point; live entries: {} blocks, {} tokens, {} lineages ({} at 8 B)",
            reached.len(),
            mib((reached.len() * PAGE) as u64),
            snapshot.slot.reusable.len(),
            snapshot.slot.pending.len(),
            spaces[0],
            spaces[1],
            spaces[2],
            mib(8 * spaces.iter().sum::<usize>() as u64)
        );
        let stats = archive.stats();
        println!(
            "in memory to read it: the root {} B, rows kept {}, index memory {}",
            snapshot.slot.root.encode().len(),
            mib(stats.rows_memory as u64),
            mib(stats.index_memory as u64)
        );
        Ok(())
    }
}
