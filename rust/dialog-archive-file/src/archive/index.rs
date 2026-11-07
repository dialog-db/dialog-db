//! The index of the file, read without a lock.
//!
//! Everything the file holds up to its end never changes: an append only
//! adds. The index is built the same way. Each structure only grows, and an
//! append adds to it only once its bytes are in the file, children before
//! the nodes that name them and tokens before the frames that name them. So
//! anything a reader finds stays true for as long as the file is open, and
//! a reader takes no lock: it finds what was published before it looked,
//! and nothing after. One writer at a time adds (the writer's turn), any
//! number of readers read.

use std::fs::File;
use std::hash::BuildHasher as _;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use hashbrown::HashTable;
use parking_lot::RwLock;
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwapOption;
use dialog_common::Blake3Hash;
use hashbrown::DefaultHashBuilder;
use memmap2::Mmap;

use super::{Extent, Lineage, Node, Place};
use crate::tokens::TokenRead;

/// The first bucket's length; each next bucket is twice the one before.
const FIRST: usize = 64;

/// Enough buckets for more than `u32::MAX` slots.
const BUCKETS: usize = 27;

/// A vector that only grows, read without a lock: one writer pushes at a
/// time (callers hold the writer's turn), any number of readers read what
/// was pushed before they looked. Slots never move: each bucket is
/// allocated once and kept.
pub(crate) struct Slots<T> {
    buckets: [OnceLock<Box<[OnceLock<T>]>>; BUCKETS],
    len: AtomicUsize,
}

impl<T> Default for Slots<T> {
    fn default() -> Self {
        Self {
            buckets: [const { OnceLock::new() }; BUCKETS],
            len: AtomicUsize::new(0),
        }
    }
}

/// The bucket slot `at` is in, and where in it.
fn locate(at: usize) -> (usize, usize) {
    let shifted = at + FIRST;
    let bit = (usize::BITS - 1 - shifted.leading_zeros()) as usize;
    (bit - FIRST.trailing_zeros() as usize, shifted - (1 << bit))
}

impl<T> Slots<T> {
    /// How many slots are published.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    /// The slot `at`, if it is published.
    pub fn get(&self, at: usize) -> Option<&T> {
        if at >= self.len() {
            return None;
        }
        let (bucket, offset) = locate(at);
        self.buckets[bucket].get()?.get(offset)?.get()
    }

    /// Publishes `value` as the next slot, and returns where it is. Only the
    /// holder of the writer's turn pushes.
    pub fn push(&self, value: T) -> usize {
        let at = self.len.load(Ordering::Relaxed);
        let (bucket, offset) = locate(at);
        let slots = self.buckets[bucket]
            .get_or_init(|| (0..FIRST << bucket).map(|_| OnceLock::new()).collect());
        let set = slots[offset].set(value);
        debug_assert!(set.is_ok(), "one writer pushes at a time");
        self.len.store(at + 1, Ordering::Release);
        at
    }

    /// Every published slot, in order.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        (0..self.len()).filter_map(|at| self.get(at))
    }

    /// The memory the allocated buckets take.
    pub fn footprint(&self) -> usize {
        self.buckets
            .iter()
            .enumerate()
            .filter(|(_, bucket)| bucket.get().is_some())
            .map(|(at, _)| (FIRST << at) * size_of::<OnceLock<T>>())
            .sum()
    }
}

/// A map read without a lock, hashed as `hashbrown` hashes: the keys are
/// hashes, ids and short names, which need no defence against crafted
/// collisions, and are looked up on every token a writer interns.
pub(crate) type Map<K, V> = papaya::HashMap<K, V, DefaultHashBuilder>;

/// The id table, held for reading: a writer holds it while it drafts an
/// append, and lets it go before its turn.
pub(crate) type Ids<'a> = parking_lot::RwLockReadGuard<'a, IdTable>;

/// Each published token's id, found by its bytes' hash and compared
/// against the token, so the table holds ids, not bytes, up to `indexed`
/// tokens. A reader never needs it, and it is not read without a lock: a
/// writer looks up every token of every node it splits, and a table
/// probed in place is what keeps that cheap.
#[derive(Default)]
pub(crate) struct IdTable {
    table: HashTable<u32>,
    indexed: usize,
}

/// Where a token is in the file, and whether it was checked as an archived
/// datum: a datum is checked once, the first time a node that holds it is
/// read.
pub(crate) struct Token {
    pub offset: u64,
    pub len: u32,
    checked: AtomicBool,
}

/// Everything the file holds, as where it is.
#[derive(Default)]
pub(crate) struct Index {
    catalogs: Slots<String>,
    catalog_ids: Map<String, u16>,
    /// Every block, by catalog and hash.
    blocks: Map<(u16, Blake3Hash), Place>,
    /// Every frame, by node id.
    nodes: Slots<Node>,
    /// The latest node of each hash, for naming children.
    node_ids: Map<Blake3Hash, u32>,
    /// The latest node of each lineage, for the next to be written against.
    lineages: Map<Lineage, u32>,
    tokens: Slots<Token>,
    /// A token's id by its bytes, for a writer to find a token it already
    /// wrote. Built on a writer's first use; a reader never needs it.
    ids: RwLock<IdTable>,
    hasher: DefaultHashBuilder,
    /// The file, mapped at least as far as every published token.
    map: ArcSwapOption<Mmap>,
    /// Bytes of whole blocks and frames, for the archive's stats.
    block_bytes: AtomicU64,
    frame_bytes: AtomicU64,
}

impl Index {
    /// The catalog `name`'s id, if the file holds anything in it.
    pub fn catalog_id(&self, name: &str) -> Option<u16> {
        self.catalog_ids.pin().get(name).copied()
    }

    /// The catalog `id` names.
    pub fn catalog(&self, id: u16) -> Option<&str> {
        self.catalogs.get(id as usize).map(String::as_str)
    }

    pub fn catalogs(&self) -> impl Iterator<Item = &str> {
        self.catalogs.iter().map(String::as_str)
    }

    /// How the file holds the block `hash` names in `catalog`.
    pub fn place(&self, catalog: &str, hash: &Blake3Hash) -> Option<Place> {
        let catalog = self.catalog_id(catalog)?;
        self.blocks.pin().get(&(catalog, hash.clone())).copied()
    }

    /// Whether the file holds the block `hash` names in `catalog`.
    pub fn held(&self, catalog: &str, hash: &Blake3Hash) -> bool {
        self.place(catalog, hash).is_some()
    }

    /// Every block, by catalog and hash, and how it is held.
    pub fn blocks(&self) -> Vec<(u16, Blake3Hash, Place)> {
        self.blocks
            .pin()
            .iter()
            .map(|((catalog, hash), place)| (*catalog, hash.clone(), *place))
            .collect()
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn node(&self, id: u32) -> Option<&Node> {
        self.nodes.get(id as usize)
    }

    pub fn nodes(&self) -> &Slots<Node> {
        &self.nodes
    }

    /// The latest node of `hash`.
    pub fn node_id(&self, hash: &Blake3Hash) -> Option<u32> {
        self.node_ids.pin().get(hash).copied()
    }

    /// The latest node of `lineage`.
    pub fn lineage(&self, lineage: &Lineage) -> Option<u32> {
        self.lineages.pin().get(lineage).copied()
    }

    pub fn tokens(&self) -> &Slots<Token> {
        &self.tokens
    }

    /// Where token `id` is.
    pub fn extent(&self, id: u32) -> Option<(u64, u32)> {
        self.tokens
            .get(id as usize)
            .map(|token| (token.offset, token.len))
    }

    /// Whether token `id` passes `check`, checking it only the first time.
    pub fn checked(&self, id: u32, bytes: &[u8], check: impl Fn(&[u8]) -> bool) -> bool {
        let Some(token) = self.tokens.get(id as usize) else {
            return false;
        };
        if token.checked.load(Ordering::Acquire) {
            return true;
        }
        if !check(bytes) {
            return false;
        }
        token.checked.store(true, Ordering::Release);
        true
    }

    /// The tokens as they are now, read through the file's mapping. Take
    /// it after finding the node or block that names them: everything a
    /// published node names was published, and mapped, before it.
    pub fn view(&self) -> View<'_> {
        View {
            index: self,
            map: self.map.load_full(),
        }
    }

    pub fn hash_of(&self, token: &[u8]) -> u64 {
        self.hasher.hash_one(token)
    }

    /// The id table, held for reading.
    pub fn ids(&self) -> Ids<'_> {
        self.ids.read()
    }

    /// The id of a published token equal to `token`, whose bytes hash to
    /// `hash`, if the id table knows it.
    pub fn find(ids: &IdTable, view: &View<'_>, hash: u64, token: &[u8]) -> Option<u32> {
        ids.table
            .find(hash, |id| view.get(*id) == Some(token))
            .copied()
    }

    /// Brings the id table up to every published token. A writer calls it
    /// before it looks tokens up, so the first append of a process reads
    /// every token once; later ones read only what other processes added.
    /// A token written twice keeps its first id.
    pub fn build_ids(&self) {
        let to = self.tokens.len();
        if self.ids.read().indexed >= to {
            return;
        }
        let view = self.view();
        let mut ids = self.ids.write();
        for id in ids.indexed..to {
            let Some(token) = view.get(id as u32) else {
                continue;
            };
            let hash = self.hash_of(token);
            if Self::find(&ids, &view, hash, token).is_none() {
                ids.table.insert_unique(hash, id as u32, |id| {
                    self.hash_of(view.get(*id).expect("an indexed token"))
                });
            }
        }
        ids.indexed = ids.indexed.max(to);
    }

    pub fn id_table_len(&self) -> usize {
        self.ids.read().table.len()
    }

    pub fn lineage_count(&self) -> usize {
        self.lineages.len()
    }

    pub fn block_bytes(&self) -> u64 {
        self.block_bytes.load(Ordering::Relaxed)
    }

    pub fn frame_bytes(&self) -> u64 {
        self.frame_bytes.load(Ordering::Relaxed)
    }

    /// Maps `file` again if the mapping does not reach `needed`. Called by
    /// the writer before it publishes tokens past the mapping.
    pub fn map(&self, file: &File, needed: u64) -> io::Result<()> {
        let mapped = self.map.load().as_ref().map_or(0, |map| map.len() as u64);
        if needed > mapped {
            // SAFETY: the file is append-only. A process changes bytes only
            // past the last footer, by an append under the file's lock, or by
            // dropping an append that did not finish; nothing any process
            // has indexed lies there. Everything read through this mapping
            // was indexed from a record before a footer, or written by this
            // process. Like any mapped file, changing it from outside dialog
            // is not supported.
            self.map.store(Some(Arc::new(unsafe { Mmap::map(file)? })));
        }
        Ok(())
    }

    /// The id of catalog `name`, adding it when it is new. Writer only.
    pub fn add_catalog(&self, name: &str) -> u16 {
        if let Some(id) = self.catalog_id(name) {
            return id;
        }
        let id = self.catalogs.push(name.to_string()) as u16;
        self.catalog_ids.pin().insert(name.to_string(), id);
        id
    }

    /// Publishes a token at `offset`, `len` bytes long, whose bytes hash to
    /// `hash` when the writer knows it. Writer only, once the mapping
    /// reaches it.
    pub fn add_token(&self, offset: u64, len: u32, hash: Option<u64>) -> u32 {
        let id = self.tokens.push(Token {
            offset,
            len,
            checked: AtomicBool::new(false),
        }) as u32;
        // A token the writer found new joins the id table at once, when the
        // table is up to it.
        if let Some(hash) = hash {
            let mut ids = self.ids.write();
            if ids.indexed == id as usize {
                let view = self.view();
                ids.table.insert_unique(hash, id, |id| {
                    self.hash_of(view.get(*id).expect("an indexed token"))
                });
                ids.indexed += 1;
            }
        }
        id
    }

    /// Publishes a whole block. Writer only.
    pub fn add_block(&self, catalog: u16, hash: Blake3Hash, extent: Extent) {
        self.block_bytes
            .fetch_add(extent.len as u64, Ordering::Relaxed);
        self.blocks
            .pin()
            .insert((catalog, hash), Place::Block(extent));
    }

    /// Publishes a frame, after the tokens and children it names. Writer
    /// only.
    pub fn add_node(&self, node: Node) -> u32 {
        let (hash, catalog, kind, lineage, len) = (
            node.hash.clone(),
            node.catalog,
            node.kind,
            node.lineage,
            node.extent.len,
        );
        let id = self.nodes.push(node) as u32;
        self.frame_bytes.fetch_add(len as u64, Ordering::Relaxed);
        if let Some(first) = lineage {
            self.lineages.pin().insert((catalog, kind, first), id);
        }
        self.node_ids.pin().insert(hash.clone(), id);
        self.blocks.pin().insert((catalog, hash), Place::Node(id));
        id
    }
}

/// The published tokens, read through one mapping of the file.
pub(crate) struct View<'a> {
    index: &'a Index,
    map: Option<Arc<Mmap>>,
}

impl View<'_> {
    pub fn mapping(&self) -> Option<Arc<Mmap>> {
        self.map.clone()
    }

    /// How many tokens are published.
    pub fn len(&self) -> u32 {
        self.index.tokens.len() as u32
    }
}

impl TokenRead for View<'_> {
    fn get(&self, id: u32) -> Option<&[u8]> {
        let (offset, len) = self.index.extent(id)?;
        let start = usize::try_from(offset).ok()?;
        self.map.as_deref()?.get(start..start + len as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_reads_every_slot_it_pushed_across_buckets() {
        let slots = Slots::default();
        for at in 0..10_000usize {
            assert_eq!(slots.push(at), at);
        }
        assert_eq!(slots.len(), 10_000);
        for at in 0..10_000usize {
            assert_eq!(slots.get(at), Some(&at));
        }
        assert_eq!(slots.get(10_000), None);
    }

    #[test]
    fn it_locates_slots_bucket_by_bucket() {
        assert_eq!(locate(0), (0, 0));
        assert_eq!(locate(FIRST - 1), (0, FIRST - 1));
        assert_eq!(locate(FIRST), (1, 0));
        assert_eq!(locate(3 * FIRST - 1), (1, 2 * FIRST - 1));
        assert_eq!(locate(3 * FIRST), (2, 0));
        assert!(locate(u32::MAX as usize).0 < BUCKETS);
    }

    /// Readers on other threads see every slot pushed before they looked,
    /// whole, while the writer keeps pushing.
    #[test]
    fn it_reads_while_one_writer_pushes() {
        let slots = Arc::new(Slots::default());
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let slots = slots.clone();
                std::thread::spawn(move || {
                    let mut seen = 0;
                    while seen < 50_000 {
                        let len = slots.len();
                        for at in seen..len {
                            assert_eq!(slots.get(at), Some(&(at as u64 * 3)));
                        }
                        seen = len;
                    }
                })
            })
            .collect();
        for at in 0..50_000u64 {
            slots.push(at * 3);
        }
        for reader in readers {
            reader.join().expect("a reader saw every slot whole");
        }
    }
}
