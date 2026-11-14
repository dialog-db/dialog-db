//! A model of the file's index, kept in memory, for choosing its layout.
//!
//! Each layout keeps its nodes in pages of a fixed size, as the file would,
//! and gives a page back once no root can reach it. The newest slot's root
//! never reaches a page an append freed, and the older slot's stops with
//! the next append, so a page freed by one append is written over from
//! `grace` appends later (one at least). A page born and freed within one
//! append was never reached by any root and is reused at once.
//!
//! Over appends shaped as a file's, it counts what the index keeps in the
//! file, what an append writes, and what a lookup reads: index pages, and
//! records read to compare a key the index does not hold (a key named by
//! its record's position). Run by hand: `index_layouts`.

use std::collections::{BTreeMap, VecDeque};

use crate::hash::hash;

/// A key (a hash) and the position of the record it names.
type Entry = (u64, u32);

/// An empty slot of a hashed bucket.
const NONE: u32 = u32::MAX;

/// A page's header: the sequence that wrote it, its checksum, its kind and
/// count.
const HEADER: usize = 16;

/// Pages of one layout, reused once no root reaches them.
struct Pages<N> {
    nodes: Vec<Option<N>>,
    born: Vec<u64>,
    /// Freed pages, oldest first, with the append that freed them.
    free: VecDeque<(u64, u32)>,
    grace: u64,
    now: u64,
    written: u64,
}

impl<N> Pages<N> {
    fn new(grace: u64) -> Self {
        Self {
            nodes: Vec::new(),
            born: Vec::new(),
            free: VecDeque::new(),
            grace,
            now: 1,
            written: 0,
        }
    }

    fn alloc(&mut self, node: N) -> u32 {
        self.written += 1;
        let page = match self.free.front() {
            Some(&(freed, page)) if freed + self.grace <= self.now => {
                self.free.pop_front();
                page
            }
            _ => {
                self.nodes.push(None);
                self.born.push(0);
                (self.nodes.len() - 1) as u32
            }
        };
        self.nodes[page as usize] = Some(node);
        self.born[page as usize] = self.now;
        page
    }

    fn release(&mut self, page: u32) -> N {
        let node = self.nodes[page as usize].take().expect("a live page");
        if self.born[page as usize] == self.now {
            // Never reached by a root: free at once.
            self.free.push_front((0, page));
        } else {
            self.free.push_back((self.now, page));
        }
        node
    }

    fn get(&self, page: u32) -> &N {
        self.nodes[page as usize].as_ref().expect("a live page")
    }

    fn live(&self) -> usize {
        self.nodes.iter().filter(|node| node.is_some()).count()
    }
}

/// What a lookup read.
#[derive(Default, Clone, Copy)]
struct Reads {
    /// Index pages, the root (in the slot) not counted.
    pages: u64,
    /// Records read to compare their key.
    derefs: u64,
    /// The same, when each entry keeps one byte of its key's hash.
    tagged: u64,
}

fn log2_ceil(n: usize) -> u64 {
    (usize::BITS - n.saturating_sub(1).leading_zeros()) as u64
}

/// `a` with `b` merged in, both sorted by key; `b` wins a tie.
fn merge(a: Vec<Entry>, b: &[Entry]) -> Vec<Entry> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].0.cmp(&b[j].0) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(b[j]);
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

/// What every layout answers.
trait Layout {
    fn insert(&mut self, entries: Vec<Entry>, keys: &[u64]);
    fn lookup(&self, key: u64, keys: &[u64]) -> (bool, Reads);
    fn advance(&mut self);
    /// Pages in the file, pages a root reaches, pages written, a page's
    /// bytes, records the writer read.
    fn counts(&self) -> (usize, usize, u64, usize, u64);
}

enum HNode {
    Leaf(Vec<Entry>),
    Internal {
        pivots: Vec<u64>,
        children: Vec<u32>,
        buffer: Vec<Entry>,
    },
}

impl Default for HNode {
    fn default() -> Self {
        HNode::Leaf(Vec::new())
    }
}

/// A hitchhiker tree in pages: the layout as built (keys held, 16 bytes an
/// entry) or with keys named by position (4 bytes an entry, pivots too).
struct Hitchhiker {
    pages: Pages<HNode>,
    root: HNode,
    page: usize,
    leaf: usize,
    fanout: usize,
    buffer: usize,
    root_buffer: usize,
    /// Keys named by position: comparing one reads its record.
    pointer: bool,
    writer_derefs: u64,
}

impl Hitchhiker {
    fn new(
        page: usize,
        entry: usize,
        pivot: usize,
        fanout: usize,
        pointer: bool,
        grace: u64,
    ) -> Self {
        let body = page - HEADER;
        Self {
            pages: Pages::new(grace),
            root: HNode::default(),
            page,
            leaf: body / entry,
            fanout,
            buffer: (body - fanout * (pivot + 4)) / entry,
            // The root lives in the slot: 32 KiB of it.
            root_buffer: 32 * 1024 / entry,
            pointer,
            writer_derefs: 0,
        }
    }

    fn write_leaves(&mut self, entries: Vec<Entry>) -> Vec<(u64, u32)> {
        let parts = entries.len().div_ceil(self.leaf).max(1);
        let size = entries.len().div_ceil(parts);
        entries
            .chunks(size)
            .map(|chunk| (chunk[0].0, self.pages.alloc(HNode::Leaf(chunk.to_vec()))))
            .collect()
    }

    fn split(&mut self, node: HNode) -> Vec<(u64, u32)> {
        let HNode::Internal {
            pivots,
            children,
            buffer,
        } = node
        else {
            unreachable!("only internal nodes split");
        };
        let parts = children.len().div_ceil(self.fanout).max(2);
        let size = children.len().div_ceil(parts);
        let mut out = Vec::new();
        let mut start = 0;
        while start < children.len() {
            let end = (start + size).min(children.len());
            let low = if start == 0 { 0 } else { pivots[start - 1] };
            let high = if end == children.len() {
                None
            } else {
                Some(pivots[end - 1])
            };
            let part = HNode::Internal {
                pivots: pivots[start..end - 1].to_vec(),
                children: children[start..end].to_vec(),
                buffer: buffer
                    .iter()
                    .copied()
                    .filter(|(key, _)| (start == 0 || *key >= low) && high.is_none_or(|h| *key < h))
                    .collect(),
            };
            out.push((low, self.pages.alloc(part)));
            start = end;
        }
        out
    }

    /// Sends the entries bound for `node`'s busiest child down into it.
    fn flush(&mut self, node: &mut HNode) {
        let HNode::Internal {
            pivots,
            children,
            buffer,
        } = node
        else {
            return;
        };
        let child_of = |key: u64| pivots.partition_point(|pivot| *pivot <= key);
        let mut counts = vec![0usize; children.len()];
        for (key, _) in buffer.iter() {
            counts[child_of(*key)] += 1;
        }
        let busiest = (0..counts.len())
            .max_by_key(|at| counts[*at])
            .expect("children");
        let (batch, kept): (Vec<Entry>, Vec<Entry>) = buffer
            .drain(..)
            .partition(|(key, _)| child_of(*key) == busiest);
        *buffer = kept;
        if self.pointer {
            // Routing compares the pivots' keys.
            self.writer_derefs += pivots.len() as u64;
        }
        let replacement = match self.pages.release(children[busiest]) {
            HNode::Leaf(leaf) => {
                if self.pointer {
                    self.writer_derefs += leaf.len() as u64;
                }
                let merged = merge(leaf, &batch);
                if merged.len() <= self.leaf {
                    let first = merged.first().map_or(0, |(key, _)| *key);
                    vec![(first, self.pages.alloc(HNode::Leaf(merged)))]
                } else {
                    self.write_leaves(merged)
                }
            }
            HNode::Internal {
                pivots: child_pivots,
                children: child_children,
                buffer: child_buffer,
            } => {
                if self.pointer {
                    self.writer_derefs += child_buffer.len() as u64;
                }
                let mut child = HNode::Internal {
                    pivots: child_pivots,
                    children: child_children,
                    buffer: merge(child_buffer, &batch),
                };
                if buffered(&child) > self.buffer {
                    while buffered(&child) > self.buffer / 2 {
                        self.flush(&mut child);
                    }
                }
                if fanout(&child) > self.fanout {
                    self.split(child)
                } else {
                    vec![(0, self.pages.alloc(child))]
                }
            }
        };
        children.splice(busiest..=busiest, replacement.iter().map(|(_, page)| *page));
        pivots.splice(
            busiest..busiest,
            replacement.iter().skip(1).map(|(first, _)| *first),
        );
    }
}

fn buffered(node: &HNode) -> usize {
    match node {
        HNode::Leaf(_) => 0,
        HNode::Internal { buffer, .. } => buffer.len(),
    }
}

fn fanout(node: &HNode) -> usize {
    match node {
        HNode::Leaf(_) => 0,
        HNode::Internal { children, .. } => children.len(),
    }
}

impl Layout for Hitchhiker {
    fn insert(&mut self, mut entries: Vec<Entry>, _: &[u64]) {
        entries.sort_unstable_by_key(|(key, _)| *key);
        entries.dedup_by(|later, earlier| {
            if later.0 == earlier.0 {
                earlier.1 = later.1;
                true
            } else {
                false
            }
        });
        let mut root = match std::mem::take(&mut self.root) {
            HNode::Leaf(leaf) => {
                let merged = merge(leaf, &entries);
                if merged.len() <= self.root_buffer {
                    HNode::Leaf(merged)
                } else {
                    let parts = self.write_leaves(merged);
                    HNode::Internal {
                        pivots: parts.iter().skip(1).map(|(first, _)| *first).collect(),
                        children: parts.into_iter().map(|(_, page)| page).collect(),
                        buffer: Vec::new(),
                    }
                }
            }
            HNode::Internal {
                pivots,
                children,
                buffer,
            } => HNode::Internal {
                pivots,
                children,
                buffer: merge(buffer, &entries),
            },
        };
        if buffered(&root) > self.root_buffer {
            while buffered(&root) > self.root_buffer / 2 {
                self.flush(&mut root);
            }
        }
        while fanout(&root) > self.fanout {
            let parts = self.split(root);
            root = HNode::Internal {
                pivots: parts.iter().skip(1).map(|(first, _)| *first).collect(),
                children: parts.into_iter().map(|(_, page)| page).collect(),
                buffer: Vec::new(),
            };
        }
        self.root = root;
    }

    fn lookup(&self, key: u64, _: &[u64]) -> (bool, Reads) {
        let mut reads = Reads::default();
        let mut node = &self.root;
        let compare = |reads: &mut Reads, n: usize| {
            if self.pointer {
                reads.derefs += log2_ceil(n + 1);
                reads.tagged += log2_ceil(n + 1);
            }
        };
        loop {
            match node {
                HNode::Leaf(leaf) => {
                    compare(&mut reads, leaf.len());
                    return (leaf.binary_search_by_key(&key, |(k, _)| *k).is_ok(), reads);
                }
                HNode::Internal {
                    pivots,
                    children,
                    buffer,
                } => {
                    compare(&mut reads, buffer.len());
                    if buffer.binary_search_by_key(&key, |(k, _)| *k).is_ok() {
                        return (true, reads);
                    }
                    compare(&mut reads, pivots.len());
                    let child = children[pivots.partition_point(|pivot| *pivot <= key)];
                    reads.pages += 1;
                    node = self.pages.get(child);
                }
            }
        }
    }

    fn advance(&mut self) {
        self.pages.now += 1;
    }

    fn counts(&self) -> (usize, usize, u64, usize, u64) {
        (
            self.pages.nodes.len(),
            self.pages.live(),
            self.pages.written,
            self.page,
            self.writer_derefs,
        )
    }
}

enum XNode {
    /// Positions, placed by their key's high bits and probed in order.
    Bucket {
        depth: u32,
        slots: Vec<u32>,
        count: usize,
    },
    Dir(Dir),
}

/// A directory: a child for each value of the next `bits` of a key's hash.
struct Dir {
    bits: u32,
    children: Vec<u32>,
}

/// Extendible hashing in pages, holding positions only. The root
/// directory lives in the slot; a directory page takes the next bits of
/// the hash once one above it is full.
struct Extendible {
    pages: Pages<XNode>,
    root: Dir,
    root_bits: u32,
    dir_bits: u32,
    page: usize,
    capacity: usize,
    load: usize,
    writer_derefs: u64,
}

fn index(key: u64, consumed: u32, bits: u32) -> usize {
    if bits == 0 {
        0
    } else {
        ((key >> consumed) & ((1u64 << bits) - 1)) as usize
    }
}

fn start(key: u64, capacity: usize) -> usize {
    ((key >> 40) as usize) % capacity
}

fn tag(key: u64) -> u8 {
    (key >> 56) as u8
}

impl Extendible {
    /// `slot` bytes a position (4, or 5 with a byte of its hash).
    fn new(page: usize, slot: usize, grace: u64) -> Self {
        let capacity = (page - HEADER) / slot;
        let dir_bits = ((page - HEADER) / 4).ilog2();
        let mut pages = Pages::new(grace);
        let first = pages.alloc(XNode::Bucket {
            depth: 0,
            slots: vec![NONE; capacity],
            count: 0,
        });
        Self {
            pages,
            root: Dir {
                bits: 0,
                children: vec![first],
            },
            // 32 KiB of the slot.
            root_bits: 13,
            dir_bits,
            page,
            capacity,
            load: capacity * 3 / 4,
            writer_derefs: 0,
        }
    }

    fn bucket(&mut self, depth: u32, entries: &[Entry]) -> u32 {
        let mut slots = vec![NONE; self.capacity];
        for (key, position) in entries {
            let mut at = start(*key, self.capacity);
            while slots[at] != NONE {
                at = (at + 1) % self.capacity;
            }
            slots[at] = *position;
        }
        self.pages.alloc(XNode::Bucket {
            depth,
            slots,
            count: entries.len(),
        })
    }

    /// Places `entries` (a bucket's, all sharing the low `depth` bits
    /// `pattern`) in buckets of `dir`, splitting, doubling the directory or
    /// moving down a level as they need.
    #[allow(clippy::too_many_arguments)]
    fn place(
        &mut self,
        dir: &mut Dir,
        max: u32,
        consumed: u32,
        depth: u32,
        pattern: usize,
        entries: Vec<Entry>,
        keys: &[u64],
    ) {
        if entries.len() <= self.load {
            let page = self.bucket(depth, &entries);
            point(dir, depth, pattern, page);
            return;
        }
        if depth == max {
            let mut sub = Dir {
                bits: 0,
                children: Vec::new(),
            };
            let empty = self.bucket(0, &[]);
            sub.children.push(empty);
            self.insert_dir(&mut sub, self.dir_bits, consumed + max, entries, keys);
            let page = self.pages.alloc(XNode::Dir(sub));
            point(dir, depth, pattern, page);
            return;
        }
        if dir.bits == depth {
            let copy = dir.children.clone();
            dir.children.extend(copy);
            dir.bits += 1;
        }
        let (low, high): (Vec<Entry>, Vec<Entry>) = entries
            .into_iter()
            .partition(|(key, _)| (key >> (consumed + depth)) & 1 == 0);
        self.place(dir, max, consumed, depth + 1, pattern, low, keys);
        self.place(
            dir,
            max,
            consumed,
            depth + 1,
            pattern | (1 << depth),
            high,
            keys,
        );
    }

    fn insert_dir(
        &mut self,
        dir: &mut Dir,
        max: u32,
        consumed: u32,
        entries: Vec<Entry>,
        keys: &[u64],
    ) {
        let mut groups: BTreeMap<u32, (usize, Vec<Entry>)> = BTreeMap::new();
        for entry in entries {
            let at = index(entry.0, consumed, dir.bits);
            groups
                .entry(dir.children[at])
                .or_insert_with(|| (at, Vec::new()))
                .1
                .push(entry);
        }
        for (page, (at, group)) in groups {
            match self.pages.release(page) {
                XNode::Dir(mut sub) => {
                    self.insert_dir(&mut sub, self.dir_bits, consumed + max, group, keys);
                    let new = self.pages.alloc(XNode::Dir(sub));
                    for child in dir.children.iter_mut().filter(|child| **child == page) {
                        *child = new;
                    }
                }
                XNode::Bucket {
                    depth,
                    mut slots,
                    count,
                } => {
                    let pattern = at & ((1usize << depth) - 1);
                    if count + group.len() <= self.load {
                        for (key, position) in &group {
                            let mut at = start(*key, self.capacity);
                            while slots[at] != NONE {
                                at = (at + 1) % self.capacity;
                            }
                            slots[at] = *position;
                        }
                        let new = self.pages.alloc(XNode::Bucket {
                            depth,
                            slots,
                            count: count + group.len(),
                        });
                        point(dir, depth, pattern, new);
                    } else {
                        // Splitting reads every held record for its key.
                        self.writer_derefs += count as u64;
                        let mut all: Vec<Entry> = slots
                            .iter()
                            .filter(|slot| **slot != NONE)
                            .map(|position| (keys[*position as usize], *position))
                            .collect();
                        all.extend(group);
                        self.place(dir, max, consumed, depth, pattern, all, keys);
                    }
                }
            }
        }
    }
}

/// Points every child of `dir` whose low `depth` bits are `pattern` at
/// `page`.
fn point(dir: &mut Dir, depth: u32, pattern: usize, page: u32) {
    let mask = (1usize << depth) - 1;
    for (at, child) in dir.children.iter_mut().enumerate() {
        if at & mask == pattern {
            *child = page;
        }
    }
}

impl Layout for Extendible {
    fn insert(&mut self, entries: Vec<Entry>, keys: &[u64]) {
        let mut root = std::mem::replace(
            &mut self.root,
            Dir {
                bits: 0,
                children: Vec::new(),
            },
        );
        let max = self.root_bits;
        self.insert_dir(&mut root, max, 0, entries, keys);
        self.root = root;
    }

    fn lookup(&self, key: u64, keys: &[u64]) -> (bool, Reads) {
        let mut reads = Reads::default();
        let (mut dir, mut max, mut consumed) = (&self.root, self.root_bits, 0);
        loop {
            let page = dir.children[index(key, consumed, dir.bits)];
            reads.pages += 1;
            match self.pages.get(page) {
                XNode::Dir(sub) => {
                    consumed += max;
                    dir = sub;
                    max = self.dir_bits;
                }
                XNode::Bucket { slots, .. } => {
                    let mut at = start(key, self.capacity);
                    loop {
                        let position = slots[at];
                        if position == NONE {
                            return (false, reads);
                        }
                        reads.derefs += 1;
                        let held = keys[position as usize];
                        if tag(held) == tag(key) {
                            reads.tagged += 1;
                        }
                        if held == key {
                            return (true, reads);
                        }
                        at = (at + 1) % self.capacity;
                    }
                }
            }
        }
    }

    fn advance(&mut self) {
        self.pages.now += 1;
    }

    fn counts(&self) -> (usize, usize, u64, usize, u64) {
        (
            self.pages.nodes.len(),
            self.pages.live(),
            self.pages.written,
            self.page,
            self.writer_derefs,
        )
    }
}

/// A file's appends: each adds `frames` frames and `tokens` tokens, a new
/// key each.
struct Shape {
    name: &'static str,
    appends: u64,
    frames: u64,
    tokens: u64,
    /// The frames and tokens the index points at, in MiB.
    data: f64,
}

fn run(shape: &Shape, name: &str, layout: &mut dyn Layout) {
    let started = std::time::Instant::now();
    let mut keys: Vec<u64> = Vec::new();
    for _ in 0..shape.appends {
        let mut entries = Vec::new();
        for _ in 0..shape.frames + shape.tokens {
            let position = keys.len() as u32;
            let key = hash(&(position as u64).to_le_bytes());
            keys.push(key);
            entries.push((key, position));
        }
        layout.insert(entries, &keys);
        layout.advance();
    }
    let (file, live, written, page, writer) = layout.counts();
    let samples = 20_000u64;
    let (mut hits, mut misses) = (Reads::default(), Reads::default());
    for n in 0..samples {
        let key = keys[(hash(&n.to_le_bytes()) % keys.len() as u64) as usize];
        let (found, reads) = layout.lookup(key, &keys);
        assert!(found, "{name} finds what it holds");
        hits.pages += reads.pages;
        hits.derefs += reads.derefs;
        hits.tagged += reads.tagged;
        let (found, reads) = layout.lookup(hash(&(u64::MAX - n).to_le_bytes()), &keys);
        assert!(!found, "{name} finds nothing it does not hold");
        misses.pages += reads.pages;
        misses.derefs += reads.derefs;
        misses.tagged += reads.tagged;
    }
    let mib = |pages: usize| (pages * page) as f64 / 1048576.0;
    let per = |n: u64| n as f64 / samples as f64;
    println!(
        "{:<12} {:<44} file {:>7.1} MiB ({:>5.1}% of data), live {:>6.1}, free {:>6.1}; \
         an append writes {:>6.0} KiB, reads {:>5.0} records; \
         a hit reads {:.1} pages + {:.1} records ({:.1} tagged), a miss {:.1} + {:.1} ({:.1}); {:.0} s",
        shape.name,
        name,
        mib(file),
        100.0 * mib(file) / shape.data,
        mib(live),
        mib(file - live),
        (written as usize * page) as f64 / 1024.0 / shape.appends as f64,
        writer as f64 / shape.appends as f64,
        per(hits.pages),
        per(hits.derefs),
        per(hits.tagged),
        per(misses.pages),
        per(misses.derefs),
        per(misses.tagged),
        started.elapsed().as_secs_f64(),
    );
}

/// Every layout over MusicBrainz's and tonk-shaped 100×'s appends. Set
/// `DIALOG_INDEX_SHAPES=100x` (or `musicbrainz`) for one; lineage entries
/// are left out (the layouts that name keys by position would find a
/// node's predecessor another way).
#[dialog_common::test]
#[ignore = "models the index's layouts; run by hand"]
fn index_layouts() {
    let shapes = [
        Shape {
            name: "100x",
            appends: 3569,
            frames: 292,
            tokens: 118,
            data: 281.3,
        },
        Shape {
            name: "musicbrainz",
            appends: 2000,
            frames: 1442,
            tokens: 1513,
            data: 944.8,
        },
    ];
    let only = std::env::var("DIALOG_INDEX_SHAPES").ok();
    let grace = std::env::var("DIALOG_INDEX_GRACE")
        .ok()
        .and_then(|grace| grace.parse().ok())
        .unwrap_or(1);
    for shape in shapes
        .iter()
        .filter(|shape| only.as_deref().is_none_or(|only| only.contains(shape.name)))
    {
        run(
            shape,
            "hitchhiker, keys held, 8 children, 4 KiB",
            &mut Hitchhiker::new(4096, 16, 8, 8, false, grace),
        );
        // Four bytes of the key's hash: a comparison reads a record only
        // when two fragments tie, about one in four billion.
        run(
            shape,
            "hitchhiker, 4-byte key fragments, 8 children, 4 KiB",
            &mut Hitchhiker::new(4096, 8, 4, 8, false, grace),
        );
        run(
            shape,
            "hitchhiker, 4-byte key fragments, 16 children, 4 KiB",
            &mut Hitchhiker::new(4096, 8, 4, 16, false, grace),
        );
        run(
            shape,
            "hitchhiker, keys by position, 8 children, 4 KiB",
            &mut Hitchhiker::new(4096, 4, 4, 8, true, grace),
        );
        for (page, slot, name) in [
            (4096, 4, "hashed, positions only, 4 KiB"),
            (1024, 4, "hashed, positions only, 1 KiB"),
            (1024, 5, "hashed, positions and a byte, 1 KiB"),
            (512, 4, "hashed, positions only, 512 B"),
        ] {
            run(shape, name, &mut Extendible::new(page, slot, grace));
        }
    }
}
