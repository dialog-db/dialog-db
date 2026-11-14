//! The file's index, kept in the file: a hitchhiker tree from 32-bit keys
//! to 32-bit values, whose root is in the slot and whose other nodes are
//! pages of [`PAGE`] bytes.
//!
//! - A leaf holds up to [`LEAF`] entries, sorted by key.
//! - An internal node holds up to [`FANOUT`] children, the pivots between
//!   them, and a buffer of up to [`BUFFER`] entries on their way down.
//! - An insert lands in the root's buffer. A buffer that overflows sends
//!   the entries bound for its busiest child down into that child, which
//!   is rewritten (a leaf takes them, splitting when full; an internal node
//!   buffers them, flushing in turn). Only the nodes a flush reaches are
//!   written, so most appends write nothing but the root.
//! - It is a map: an entry for a key shadows every older entry for it, and
//!   a lookup stops at the first it meets on the way down from the root.
//!
//! A node is never changed where it is: a rewrite writes a new page and
//! gives the old one back ([`Pages::free`]), and the file writes over a
//! page once no root it may still be read from reaches it. So a reader
//! holding an old root can find a page it reaches written over. Every page
//! says which append wrote it and carries a checksum, and a parent names a
//! child by its page and that append ([`Child`]): a page that is not the
//! one its parent names, or that was caught half written, is
//! [stale](is_stale), and the reader looks again from a newer root.
//!
//! A page: the append's sequence (u32), a checksum (u32, crc32 over the
//! page but these four bytes), the kind (`0` leaf, `1` internal), the
//! children (u8), the entries (u16), four bytes unused, then
//!
//! - leaf: entries (key u32, value u32);
//! - internal: pivots (u32, one fewer than children), children (page u32,
//!   sequence u32), buffered entries.
//!
//! The root, in the slot, is the same without the first eight bytes and the
//! four unused. All little-endian, fixed width, so a lookup binary-searches
//! a page as it reads it.

use std::fmt;
use std::io;
use std::ops::Deref;
use std::sync::Arc;

use crate::rows::invalid;

/// A key and its value.
pub(crate) type Entry = (u32, u32);

/// The bytes of a page, and its alignment in the file.
pub(crate) const PAGE: usize = 4096;
/// What a page says before its node.
const PAGE_HEADER: usize = 16;
/// What the root says before its node.
const ROOT_HEADER: usize = 4;
/// The most entries a leaf holds.
pub(crate) const LEAF: usize = (PAGE - PAGE_HEADER) / 8;
/// The most children an internal node has. Few children and a deep buffer
/// send each flush's entries down in large groups: over tonk-shaped 100×'s
/// appends, 8 children write about half what 32 do (`index_layouts`).
pub(crate) const FANOUT: usize = 8;
/// The most entries an internal node buffers: what a page has room for.
pub(crate) const BUFFER: usize = (PAGE - PAGE_HEADER - FANOUT * 8 - (FANOUT - 1) * 4) / 8;
/// The most entries the root buffers. The root lives in the slot, which
/// every append writes again, so its buffer costs the file nothing, and a
/// large one sends a flush's entries down in large groups.
pub(crate) const ROOT_BUFFER: usize = 2048;

/// A node's page, and the append that wrote it (its sequence's low bits).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Child {
    pub page: u32,
    pub sequence: u32,
}

/// A page's bytes, borrowed or shared.
#[derive(Debug)]
pub(crate) enum Page<'a> {
    Borrowed(&'a [u8]),
    Shared(Arc<[u8]>),
}

impl Deref for Page<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Page::Borrowed(bytes) => bytes,
            Page::Shared(bytes) => bytes,
        }
    }
}

/// Where the tree's pages are read from.
pub(crate) trait Nodes {
    /// The page `child` names, [checked](check).
    fn read(&self, child: Child) -> io::Result<Page<'_>>;
}

/// Where a rewrite puts its pages.
pub(crate) trait Pages: Nodes {
    /// Puts `node` on a page, stamped with the append writing it.
    fn write(&mut self, node: &Node) -> io::Result<Child>;
    /// Gives back a page a rewrite replaced.
    fn free(&mut self, child: Child);
}

/// A page that is not the one its parent names, or not whole.
#[derive(Debug)]
pub(crate) struct Stale;

impl fmt::Display for Stale {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an index page written over since its root was read")
    }
}

impl std::error::Error for Stale {}

fn stale() -> io::Error {
    io::Error::other(Stale)
}

/// Whether `error` says a page was written over since its root was read.
pub(crate) fn is_stale(error: &io::Error) -> bool {
    error.get_ref().is_some_and(|inner| inner.is::<Stale>())
}

fn page_checksum(bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[..4]);
    hasher.update(&bytes[8..]);
    hasher.finalize()
}

/// Checks that `bytes` are the page `child` names: whole, by its checksum,
/// and written by the append `child` says.
pub(crate) fn check(bytes: &[u8], child: Child) -> io::Result<()> {
    if bytes.len() != PAGE
        || u32_at(bytes, 0)? != child.sequence
        || u32_at(bytes, 4)? != page_checksum(bytes)
    {
        return Err(stale());
    }
    Ok(())
}

/// A node held in memory: the root, or one being rewritten.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Node {
    Leaf(Vec<Entry>),
    Internal {
        pivots: Vec<u32>,
        children: Vec<Child>,
        buffer: Vec<Entry>,
    },
}

impl Default for Node {
    fn default() -> Self {
        Node::Leaf(Vec::new())
    }
}

fn u32_at(bytes: &[u8], at: usize) -> io::Result<u32> {
    Ok(u32::from_le_bytes(
        bytes
            .get(at..at + 4)
            .ok_or_else(|| invalid("an index node ends early"))?
            .try_into()
            .expect("four bytes"),
    ))
}

/// Entries as a node holds them, read where they sit.
struct Entries<'a> {
    bytes: &'a [u8],
    count: usize,
}

impl Entries<'_> {
    fn key(&self, at: usize) -> io::Result<u32> {
        u32_at(self.bytes, at * 8)
    }

    fn get(&self, key: u32) -> io::Result<Option<u32>> {
        let (mut low, mut high) = (0, self.count);
        while low < high {
            let middle = (low + high) / 2;
            match self.key(middle)?.cmp(&key) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(Some(u32_at(self.bytes, middle * 8 + 4)?)),
            }
        }
        Ok(None)
    }

    fn to_vec(&self) -> io::Result<Vec<Entry>> {
        (0..self.count)
            .map(|at| Ok((self.key(at)?, u32_at(self.bytes, at * 8 + 4)?)))
            .collect()
    }
}

/// A node read where it sits.
enum View<'a> {
    Leaf(Entries<'a>),
    Internal {
        pivots: &'a [u8],
        children: &'a [u8],
        count: usize,
        buffer: Entries<'a>,
    },
}

impl<'a> View<'a> {
    /// A node from its kind, children and entries (`header`), then `body`.
    fn of(header: &'a [u8], body: &'a [u8]) -> io::Result<Self> {
        let slice = |from: usize, len: usize| {
            body.get(from..from + len)
                .ok_or_else(|| invalid("an index node ends early"))
        };
        let children = header[1] as usize;
        let count = u16::from_le_bytes([header[2], header[3]]) as usize;
        match header[0] {
            0 => Ok(View::Leaf(Entries {
                bytes: slice(0, count * 8)?,
                count,
            })),
            1 => {
                if children == 0 {
                    return Err(invalid("an index node with no children"));
                }
                let pivots = slice(0, (children - 1) * 4)?;
                let links = slice((children - 1) * 4, children * 8)?;
                let buffer = slice((children - 1) * 4 + children * 8, count * 8)?;
                Ok(View::Internal {
                    pivots,
                    children: links,
                    count: children,
                    buffer: Entries {
                        bytes: buffer,
                        count,
                    },
                })
            }
            _ => Err(invalid("not an index node")),
        }
    }

    /// A page's node; the page was [checked](check) when read.
    fn page(bytes: &'a [u8]) -> io::Result<Self> {
        Self::of(&bytes[8..12], &bytes[PAGE_HEADER..])
    }

    fn child(children: &[u8], at: usize) -> io::Result<Child> {
        Ok(Child {
            page: u32_at(children, at * 8)?,
            sequence: u32_at(children, at * 8 + 4)?,
        })
    }

    fn owned(&self) -> io::Result<Node> {
        Ok(match self {
            View::Leaf(entries) => Node::Leaf(entries.to_vec()?),
            View::Internal {
                pivots,
                children,
                count,
                buffer,
            } => Node::Internal {
                pivots: (0..count - 1)
                    .map(|at| u32_at(pivots, at * 4))
                    .collect::<io::Result<_>>()?,
                children: (0..*count)
                    .map(|at| Self::child(children, at))
                    .collect::<io::Result<_>>()?,
                buffer: buffer.to_vec()?,
            },
        })
    }
}

/// The child of a node with `pivots` that `key` belongs under: child `i`
/// holds the keys from pivot `i - 1` up to pivot `i`.
fn route(pivots: impl Fn(usize) -> io::Result<u32>, count: usize, key: u32) -> io::Result<usize> {
    let (mut low, mut high) = (0, count.saturating_sub(1));
    while low < high {
        let middle = (low + high) / 2;
        if pivots(middle)? <= key {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    Ok(low)
}

impl Node {
    fn header(&self) -> [u8; 4] {
        let (kind, children, count) = match self {
            Node::Leaf(leaf) => (0, 0, leaf.len()),
            Node::Internal {
                children, buffer, ..
            } => (1, children.len(), buffer.len()),
        };
        let count = u16::try_from(count).expect("a node's entries fit 16 bits");
        let [low, high] = count.to_le_bytes();
        [
            kind,
            u8::try_from(children).expect("a node's children fit 8 bits"),
            low,
            high,
        ]
    }

    fn body(&self, out: &mut Vec<u8>) {
        let entries = |out: &mut Vec<u8>, entries: &[Entry]| {
            for (key, value) in entries {
                out.extend_from_slice(&key.to_le_bytes());
                out.extend_from_slice(&value.to_le_bytes());
            }
        };
        match self {
            Node::Leaf(leaf) => entries(out, leaf),
            Node::Internal {
                pivots,
                children,
                buffer,
            } => {
                for pivot in pivots {
                    out.extend_from_slice(&pivot.to_le_bytes());
                }
                for child in children {
                    out.extend_from_slice(&child.page.to_le_bytes());
                    out.extend_from_slice(&child.sequence.to_le_bytes());
                }
                entries(out, buffer);
            }
        }
    }

    /// The root's bytes, as the slot keeps them.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.header().to_vec();
        self.body(&mut out);
        out
    }

    /// The root from bytes [`encode`](Self::encode) wrote.
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let header = bytes
            .get(..ROOT_HEADER)
            .ok_or_else(|| invalid("an index root ends early"))?;
        View::of(header, &bytes[ROOT_HEADER..])?.owned()
    }

    /// This node as a page written by the append `sequence` names.
    pub fn page(&self, sequence: u32) -> io::Result<Vec<u8>> {
        let mut out = Vec::with_capacity(PAGE);
        out.extend_from_slice(&sequence.to_le_bytes());
        out.extend_from_slice(&[0; 4]);
        out.extend_from_slice(&self.header());
        out.extend_from_slice(&[0; 4]);
        self.body(&mut out);
        if out.len() > PAGE {
            return Err(invalid("an index node over a page"));
        }
        out.resize(PAGE, 0);
        let checksum = page_checksum(&out);
        out[4..8].copy_from_slice(&checksum.to_le_bytes());
        Ok(out)
    }

    /// A node from a page `child` names, read through `nodes`.
    fn read(nodes: &impl Nodes, child: Child) -> io::Result<Self> {
        View::page(&nodes.read(child)?)?.owned()
    }

    /// The value of `key`, if the tree holds one.
    pub fn get(&self, nodes: &impl Nodes, key: u32) -> io::Result<Option<u32>> {
        let mut at = match self {
            Node::Leaf(leaf) => {
                return Ok(leaf
                    .binary_search_by_key(&key, |(key, _)| *key)
                    .ok()
                    .map(|at| leaf[at].1));
            }
            Node::Internal {
                pivots,
                children,
                buffer,
            } => {
                if let Ok(at) = buffer.binary_search_by_key(&key, |(key, _)| *key) {
                    return Ok(Some(buffer[at].1));
                }
                children[route(|at| Ok(pivots[at]), children.len(), key)?]
            }
        };
        loop {
            let bytes = nodes.read(at)?;
            match View::page(&bytes)? {
                View::Leaf(entries) => return entries.get(key),
                View::Internal {
                    pivots,
                    children,
                    count,
                    buffer,
                } => {
                    if let Some(value) = buffer.get(key)? {
                        return Ok(Some(value));
                    }
                    at = View::child(children, route(|at| u32_at(pivots, at * 4), count, key)?)?;
                }
            }
        }
    }

    /// Every entry the tree holds, newest value for each key, sorted.
    pub fn entries(&self, nodes: &impl Nodes) -> io::Result<Vec<Entry>> {
        match self {
            Node::Leaf(leaf) => Ok(leaf.clone()),
            Node::Internal {
                children, buffer, ..
            } => {
                let mut below = Vec::new();
                for child in children {
                    below.extend(Node::read(nodes, *child)?.entries(nodes)?);
                }
                Ok(merge(below, buffer))
            }
        }
    }

    /// Every page below this node, each read and so checked.
    #[cfg(test)]
    pub fn pages(&self, nodes: &impl Nodes, out: &mut Vec<Child>) -> io::Result<()> {
        self.pages_since(nodes, None, out)
    }

    /// The pages below this node written after the append `since` names
    /// (all of them for `None`), each read and so checked. A page written
    /// no later than `since` is not read, nor anything below it: a page
    /// below one is never newer than it.
    pub fn pages_since(
        &self,
        nodes: &impl Nodes,
        since: Option<u32>,
        out: &mut Vec<Child>,
    ) -> io::Result<()> {
        let Node::Internal { children, .. } = self else {
            return Ok(());
        };
        for child in children {
            if since.is_some_and(|since| (child.sequence.wrapping_sub(since) as i32) <= 0) {
                continue;
            }
            out.push(*child);
            Node::read(nodes, *child)?.pages_since(nodes, since, out)?;
        }
        Ok(())
    }

    /// The tree with `entries` inserted (each shadowing any older entry for
    /// its key), as a new root. The nodes it rewrites are written to
    /// `pages` and the pages they replace given back to it.
    pub fn insert(self, pages: &mut impl Pages, mut entries: Vec<Entry>) -> io::Result<Node> {
        entries.sort_unstable_by_key(|(key, _)| *key);
        entries.dedup_by(|later, earlier| {
            // Of two entries for a key, the later one given wins.
            if later.0 == earlier.0 {
                earlier.1 = later.1;
                true
            } else {
                false
            }
        });
        let mut root = match self {
            Node::Leaf(leaf) => {
                let leaf = merge(leaf, &entries);
                if leaf.len() <= LEAF {
                    return Ok(Node::Leaf(leaf));
                }
                let (pivots, children) = write_leaves(leaf, pages)?;
                Node::Internal {
                    pivots,
                    children,
                    buffer: Vec::new(),
                }
            }
            Node::Internal {
                pivots,
                children,
                buffer,
            } => Node::Internal {
                pivots,
                children,
                buffer: merge(buffer, &entries),
            },
        };
        // An overflowing buffer is flushed down to half, a group at a time:
        // the next appends fill it again without flushing.
        if root.buffered() > ROOT_BUFFER {
            while root.buffered() > ROOT_BUFFER / 2 {
                root.flush(pages)?;
            }
        }
        // A root over too many children splits, and a new root goes above,
        // keeping the buffer: a page has no room for the root's.
        while root.fanout() > FANOUT {
            let Node::Internal {
                pivots,
                children,
                buffer,
            } = root
            else {
                unreachable!("only an internal node has children");
            };
            let parts = Node::Internal {
                pivots,
                children,
                buffer: Vec::new(),
            }
            .split(pages)?;
            root = Node::Internal {
                pivots: parts.iter().skip(1).map(|(first, _)| *first).collect(),
                children: parts.into_iter().map(|(_, child)| child).collect(),
                buffer,
            };
        }
        Ok(root)
    }

    fn buffered(&self) -> usize {
        match self {
            Node::Leaf(_) => 0,
            Node::Internal { buffer, .. } => buffer.len(),
        }
    }

    fn fanout(&self) -> usize {
        match self {
            Node::Leaf(_) => 0,
            Node::Internal { children, .. } => children.len(),
        }
    }

    /// Sends the entries bound for this node's busiest child down into it.
    fn flush(&mut self, pages: &mut impl Pages) -> io::Result<()> {
        let Node::Internal {
            pivots,
            children,
            buffer,
        } = self
        else {
            return Ok(());
        };
        let child_of = |key: u32| route(|at| Ok(pivots[at]), children.len(), key);
        let mut counts = vec![0usize; children.len()];
        for (key, _) in buffer.iter() {
            counts[child_of(*key)?] += 1;
        }
        let busiest = (0..counts.len())
            .max_by_key(|at| counts[*at])
            .expect("a node with children");
        let mut batch = Vec::with_capacity(counts[busiest]);
        let mut kept = Vec::with_capacity(buffer.len() - counts[busiest]);
        for entry in buffer.drain(..) {
            if child_of(entry.0)? == busiest {
                batch.push(entry);
            } else {
                kept.push(entry);
            }
        }
        *buffer = kept;

        let mut child = Node::read(&*pages, children[busiest])?;
        pages.free(children[busiest]);
        let replacement: Vec<(u32, Child)> = match &mut child {
            Node::Leaf(leaf) => {
                let merged = merge(std::mem::take(leaf), &batch);
                let first = merged.first().map_or(0, |(key, _)| *key);
                if merged.len() <= LEAF {
                    vec![(first, pages.write(&Node::Leaf(merged))?)]
                } else {
                    let (pivots, children) = write_leaves(merged, pages)?;
                    std::iter::once(first).chain(pivots).zip(children).collect()
                }
            }
            Node::Internal { buffer, .. } => {
                *buffer = merge(std::mem::take(buffer), &batch);
                if child.buffered() > BUFFER {
                    while child.buffered() > BUFFER / 2 {
                        child.flush(pages)?;
                    }
                }
                if child.fanout() > FANOUT {
                    child.split(pages)?
                } else {
                    vec![(0, pages.write(&child)?)]
                }
            }
        };
        // The pivot left of the busiest child stays; the parts after the
        // first bring pivots of their own.
        let at = busiest;
        children.splice(at..=at, replacement.iter().map(|(_, child)| *child));
        pivots.splice(at..at, replacement.iter().skip(1).map(|(first, _)| *first));
        Ok(())
    }

    /// Writes this internal node as parts of at most [`FANOUT`] children
    /// each, splitting its buffer by the pivots between them; returns each
    /// part's first key (the pivot before it) and where it was written.
    fn split(&self, pages: &mut impl Pages) -> io::Result<Vec<(u32, Child)>> {
        let Node::Internal {
            pivots,
            children,
            buffer,
        } = self
        else {
            return Ok(Vec::new());
        };
        let parts = children.len().div_ceil(FANOUT).max(2);
        let size = children.len().div_ceil(parts);
        let mut out = Vec::with_capacity(parts);
        let mut start = 0;
        while start < children.len() {
            let end = (start + size).min(children.len());
            let low = if start == 0 {
                None
            } else {
                Some(pivots[start - 1])
            };
            let high = pivots
                .get(end - 1)
                .copied()
                .filter(|_| end < children.len());
            let part = Node::Internal {
                pivots: pivots[start..end - 1].to_vec(),
                children: children[start..end].to_vec(),
                buffer: buffer
                    .iter()
                    .filter(|(key, _)| {
                        low.is_none_or(|low| *key >= low) && high.is_none_or(|high| *key < high)
                    })
                    .copied()
                    .collect(),
            };
            out.push((low.unwrap_or(0), pages.write(&part)?));
            start = end;
        }
        Ok(out)
    }
}

/// `older` with `newer` merged in, both sorted by key, `newer` winning on
/// equal keys.
fn merge(older: Vec<Entry>, newer: &[Entry]) -> Vec<Entry> {
    let mut out = Vec::with_capacity(older.len() + newer.len());
    let (mut a, mut b) = (older.into_iter().peekable(), newer.iter().peekable());
    loop {
        match (a.peek(), b.peek()) {
            (Some(x), Some(y)) if x.0 < y.0 => out.push(a.next().expect("peeked")),
            (Some(x), Some(y)) if x.0 == y.0 => {
                a.next();
                out.push(*b.next().expect("peeked"));
            }
            (_, Some(_)) => out.push(*b.next().expect("peeked")),
            (Some(_), None) => out.push(a.next().expect("peeked")),
            (None, None) => return out,
        }
    }
}

/// Writes `entries` (sorted, over [`LEAF`]) as leaves of about equal size;
/// returns the pivots between them and where each was written.
fn write_leaves(entries: Vec<Entry>, pages: &mut impl Pages) -> io::Result<(Vec<u32>, Vec<Child>)> {
    // Leaves start three quarters full, so the next few inserts into one
    // do not split it straight away.
    let parts = entries.len().div_ceil(LEAF * 3 / 4).max(1);
    let size = entries.len().div_ceil(parts);
    let (mut pivots, mut children) = (Vec::new(), Vec::new());
    for (at, chunk) in entries.chunks(size).enumerate() {
        if at > 0 {
            pivots.push(chunk[0].0);
        }
        children.push(pages.write(&Node::Leaf(chunk.to_vec()))?);
    }
    Ok((pivots, children))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashSet};

    /// Pages in memory, reused once freed when `reuse` is set, each append
    /// stamped with `sequence`.
    #[derive(Default)]
    pub(crate) struct Memory {
        pub pages: Vec<Vec<u8>>,
        pub free: Vec<u32>,
        pub written: usize,
        pub sequence: u32,
        pub reuse: bool,
        /// Pages freed by this append: reused from the next.
        pub freed: Vec<u32>,
    }

    impl Memory {
        pub fn reusing() -> Self {
            Self {
                reuse: true,
                ..Self::default()
            }
        }

        /// The next append: what the last one freed may be written over.
        pub fn next(&mut self) {
            self.sequence += 1;
            self.free.append(&mut self.freed);
        }
    }

    impl Nodes for Memory {
        fn read(&self, child: Child) -> io::Result<Page<'_>> {
            let bytes = self
                .pages
                .get(child.page as usize)
                .ok_or_else(|| invalid("no such page"))?;
            check(bytes, child)?;
            Ok(Page::Borrowed(bytes))
        }
    }

    impl Pages for Memory {
        fn write(&mut self, node: &Node) -> io::Result<Child> {
            let bytes = node.page(self.sequence)?;
            self.written += PAGE;
            let page = match self.free.pop().filter(|_| self.reuse) {
                Some(page) => {
                    self.pages[page as usize] = bytes;
                    page
                }
                None => {
                    self.pages.push(bytes);
                    self.pages.len() as u32 - 1
                }
            };
            Ok(Child {
                page,
                sequence: self.sequence,
            })
        }

        fn free(&mut self, child: Child) {
            self.freed.push(child.page);
        }
    }

    pub(crate) fn key(n: u64) -> u32 {
        crate::hash::hash(&n.to_le_bytes()) as u32
    }

    /// Inserts, a batch at a time as appends would, agree with a map at
    /// every step, and reading back from an old root, while no page is
    /// written over, sees the tree as it was.
    #[dialog_common::test]
    fn it_finds_what_it_holds_as_a_map_does() -> io::Result<()> {
        let mut memory = Memory::default();
        let mut root = Node::default();
        let mut map = BTreeMap::new();
        let mut old = None;
        for batch in 0..2000u64 {
            let entries: Vec<Entry> = (0..(batch % 17 + 1))
                .map(|n| (key(batch * 31 + n), (batch * 1000 + n) as u32))
                .chain((batch % 5 == 0).then(|| (key(batch * 31 / 2), batch as u32)))
                .collect();
            for (k, v) in &entries {
                map.insert(*k, *v);
            }
            root = root.insert(&mut memory, entries)?;
            memory.next();
            if batch == 1000 {
                old = Some((root.clone(), map.clone()));
            }
        }
        assert!(matches!(root, Node::Internal { .. }), "the tree grew pages");
        for (k, v) in &map {
            assert_eq!(root.get(&memory, *k)?, Some(*v));
        }
        assert_eq!(
            root.entries(&memory)?,
            map.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>()
        );
        let (old_root, old_map) = old.expect("kept");
        for (k, v) in &old_map {
            assert_eq!(old_root.get(&memory, *k)?, Some(*v));
        }
        Ok(())
    }

    /// With pages written over once freed, the tree keeps no more pages
    /// than it reaches and the last append freed; an old root that reaches
    /// a page written over finds it stale, never another node's entries.
    #[dialog_common::test]
    fn it_reuses_pages_and_an_old_root_finds_them_stale() -> io::Result<()> {
        let mut memory = Memory::reusing();
        let mut root = Node::default();
        let mut map = BTreeMap::new();
        let mut old = None;
        for batch in 0..3000u64 {
            let entries: Vec<Entry> = (0..40)
                .map(|n| (key(batch * 40 + n), (batch * 40 + n) as u32))
                .collect();
            for (k, v) in &entries {
                map.insert(*k, *v);
            }
            root = root.insert(&mut memory, entries)?;
            memory.next();
            if batch == 1500 {
                old = Some(root.clone());
            }
        }
        for (k, v) in &map {
            assert_eq!(root.get(&memory, *k)?, Some(*v));
        }
        let mut reached = Vec::new();
        root.pages(&memory, &mut reached)?;
        let reached: HashSet<u32> = reached.iter().map(|child| child.page).collect();
        let free: HashSet<u32> = memory.free.iter().copied().collect();
        assert_eq!(
            reached.len() + free.len(),
            memory.pages.len(),
            "every page is reached or free"
        );
        assert!(reached.is_disjoint(&free));
        assert!(
            free.len() < reached.len() / 4,
            "{} pages free against {} reached",
            free.len(),
            reached.len()
        );

        let old = old.expect("kept");
        let mut stale = 0;
        for (k, v) in map.iter().take(20_000) {
            match old.get(&memory, *k) {
                Ok(found) => assert!(found.is_none() || found == Some(*v)),
                Err(error) if is_stale(&error) => stale += 1,
                Err(error) => return Err(error),
            }
        }
        assert!(stale > 0, "the old root reaches pages written over");
        Ok(())
    }

    /// A page caught half written fails its checksum and reads as stale.
    #[dialog_common::test]
    fn it_finds_a_torn_page_stale() -> io::Result<()> {
        let mut memory = Memory::default();
        let child = memory.write(&Node::Leaf(vec![(1, 2), (3, 4)]))?;
        assert!(memory.read(child).is_ok());
        memory.pages[child.page as usize][100] ^= 1;
        assert!(is_stale(&memory.read(child).expect_err("torn")));
        let other = Child {
            sequence: child.sequence + 1,
            ..child
        };
        memory.pages[child.page as usize][100] ^= 1;
        assert!(is_stale(&memory.read(other).expect_err("another append's")));
        Ok(())
    }

    /// Only the pages written after an append are read to check them.
    #[dialog_common::test]
    fn it_lists_the_pages_written_since_an_append() -> io::Result<()> {
        let mut memory = Memory::reusing();
        let mut root = Node::default();
        for batch in 0..2000u64 {
            let entries = (0..40).map(|n| (key(batch * 40 + n), n as u32)).collect();
            root = root.insert(&mut memory, entries)?;
            memory.next();
        }
        let mut all = Vec::new();
        root.pages(&memory, &mut all)?;
        let newest = all.iter().map(|child| child.sequence).max().expect("pages");
        let mut since = Vec::new();
        root.pages_since(&memory, Some(newest - 1), &mut since)?;
        assert!(!since.is_empty() && since.len() < all.len());
        assert!(since.iter().all(|child| child.sequence == newest));
        Ok(())
    }

    /// The root fits a slot with room to spare, whatever it buffers.
    #[dialog_common::test]
    fn it_keeps_the_root_small() -> io::Result<()> {
        let mut memory = Memory::reusing();
        let mut root = Node::default();
        for batch in 0..1000u64 {
            let entries = (0..100).map(|n| (key(batch * 100 + n), n as u32)).collect();
            root = root.insert(&mut memory, entries)?;
            memory.next();
            assert!(root.encode().len() <= 4 + ROOT_BUFFER * 8 + FANOUT * 12);
        }
        assert!(
            (root.encode().len() as u64) < super::super::slot::SLOT / 2,
            "the root leaves half a slot"
        );
        Ok(())
    }
}
