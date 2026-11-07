//! The file's index, kept in the file: a hitchhiker tree from 64-bit keys
//! to 64-bit values, whose root is in the slot and whose other nodes are
//! records, read in place.
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
//! A node is written once and never changes, so a reader holding an old
//! root reads the tree as it was.
//!
//! Nodes are encoded with fixed-width fields, so a lookup binary-searches
//! a node where it sits:
//!
//! - leaf: `0`, count (u32), then entries (key u64, value u64);
//! - internal: `1`, children (u32), buffered (u32), pivots (u64, one fewer
//!   than children), children's offsets (u64), buffered entries.
//!
//! All little-endian.

use std::borrow::Cow;
use std::io;

use crate::rows::invalid;

/// A key and its value.
pub(crate) type Entry = (u64, u64);

/// The most entries a leaf holds.
pub(crate) const LEAF: usize = 64;
/// The most children an internal node has. Few children and a deep buffer
/// send each flush's entries down in large groups: over tonk-shaped 100×'s
/// appends (`write_amplification_of_a_file`), 8 children and buffers of 512
/// write 114 bytes an entry, 32 and 256 write 221. Over its 1.5 million
/// entries a cold lookup reads about five levels, one more than at 32.
pub(crate) const FANOUT: usize = 8;
/// The most entries an internal node buffers.
pub(crate) const BUFFER: usize = 512;
/// The most entries the root buffers. The root is not a record (it lives
/// in the slot, rewritten by every append), so its buffer costs the file
/// nothing, and a large one sends a flush's entries down in large groups.
pub(crate) const ROOT_BUFFER: usize = 2048;

/// Where the tree's nodes are read from: the bytes of the node at an
/// offset, as [`encode`](Node::encode) wrote them.
pub(crate) trait Nodes {
    fn read(&self, at: u64) -> io::Result<Cow<'_, [u8]>>;
}

/// A node held in memory: the root, or one being rewritten.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Node {
    Leaf(Vec<Entry>),
    Internal {
        pivots: Vec<u64>,
        children: Vec<u64>,
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

fn u64_at(bytes: &[u8], at: usize) -> io::Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(at..at + 8)
            .ok_or_else(|| invalid("an index node ends early"))?
            .try_into()
            .expect("eight bytes"),
    ))
}

/// Entries as a node holds them, read where they sit.
struct Entries<'a> {
    bytes: &'a [u8],
    count: usize,
}

impl Entries<'_> {
    fn key(&self, at: usize) -> io::Result<u64> {
        u64_at(self.bytes, at * 16)
    }

    fn get(&self, key: u64) -> io::Result<Option<u64>> {
        let (mut low, mut high) = (0, self.count);
        while low < high {
            let middle = (low + high) / 2;
            match self.key(middle)?.cmp(&key) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(Some(u64_at(self.bytes, middle * 16 + 8)?)),
            }
        }
        Ok(None)
    }

    fn to_vec(&self) -> io::Result<Vec<Entry>> {
        (0..self.count)
            .map(|at| Ok((self.key(at)?, u64_at(self.bytes, at * 16 + 8)?)))
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
    fn of(bytes: &'a [u8]) -> io::Result<Self> {
        let slice = |from: usize, len: usize| {
            bytes
                .get(from..from + len)
                .ok_or_else(|| invalid("an index node ends early"))
        };
        match bytes.first() {
            Some(0) => {
                let count = u32_at(bytes, 1)? as usize;
                Ok(View::Leaf(Entries {
                    bytes: slice(5, count * 16)?,
                    count,
                }))
            }
            Some(1) => {
                let count = u32_at(bytes, 1)? as usize;
                let buffered = u32_at(bytes, 5)? as usize;
                if count == 0 {
                    return Err(invalid("an index node with no children"));
                }
                let pivots = slice(9, (count - 1) * 8)?;
                let children = slice(9 + (count - 1) * 8, count * 8)?;
                let buffer = slice(9 + (count - 1) * 8 + count * 8, buffered * 16)?;
                Ok(View::Internal {
                    pivots,
                    children,
                    count,
                    buffer: Entries {
                        bytes: buffer,
                        count: buffered,
                    },
                })
            }
            _ => Err(invalid("not an index node")),
        }
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
                    .map(|at| u64_at(pivots, at * 8))
                    .collect::<io::Result<_>>()?,
                children: (0..*count)
                    .map(|at| u64_at(children, at * 8))
                    .collect::<io::Result<_>>()?,
                buffer: buffer.to_vec()?,
            },
        })
    }
}

/// The child of a node with `pivots` that `key` belongs under: child `i`
/// holds the keys from pivot `i - 1` up to pivot `i`.
fn route(pivots: impl Fn(usize) -> io::Result<u64>, count: usize, key: u64) -> io::Result<usize> {
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
    /// The node's bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let entries = |out: &mut Vec<u8>, entries: &[Entry]| {
            for (key, value) in entries {
                out.extend_from_slice(&key.to_le_bytes());
                out.extend_from_slice(&value.to_le_bytes());
            }
        };
        match self {
            Node::Leaf(leaf) => {
                out.push(0);
                out.extend_from_slice(&(leaf.len() as u32).to_le_bytes());
                entries(&mut out, leaf);
            }
            Node::Internal {
                pivots,
                children,
                buffer,
            } => {
                out.push(1);
                out.extend_from_slice(&(children.len() as u32).to_le_bytes());
                out.extend_from_slice(&(buffer.len() as u32).to_le_bytes());
                for pivot in pivots {
                    out.extend_from_slice(&pivot.to_le_bytes());
                }
                for child in children {
                    out.extend_from_slice(&child.to_le_bytes());
                }
                entries(&mut out, buffer);
            }
        }
        out
    }

    /// A node from bytes [`encode`](Self::encode) wrote.
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        View::of(bytes)?.owned()
    }

    /// The value of `key`, if the tree holds one.
    pub fn get(&self, nodes: &impl Nodes, key: u64) -> io::Result<Option<u64>> {
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
            match View::of(&bytes)? {
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
                    at = u64_at(
                        children,
                        route(|at| u64_at(pivots, at * 8), count, key)? * 8,
                    )?;
                }
            }
        }
    }

    /// Every entry the tree holds, newest value for each key, sorted.
    pub fn entries(&self, nodes: &impl Nodes) -> io::Result<Vec<Entry>> {
        fn all(node: &Node, nodes: &impl Nodes) -> io::Result<Vec<Entry>> {
            match node {
                Node::Leaf(leaf) => Ok(leaf.clone()),
                Node::Internal {
                    children, buffer, ..
                } => {
                    let mut below = Vec::new();
                    for child in children {
                        below.extend(all(&Node::decode(&nodes.read(*child)?)?, nodes)?);
                    }
                    Ok(merge(below, buffer))
                }
            }
        }
        all(self, nodes)
    }

    /// How many nodes below this one the tree reaches, and their bytes: the
    /// index the root still uses, against all the file holds.
    #[cfg(test)]
    pub fn reachable(&self, nodes: &impl Nodes) -> io::Result<(usize, usize)> {
        let Node::Internal { children, .. } = self else {
            return Ok((0, 0));
        };
        let (mut count, mut bytes) = (0, 0);
        for child in children {
            let read = nodes.read(*child)?;
            let (below, below_bytes) = Node::decode(&read)?.reachable(nodes)?;
            count += 1 + below;
            bytes += read.len() + below_bytes;
        }
        Ok((count, bytes))
    }

    /// The tree with `entries` inserted (each shadowing any older entry for
    /// its key), as a new root. The nodes it rewrites are passed to
    /// `write`, which returns where each was put; the old root and every
    /// node it reaches stay as they are.
    pub fn insert(
        self,
        nodes: &impl Nodes,
        mut entries: Vec<Entry>,
        write: &mut impl FnMut(&[u8]) -> io::Result<u64>,
    ) -> io::Result<Node> {
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
                let (pivots, children) = write_leaves(leaf, write)?;
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
                root.flush(nodes, write)?;
            }
        }
        // A root over too many children splits, and a new root goes above.
        while root.fanout() > FANOUT {
            let parts = root.split(write)?;
            root = Node::Internal {
                pivots: parts.iter().skip(1).map(|(first, _)| *first).collect(),
                children: parts.into_iter().map(|(_, at)| at).collect(),
                buffer: Vec::new(),
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
    fn flush(
        &mut self,
        nodes: &impl Nodes,
        write: &mut impl FnMut(&[u8]) -> io::Result<u64>,
    ) -> io::Result<()> {
        let Node::Internal {
            pivots,
            children,
            buffer,
        } = self
        else {
            return Ok(());
        };
        let child_of = |key: u64| route(|at| Ok(pivots[at]), children.len(), key);
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

        let mut child = Node::decode(&nodes.read(children[busiest])?)?;
        let replacement: Vec<(u64, u64)> = match &mut child {
            Node::Leaf(leaf) => {
                let merged = merge(std::mem::take(leaf), &batch);
                let first = merged.first().map_or(0, |(key, _)| *key);
                if merged.len() <= LEAF {
                    vec![(first, write(&Node::Leaf(merged).encode())?)]
                } else {
                    let (pivots, children) = write_leaves(merged, write)?;
                    std::iter::once(first).chain(pivots).zip(children).collect()
                }
            }
            Node::Internal { buffer, .. } => {
                *buffer = merge(std::mem::take(buffer), &batch);
                if child.buffered() > BUFFER {
                    while child.buffered() > BUFFER / 2 {
                        child.flush(nodes, write)?;
                    }
                }
                if child.fanout() > FANOUT {
                    child.split(write)?
                } else {
                    vec![(0, write(&child.encode())?)]
                }
            }
        };
        // The pivot left of the busiest child stays; the parts after the
        // first bring pivots of their own.
        let at = busiest;
        children.splice(at..=at, replacement.iter().map(|(_, offset)| *offset));
        pivots.splice(at..at, replacement.iter().skip(1).map(|(first, _)| *first));
        Ok(())
    }

    /// Writes this internal node as parts of at most [`FANOUT`] children
    /// each, splitting its buffer by the pivots between them; returns each
    /// part's first key (the pivot before it) and where it was written.
    fn split(
        &self,
        write: &mut impl FnMut(&[u8]) -> io::Result<u64>,
    ) -> io::Result<Vec<(u64, u64)>> {
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
            out.push((low.unwrap_or(0), write(&part.encode())?));
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
fn write_leaves(
    entries: Vec<Entry>,
    write: &mut impl FnMut(&[u8]) -> io::Result<u64>,
) -> io::Result<(Vec<u64>, Vec<u64>)> {
    // Leaves start three quarters full, so the next few inserts into one
    // do not split it straight away.
    let parts = entries.len().div_ceil(LEAF * 3 / 4).max(1);
    let size = entries.len().div_ceil(parts);
    let (mut pivots, mut children) = (Vec::new(), Vec::new());
    for (at, chunk) in entries.chunks(size).enumerate() {
        if at > 0 {
            pivots.push(chunk[0].0);
        }
        children.push(write(&Node::Leaf(chunk.to_vec()).encode())?);
    }
    Ok((pivots, children))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    /// Nodes in memory, at the offsets they were written to.
    #[derive(Default)]
    struct Memory {
        nodes: RefCell<Vec<Vec<u8>>>,
        written: RefCell<usize>,
    }

    impl Memory {
        fn write(&self, bytes: &[u8]) -> io::Result<u64> {
            let mut nodes = self.nodes.borrow_mut();
            nodes.push(bytes.to_vec());
            *self.written.borrow_mut() += bytes.len();
            Ok(nodes.len() as u64 - 1)
        }
    }

    impl Nodes for Memory {
        fn read(&self, at: u64) -> io::Result<Cow<'_, [u8]>> {
            let nodes = self.nodes.borrow();
            let bytes = nodes
                .get(at as usize)
                .ok_or_else(|| invalid("no such node"))?;
            Ok(Cow::Owned(bytes.clone()))
        }
    }

    fn key(n: u64) -> u64 {
        crate::hash::hash(&n.to_le_bytes())
    }

    /// Inserts, a batch at a time as appends would, agree with a map at
    /// every step, and reading back from an old root sees the tree as it
    /// was.
    #[test]
    fn it_finds_what_it_holds_as_a_map_does() -> io::Result<()> {
        let memory = Memory::default();
        let mut root = Node::default();
        let mut map = BTreeMap::new();
        let mut old = None;
        for batch in 0..400u64 {
            let entries: Vec<Entry> = (0..(batch % 17 + 1))
                .map(|n| (key(batch * 31 + n), batch * 1000 + n))
                .chain((batch % 5 == 0).then(|| (key(batch * 31 / 2), batch)))
                .collect();
            for (k, v) in &entries {
                map.insert(*k, *v);
            }
            root = root.insert(&memory, entries, &mut |bytes| memory.write(bytes))?;
            if batch == 200 {
                old = Some((root.clone(), map.clone()));
            }
        }
        for (k, v) in &map {
            assert_eq!(root.get(&memory, *k)?, Some(*v));
        }
        assert_eq!(root.get(&memory, 12345)?, None);
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

    /// Bytes written for each entry inserted, in batches of several sizes,
    /// as appends of several sizes would insert. Run by hand.
    #[test]
    #[ignore = "measures write amplification"]
    fn write_amplification() -> io::Result<()> {
        for batch in [4u64, 8, 32, 256] {
            let memory = Memory::default();
            let mut root = Node::default();
            let count = 60_000u64;
            for at in 0..count / batch {
                let entries = (0..batch).map(|n| (key(at * batch + n), n)).collect();
                root = root.insert(&memory, entries, &mut |bytes| memory.write(bytes))?;
            }
            let written = *memory.written.borrow();
            println!(
                "batches of {batch}: {:.0} bytes written an entry, {} nodes, root {} B",
                written as f64 / count as f64,
                memory.nodes.borrow().len(),
                root.encode().len()
            );
        }
        Ok(())
    }

    /// Bytes written for each entry over appends shaped as a growing
    /// file's are: each append files `DIALOG_INDEX_FRAMES` new frames (a new
    /// block key, and its lineage's key overwritten, out of
    /// `DIALOG_INDEX_LINEAGES`, none when it is 0) and `DIALOG_INDEX_TOKENS` new tokens, for
    /// `DIALOG_INDEX_APPENDS` appends. The defaults are tonk-shaped 100×'s
    /// owner: 3,569 appends of 292 frames and 118 tokens. Run by hand.
    #[test]
    #[ignore = "measures write amplification over a file's appends"]
    fn write_amplification_of_a_file() -> io::Result<()> {
        let var = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        };
        let appends = var("DIALOG_INDEX_APPENDS", 3569);
        let frames = var("DIALOG_INDEX_FRAMES", 292);
        let tokens = var("DIALOG_INDEX_TOKENS", 118);
        let lineages = var("DIALOG_INDEX_LINEAGES", 50_000);
        let memory = Memory::default();
        let mut root = Node::default();
        let (mut fresh, mut inserted) = (0u64, 0u64);
        for append in 0..appends {
            let mut entries = Vec::new();
            for frame in 0..frames {
                fresh += 1;
                entries.push((key(fresh), fresh));
                if lineages > 0 {
                    let lineage = key(u64::MAX - key(append * frames + frame) % lineages);
                    entries.push((lineage, fresh));
                }
            }
            for _ in 0..tokens {
                fresh += 1;
                entries.push((key(fresh), fresh));
            }
            entries.sort_unstable();
            entries.dedup_by_key(|(key, _)| *key);
            inserted += entries.len() as u64;
            root = root.insert(&memory, entries, &mut |bytes| memory.write(bytes))?;
        }
        let written = *memory.written.borrow();
        println!(
            "{inserted} entries in {appends} appends: {:.0} MiB written, {:.0} bytes an entry, \
             {} nodes, root {} B",
            written as f64 / 1048576.0,
            written as f64 / inserted as f64,
            memory.nodes.borrow().len(),
            root.encode().len()
        );
        Ok(())
    }

    /// Most inserts write nothing but the root, and the tree stays shallow
    /// and small against what it holds.
    #[test]
    fn it_writes_little_for_what_it_holds() -> io::Result<()> {
        let memory = Memory::default();
        let mut root = Node::default();
        let count = 100_000u64;
        for batch in 0..count / 10 {
            let entries = (0..10).map(|n| (key(batch * 10 + n), n)).collect();
            root = root.insert(&memory, entries, &mut |bytes| memory.write(bytes))?;
        }
        let written = *memory.written.borrow();
        let per_entry = written as f64 / count as f64;
        assert!(per_entry < 100.0, "{per_entry:.0} bytes written an entry");
        for n in (0..count).step_by(997) {
            assert_eq!(root.get(&memory, key(n))?, Some(n % 10));
        }
        // The root's buffer is the largest part of a slot: it must leave
        // room for the rest.
        assert!(
            root.encode().len() as u64 <= super::super::slot::SLOT / 2,
            "the root fits a slot"
        );
        Ok(())
    }
}
