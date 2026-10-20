//! The Bao tree of a blob, over groups of 16 KiB.
//!
//! The BLAKE3 hash of a blob is also the root of a tree of hashes over its
//! pieces: a Bao tree. A store that keeps the hashes of that tree beside the
//! bytes of a blob, its *outboard*, can prove any range of the blob against
//! the hash alone, group by group, with the hashes on the path from the root
//! to each group. This module holds what the stores share of that tree, with
//! the geometry of [`bao_tree`]:
//!
//! - [`GROUP`], the 16 KiB group that the tree proves, as `iroh-blobs` uses.
//! - [`OutboardBuilder`], which builds the outboard of a blob in one pass, as
//!   the bytes stream in, so a store never holds the whole blob.
//! - [`Decoder`], which verifies the Bao encoding of a range of a blob as it
//!   arrives, item by item, against the hash of the blob alone.
//! - [`Groups`], the groups of a blob that a store holds in part.
//!
//! The encoding of a range is that of `bao-tree`: in pre order, the 64 bytes
//! of each parent of the tree on the path to a group of the range, and the
//! bytes of each group of the range, with groups of 16 KiB. A decoder that
//! knows the hash, the size, and the range checks each parent against the
//! hash that it holds for it, and each group against its parent, so each
//! group is proven before the next arrives.
//!
//! The outboard is in post order, as [`bao_tree::io::outboard::PostOrderMemOutboard`]
//! lays it out: 64 bytes, the hashes of the two children, for each parent of
//! the tree. A tree of `n` groups has `n - 1` parents, so a blob of one group
//! or less, 16 KiB at most, has no outboard.

use bao_tree::iter::{BaoChunk, ResponseIter};
use bao_tree::{BaoTree, BlockSize, ChunkNum, ChunkRanges, TreeNode};
use blake3::hazmat::{
    ChainingValue, HasherExt as _, Mode, merge_subtrees_non_root, merge_subtrees_root,
};
use dialog_common::Blake3Hash;

/// The log of the chunks of 1 KiB in a group: 16 chunks, 16 KiB.
const GROUP_LOG: u8 = 4;

/// The size of a group, the unit that the Bao tree of a blob proves: 16 KiB,
/// at an offset that is a multiple of 16 KiB. The last group of a blob can
/// be shorter.
pub const GROUP: u64 = 1024 << GROUP_LOG;

/// The block size of the trees of [`bao_tree`]: one [`GROUP`].
pub const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(GROUP_LOG);

/// The bytes of one parent of the outboard: the hashes of its two children.
pub const PAIR: usize = 64;

/// The Bao tree of a blob of `size` bytes.
pub fn tree(size: u64) -> BaoTree {
    BaoTree::new(size, BLOCK_SIZE)
}

/// The length of the outboard of a blob of `size` bytes: [`PAIR`] bytes for
/// each group after the first.
pub fn outboard_size(size: u64) -> u64 {
    tree(size).outboard_size()
}

/// Builds the outboard of a blob as its bytes stream in, and its hash.
///
/// It hashes each group as BLAKE3 hashes a subtree, and merges the hashes of
/// two subtrees once a later byte shows that the merge is not the root, as an
/// incremental BLAKE3 hasher does. Each merge is one parent of the tree, and
/// the merges come in post order. So [`OutboardBuilder::update`] gives the
/// bytes of the outboard that it completed, to append to it, and holds at
/// most one group and the hashes of the open subtrees.
pub struct OutboardBuilder {
    /// The hasher of the group under way.
    group: blake3::Hasher,
    /// The bytes of the group under way.
    filled: u64,
    /// The groups before the group under way.
    groups: u64,
    /// The hashes of the complete subtrees that wait for a merge, the
    /// leftmost first.
    open: Vec<ChainingValue>,
}

impl Default for OutboardBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OutboardBuilder {
    /// A builder of a blob with no bytes yet.
    pub fn new() -> Self {
        Self {
            group: blake3::Hasher::new(),
            filled: 0,
            groups: 0,
            open: Vec::new(),
        }
    }

    /// The bytes that the builder took.
    pub fn size(&self) -> u64 {
        self.groups * GROUP + self.filled
    }

    /// Takes `bytes`, the next bytes of the blob, and gives the bytes of the
    /// outboard that they complete, in order.
    pub fn update(&mut self, mut bytes: &[u8]) -> Vec<u8> {
        let mut pairs = Vec::new();
        while !bytes.is_empty() {
            // A full group is not the last once a byte follows it.
            if self.filled == GROUP {
                let hash = self.group.finalize_non_root();
                self.groups += 1;
                self.push(hash, &mut pairs);
                self.group = blake3::Hasher::new();
                self.group.set_input_offset(self.groups * GROUP);
                self.filled = 0;
            }
            let room = usize::try_from(GROUP - self.filled).unwrap_or(usize::MAX);
            let (taken, rest) = bytes.split_at(room.min(bytes.len()));
            self.group.update(taken);
            self.filled += taken.len() as u64;
            bytes = rest;
        }
        pairs
    }

    /// Ends the blob, and gives its hash and the rest of its outboard: the
    /// parents of the right edge of the tree, from the lowest to the root.
    pub fn finish(mut self) -> (Blake3Hash, Vec<u8>) {
        if self.groups == 0 {
            let hash = self.group.finalize();
            return (Blake3Hash::from(*hash.as_bytes()), Vec::new());
        }
        let mut pairs = Vec::new();
        let mut right = self.group.finalize_non_root();
        loop {
            let left = self
                .open
                .pop()
                .expect("a tree of more than one group has a subtree on its left");
            pairs.extend_from_slice(&left);
            pairs.extend_from_slice(&right);
            if self.open.is_empty() {
                let root = merge_subtrees_root(&left, &right, Mode::Hash);
                return (Blake3Hash::from(*root.as_bytes()), pairs);
            }
            right = merge_subtrees_non_root(&left, &right, Mode::Hash);
        }
    }

    /// Takes the hash of the complete group `self.groups - 1`, and merges the
    /// subtrees that it completes, as BLAKE3 merges chunks: one merge for
    /// each trailing zero of the count of the groups.
    fn push(&mut self, mut hash: ChainingValue, pairs: &mut Vec<u8>) {
        let mut count = self.groups;
        while count & 1 == 0 {
            let left = self
                .open
                .pop()
                .expect("an even count of groups has a subtree to merge");
            pairs.extend_from_slice(&left);
            pairs.extend_from_slice(&hash);
            hash = merge_subtrees_non_root(&left, &hash, Mode::Hash);
            count >>= 1;
        }
        self.open.push(hash);
    }
}

/// The groups of a blob of `size` bytes.
pub fn groups(size: u64) -> u64 {
    tree(size).blocks()
}

/// The chunk ranges of `bao-tree` for the groups from `start` to `end`, a
/// range of bytes of a blob of `size`: from the group that holds `start` to
/// the group that holds the byte before `end`.
pub fn chunk_ranges(size: u64, start: u64, end: u64) -> ChunkRanges {
    let first = ChunkNum(start / GROUP * (GROUP / 1024));
    if end >= size {
        return ChunkRanges::from(first..);
    }
    let last = ChunkNum(end.div_ceil(GROUP) * (GROUP / 1024));
    ChunkRanges::from(first..last)
}

/// The end of the range from `offset`, for `length` bytes or to the end of a
/// blob of `size` bytes, if it runs from the start of a group to the end of a
/// group or of the blob, as a verified range does.
pub fn group_range(size: u64, offset: u64, length: Option<u64>) -> Result<u64, String> {
    let end = length.map_or(size, |length| offset.saturating_add(length).min(size));
    if !offset.is_multiple_of(GROUP) || (!end.is_multiple_of(GROUP) && end != size) || end < offset
    {
        return Err(format!(
            "a verified range runs from the start of a group to the end of a group or of the \
             blob, and {offset}..{end} does not"
        ));
    }
    Ok(end)
}

/// The items of the encoding of the groups from `start` to `end` of a blob
/// of `size`, in pre order, or none for a range with no group.
fn items(size: u64, start: u64, end: u64) -> Option<ResponseIter> {
    let ranges = chunk_ranges(size, start, end);
    (start < end && !ranges.is_empty()).then(|| ResponseIter::new(tree(size), ranges))
}

/// The byte offset in the post-order outboard of a blob of `tree` of the
/// pair of hashes of `node`, if the outboard keeps one.
pub fn pair_offset(tree: &BaoTree, node: TreeNode) -> Option<u64> {
    tree.post_order_offset(node)
        .map(|offset| offset.value() * PAIR as u64)
}

/// The hash of a group of `bytes` at the chunk `start_chunk`, as BLAKE3
/// hashes a subtree: the root hash of the blob, if the group is the whole of
/// it.
fn group_hash(start_chunk: u64, bytes: &[u8], is_root: bool) -> blake3::Hash {
    if is_root {
        return blake3::hash(bytes);
    }
    let mut hasher = blake3::Hasher::new();
    hasher.set_input_offset(start_chunk * 1024);
    hasher.update(bytes);
    blake3::Hash::from(hasher.finalize_non_root())
}

/// The hash of a parent of the tree from the hashes of its children.
fn parent_hash(left: &blake3::Hash, right: &blake3::Hash, is_root: bool) -> blake3::Hash {
    let (left, right) = (*left.as_bytes(), *right.as_bytes());
    if is_root {
        merge_subtrees_root(&left, &right, Mode::Hash)
    } else {
        blake3::Hash::from(merge_subtrees_non_root(&left, &right, Mode::Hash))
    }
}

/// One verified item of the encoding of a range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    /// The hashes of the two children of `node`, which a parent of the tree
    /// proved: the 64 bytes that the outboard keeps for it.
    Parent {
        /// The node of the tree.
        node: TreeNode,
        /// The hash of the left child, and then of the right.
        pair: [u8; PAIR],
    },
    /// A group of the blob, which its parent proved.
    Group {
        /// The offset of the group in the blob.
        offset: u64,
        /// The bytes of the group.
        bytes: Vec<u8>,
    },
}

/// An item of an encoding that does not verify against the hash of its blob.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unverified {
    /// A pair of hashes that does not hash to the hash of its parent.
    #[error("the hashes of the node {0:?} of the tree do not verify")]
    Parent(TreeNode),
    /// A group that does not hash to the hash that its parent holds for it.
    #[error("the group at the byte {0} does not verify")]
    Group(u64),
}

/// Verifies the Bao encoding of a range of a blob as it arrives, item by
/// item, against the hash of the blob alone. [`Decoder::push`] takes the
/// bytes as they come, in any cut, and [`Decoder::next_item`] gives each item once
/// its bytes are all there and it verifies. An item that does not verify ends
/// the decoder: it gives nothing after it.
pub struct Decoder {
    /// The items of the range to come after `next`: none for an empty
    /// range, which `bao-tree` iterates over no tree.
    iter: Option<ResponseIter>,
    /// The item that the decoder waits for the bytes of.
    next: Option<BaoChunk>,
    /// The hashes that the items to come must verify against, the next on
    /// top.
    stack: Vec<blake3::Hash>,
    /// The bytes that came and that no item took yet, from `taken` on.
    buffer: Vec<u8>,
    taken: usize,
    /// Whether an item did not verify.
    failed: bool,
}

impl Decoder {
    /// A decoder of the groups from `start` to `end` of the blob `hash` of
    /// `size` bytes: from the group that holds `start` to the group that holds
    /// the byte before `end`.
    pub fn new(hash: &Blake3Hash, size: u64, start: u64, end: u64) -> Self {
        let mut iter = items(size, start, end);
        let next = iter.as_mut().and_then(Iterator::next);
        Self {
            iter,
            next,
            stack: vec![blake3::Hash::from(*hash.as_bytes())],
            buffer: Vec::new(),
            taken: 0,
            failed: false,
        }
    }

    /// Takes the next bytes of the encoding.
    pub fn push(&mut self, bytes: &[u8]) {
        if self.taken > 0 && self.taken * 2 >= self.buffer.len() {
            self.buffer.drain(..self.taken);
            self.taken = 0;
        }
        self.buffer.extend_from_slice(bytes);
    }

    /// Whether the decoder took each item of the range.
    pub fn is_done(&self) -> bool {
        self.next.is_none() && !self.failed
    }

    /// The bytes that came after the end of the encoding, if any.
    pub fn excess(&self) -> usize {
        if self.next.is_none() {
            self.buffer.len() - self.taken
        } else {
            0
        }
    }

    /// The next item, once its bytes are all there and it verifies; none
    /// while the decoder waits for more bytes, and after the last item.
    pub fn next_item(&mut self) -> Option<Result<Item, Unverified>> {
        if self.failed {
            return None;
        }
        let chunk = self.next?;
        let available = self.buffer.len() - self.taken;
        let wanted = match chunk {
            BaoChunk::Parent { .. } => PAIR,
            BaoChunk::Leaf { size, .. } => size,
        };
        if available < wanted {
            return None;
        }
        let bytes = &self.buffer[self.taken..self.taken + wanted];
        let expected = self
            .stack
            .pop()
            .expect("each item has a hash to verify against");
        let item = match chunk {
            BaoChunk::Parent {
                node,
                is_root,
                left,
                right,
                ..
            } => {
                let pair: [u8; PAIR] = bytes.try_into().expect("a pair is 64 bytes");
                let (l, r) = split(&pair);
                if parent_hash(&l, &r, is_root) != expected {
                    self.failed = true;
                    return Some(Err(Unverified::Parent(node)));
                }
                if right {
                    self.stack.push(r);
                }
                if left {
                    self.stack.push(l);
                }
                Item::Parent { node, pair }
            }
            BaoChunk::Leaf {
                start_chunk,
                is_root,
                ..
            } => {
                let offset = start_chunk.to_bytes();
                if group_hash(start_chunk.0, bytes, is_root) != expected {
                    self.failed = true;
                    return Some(Err(Unverified::Group(offset)));
                }
                Item::Group {
                    offset,
                    bytes: bytes.to_vec(),
                }
            }
        };
        self.taken += wanted;
        self.next = self.iter.as_mut().and_then(Iterator::next);
        Some(Ok(item))
    }
}

/// The hashes of the left and of the right child in `pair`.
fn split(pair: &[u8; PAIR]) -> (blake3::Hash, blake3::Hash) {
    let (left, right) = pair.split_at(32);
    let hash = |half: &[u8]| blake3::Hash::from(<[u8; 32]>::try_from(half).expect("32 bytes"));
    (hash(left), hash(right))
}

/// What an [`Encoder`] needs next from its store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    /// The [`PAIR`] bytes of the outboard at `offset`.
    Pair {
        /// The offset in the outboard.
        offset: u64,
    },
    /// `length` bytes of the blob at `offset`: one group.
    Group {
        /// The offset in the blob.
        offset: u64,
        /// The bytes of the group.
        length: usize,
    },
}

/// The bytes of the header of a verified range: the size of the blob, and
/// the end of the part of the range that the answer covers.
pub const HEADER: usize = 16;

/// The header of a verified range of a blob of `size` bytes that covers the
/// range up to `covered`.
pub fn header(size: u64, covered: u64) -> [u8; HEADER] {
    let mut header = [0; HEADER];
    header[..8].copy_from_slice(&size.to_le_bytes());
    header[8..].copy_from_slice(&covered.to_le_bytes());
    header
}

/// The size and the end of the covered part of the header `bytes`.
pub fn read_header(bytes: &[u8; HEADER]) -> (u64, u64) {
    let (size, covered) = bytes.split_at(8);
    let number = |bytes: &[u8]| u64::from_le_bytes(bytes.try_into().expect("8 bytes"));
    (number(size), number(covered))
}

/// The groups from the group that holds `start` to `end`, as a verified
/// range covers them: from the start of that group, to the end of the group
/// that holds the byte before `end`, or to the end of the blob of `size`.
pub fn groups_of(size: u64, start: u64, end: u64) -> (u64, u64) {
    let first = (start / GROUP * GROUP).min(size);
    let last = end
        .min(size)
        .div_ceil(GROUP)
        .saturating_mul(GROUP)
        .min(size);
    (first, last.max(first))
}

/// Encodes a verified range of a blob, item by item, in the order of
/// `bao-tree`: [`Encoder::need`] says which bytes of the outboard or of the
/// blob the store reads next, and [`Encoder::take`] takes them and gives
/// their encoding. The encoder verifies each item as it encodes it, so a
/// store whose bytes or hashes went bad ends the range before the bad item,
/// and sends nothing that would not verify.
pub struct Encoder {
    tree: BaoTree,
    iter: Option<ResponseIter>,
    next: Option<BaoChunk>,
    check: Decoder,
}

impl Encoder {
    /// An encoder of the groups from `start` to `end` of the blob `hash` of
    /// `size` bytes: each a group boundary, or `end` the end of the blob.
    pub fn new(hash: &Blake3Hash, size: u64, start: u64, end: u64) -> Self {
        let tree = tree(size);
        let mut iter = items(size, start, end);
        let next = iter.as_mut().and_then(Iterator::next);
        Self {
            tree,
            iter,
            next,
            check: Decoder::new(hash, size, start, end),
        }
    }

    /// What the encoder needs next, or none once it encoded the range.
    pub fn need(&self) -> Option<Need> {
        match self.next? {
            BaoChunk::Parent { node, .. } => {
                pair_offset(&self.tree, node).map(|offset| Need::Pair { offset })
            }
            BaoChunk::Leaf {
                start_chunk, size, ..
            } => Some(Need::Group {
                offset: start_chunk.to_bytes(),
                length: size,
            }),
        }
    }

    /// Takes `bytes`, the bytes that [`Encoder::need`] named, and gives their
    /// encoding, or why they do not verify. Bytes of another length than the
    /// item, as a store that holds less than it named gives, do not verify.
    pub fn take(&mut self, bytes: &[u8]) -> Result<Vec<u8>, Unverified> {
        let unverified = match self.next {
            Some(BaoChunk::Parent { node, .. }) => Unverified::Parent(node),
            Some(BaoChunk::Leaf { start_chunk, .. }) => Unverified::Group(start_chunk.to_bytes()),
            None => Unverified::Group(self.tree.size()),
        };
        let wanted = match self.next {
            Some(chunk) => chunk.size(),
            None => 0,
        };
        if bytes.len() != wanted {
            self.next = None;
            return Err(unverified);
        }
        self.check.push(bytes);
        match self.check.next_item() {
            Some(Ok(_)) => {
                self.next = self.iter.as_mut().and_then(Iterator::next);
                Ok(bytes.to_vec())
            }
            Some(Err(error)) => {
                self.next = None;
                Err(error)
            }
            None => {
                self.next = None;
                Err(unverified)
            }
        }
    }
}

/// The groups of a blob that a store holds, as ranges of the numbers of
/// the groups, each from its first group to the group after its last, in
/// order, with no two that touch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Groups(Vec<(u64, u64)>);

impl Groups {
    /// No groups.
    pub fn new() -> Self {
        Self::default()
    }

    /// The ranges of the groups, from the first.
    pub fn ranges(&self) -> &[(u64, u64)] {
        &self.0
    }

    /// Whether the store holds no group.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether these are each of the `count` groups of a blob.
    pub fn covers(&self, count: u64) -> bool {
        count == 0 || self.0.as_slice() == [(0, count)]
    }

    /// Whether the store holds the group `group`.
    pub fn contains(&self, group: u64) -> bool {
        self.0
            .iter()
            .any(|(start, end)| *start <= group && group < *end)
    }

    /// Adds the group `group`.
    pub fn insert(&mut self, group: u64) {
        self.add(group, group + 1);
    }

    /// Adds each group of `other`.
    pub fn extend(&mut self, other: &Groups) {
        for (start, end) in &other.0 {
            self.add(*start, *end);
        }
    }

    /// Adds the groups from `start` to the group before `end`.
    fn add(&mut self, mut start: u64, mut end: u64) {
        let mut kept = Vec::with_capacity(self.0.len() + 1);
        for &(from, to) in &self.0 {
            if to < start || end < from {
                kept.push((from, to));
            } else {
                start = start.min(from);
                end = end.max(to);
            }
        }
        kept.push((start, end));
        kept.sort_unstable();
        self.0 = kept;
    }

    /// The bytes of the pairs of hashes that verify these groups of a blob
    /// of `size` bytes: of each parent on the path from the root to one of
    /// them, once. A partial blob keeps these in its outboard.
    pub fn pairs(&self, size: u64) -> u64 {
        let mut ranges = ChunkRanges::empty();
        for (start, end) in self.bytes(size) {
            ranges |= chunk_ranges(size, start, end);
        }
        if ranges.is_empty() {
            return 0;
        }
        let parents = ResponseIter::new(tree(size), ranges)
            .filter(|chunk| matches!(chunk, BaoChunk::Parent { .. }))
            .count();
        parents as u64 * PAIR as u64
    }

    /// The ranges of bytes of these groups, of a blob of `size` bytes.
    pub fn bytes(&self, size: u64) -> Vec<(u64, u64)> {
        self.0
            .iter()
            .map(|(start, end)| (start * GROUP, (end * GROUP).min(size)))
            .collect()
    }

    /// The record of these groups of a blob of `size` bytes, as a store keeps
    /// it: the size, and then the first group and the group after the last
    /// of each range, each as 8 bytes, little endian.
    pub fn encode(&self, size: u64) -> Vec<u8> {
        let mut record = Vec::with_capacity(8 + 16 * self.0.len());
        record.extend_from_slice(&size.to_le_bytes());
        for (start, end) in &self.0 {
            record.extend_from_slice(&start.to_le_bytes());
            record.extend_from_slice(&end.to_le_bytes());
        }
        record
    }

    /// The size and the groups of `record`, as [`Groups::encode`] writes
    /// them, if it is one.
    pub fn decode(record: &[u8]) -> Option<(u64, Groups)> {
        let (size, rest) = record.split_first_chunk::<8>()?;
        let (pairs, tail) = rest.as_chunks::<16>();
        if !tail.is_empty() {
            return None;
        }
        let mut groups = Groups::new();
        for pair in pairs {
            let start = u64::from_le_bytes(pair[..8].try_into().ok()?);
            let end = u64::from_le_bytes(pair[8..].try_into().ok()?);
            if start >= end {
                return None;
            }
            groups.add(start, end);
        }
        Some((u64::from_le_bytes(*size), groups))
    }
}

/// The hash and the outboard of `bytes`, whole.
pub fn outboard(bytes: &[u8]) -> (Blake3Hash, Vec<u8>) {
    let mut builder = OutboardBuilder::new();
    let mut outboard = builder.update(bytes);
    let (hash, rest) = builder.finish();
    outboard.extend(rest);
    (hash, outboard)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bao_tree::io::outboard::PostOrderMemOutboard;

    /// The bytes of a blob of `size`, which differ from group to group.
    fn blob(size: usize) -> Vec<u8> {
        (0..size)
            .map(|i| (i % 251) as u8 ^ (i / 4093) as u8)
            .collect()
    }

    /// The sizes around the edges of the groups, and of the subtrees.
    const SIZES: &[usize] = &[
        0,
        1,
        1024,
        16 * 1024 - 1,
        16 * 1024,
        16 * 1024 + 1,
        32 * 1024,
        32 * 1024 + 1,
        48 * 1024 + 5,
        64 * 1024,
        100 * 1024,
        128 * 1024 + 1,
        1024 * 1024 + 123,
        3 * 1024 * 1024,
    ];

    #[dialog_common::test]
    fn it_builds_the_outboard_of_bao_tree_in_one_pass() {
        for &size in SIZES {
            let bytes = blob(size);
            let expected = PostOrderMemOutboard::create(&bytes, BLOCK_SIZE);
            let (hash, built) = outboard(&bytes);
            assert_eq!(
                hash.as_bytes(),
                expected.root.as_bytes(),
                "the hash of {size}"
            );
            assert_eq!(built, expected.data, "the outboard of {size}");
        }
    }

    #[dialog_common::test]
    fn it_builds_the_same_outboard_from_any_cut_of_the_bytes() {
        let bytes = blob(200 * 1024 + 17);
        let (_, whole) = outboard(&bytes);
        for cut in [1, 7, 1000, 16 * 1024, 16 * 1024 + 1, 65_536] {
            let mut builder = OutboardBuilder::new();
            let mut built = Vec::new();
            for piece in bytes.chunks(cut) {
                built.extend(builder.update(piece));
            }
            let (hash, rest) = builder.finish();
            built.extend(rest);
            assert_eq!(
                hash.as_bytes(),
                blake3::hash(&bytes).as_bytes(),
                "cut {cut}"
            );
            assert_eq!(built, whole, "cut {cut}");
        }
    }

    #[dialog_common::test]
    fn it_has_the_hash_of_the_blob_as_the_root() {
        for &size in SIZES {
            let bytes = blob(size);
            let (hash, _) = outboard(&bytes);
            assert_eq!(hash.as_bytes(), blake3::hash(&bytes).as_bytes(), "{size}");
        }
    }

    /// The bao-tree encoding of the groups from `start` to `end` of `bytes`.
    fn encoding(bytes: &[u8], start: u64, end: u64) -> Vec<u8> {
        let outboard = PostOrderMemOutboard::create(bytes, BLOCK_SIZE);
        let ranges = chunk_ranges(bytes.len() as u64, start, end);
        let mut encoded = Vec::new();
        bao_tree::io::sync::encode_ranges_validated(bytes, &outboard, &ranges, &mut encoded)
            .expect("the encoding of a range of bytes in memory");
        encoded
    }

    /// The items of `encoded`, pushed `cut` bytes at a time.
    fn decode(
        bytes: &[u8],
        start: u64,
        end: u64,
        encoded: &[u8],
        cut: usize,
    ) -> Vec<Result<Item, Unverified>> {
        let hash = Blake3Hash::from(*blake3::hash(bytes).as_bytes());
        let mut decoder = Decoder::new(&hash, bytes.len() as u64, start, end);
        let mut items = Vec::new();
        for piece in encoded.chunks(cut) {
            decoder.push(piece);
            while let Some(item) = decoder.next_item() {
                items.push(item);
            }
        }
        items
    }

    #[dialog_common::test]
    fn it_decodes_the_encoding_of_bao_tree_group_by_group() {
        let bytes = blob(300 * 1024 + 9);
        let size = bytes.len() as u64;
        for (start, end) in [
            (0, size),
            (16 * 1024, 64 * 1024),
            (100 * 1024 + 5, 100 * 1024 + 6),
            (290 * 1024, size),
        ] {
            let encoded = encoding(&bytes, start, end);
            for cut in [1, 63, 64, 1000, 16 * 1024 + 7, encoded.len()] {
                let items = decode(&bytes, start, end, &encoded, cut);
                let groups: Vec<(u64, Vec<u8>)> = items
                    .into_iter()
                    .filter_map(|item| match item.unwrap() {
                        Item::Group { offset, bytes } => Some((offset, bytes)),
                        Item::Parent { .. } => None,
                    })
                    .collect();
                let first = start / GROUP * GROUP;
                let last = (end.div_ceil(GROUP) * GROUP).min(size);
                assert_eq!(groups.first().map(|(offset, _)| *offset), Some(first));
                let joined: Vec<u8> = groups.into_iter().flat_map(|(_, bytes)| bytes).collect();
                assert_eq!(
                    joined,
                    bytes[first as usize..last as usize],
                    "{start}..{end} by {cut}"
                );
            }
        }
    }

    #[dialog_common::test]
    fn it_fails_a_changed_group_and_no_group_before_it() {
        let bytes = blob(200 * 1024);
        let size = bytes.len();
        let mut encoded = encoding(&bytes, 0, size as u64);
        // The place of the tenth group in the encoding: the bytes of each
        // item before it, in pre order.
        let tenth = ChunkNum(9 * GROUP / 1024);
        let at: usize = ResponseIter::new(tree(size as u64), ChunkRanges::all())
            .take_while(|chunk| !matches!(chunk, BaoChunk::Leaf { start_chunk, .. } if *start_chunk == tenth))
            .map(|chunk| chunk.size())
            .sum();
        encoded[at + 10] ^= 1;

        let items = decode(&bytes, 0, size as u64, &encoded, 4096);

        let groups = items
            .iter()
            .filter(|item| matches!(item, Ok(Item::Group { .. })))
            .count();
        assert_eq!(groups, 9, "{items:?}");
        assert_eq!(items.last(), Some(&Err(Unverified::Group(9 * GROUP))));
    }

    #[dialog_common::test]
    fn it_decodes_a_blob_of_each_size_whole() {
        for &size in SIZES {
            let bytes = blob(size);
            let size = size as u64;
            let encoded = encoding(&bytes, 0, size);
            let groups: Vec<u8> = decode(&bytes, 0, size, &encoded, 777)
                .into_iter()
                .filter_map(|item| match item.unwrap() {
                    Item::Group { bytes, .. } => Some(bytes),
                    Item::Parent { .. } => None,
                })
                .flatten()
                .collect();
            assert_eq!(groups, bytes, "{size}");
        }
    }

    #[dialog_common::test]
    fn it_fails_a_forged_pair_of_hashes() {
        for size in [2 * GROUP as usize, 5 * GROUP as usize + 3] {
            let bytes = blob(size);
            let mut encoded = encoding(&bytes, 0, size as u64);
            // The encoding of a whole blob starts with the pair of the root.
            encoded[5] ^= 1;

            let items = decode(&bytes, 0, size as u64, &encoded, 4096);

            assert!(
                matches!(items.as_slice(), [Err(Unverified::Parent(_))]),
                "{size}: {items:?}"
            );
        }
    }

    #[dialog_common::test]
    fn it_fails_a_changed_blob_of_one_group() {
        let bytes = blob(GROUP as usize);
        let mut encoded = encoding(&bytes, 0, GROUP);
        encoded[100] ^= 1;

        let items = decode(&bytes, 0, GROUP, &encoded, 4096);

        assert_eq!(items, [Err(Unverified::Group(0))]);
    }

    #[dialog_common::test]
    fn it_keeps_the_pair_of_each_parent_where_the_outboard_keeps_it() {
        let bytes = blob(500 * 1024 + 3);
        let outboard = PostOrderMemOutboard::create(&bytes, BLOCK_SIZE);
        let tree = tree(bytes.len() as u64);
        let encoded = encoding(&bytes, 0, bytes.len() as u64);

        for item in decode(&bytes, 0, bytes.len() as u64, &encoded, 999) {
            if let Ok(Item::Parent { node, pair }) = item {
                let offset = pair_offset(&tree, node).expect("the outboard keeps each pair");
                let offset = usize::try_from(offset).unwrap();
                assert_eq!(&outboard.data[offset..offset + PAIR], &pair[..]);
            }
        }
    }

    #[dialog_common::test]
    fn it_merges_the_groups_that_a_store_holds() {
        let mut groups = Groups::new();
        for group in [5, 3, 4, 9, 0, 1] {
            groups.insert(group);
        }
        assert_eq!(groups.ranges(), [(0, 2), (3, 6), (9, 10)]);
        groups.insert(2);
        assert_eq!(groups.ranges(), [(0, 6), (9, 10)]);
        let mut rest = Groups::new();
        for group in [6, 7, 8] {
            rest.insert(group);
        }
        groups.extend(&rest);
        assert!(groups.covers(10));
        let size = 9 * GROUP + 1;
        assert_eq!(Groups::decode(&groups.encode(size)), Some((size, groups)));
    }

    #[dialog_common::test]
    fn it_keeps_64_bytes_for_each_group_after_the_first() {
        for &size in SIZES {
            let (_, built) = outboard(&blob(size));
            let groups = (size as u64).div_ceil(GROUP).max(1);
            assert_eq!(built.len() as u64, 64 * (groups - 1), "{size}");
            assert_eq!(built.len() as u64, outboard_size(size as u64), "{size}");
        }
    }
}

/// What each store of partial blobs must do, as tests that each provider
/// runs over itself. See `ImportRange` and `Held`.
#[cfg(test)]
pub(crate) mod conformance {
    use super::*;
    use bao_tree::io::outboard::PostOrderMemOutboard;
    use dialog_capability::{Provider, Subject};
    use dialog_effects::blob::{BlobError, BlobReader, ByteRange, Held, ImportRange, Read, Size};
    use dialog_effects::prelude::*;

    /// A blob of `size` bytes whose groups differ.
    pub fn blob(size: usize) -> Vec<u8> {
        (0..size)
            .map(|i| (i % 251) as u8 ^ (i / 16_411) as u8)
            .collect()
    }

    /// The bao-tree encoding of the groups from `start` to `end` of `bytes`.
    pub fn encoding(bytes: &[u8], start: u64, end: u64) -> Vec<u8> {
        let outboard = PostOrderMemOutboard::create(bytes, BLOCK_SIZE);
        let ranges = chunk_ranges(bytes.len() as u64, start, end);
        let mut encoded = Vec::new();
        bao_tree::io::sync::encode_ranges_validated(bytes, &outboard, &ranges, &mut encoded)
            .expect("the encoding of a range of bytes in memory");
        encoded
    }

    async fn drain(mut reader: BlobReader) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            out.extend(chunk);
        }
        out
    }

    /// Imports the encoding of the groups from `start` to `end` of `bytes`
    /// into `store`, in pieces of 5000 bytes, and gives what the sink said.
    pub async fn import<P>(
        store: &P,
        subject: &Subject,
        bytes: &[u8],
        start: u64,
        end: u64,
        encoded: &[u8],
    ) -> Result<(), BlobError>
    where
        P: Provider<ImportRange> + dialog_common::ConditionalSync,
    {
        let digest = Blake3Hash::from(*blake3::hash(bytes).as_bytes());
        import_at(
            store,
            subject,
            digest,
            bytes.len() as u64,
            start,
            end,
            encoded,
        )
        .await
    }

    /// Imports `encoded`, the encoding of the groups from `start` to `end`
    /// of the blob `digest`, as a blob of `size` bytes, into `store`, in
    /// pieces of 5000 bytes, and gives what the sink said.
    pub async fn import_at<P>(
        store: &P,
        subject: &Subject,
        digest: Blake3Hash,
        size: u64,
        start: u64,
        end: u64,
        encoded: &[u8],
    ) -> Result<(), BlobError>
    where
        P: Provider<ImportRange> + dialog_common::ConditionalSync,
    {
        let range = ByteRange {
            offset: start,
            length: Some(end - start),
        };
        let mut sink = subject
            .clone()
            .writer()
            .archive()
            .blob()
            .import_range(digest, size, range)
            .perform(store)
            .await?;
        let mut written = Ok(());
        for piece in encoded.chunks(5000) {
            written = sink.write_all(piece).await;
            if written.is_err() {
                break;
            }
        }
        sink.finish().await?;
        written
    }

    /// The bytes that a read of `bytes` from `offset` gives, or none.
    async fn read<P>(store: &P, subject: &Subject, bytes: &[u8], offset: u64) -> Option<Vec<u8>>
    where
        P: Provider<Read> + dialog_common::ConditionalSync,
    {
        let digest = Blake3Hash::from(*blake3::hash(bytes).as_bytes());
        match subject
            .clone()
            .reader()
            .archive()
            .blob()
            .invoke(Read::range(digest, offset, None))
            .perform(store)
            .await
        {
            Ok(reader) => Some(drain(reader).await),
            Err(BlobError::NotFound(_)) => None,
            Err(error) => panic!("a read fails: {error}"),
        }
    }

    /// A store that keeps the groups of the middle of a blob holds no group
    /// before them, and a read of the store says where the held part ends.
    pub async fn it_keeps_the_groups_of_the_middle_alone<P>(store: &P, subject: &Subject)
    where
        P: Provider<ImportRange>
            + Provider<Held>
            + Provider<Read>
            + Provider<Size>
            + dialog_common::ConditionalSync,
    {
        let bytes = blob(10 * GROUP as usize + 99);
        let digest = Blake3Hash::from(*blake3::hash(&bytes).as_bytes());
        let (start, end) = (4 * GROUP, 6 * GROUP);

        import(
            store,
            subject,
            &bytes,
            start,
            end,
            &encoding(&bytes, start, end),
        )
        .await
        .unwrap();

        let held = subject
            .clone()
            .reader()
            .archive()
            .blob()
            .held(digest.clone())
            .perform(store)
            .await
            .unwrap()
            .expect("the store holds a part of the blob");
        assert_eq!(held.ranges, [(start, end)]);
        assert_eq!(held.held_from(start + 7), end);
        assert_eq!(held.held_from(0), 0);
        assert_eq!(read(store, subject, &bytes, 0).await, None);
        assert_eq!(
            read(store, subject, &bytes, start + 7).await,
            Some(bytes[start as usize + 7..end as usize].to_vec())
        );
        let size = subject
            .clone()
            .reader()
            .archive()
            .blob()
            .size(digest)
            .perform(store)
            .await
            .unwrap();
        assert_eq!(size, None, "a store answers no size of a blob in part");
    }

    /// A store that keeps each group of a blob, in any order, answers its
    /// size and reads it as a whole blob.
    pub async fn it_holds_a_blob_whole_once_it_keeps_each_group<P>(store: &P, subject: &Subject)
    where
        P: Provider<ImportRange>
            + Provider<Held>
            + Provider<Read>
            + Provider<Size>
            + dialog_common::ConditionalSync,
    {
        let bytes = blob(7 * GROUP as usize + 5);
        let size = bytes.len() as u64;
        let digest = Blake3Hash::from(*blake3::hash(&bytes).as_bytes());

        for (start, end) in [(5 * GROUP, size), (0, 2 * GROUP), (2 * GROUP, 5 * GROUP)] {
            import(
                store,
                subject,
                &bytes,
                start,
                end,
                &encoding(&bytes, start, end),
            )
            .await
            .unwrap();
        }

        let blob = subject.clone().reader().archive().blob();
        assert_eq!(
            blob.clone()
                .size(digest.clone())
                .perform(store)
                .await
                .unwrap(),
            Some(size)
        );
        assert_eq!(read(store, subject, &bytes, 0).await, Some(bytes.clone()));
        let held = blob.held(digest).perform(store).await.unwrap().unwrap();
        assert!(held.is_complete(), "{held:?}");
    }

    /// A group whose bytes do not verify is refused, and the store keeps
    /// nothing of it, and the groups before it.
    pub async fn it_refuses_a_group_that_does_not_verify<P>(store: &P, subject: &Subject)
    where
        P: Provider<ImportRange> + Provider<Held> + dialog_common::ConditionalSync,
    {
        let bytes = blob(12 * GROUP as usize);
        let size = bytes.len() as u64;
        let digest = Blake3Hash::from(*blake3::hash(&bytes).as_bytes());
        let mut encoded = encoding(&bytes, 0, size);
        let tenth = ChunkNum(9 * GROUP / 1024);
        let at: usize = ResponseIter::new(tree(size), ChunkRanges::all())
            .take_while(
                |chunk| !matches!(chunk, BaoChunk::Leaf { start_chunk, .. } if *start_chunk == tenth),
            )
            .map(|chunk| chunk.size())
            .sum();
        encoded[at + 10] ^= 1;

        let imported = import(store, subject, &bytes, 0, size, &encoded).await;

        assert!(
            matches!(imported, Err(BlobError::Unverified { .. })),
            "{imported:?}"
        );
        let held = subject
            .clone()
            .reader()
            .archive()
            .blob()
            .held(digest)
            .perform(store)
            .await
            .unwrap()
            .expect("the store keeps the groups before the one that does not verify");
        assert_eq!(held.ranges, [(0, 9 * GROUP)]);
    }

    /// Two imports at once of two ranges of one blob each keep their groups:
    /// the record of the partial blob holds both.
    pub async fn it_keeps_both_of_two_imports_at_once<P>(store: &P, subject: &Subject)
    where
        P: Provider<ImportRange> + Provider<Held> + dialog_common::ConditionalSync,
    {
        let bytes = blob(400 * GROUP as usize);
        let digest = Blake3Hash::from(*blake3::hash(&bytes).as_bytes());
        let (first, second) = ((0, 150 * GROUP), (200 * GROUP, 380 * GROUP));
        let (one, two) = (
            encoding(&bytes, first.0, first.1),
            encoding(&bytes, second.0, second.1),
        );

        let (left, right) = futures_util::join!(
            import(store, subject, &bytes, first.0, first.1, &one),
            import(store, subject, &bytes, second.0, second.1, &two),
        );

        left.unwrap();
        right.unwrap();
        let held = subject
            .clone()
            .reader()
            .archive()
            .blob()
            .held(digest)
            .perform(store)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(held.ranges, [first, second]);
    }

    /// The size and the ranges of the part of `digest` that `store` holds.
    async fn holding<P>(
        store: &P,
        subject: &Subject,
        digest: &Blake3Hash,
    ) -> Option<(u64, Vec<(u64, u64)>)>
    where
        P: Provider<Held> + dialog_common::ConditionalSync,
    {
        subject
            .clone()
            .reader()
            .archive()
            .blob()
            .held(digest.clone())
            .perform(store)
            .await
            .unwrap()
            .map(|held| (held.size, held.ranges))
    }

    /// A sink at a size whose tree has another depth, whose first group does
    /// not verify, and a sink of another size that ends with nothing, bind no
    /// size: an import at the size of the blob then keeps its groups.
    pub async fn it_imports_a_blob_after_a_sink_of_another_size_kept_nothing<P>(
        store: &P,
        subject: &Subject,
    ) where
        P: Provider<ImportRange> + Provider<Held> + dialog_common::ConditionalSync,
    {
        let bytes = blob(6 * GROUP as usize);
        let size = bytes.len() as u64;
        let digest = Blake3Hash::from(*blake3::hash(&bytes).as_bytes());
        let encoded = encoding(&bytes, 0, 2 * GROUP);
        let range = ByteRange {
            offset: 0,
            length: Some(2 * GROUP),
        };
        let unused = subject
            .clone()
            .writer()
            .archive()
            .blob()
            .import_range(digest.clone(), size + 7, range)
            .perform(store)
            .await
            .unwrap();
        unused.finish().await.unwrap();

        let refused = import_at(
            store,
            subject,
            digest.clone(),
            size / 2,
            0,
            2 * GROUP,
            &encoded,
        )
        .await;
        let none = holding(store, subject, &digest).await;
        import(store, subject, &bytes, 0, 2 * GROUP, &encoded)
            .await
            .unwrap();

        assert!(
            matches!(refused, Err(BlobError::Unverified { .. })),
            "{refused:?}"
        );
        assert_eq!(none, None, "a sink that kept nothing binds no size");
        assert_eq!(
            holding(store, subject, &digest).await,
            Some((size, vec![(0, 2 * GROUP)]))
        );
    }

    /// Bao proves a size only with the last group, so the groups of the
    /// start of a blob verify at two sizes whose trees have one shape. The
    /// store holds the part of the size whose group verified last, so a
    /// wrong size of an asset never stops the fetch of the blob at its
    /// size for good.
    pub async fn it_holds_the_part_of_the_size_whose_group_verified_last<P>(
        store: &P,
        subject: &Subject,
    ) where
        P: Provider<ImportRange> + Provider<Held> + dialog_common::ConditionalSync,
    {
        let bytes = blob(6 * GROUP as usize - 100);
        let size = bytes.len() as u64;
        let digest = Blake3Hash::from(*blake3::hash(&bytes).as_bytes());
        let start = encoding(&bytes, 0, 2 * GROUP);

        import(store, subject, &bytes, 0, 2 * GROUP, &start)
            .await
            .unwrap();
        let first = holding(store, subject, &digest).await;
        import_at(
            store,
            subject,
            digest.clone(),
            size + 1,
            0,
            2 * GROUP,
            &start,
        )
        .await
        .unwrap();
        let wrong = holding(store, subject, &digest).await;
        import(
            store,
            subject,
            &bytes,
            2 * GROUP,
            4 * GROUP,
            &encoding(&bytes, 2 * GROUP, 4 * GROUP),
        )
        .await
        .unwrap();

        assert_eq!(first, Some((size, vec![(0, 2 * GROUP)])));
        assert_eq!(wrong, Some((size + 1, vec![(0, 2 * GROUP)])));
        assert_eq!(
            holding(store, subject, &digest).await,
            Some((size, vec![(2 * GROUP, 4 * GROUP)]))
        );
    }

    /// A store that holds a blob whole still verifies an import of a range
    /// of it: a part that does not verify fails the write.
    pub async fn it_verifies_a_range_of_a_blob_that_it_holds_whole<P>(store: &P, subject: &Subject)
    where
        P: Provider<ImportRange> + dialog_common::ConditionalSync,
    {
        let bytes = blob(3 * GROUP as usize);
        let size = bytes.len() as u64;
        import(store, subject, &bytes, 0, size, &encoding(&bytes, 0, size))
            .await
            .unwrap();
        let mut encoded = encoding(&bytes, 0, size);
        let last = encoded.len() - 1;
        encoded[last] ^= 1;

        let imported = import(store, subject, &bytes, 0, size, &encoded).await;

        assert!(
            matches!(imported, Err(BlobError::Unverified { .. })),
            "{imported:?}"
        );
    }

    /// The usage of a store counts each whole blob and its outboard, and the
    /// groups that a partial blob holds, and not the holes between them.
    pub async fn it_counts_the_bytes_that_it_keeps<P>(store: &P, subject: &Subject)
    where
        P: Provider<ImportRange>
            + Provider<dialog_effects::blob::Usage>
            + Provider<dialog_effects::blob::Write>
            + dialog_common::ConditionalSync,
    {
        let usage = async || {
            subject
                .clone()
                .reader()
                .archive()
                .blob()
                .usage()
                .perform(store)
                .await
                .unwrap()
        };
        assert_eq!(usage().await, 0);
        let whole = blob(5 * GROUP as usize + 1);
        let mut sink = subject
            .clone()
            .writer()
            .archive()
            .blob()
            .write()
            .perform(store)
            .await
            .unwrap();
        sink.write_all(&whole).await.unwrap();
        sink.finish().await.unwrap();
        let after_whole = usage().await;
        let part = blob(100 * GROUP as usize);
        import(
            store,
            subject,
            &part,
            40 * GROUP,
            42 * GROUP,
            &encoding(&part, 40 * GROUP, 42 * GROUP),
        )
        .await
        .unwrap();
        let after_part = usage().await;

        assert_eq!(
            after_whole,
            whole.len() as u64 + outboard_size(whole.len() as u64)
        );
        // Two groups, the pairs on their paths from the root, and at most a
        // record: not the whole outboard of the partial blob.
        let mut held = Groups::new();
        held.insert(40);
        held.insert(41);
        let pairs = held.pairs(part.len() as u64);
        let kept = after_part - after_whole;
        assert!(
            kept >= 2 * GROUP + pairs && kept <= 2 * GROUP + pairs + 64,
            "{kept}"
        );
        assert!(pairs <= 8 * PAIR as u64, "{pairs}");
    }

    /// The header and the rest of a verified read of `range` of the blob
    /// `digest` from `store`.
    pub async fn verified<P>(
        store: &P,
        subject: &Subject,
        digest: &Blake3Hash,
        offset: u64,
        length: Option<u64>,
    ) -> Result<((u64, u64), Vec<u8>), BlobError>
    where
        P: Provider<Read> + dialog_common::ConditionalSync,
    {
        let mut reader = subject
            .clone()
            .reader()
            .archive()
            .blob()
            .invoke(Read::verified(digest.clone(), offset, length))
            .perform(store)
            .await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = reader.next().await? {
            bytes.extend(chunk);
        }
        let header: [u8; HEADER] = bytes[..HEADER].try_into().expect("a header");
        Ok((read_header(&header), bytes.split_off(HEADER)))
    }

    /// A store that holds a blob whole answers a verified range of it, which
    /// the decoder of `bao-tree` verifies group by group.
    pub async fn it_answers_a_verified_range_of_a_whole_blob<P>(store: &P, subject: &Subject)
    where
        P: Provider<Read> + Provider<dialog_effects::blob::Write> + dialog_common::ConditionalSync,
    {
        let bytes = blob(20 * GROUP as usize + 77);
        let size = bytes.len() as u64;
        let mut sink = subject
            .clone()
            .writer()
            .archive()
            .blob()
            .write()
            .perform(store)
            .await
            .unwrap();
        sink.write_all(&bytes).await.unwrap();
        let digest = sink.finish().await.unwrap();

        for (offset, length) in [
            (0, None),
            (5 * GROUP + 3, Some(2 * GROUP)),
            (size - 10, Some(10)),
        ] {
            let ((held_size, covered), encoded) = verified(store, subject, &digest, offset, length)
                .await
                .unwrap();
            let end = length.map_or(size, |length| offset + length);
            let (first, last) = groups_of(size, offset, end);
            assert_eq!((held_size, covered), (size, last), "{offset}");
            let ranges = chunk_ranges(size, first, last);
            let hash = blake3::Hash::from(*digest.as_bytes());
            let decoded: Vec<u8> = bao_tree::io::sync::DecodeResponseIter::new(
                hash,
                tree(size),
                std::io::Cursor::new(encoded),
                &ranges,
            )
            .filter_map(|item| match item.expect("each item verifies") {
                bao_tree::io::BaoContentItem::Leaf(leaf) => Some(leaf.data.to_vec()),
                bao_tree::io::BaoContentItem::Parent(_) => None,
            })
            .flatten()
            .collect();
            assert_eq!(decoded, bytes[first as usize..last as usize], "{offset}");
        }
    }

    /// A store that holds the first part of a range answers its groups, and
    /// a covered end short of the range. A store that holds none of the start
    /// of a range answers at once that it covers nothing.
    pub async fn it_answers_the_held_start_of_a_range<P>(store: &P, subject: &Subject)
    where
        P: Provider<Read> + Provider<ImportRange> + dialog_common::ConditionalSync,
    {
        let bytes = blob(12 * GROUP as usize);
        let size = bytes.len() as u64;
        let digest = Blake3Hash::from(*blake3::hash(&bytes).as_bytes());
        import(
            store,
            subject,
            &bytes,
            2 * GROUP,
            6 * GROUP,
            &encoding(&bytes, 2 * GROUP, 6 * GROUP),
        )
        .await
        .unwrap();

        let ((_, short), encoded) = verified(store, subject, &digest, 3 * GROUP + 5, None)
            .await
            .unwrap();
        let ((_, none), nothing) = verified(store, subject, &digest, 7 * GROUP, Some(GROUP))
            .await
            .unwrap();

        assert_eq!(short, 6 * GROUP, "the answer covers the held part alone");
        let mut decoder = Decoder::new(&digest, size, 3 * GROUP, short);
        decoder.push(&encoded);
        let mut groups = Vec::new();
        while let Some(item) = decoder.next_item() {
            if let Item::Group { bytes, .. } = item.unwrap() {
                groups.extend(bytes);
            }
        }
        assert!(decoder.is_done());
        assert_eq!(groups, bytes[3 * GROUP as usize..6 * GROUP as usize]);
        assert_eq!((none, nothing.len()), (7 * GROUP, 0));
    }
}
