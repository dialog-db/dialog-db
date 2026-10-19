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
//!
//! The outboard is in post order, as [`bao_tree::io::outboard::PostOrderMemOutboard`]
//! lays it out: 64 bytes, the hashes of the two children, for each parent of
//! the tree. A tree of `n` groups has `n - 1` parents, so a blob of one group
//! or less, 16 KiB at most, has no outboard.

use bao_tree::{BaoTree, BlockSize};
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
