//! A node read in place from a store that keeps nodes as rows, not blocks.
//!
//! A store such as the single-file archive keeps a node as rows of ids into
//! shared tokens: a key is a few tokens laid side by side, a value is an
//! archived value of its own, and a child is named by its hash. Such a
//! store need not build the node's block for a read: it hands the tree a
//! [`NodeRows`], and the tree reads keys, values, separators and children
//! where the store keeps them (for the archive file, in a mapping of the
//! file). The node's block is built only if something asks for its bytes
//! ([`PersistentNode::buffer`](crate::PersistentNode::buffer)), which only
//! sync and persistence do.

use std::any::TypeId;
use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, Holds, held_key};
use dialog_effects::archive::prelude::CatalogScope;

use crate::{DialogSearchTreeError, Manifest};

/// A key as the slices it is made of, in key order: the key's bytes are
/// their concatenation. A store keeps a key's parts apart (an entity, an
/// attribute and a value, each once), so a key is compared part by part,
/// in place, and written out whole only into a caller's scratch buffer.
#[derive(Clone, Copy, Debug)]
pub struct KeyParts<'a> {
    parts: [&'a [u8]; MAX_PARTS],
    count: u8,
}

/// The most slices a [`KeyParts`] holds.
pub const MAX_PARTS: usize = 6;

impl<'a> KeyParts<'a> {
    /// The key whose bytes are `parts`, in order. At most [`MAX_PARTS`].
    pub fn new(parts: &[&'a [u8]]) -> Self {
        assert!(
            parts.len() <= MAX_PARTS,
            "a key of at most {MAX_PARTS} parts"
        );
        let mut held: [&'a [u8]; MAX_PARTS] = [&[]; MAX_PARTS];
        held[..parts.len()].copy_from_slice(parts);
        Self {
            parts: held,
            count: parts.len() as u8,
        }
    }

    /// The parts, in key order.
    pub fn parts(&self) -> &[&'a [u8]] {
        &self.parts[..self.count as usize]
    }

    /// The key's length in bytes.
    pub fn len(&self) -> usize {
        self.parts().iter().map(|part| part.len()).sum()
    }

    /// Whether the key has no bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How the key orders against `probe`, as its bytes would: part by
    /// part, without putting the key together.
    pub fn cmp_bytes(&self, probe: &[u8]) -> Ordering {
        let mut rest = probe;
        for part in self.parts() {
            let shared = part.len().min(rest.len());
            match part[..shared].cmp(&rest[..shared]) {
                Ordering::Equal if shared < part.len() => return Ordering::Greater,
                Ordering::Equal => rest = &rest[shared..],
                unequal => return unequal,
            }
        }
        if rest.is_empty() {
            Ordering::Equal
        } else {
            Ordering::Less
        }
    }

    /// Writes the key's bytes into `out`, replacing what it held.
    pub fn write_into(&self, out: &mut Vec<u8>) {
        out.clear();
        for part in self.parts() {
            out.extend_from_slice(part);
        }
    }

    /// The key's bytes.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len());
        self.write_into(&mut out);
        out
    }
}

/// Which kind of node rows stand for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowsKind {
    /// A leaf: entries, each a key and a value.
    Segment,
    /// An index: links to children, each a separator, a child hash and a
    /// scale, and buffered operations riding the links.
    Index,
}

/// A node as a store keeps it in rows, read in place.
///
/// Positions are in entry (segment) or link (index) order, and every
/// position below [`len`](Self::len) (or, for buffered operations, within
/// the ranges [`buffer`](Self::buffer) names) is valid: the tree checks
/// positions against the counts before it asks.
///
/// # Safety
///
/// Every value a node hands out ([`value`](Self::value),
/// [`op_value`](Self::op_value)) must be the bytes of a valid, standalone
/// archive (the bytes `rkyv::to_bytes` makes) of the value type
/// [`values`](Self::values) names, starting at an address aligned to 16, for
/// as long as the node lives. The tree reads them as that type in place,
/// without checking them again. And the node's rows must be exactly the node
/// [`hash`](Self::hash) names: the tree trusts the store's hash, and builds
/// the node's block from the rows when asked for its bytes.
pub unsafe trait NodeRows: ConditionalSend + ConditionalSync + 'static {
    /// The hash of the block these rows stand for.
    fn hash(&self) -> &Blake3Hash;

    /// The tree's format, as the block carries it.
    fn manifest(&self) -> &Manifest;

    /// The type the node's values are archives of.
    fn values(&self) -> TypeId;

    /// Which kind of node this is.
    fn kind(&self) -> RowsKind;

    /// How many entries (a segment) or links (an index) the node has.
    fn len(&self) -> usize;

    /// Whether the node has no entries or links.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The key of a segment's entry `at`.
    fn key(&self, at: usize) -> KeyParts<'_>;

    /// The archived value of a segment's entry `at`.
    fn value(&self, at: usize) -> &[u8];

    /// The separator of an index's link `at`.
    fn separator(&self, at: usize) -> &[u8];

    /// The hash of the child an index's link `at` names.
    fn child(&self, at: usize) -> &Blake3Hash;

    /// The scale of an index's link `at`, encoded as [`Scale::from_u8`](crate::Scale::from_u8) reads it.
    fn scale(&self, at: usize) -> u8;

    /// How many buffers of operations an index carries.
    fn buffers(&self) -> usize;

    /// Buffer `which`: the link it rides, and the positions of its
    /// operations, in key order, the newest for a key last.
    fn buffer(&self, which: usize) -> (u32, Range<usize>);

    /// The key of buffered operation `at`.
    fn op_key(&self, at: usize) -> KeyParts<'_>;

    /// The archived value buffered operation `at` asserts, or `None` for a
    /// retraction.
    fn op_value(&self, at: usize) -> Option<&[u8]>;

    /// The memory the node takes beyond what the store keeps anyway: what a
    /// node cache charges for holding it.
    fn footprint(&self) -> usize;

    /// The block these rows stand for, built by the tree's encoder: what a
    /// reader that wants the node's bytes (sync, persistence) is given.
    fn block(&self) -> Result<dialog_common::Buffer, DialogSearchTreeError>;
}

/// A store that can hand the tree its nodes as rows.
///
/// An environment holds one per subject ([`NodeSource::serve`]); a tree read
/// asks it first ([`NodeSource::of`]) and loads the block only for a node it
/// does not have.
pub trait NodeSource: ConditionalSend + ConditionalSync {
    /// The node `hash` names in `catalog`, as rows, if the store holds it
    /// that way.
    fn node(&self, catalog: &CatalogScope, hash: &Blake3Hash) -> Option<Arc<dyn NodeRows>>;
}

/// What an environment holds a subject's node source as.
type Served = Arc<dyn NodeSource>;

fn served_key(subject: &impl std::fmt::Display) -> String {
    held_key::<Served>(&format!("dialog-search-tree/node-source/{subject}"))
}

impl dyn NodeSource {
    /// The node source `env` holds for `subject`, if any.
    pub fn of(
        env: &(impl Holds + ?Sized),
        subject: &impl std::fmt::Display,
    ) -> Option<Arc<dyn NodeSource>> {
        let held = env.held(&served_key(subject))?;
        held.downcast_ref::<Served>().cloned()
    }

    /// Makes `env` hand every tree read of `subject` to `source` first.
    pub fn serve(
        env: &(impl Holds + ?Sized),
        subject: &impl std::fmt::Display,
        source: Arc<dyn NodeSource>,
    ) {
        env.hold(served_key(subject), Arc::new(source));
    }
}

/// Reads `bytes`, which a [`NodeRows`] vouched for, as an archived `T`.
///
/// # Safety
///
/// `bytes` must be a valid standalone archive of the type `T` archives,
/// aligned to 16, as [`NodeRows`] requires of every value it hands out, and
/// the source's [`values`](NodeRows::values) must have named that type.
pub(crate) unsafe fn archived<T: rkyv::Portable>(bytes: &[u8]) -> &T {
    // SAFETY: the caller passes bytes a `NodeRows` vouched are a valid,
    // aligned archive of `T`'s value type.
    unsafe { rkyv::access_unchecked::<T>(bytes) }
}

/// A position past a node's rows: what a view returns instead of asking.
pub(crate) fn out_of_range(what: &str) -> DialogSearchTreeError {
    DialogSearchTreeError::Encoding(format!("{what} out of range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_orders_parts_as_their_bytes_would() {
        let key = KeyParts::new(&[b"ab", b"", b"cd"]);
        for probe in [
            &b""[..],
            b"a",
            b"ab",
            b"abc",
            b"abcd",
            b"abcde",
            b"abd",
            b"b",
            b"aa",
        ] {
            assert_eq!(
                key.cmp_bytes(probe),
                b"abcd"[..].cmp(probe),
                "against {probe:?}"
            );
        }
        assert_eq!(key.to_vec(), b"abcd");
        assert_eq!(key.len(), 4);
    }
}
