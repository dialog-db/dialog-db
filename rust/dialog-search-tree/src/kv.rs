use dialog_common::ConditionalSend;
use rkyv::Archive;
use std::fmt::Debug;

use crate::{DialogSearchTreeError, Schema};

/// Trait for types that can be used as keys in a search tree.
///
/// Keys are ordered byte strings: the tree stores them in leaf nodes and
/// reconstructs them from bytes on read, so a key must expose its bytes
/// ([`AsRef<[u8]>`]) and be reconstructible from them
/// ([`try_from_bytes`](Key::try_from_bytes)). The key's [`Ord`] must agree
/// with the lexicographic order of its bytes; routing and range scans compare
/// the two interchangeably.
///
/// A key type may additionally expose its **component structure** via
/// [`schema`](Key::schema) and [`components`](Key::components), so a leaf can
/// store each component in the column that fits it (see
/// [`Schema`](crate::Schema)). The default is a single whole-key arena
/// component, under which the columnar leaf degrades to a single front-coded
/// arena. The concatenation of a key's components, in schema order, must
/// equal the key's comparison bytes.
pub trait Key:
    Clone
    + Debug
    + Sized
    + AsRef<[u8]>
    + std::hash::Hash
    + PartialOrd
    + Ord
    + PartialEq
    + Eq
    + ConditionalSend
{
    /// Reconstructs a key from its byte representation, as previously
    /// produced by [`AsRef<[u8]>`]. Errors if the bytes do not form a valid
    /// key (for a fixed-size key, a length mismatch).
    fn try_from_bytes(bytes: &[u8]) -> Result<Self, DialogSearchTreeError>;

    /// Reconstructs a key from an owned byte buffer. The scan reconstructs each
    /// key by concatenating its columns into a fresh `Vec`; a variable-length
    /// key backed by a `Vec<u8>` can then take ownership of that buffer instead
    /// of copying it again. Defaults to [`try_from_bytes`](Self::try_from_bytes)
    /// (a copy) for fixed-size keys, which cannot adopt the buffer.
    fn try_from_bytes_owned(bytes: Vec<u8>) -> Result<Self, DialogSearchTreeError> {
        Self::try_from_bytes(&bytes)
    }

    /// Returns the minimum possible value for this key type.
    fn min() -> Self;

    /// Returns the maximum possible value for this key type.
    fn max() -> Self;

    /// The layout id of this key, selecting which [`schema`](Key::schema)
    /// applies to it. A key type with a single uniform layout (the default,
    /// and `[u8; N]`) returns `0`. A key whose component layout varies (the
    /// dialog artifact key, whose EAV/AEV/VAE orderings put their fields in
    /// different byte positions) returns a distinct id per layout, typically
    /// derived from a leading tag byte.
    ///
    /// Every key in one leaf has the same layout id (leaves are partitioned
    /// by the leading component), so a leaf's columns are encoded and decoded
    /// under a single schema, recorded once in the leaf.
    fn layout(&self) -> u8 {
        0
    }

    /// The component layout for a given layout id, describing how each
    /// component is stored in a columnar leaf. Defaults to a single whole-key
    /// arena component (no finer structure) for the only id `0`, so key types
    /// that do not decompose still round-trip through the columnar codec.
    fn schema(layout: u8) -> Schema {
        let _ = layout;
        Schema::opaque()
    }

    /// Splits this key into its component byte-slices, in the schema order for
    /// its own [`layout`](Key::layout), appending each to `out`.
    ///
    /// Concatenating the pushed slices must reproduce `self.as_ref()`, and the
    /// slice count and per-position widths must match
    /// `Self::schema(self.layout())`. The default pushes the whole key as a
    /// single component, matching [`Schema::opaque`](crate::Schema::opaque).
    fn components<'a>(&'a self, out: &mut Vec<&'a [u8]>) {
        out.push(self.as_ref());
    }
}

impl<const N: usize> Key for [u8; N] {
    fn try_from_bytes(bytes: &[u8]) -> Result<Self, DialogSearchTreeError> {
        bytes.try_into().map_err(|_| {
            DialogSearchTreeError::Encoding(format!(
                "Expected a {N}-byte key, got {} bytes",
                bytes.len()
            ))
        })
    }

    fn min() -> Self {
        [u8::MIN; N]
    }

    fn max() -> Self {
        [u8::MAX; N]
    }
}

/// Trait for types that can be used as values in a search tree.
///
/// Values must be cloneable and serializable.
pub trait Value: Clone + Debug + Sized + Archive + ConditionalSend {}

impl Value for Vec<u8> {}
