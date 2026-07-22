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
    ///
    /// The id `u8::MAX` is reserved: it marks a leaf that straddles a layout
    /// boundary (see [`MIXED_LAYOUT`](crate::MIXED_LAYOUT)) and is encoded
    /// under the opaque whole-key schema. A key type must not use it as a
    /// real layout id with a multi-component schema.
    fn layout(&self) -> u8 {
        0
    }

    /// The layout id of a key given only its stored bytes, without
    /// reconstructing the typed key. Must agree with
    /// `Self::try_from_bytes(bytes)?.layout()`.
    ///
    /// The default reconstructs the key and asks it. A key type whose layout
    /// is derived from a leading tag byte should override this to read the
    /// tag straight from the bytes: the novelty encoder classifies a whole
    /// buffer by layout first and skips the typed parse entirely when the
    /// buffer straddles layouts (the opaque fallback needs no components).
    fn layout_of(bytes: &[u8]) -> Result<u8, DialogSearchTreeError> {
        Ok(Self::try_from_bytes(bytes)?.layout())
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

    /// Splits a key's raw stored bytes into its component slices for `layout`,
    /// borrowing from `bytes`, without reconstructing the typed key.
    ///
    /// Must agree with [`components`](Key::components) on every valid key:
    /// `components_of(key.as_ref(), key.layout(), out)` pushes exactly the
    /// slices `key.components(out)` pushes. The column encoders split from
    /// raw bytes through this (a buffered op stores raw key bytes, and even a
    /// typed key's slices borrow from the same bytes), so no key is copied or
    /// parsed into typed form just to be taken apart again.
    ///
    /// The default pushes the whole key as the single opaque component,
    /// matching the default [`schema`](Key::schema); a key type that
    /// overrides `schema`/`components` must override this to match. A
    /// mismatch cannot pass silently: the encoders check the slice count and
    /// coverage against the schema and refuse to encode.
    fn components_of<'a>(
        bytes: &'a [u8],
        layout: u8,
        out: &mut Vec<&'a [u8]>,
    ) -> Result<(), DialogSearchTreeError> {
        let _ = layout;
        out.push(bytes);
        Ok(())
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
pub trait Value: Clone + Debug + Sized + Archive + ConditionalSend {
    /// The weight this value's payload contributes to its entry for byte
    /// pacing (`Manifest::max_segment`): an estimate of the value's encoded
    /// footprint in a leaf, in bytes. A pure function of the value's
    /// content — it feeds boundary decisions, so two replicas holding the
    /// same value must weigh it identically.
    ///
    /// The default is the historical fixed slot estimate (32 bytes), which
    /// keeps every existing value type's tree shapes unchanged. Types whose
    /// payloads vary meaningfully (the artifact `State<Datum>` carries
    /// cause and version data from a dozen bytes to hundreds) override it
    /// with a calibrated per-content estimate so leaves pace against real
    /// encoded bytes rather than a flat guess — otherwise value-heavy
    /// leaves overshoot the byte target by whatever the guess missed.
    fn payload_weight(&self) -> usize {
        32
    }

    /// Domain-specific conflict resolution for
    /// [`integrate`](crate::TransientTree::integrate): when two
    /// *different* values contend for the same key, return `Some(true)`
    /// if `self` (the incoming value) must replace `existing`,
    /// `Some(false)` if `existing` must be kept, or `None` to fall back
    /// to the default last-write-wins hash race.
    ///
    /// Implementations must be deterministic and antisymmetric
    /// (`a.prevails_over(b) == Some(true)` iff
    /// `b.prevails_over(a) == Some(false)`), so that two replicas
    /// integrating the same contended pair in opposite directions
    /// converge on the same winner. The default (`None`, hash race) has
    /// this property; overrides exist to encode semantics the raw bytes
    /// cannot — e.g. a deletion tombstone that must beat any concurrent
    /// assertion regardless of how their encodings happen to hash.
    fn prevails_over(&self, existing: &Self) -> Option<bool> {
        let _ = existing;
        None
    }

    /// Domain-specific fusion for [`integrate`](crate::TransientTree::integrate)
    /// contests: after the winner of a same-key contest is chosen, fold
    /// whatever the LOSING value carries that must survive the contest into
    /// the winner. The default keeps the winner untouched (pure
    /// last-write-wins). An override exists for values that aggregate — the
    /// dialog artifact datum collapses same-fact claim versions from both
    /// sides into one entry, so a contest unions the two sets instead of
    /// silently orphaning the loser's.
    ///
    /// Implementations must be deterministic and winner-directional: both
    /// replicas resolve the contest with the same `(winner, loser)` roles
    /// (the winner choice is antisymmetric), so `fuse(winner, &loser)`
    /// computing the same bytes on both sides is what keeps them
    /// convergent.
    fn fuse(winner: Self, loser: &Self) -> Self {
        let _ = loser;
        winner
    }
}

impl Value for Vec<u8> {}
