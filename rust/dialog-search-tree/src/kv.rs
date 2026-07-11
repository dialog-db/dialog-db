use dialog_common::ConditionalSend;
use rkyv::Archive;
use std::fmt::Debug;

use crate::SymmetryWith;

/// Trait for types that can be used as keys in a search tree.
///
/// Keys must be fixed-size, comparable, and serializable.
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
    + Archive
    + ConditionalSend
where
    Self: PartialOrd<Self::Archived>,
    Self::Archived: PartialOrd<Self> + PartialEq<Self> + SymmetryWith<Self> + Ord,
{
    /// The fixed size of this key type in bytes.
    const LENGTH: usize;

    /// Returns the minimum possible value for this key type.
    fn min() -> Self;

    /// Returns the maximum possible value for this key type.
    fn max() -> Self;
}

impl<const N: usize> Key for [u8; N] {
    const LENGTH: usize = N;

    fn min() -> Self {
        [u8::MIN; N]
    }

    fn max() -> Self {
        [u8::MAX; N]
    }
}

impl<const N: usize> SymmetryWith<[u8; N]> for [u8; N] {}

/// Trait for types that can be used as values in a search tree.
///
/// Values must be cloneable and serializable.
pub trait Value: Clone + Debug + Sized + Archive + ConditionalSend {
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
}

impl Value for Vec<u8> {}
