use rkyv::Archive;
use std::fmt::Debug;

use crate::SymmetryWith;

/// Trait for types that can be used as keys in a search tree.
///
/// Keys must be fixed-size, comparable, and serializable.
pub trait Key:
    Clone + Debug + Sized + AsRef<[u8]> + std::hash::Hash + PartialOrd + Ord + PartialEq + Eq + Archive
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
pub trait Value: Clone + Debug + Sized + Archive {}

impl Value for Vec<u8> {}
