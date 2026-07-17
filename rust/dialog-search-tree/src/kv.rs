use dialog_common::ConditionalSend;
use rkyv::Archive;
use std::fmt::Debug;

use crate::DialogSearchTreeError;

/// Trait for types that can be used as keys in a search tree.
///
/// Keys are ordered byte strings: the tree stores them front-coded in leaf
/// nodes and reconstructs them from bytes on read, so a key must expose its
/// bytes ([`AsRef<[u8]>`]) and be reconstructible from them
/// ([`try_from_bytes`](Key::try_from_bytes)). The key's [`Ord`] must agree
/// with the lexicographic order of its bytes; routing and range scans compare
/// the two interchangeably.
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
