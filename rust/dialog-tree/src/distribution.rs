mod geometric;
pub use geometric::*;

use crate::{KeyBuffer, Node, ValueBuffer};

/// A rank determines how a tree's segments should be chunked
pub type Rank = u32;

/// A trait that may be implemented by any type that defines how to derive the
/// [`Rank`] of a value in a tree
pub trait Distribution<'a, Key, Value>
where
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
{
    /// Compute the [`Rank`] of a value given its key
    // TODO: support tree state e.g., fn rank(state: &TreeState, key: &Key) -> Rank;
    fn rank(key: &'a Key, value: &'a Value, candidate: &'a Node<'a, Key, Value>) -> Rank;
}
