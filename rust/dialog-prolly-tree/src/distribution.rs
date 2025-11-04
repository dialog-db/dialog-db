mod geometric;
use dialog_storage::HashType;
pub use geometric::*;

use crate::KeyType;

/// A rank determines how a tree's segments should be chunked
pub type Rank = u32;

/// A trait that may be implemented by any type that defines how to derive the
/// [`Rank`] of a value in a tree
pub trait Distribution<Key, Hash>
where
    Key: KeyType,
    Hash: HashType,
{
    /// The branch factor used by this distribution strategy
    const BRANCH_FACTOR: u32;

    /// Compute the [`Rank`] of a value given its key
    // TODO: support tree state e.g., fn rank(state: &TreeState, key: &Key) -> Rank;
    fn rank(key: &Key) -> Rank;
}
