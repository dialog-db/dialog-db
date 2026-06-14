use base58::ToBase58;
use dialog_storage::Blake3Hash;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

/// A hash representing an empty (usually newly created) search tree.
///
/// Matches the search tree's null root sentinel
/// (`dialog_common::NULL_BLAKE3_HASH`) byte for byte.
pub const EMPTY_TREE_HASH: Blake3Hash = [0; 32];

/// Reference to a search tree by its root hash.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TreeReference(Blake3Hash);

impl TreeReference {
    /// Returns a reference to the underlying hash.
    pub fn hash(&self) -> &Blake3Hash {
        &self.0
    }
}

impl Default for TreeReference {
    /// By default, a [`TreeReference`] points at the empty search tree.
    fn default() -> Self {
        Self(EMPTY_TREE_HASH)
    }
}

impl Display for TreeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let bytes: &[u8] = self.hash();
        write!(f, "#{}", ToBase58::to_base58(bytes))
    }
}

impl Debug for TreeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self, f)
    }
}

impl From<Blake3Hash> for TreeReference {
    fn from(hash: Blake3Hash) -> Self {
        Self(hash)
    }
}

impl From<TreeReference> for Blake3Hash {
    fn from(value: TreeReference) -> Self {
        let TreeReference(hash) = value;
        hash
    }
}
