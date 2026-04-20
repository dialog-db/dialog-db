use base58::ToBase58;
use dialog_prolly_tree::EMPT_TREE_HASH;
use dialog_storage::Blake3Hash;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

/// We reference a tree by the root hash.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeReference(Blake3Hash);

impl NodeReference {
    /// Returns a reference to the underlying hash.
    pub fn hash(&self) -> &Blake3Hash {
        &self.0
    }
}

impl Default for NodeReference {
    /// By default, a [`NodeReference`] is created to empty search tree.
    fn default() -> Self {
        Self(EMPT_TREE_HASH)
    }
}

impl Display for NodeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let bytes: &[u8] = self.hash();
        write!(f, "#{}", ToBase58::to_base58(bytes))
    }
}

impl Debug for NodeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self, f)
    }
}

impl From<Blake3Hash> for NodeReference {
    fn from(hash: Blake3Hash) -> Self {
        Self(hash)
    }
}

impl From<NodeReference> for Blake3Hash {
    fn from(value: NodeReference) -> Self {
        let NodeReference(hash) = value;
        hash
    }
}
