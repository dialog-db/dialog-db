use base58::ToBase58;
use dialog_prolly_tree::EMPT_TREE_HASH;
use dialog_storage::Blake3Hash;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};

/// We reference a tree by the root hash.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeReference(pub(crate) Blake3Hash);

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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bytes: &[u8] = self.hash();
        write!(f, "#{}", ToBase58::to_base58(bytes))
    }
}

impl Debug for NodeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

impl From<NodeReference> for Blake3Hash {
    fn from(value: NodeReference) -> Self {
        let NodeReference(hash) = value;
        hash
    }
}
