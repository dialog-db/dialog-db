use dialog_common::Blake3Hash;
use rkyv::{Archive, Deserialize, Serialize};

/// A reference to a child node in an index node.
///
/// Links connect index nodes to their children, storing the child's upper bound
/// key and its content hash.
#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
pub struct Link<Key> {
    /// The maximum key contained in the referenced node's subtree.
    pub upper_bound: Key,
    /// The [`Blake3Hash`] of the referenced node.
    pub node: Blake3Hash,
}
