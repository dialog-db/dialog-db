use dialog_common::Blake3Hash;
use rkyv::{Archive, Deserialize, Serialize};

/// A reference to a child node in an index node.
///
/// Links connect index nodes to their children, storing the child's content
/// hash and the separator that routes searches across the seam at the child's
/// left edge.
///
/// The separator follows the lower-bound convention: it is the shortest byte
/// string that sorts strictly above every key in the left-adjacent subtree
/// and at or below every key in this link's subtree (it is always a prefix of
/// this subtree's minimum leaf key). The global leftmost link at every level
/// carries the empty separator, which reads as negative infinity. Routing
/// descends into the last child whose separator is at or below the probe.
///
/// Storing a truncated separator instead of a full bound key is what keeps
/// index nodes small once keys become variable-length; the full key exists
/// exactly once, in its leaf.
#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
pub struct Link {
    /// The separator at the left edge of the referenced subtree. Empty for
    /// the global leftmost subtree of a level.
    pub separator: Vec<u8>,
    /// The [`Blake3Hash`] of the referenced node.
    pub node: Blake3Hash,
}
