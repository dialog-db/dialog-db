//! Tree-node inspection: the [`Load`] effect and pure node decoders.
//!
//! Together these back the query engine's tree procedures (`tree/node`,
//! `tree/link`, `tree/key`): [`Load`] fetches a raw node block by content
//! hash through the evaluation environment, and the `inspect_*` functions
//! project structure out of the fetched bytes without touching storage.
//!
//! # Why this is sound under differential subscriptions
//!
//! [`Load`] is *idempotent*: a node block is content-addressed, so the
//! bytes behind a hash — and therefore every row projected from them —
//! can never change. A different tree is a different hash. This includes
//! buffered novelty: a node's hash covers the ops riding on it. Rows
//! derived through `Load` are permanent; they can become unnecessary,
//! never wrong, so no invalidation machinery is required for them. The
//! only mutable fact in the domain is "what is the current root?", which
//! reaches queries as an ordinary tracked fact (`dialog.branch/tree`),
//! never through this effect. Contrast a locality probe ("is this block
//! cached here?"): that answer changes without a commit, is *not*
//! idempotent, and must not be served through this module.

use dialog_capability::Command;
use dialog_search_tree::{Buffer, Distribution, Geometric, PersistentNode, Rank};
use dialog_storage::Blake3Hash;

use crate::{Datum, DialogArtifactsError, Key, State};

/// The raw content hash a [`Load`] resolves: the same 32 bytes a
/// revision's tree reference and an index link's child hash carry.
pub type NodeReference = Blake3Hash;

/// Command for loading a raw tree node block by its content hash.
///
/// The counterpart of [`Select`](crate::Select) for the tree procedures:
/// where `Select` scans key ranges and yields artifacts, `Load` fetches
/// one content-addressed block for structural inspection. `Ok(None)`
/// means the block is not available anywhere the provider can reach —
/// the unreplicated-contributes-nothing convention.
pub struct Load;

impl Command for Load {
    type Input = Blake3Hash;
    type Output = Result<Option<Vec<u8>>, DialogArtifactsError>;
}

/// The node type the artifact tree persists, instantiated for inspection.
type ArtifactNode = PersistentNode<Key, State<Datum>>;

/// Structural description of one persisted node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSummary {
    /// `"index"` for a node of child links, `"segment"` for a leaf of
    /// entries.
    pub kind: &'static str,
    /// Serialized block size in bytes (what a fetch pays for).
    pub size: u64,
    /// Child-link count for an index, entry count for a segment.
    pub count: u64,
    /// The node's upper-bound key: the last entry key of a segment.
    /// Empty for an index node (its table holds separators, not whole
    /// keys) — the per-link separators are exposed by [`inspect_links`].
    pub bound: Vec<u8>,
    /// Rank of the upper bound under the node's own embedded manifest
    /// (0 when there is no bound). Higher rank ⇒ higher boundary in the
    /// tree.
    pub rank: Rank,
    /// The node's [`Scale`](dialog_search_tree::Scale) code: a one-byte
    /// log-scale estimate of the subtree's entry count.
    pub scale: u64,
    /// Buffered hitchhiker ops riding this node (always 0 for a
    /// segment): the window into buffered-vs-canonical cost.
    pub novelty: u64,
}

/// Structural description of one child link of an index node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkSummary {
    /// Position among siblings.
    pub at: u64,
    /// Content hash of the referenced child node.
    pub node: Blake3Hash,
    /// The link's separator: the left-edge boundary of the referenced
    /// subtree, a front-coded prefix of its minimum leaf key. Empty for
    /// the level's global leftmost link (reads as negative infinity).
    pub separator: Vec<u8>,
    /// The child subtree's advisory [`Scale`](dialog_search_tree::Scale)
    /// code.
    pub scale: u64,
    /// Rank of the separator under the node's embedded manifest: the
    /// seam level this boundary falls on.
    pub rank: Rank,
}

/// Decode the node behind `bytes` into its [`NodeSummary`].
pub fn inspect_node(bytes: Vec<u8>) -> Result<NodeSummary, DialogArtifactsError> {
    let size = bytes.len() as u64;
    let node = ArtifactNode::new(Buffer::from(bytes));
    let manifest = node.manifest()?;
    let scale = node.scale()?.as_u8() as u64;
    let bound = node.upper_bound()?.unwrap_or_default();
    let rank = if bound.is_empty() {
        0
    } else {
        Geometric::rank(&bound, &manifest)
    };

    Ok(match node.as_index() {
        Ok(index) => NodeSummary {
            kind: "index",
            size,
            count: index.len() as u64,
            bound,
            rank,
            scale,
            novelty: index.novelty_len() as u64,
        },
        Err(_) => {
            let segment = node.as_segment()?;
            NodeSummary {
                kind: "segment",
                size,
                count: segment.len() as u64,
                bound,
                rank,
                scale,
                novelty: 0,
            }
        }
    })
}

/// Decode the index node behind `bytes` into one [`LinkSummary`] per
/// child link, in position order. A segment has no links and yields an
/// empty vector (not an error — queries union over mixed levels).
pub fn inspect_links(bytes: Vec<u8>) -> Result<Vec<LinkSummary>, DialogArtifactsError> {
    let node = ArtifactNode::new(Buffer::from(bytes));
    let Ok(index) = node.as_index() else {
        return Ok(Vec::new());
    };
    let manifest = node.manifest()?;

    let mut links = Vec::with_capacity(index.len());
    for at in 0..index.len() {
        let separator = index.separator(at)?;
        let rank = if separator.is_empty() {
            0
        } else {
            Geometric::rank(&separator, &manifest)
        };
        links.push(LinkSummary {
            at: at as u64,
            node: *index.hash_at(at)?.as_bytes(),
            separator,
            scale: index.scale_at(at)?.as_u8() as u64,
            rank,
        });
    }
    Ok(links)
}

/// Decode the segment node behind `bytes` into its entry keys, in entry
/// order. An index node has no entries and yields an empty vector.
pub fn inspect_keys(bytes: Vec<u8>) -> Result<Vec<Vec<u8>>, DialogArtifactsError> {
    let node = ArtifactNode::new(Buffer::from(bytes));
    if node.as_index().is_ok() {
        return Ok(Vec::new());
    }
    let segment = node.as_segment()?;
    let mut keys = Vec::with_capacity(segment.len());
    let mut cursor = segment.keys::<Key>()?;
    while let Some((_, key)) = cursor.next_key()? {
        keys.push(key.to_vec());
    }
    Ok(keys)
}
