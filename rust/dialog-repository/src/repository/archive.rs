//! Loading a branch's tree through archive capabilities.
//!
//! - [`local`] -- loads nodes and spilled values from the local archive
//! - [`networked`] -- falls back to a remote site on a local miss

use dialog_artifacts::{Datum, Key, State};
use dialog_search_tree::{DialogSearchTreeError, NodeBody, NoveltyOp, PersistentNode, into_owned};

/// Loads nodes and spilled values from the local archive.
pub mod local;
pub use local::*;

/// Loads nodes and spilled values locally, falling back to a remote site
/// and caching locally on a read miss.
pub mod networked;
pub use networked::*;

mod persist;
pub(crate) use persist::persist_line;

/// Every entry `node` holds that can reference content: a segment's stored
/// entries, and the asserts buffered in an index node, whose keys and values
/// reference content exactly as stored entries do (a buffered retract
/// references nothing of its own). Sealing, push and export each classify
/// these with [`shipment_ref`](dialog_artifacts::shipment_ref), so they agree
/// on what a node names.
pub(crate) fn node_entries(
    node: &PersistentNode<Key, State<Datum>>,
) -> Result<Vec<(Key, State<Datum>)>, DialogSearchTreeError> {
    let mut entries = Vec::new();
    match node.body() {
        NodeBody::Segment(segment) => {
            segment.for_each_entry::<Key, _>(|key, value| {
                entries.push((Key::from(key.to_vec()), into_owned(value)?));
                Ok(())
            })?;
        }
        NodeBody::Index(index) => {
            for entry in index.all_novelty::<Key>()? {
                if let NoveltyOp::Assert(value) = entry.op {
                    entries.push((Key::from(entry.key), value));
                }
            }
        }
    }
    Ok(entries)
}
