mod persistent;
pub use persistent::*;

mod archive;
pub use archive::*;

pub(crate) mod codec;

pub(crate) mod columnar;
pub use columnar::ColumnData;

mod transient;
pub use transient::*;

use dialog_common::Blake3Hash;
use rkyv::{
    Serialize,
    bytecheck::CheckBytes,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{Buffer, Delta, DialogSearchTreeError, Key, Link, Value};

/// A tree node in either of its two representations.
///
/// A [`Node`] is the unit the tree-shaping algorithm operates on. It is either
/// a [`PersistentNode`] (serialized, content-addressed, the read and storage
/// form) or a [`TransientNode`] (live, editable, produced while a batch of
/// edits is in flight). Editing builds [`Node::Transient`] subtrees in place;
/// persisting resolves them bottom-up back into [`Node::Persistent`].
///
/// The persistent arm holds a [`Link`] (hash plus separator), not a loaded
/// node: untouched subtrees stay as cheap references and are loaded only when
/// an edit descends into them, at which point they are replaced by a
/// [`Node::Transient`]. This is what gives the edit batch its structural
/// sharing, and lets [`persist`](TransientNode::persist) re-emit an untouched
/// child's link without re-serializing it.
#[derive(Debug)]
pub enum Node<Key, Value> {
    /// A reference to a serialized, content-addressed node.
    Persistent(Link),
    /// A live, editable node not yet serialized.
    Transient(TransientNode<Key, Value>),
}

impl<Key, Value> Node<Key, Value> {
    /// Unwraps an already-lifted node to its [`TransientNode`].
    ///
    /// Errors if the node is still a [`Node::Persistent`] reference. Callers use
    /// this where an edit has already lifted the node along its path, so a
    /// persistent node here would be an invariant violation, not a normal case.
    pub fn into_transient(self) -> Result<TransientNode<Key, Value>, DialogSearchTreeError> {
        match self {
            Node::Transient(transient) => Ok(transient),
            Node::Persistent(_) => Err(DialogSearchTreeError::Node(
                "Expected a lifted transient node".into(),
            )),
        }
    }

    /// The separator at this node's left edge, borrowed without cloning.
    ///
    /// A persistent node's separator lives in its owned [`Link`]; a transient
    /// node's is stored on its leftmost descendant segment (or the leftmost
    /// persistent link on the way down), reached by following first children.
    /// Used for routing and for the seam coin, so descending never copies a
    /// separator. Errors if a node on the leftmost path is empty.
    pub fn separator(&self) -> Result<&[u8], DialogSearchTreeError> {
        match self {
            Node::Persistent(link) => Ok(link.separator.as_slice()),
            Node::Transient(transient) => transient.separator(),
        }
    }
}

impl<Key, Value> From<TransientNode<Key, Value>> for Node<Key, Value> {
    fn from(transient: TransientNode<Key, Value>) -> Self {
        Node::Transient(transient)
    }
}

impl<Key, Value> Node<Key, Value>
where
    Key: self::Key,
    Value: self::Value
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Resolves this node into a [`Link`] referencing its serialized form,
    /// recording any newly created nodes in `delta`.
    ///
    /// A [`Node::Persistent`] already is a link and is returned as-is, with no
    /// re-serialization; its separator rides along unchanged. A
    /// [`Node::Transient`] captures its separator (stored on its leftmost
    /// segment) before serializing bottom-up (see [`TransientNode::persist`]).
    /// This makes no shape decisions; the shape was already established by the
    /// edits that built the transient.
    pub fn into_link(
        self,
        delta: &mut Delta<Blake3Hash, Buffer>,
    ) -> Result<Link, DialogSearchTreeError> {
        match self {
            Node::Persistent(link) => Ok(link),
            Node::Transient(transient) => {
                let separator = transient.separator()?.to_vec();
                transient.persist(delta)?.to_link(separator)
            }
        }
    }
}
