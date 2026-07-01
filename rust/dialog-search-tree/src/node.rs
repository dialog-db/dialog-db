mod persistent;
pub use persistent::*;

mod archive;

mod transient;
pub use transient::*;

use dialog_common::Blake3Hash;
use rkyv::{
    Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{Buffer, Delta, DialogSearchTreeError, Key, Link, SymmetryWith, Value};

/// A tree node in either of its two representations.
///
/// A [`Node`] is the unit the tree-shaping algorithm operates on. It is either
/// a [`PersistentNode`] (serialized, content-addressed, the read and storage
/// form) or a [`TransientNode`] (live, editable, produced while a batch of
/// edits is in flight). Editing builds [`Node::Transient`] subtrees in place;
/// persisting resolves them bottom-up back into [`Node::Persistent`].
///
/// The persistent arm holds a [`Link`] (hash plus upper bound), not a loaded
/// node: untouched subtrees stay as cheap references and are loaded only when
/// an edit descends into them, at which point they are replaced by a
/// [`Node::Transient`]. This is what gives the edit batch its structural
/// sharing, and lets [`persist`](Node::persist) re-emit an untouched child's
/// link without re-serializing it.
#[derive(Debug)]
pub enum Node<Key, Value> {
    /// A reference to a serialized, content-addressed node.
    Persistent(Link<Key>),
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
}

impl<Key, Value> From<TransientNode<Key, Value>> for Node<Key, Value> {
    fn from(transient: TransientNode<Key, Value>) -> Self {
        Node::Transient(transient)
    }
}

impl<Key, Value> Node<Key, Value>
where
    Key: self::Key,
    Key::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>
        + PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Returns the upper bound key of this node as an owned key.
    ///
    /// For a persistent node the bound is recovered from its serialized buffer;
    /// for a transient node it is read from the live structure. Errors if the
    /// node is empty or a persistent bound cannot be recovered.
    pub fn upper_bound(&self) -> Result<Key, DialogSearchTreeError> {
        self.upper_bound_ref().cloned()
    }

    /// Borrows the upper bound key of this node without cloning it.
    ///
    /// A persistent node's bound lives in its owned [`Link`]; a transient node's
    /// is the key of its rightmost descendant entry, reached by following the
    /// last child down to a segment. Used on the read-only descent so routing a
    /// key never copies a bound. Errors if a node on the rightmost path is
    /// empty.
    pub fn upper_bound_ref(&self) -> Result<&Key, DialogSearchTreeError> {
        match self {
            Node::Persistent(link) => Ok(&link.upper_bound),
            Node::Transient(TransientNode::Index(index)) => index
                .children
                .last()
                .ok_or_else(|| DialogSearchTreeError::Node("Index was unexpectedly empty".into()))?
                .upper_bound_ref(),
            Node::Transient(TransientNode::Segment(segment)) => segment
                .entries
                .last()
                .map(|entry| &entry.key)
                .ok_or_else(|| {
                    DialogSearchTreeError::Node("Segment was unexpectedly empty".into())
                }),
        }
    }
}

impl<Key, Value> Node<Key, Value>
where
    Key: self::Key
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    Key::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>
        + PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord,
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
    /// re-serialization. A [`Node::Transient`] is serialized bottom-up (see
    /// [`TransientNode::persist`]) and converted to a link. This makes no shape
    /// decisions; the shape was already established by the edits that built the
    /// transient.
    pub fn into_link(
        self,
        delta: &mut Delta<Blake3Hash, Buffer>,
    ) -> Result<Link<Key>, DialogSearchTreeError> {
        match self {
            Node::Persistent(link) => Ok(link),
            Node::Transient(transient) => transient.persist(delta)?.to_link(),
        }
    }
}
