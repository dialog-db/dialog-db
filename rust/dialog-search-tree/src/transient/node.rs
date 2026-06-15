use dialog_common::Blake3Hash;
use rkyv::{
    Deserialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    ArchivedNodeBody, DialogSearchTreeError, Entry, Key, Link, Node, SymmetryWith, Value,
    into_owned,
};

/// A node within a [`Transient`](super::Transient) edit.
///
/// The names follow the persistent/transient data structure vocabulary, not
/// Rust's borrow/own distinction: both variants own their data (a `Persistent`
/// node owns a [`Link`], which is an owned key plus an owned hash), and neither
/// holds a reference into the original tree, so the type needs no lifetime.
///
/// - [`Persistent`](Self::Persistent): a sealed node of the durable tree, named
///   by its [`Link`] (hash plus upper bound). It is shared with the original by
///   hash, never decoded, and seals back to itself for free. The batch loads it
///   from storage only if a descent enters it, turning it into a `Transient`.
/// - [`Transient`](Self::Transient): a node this batch is editing. It holds its
///   children in memory and has no hash; a hash is assigned only when the batch
///   is persisted.
pub enum NodeEdit<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Value: self::Value,
    // `Value::Archived` must be byte-checkable so a node read back from storage
    // can be validated; see the bound explainer on `load` below.
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// A sealed node of the durable tree, named by hash and upper bound.
    Persistent(Link<Key>),
    /// A node being edited in this batch: its children held in memory, no hash
    /// yet, paired with its upper bound key for ordering within its parent.
    Transient {
        /// The largest key in this node's subtree.
        upper_bound: Key,
        /// The in-memory children or entries.
        body: TransientBody<Key, Value>,
    },
}

/// The in-memory body of a [`NodeEdit::Transient`] node.
///
/// An index holds its children as further [`NodeEdit`]s (each still
/// [`Persistent`](NodeEdit::Persistent) until the batch descends into it); a
/// segment holds its entries directly. Both are kept in key order at all times.
pub enum TransientBody<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// An index node: ordered child edits.
    Index(Vec<NodeEdit<Key, Value>>),
    /// A leaf segment: ordered key/value entries.
    Segment(Vec<Entry<Key, Value>>),
}

impl<Key, Value> NodeEdit<Key, Value>
where
    Key: self::Key + Clone,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    // These bounds are what `rkyv` needs to turn an archived (in-buffer) node
    // back into owned Rust values, which `load` does:
    //   - `for<'a> CheckBytes<Validator<...>>`: the archived bytes can be
    //     validated for any borrow of the buffer. The `for<'a>` is higher
    //     ranked because validation borrows the buffer for a lifetime the
    //     caller, not this bound, chooses.
    //   - `Deserialize<Key, Strategy<Pool, _>>`: the archived key can be
    //     reconstructed into an owned `Key` (the `Pool` resolves any shared
    //     pointers during deserialization).
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
{
    /// The largest key in this node's subtree, used to order it within a parent.
    pub fn upper_bound(&self) -> &Key {
        match self {
            NodeEdit::Persistent(link) => &link.upper_bound,
            NodeEdit::Transient { upper_bound, .. } => upper_bound,
        }
    }

    /// Loads a persistent node into transient form, decoding its child links
    /// (for an index) or entries (for a segment) into memory. An
    /// already-transient node is returned untouched. This is the copy a node
    /// pays on its first edit in a batch; later edits mutate the transient body
    /// in place.
    pub fn load(node: &Node<Key, Value>, upper_bound: Key) -> Result<Self, DialogSearchTreeError> {
        let body = match node.body()? {
            ArchivedNodeBody::Index(index) => {
                let children = index
                    .links
                    .iter()
                    .map(|link| into_owned::<Link<Key>>(link).map(NodeEdit::Persistent))
                    .collect::<Result<Vec<_>, DialogSearchTreeError>>()?;
                TransientBody::Index(children)
            }
            ArchivedNodeBody::Segment(segment) => {
                let entries = segment
                    .entries
                    .iter()
                    .map(into_owned::<Entry<Key, Value>>)
                    .collect::<Result<Vec<_>, DialogSearchTreeError>>()?;
                TransientBody::Segment(entries)
            }
        };
        Ok(NodeEdit::Transient { upper_bound, body })
    }

    /// The hash of a persistent node, or `None` for a transient (unsealed) one.
    pub fn persistent_hash(&self) -> Option<&Blake3Hash> {
        match self {
            NodeEdit::Persistent(link) => Some(&link.node),
            NodeEdit::Transient { .. } => None,
        }
    }
}
