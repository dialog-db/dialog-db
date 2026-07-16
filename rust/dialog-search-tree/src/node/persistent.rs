use dialog_common::Blake3Hash;
use rkyv::{
    Archive, Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{Buffer, DialogSearchTreeError, Entry, Key, Link, SymmetryWith, Value};
use std::marker::PhantomData;

/// A tree node in its serialized, content-addressed form.
///
/// A [`PersistentNode`] holds the serialized [`PersistentNodeBody`] as bytes in
/// a [`Buffer`] and is identified by its [`Blake3Hash`]. The structured
/// contents are recovered as a zero-copy [`ArchivedNodeBody`] view via
/// [`body`](PersistentNode::body).
#[derive(Clone, Debug)]
pub struct PersistentNode<Key, Value> {
    key: PhantomData<Key>,
    value: PhantomData<Value>,

    buffer: Buffer,
}

impl<Key, Value> PersistentNode<Key, Value>
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
    /// Creates a new node from a serialized buffer.
    pub fn new(buffer: Buffer) -> Self {
        Self {
            buffer,
            key: PhantomData,
            value: PhantomData,
        }
    }

    /// Returns the content hash of this node.
    pub fn hash(&self) -> &Blake3Hash {
        self.buffer.blake3_hash()
    }

    /// Returns the underlying buffer containing serialized node data.
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    /// Converts this node into a [`Link`] referencing it, carrying the
    /// separator at the subtree's left edge.
    ///
    /// The separator is a seam property, not derivable from the node's own
    /// body (it depends on the left-adjacent subtree), so the caller threads
    /// it in from the context that knows the seam.
    pub fn to_link(&self, separator: Vec<u8>) -> Result<Link, DialogSearchTreeError> {
        Ok(Link {
            separator,
            node: self.buffer.blake3_hash().clone(),
        })
    }

    /// Returns the upper bound key of this segment node, if it has one.
    ///
    /// Index nodes carry no full keys (links hold separators), so this
    /// returns `None` for an index; full bounds exist only in leaves.
    pub fn upper_bound(&self) -> Result<Option<&Key::Archived>, DialogSearchTreeError> {
        self.body().map(|body| match body {
            ArchivedNodeBody::Index(_) => None,
            ArchivedNodeBody::Segment(segment) => segment.upper_bound(),
        })
    }

    /// Accesses the deserialized body of this node.
    pub fn body(&self) -> Result<&ArchivedNodeBody<Key, Value>, DialogSearchTreeError> {
        rkyv::access::<_, rkyv::rancor::Error>(self.buffer.as_ref())
            .map_err(|error| DialogSearchTreeError::Access(format!("{error}")))
    }

    /// Interprets this node as an index node, returning an error if it's a
    /// segment.
    pub fn as_index(&self) -> Result<&ArchivedIndex, DialogSearchTreeError> {
        self.body().and_then(|body| match body {
            ArchivedNodeBody::Index(index) => Ok(index),
            ArchivedNodeBody::Segment(_) => Err(DialogSearchTreeError::Access(
                "Attempted to interpret a segment node as an index node".to_string(),
            )),
        })
    }

    /// Interprets this node as a segment node, returning an error if it's an
    /// index.
    pub fn as_segment(&self) -> Result<&ArchivedSegment<Key, Value>, DialogSearchTreeError> {
        self.body().and_then(|body| match body {
            ArchivedNodeBody::Segment(segment) => Ok(segment),
            ArchivedNodeBody::Index(_) => Err(DialogSearchTreeError::Access(
                "Attempted to interpret a index node as an segment node".to_string(),
            )),
        })
    }
}

/// An index node containing links to child nodes.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedIndex)]
pub struct PersistentIndex {
    /// The child node links stored in this index.
    pub links: Vec<Link>,
}

impl PersistentIndex {
    /// Creates a new [`PersistentIndex`] containing a single link.
    pub fn new(link: Link) -> Self {
        Self { links: vec![link] }
    }
}

/// A leaf segment containing key-value entries.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedSegment)]
pub struct PersistentSegment<Key, Value> {
    /// The key-value entries stored in this segment.
    pub entries: Vec<Entry<Key, Value>>,
}

impl<Key, Value> PersistentSegment<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value,
{
    /// Creates a new [`PersistentSegment`] containing a single entry.
    pub fn new(entry: Entry<Key, Value>) -> Self {
        Self {
            entries: vec![entry],
        }
    }

    /// Returns the key of the last entry in this segment.
    pub fn upper_bound(&self) -> Option<&Key> {
        self.entries.last().map(|entry| &entry.key)
    }
}

/// The body of a tree node, either an index or a leaf segment.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedNodeBody)]
pub enum PersistentNodeBody<Key, Value> {
    /// An index node containing links to child nodes.
    Index(PersistentIndex),
    /// A leaf segment containing key-value entries.
    Segment(PersistentSegment<Key, Value>),
}

impl<Key, Value> PersistentNodeBody<Key, Value>
where
    Key: self::Key
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
{
    /// Serializes this node body to bytes.
    ///
    /// Returns the serializer's [`AlignedVec`] directly so the alignment
    /// that in-place archive access depends on is preserved all the way
    /// into the node [`Buffer`](crate::Buffer).
    pub fn as_bytes(&self) -> Result<AlignedVec, DialogSearchTreeError> {
        rkyv::to_bytes(self).map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))
    }
}

impl<Key, Value> TryFrom<Vec<Link>> for PersistentNodeBody<Key, Value> {
    type Error = DialogSearchTreeError;

    fn try_from(links: Vec<Link>) -> Result<Self, Self::Error> {
        if links.is_empty() {
            return Err(DialogSearchTreeError::Node(
                "Attempted to create an index from zero links".into(),
            ));
        }
        Ok(PersistentNodeBody::Index(PersistentIndex { links }))
    }
}

impl<Key, Value> TryFrom<Vec<Entry<Key, Value>>> for PersistentNodeBody<Key, Value> {
    type Error = DialogSearchTreeError;

    fn try_from(entries: Vec<Entry<Key, Value>>) -> Result<Self, Self::Error> {
        if entries.is_empty() {
            return Err(DialogSearchTreeError::Node(
                "Attempted to create an index from zero links".into(),
            ));
        }
        Ok(PersistentNodeBody::Segment(PersistentSegment { entries }))
    }
}
