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

use crate::{Buffer, DialogSearchTreeError, Entry, Key, Link, SymmetryWith, Value, into_owned};
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

    /// Converts this node into a [`Link`] referencing it.
    pub fn to_link(&self) -> Result<Link<Key>, DialogSearchTreeError> {
        let upper_bound: Key = self.body()?.upper_bound().and_then(into_owned)?;
        let self_hash = self.buffer.blake3_hash().clone();

        Ok(Link {
            upper_bound,
            node: self_hash,
        })
    }

    /// Returns the upper bound key of this node, if it has one.
    pub fn upper_bound(&self) -> Result<Option<&Key::Archived>, DialogSearchTreeError> {
        self.body().map(|body| match body {
            ArchivedNodeBody::Index(index) => index.upper_bound(),
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
    pub fn as_index(&self) -> Result<&ArchivedIndex<Key, Value>, DialogSearchTreeError> {
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

    /// Finds the index of the child containing the given key.
    pub fn get_child_index(
        &self,
        key: &Key::Archived,
    ) -> Result<Option<usize>, DialogSearchTreeError> {
        self.body().map(|body| match body {
            ArchivedNodeBody::Index(index) => index
                .links
                .binary_search_by(|link| Ord::cmp(&link.upper_bound, key))
                .ok(),
            ArchivedNodeBody::Segment(segment) => segment
                .entries
                .binary_search_by(|entry| Ord::cmp(&entry.key, key))
                .ok(),
        })
    }
}

/// A pending operation buffered at an index node (the node's novelty).
///
/// An insert or update is an [`Assert`](NoveltyOp::Assert) carrying the value;
/// a delete is a [`Retract`](NoveltyOp::Retract) tombstone. Both flow down the
/// tree with a flush and are resolved against the leaf segment; within a key the
/// last op wins.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedNoveltyOp)]
pub enum NoveltyOp<Value> {
    /// Assert (insert or update) the value.
    Assert(Value),
    /// Retract (delete) the key.
    Retract,
}

/// A single buffered op together with the key it applies to.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedNoveltyEntry)]
pub struct NoveltyEntry<Key, Value> {
    /// The key this op applies to.
    pub key: Key,
    /// The buffered op.
    pub op: NoveltyOp<Value>,
}

/// An index node containing links to child nodes and a novelty buffer.
///
/// `novelty` holds ops pending against the subtree rooted at this node, sorted
/// by key. An empty `novelty` makes this node byte-identical to a canonical
/// (fully flushed) index, so [`canonicalize`](crate::HitchhikerTree::canonicalize)
/// reproduces the canonical tree exactly.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedIndex)]
pub struct PersistentIndex<Key, Value> {
    /// The child node links stored in this index.
    pub links: Vec<Link<Key>>,
    /// Ops pending against this subtree, sorted by key (the node's novelty).
    pub novelty: Vec<NoveltyEntry<Key, Value>>,
}

impl<Key, Value> PersistentIndex<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
{
    /// Creates a new [`PersistentIndex`] containing a single link and no novelty.
    pub fn new(link: Link<Key>) -> Self {
        Self {
            links: vec![link],
            novelty: Vec::new(),
        }
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
    Index(PersistentIndex<Key, Value>),
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

impl<Key, Value> TryFrom<Vec<Link<Key>>> for PersistentNodeBody<Key, Value> {
    type Error = DialogSearchTreeError;

    fn try_from(links: Vec<Link<Key>>) -> Result<Self, Self::Error> {
        if links.is_empty() {
            return Err(DialogSearchTreeError::Node(
                "Attempted to create an index from zero links".into(),
            ));
        }
        Ok(PersistentNodeBody::Index(PersistentIndex {
            links,
            novelty: Vec::new(),
        }))
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
