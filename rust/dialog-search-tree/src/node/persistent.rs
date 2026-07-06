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
///
/// Validity is a type invariant: a `PersistentNode` can only be constructed
/// from bytes that passed archive validation ([`try_new`](Self::try_new)) or
/// bytes this crate itself just serialized
/// ([`from_serialized`](Self::from_serialized)), so
/// [`body`](Self::body) is infallible and costs a pointer cast rather than a
/// bytecheck pass per access.
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
    /// Creates a node from a buffer of untrusted bytes (storage, cache, the
    /// network), validating that it archives as
    /// `ArchivedNodeBody<Key, Value>`. This is the only validation the node
    /// ever runs: it proves the invariant that [`body`](Self::body) relies
    /// on for the lifetime of the node and all of its clones.
    pub fn try_new(buffer: Buffer) -> Result<Self, DialogSearchTreeError> {
        rkyv::access::<ArchivedNodeBody<Key, Value>, rkyv::rancor::Error>(buffer.as_ref())
            .map_err(|error| DialogSearchTreeError::Access(format!("{error}")))?;
        Ok(Self {
            buffer,
            key: PhantomData,
            value: PhantomData,
        })
    }

    /// Creates a node from bytes this crate just serialized out of a
    /// [`PersistentNodeBody<Key, Value>`], which are a valid archive of that
    /// exact type by construction — no validation pass is needed to uphold
    /// [`body`](Self::body)'s invariant.
    ///
    /// The serializer's [`AlignedVec`] is taken directly so the alignment
    /// that in-place archive access depends on is preserved into the
    /// [`Buffer`].
    pub(crate) fn from_serialized(bytes: AlignedVec) -> Self {
        Self {
            buffer: Buffer::from(bytes),
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
        let upper_bound: Key = self.body().upper_bound().and_then(into_owned)?;
        let self_hash = self.buffer.blake3_hash().clone();

        Ok(Link {
            upper_bound,
            node: self_hash,
        })
    }

    /// Returns the upper bound key of this node, if it has one.
    pub fn upper_bound(&self) -> Option<&Key::Archived> {
        match self.body() {
            ArchivedNodeBody::Index(index) => index.upper_bound(),
            ArchivedNodeBody::Segment(segment) => segment.upper_bound(),
        }
    }

    /// Accesses the deserialized body of this node.
    ///
    /// Infallible: validity is the type's construction invariant (see
    /// [`try_new`](Self::try_new) / [`from_serialized`](Self::from_serialized)),
    /// so no per-access validation runs.
    pub fn body(&self) -> &ArchivedNodeBody<Key, Value> {
        // SAFETY: every constructor upholds the invariant that `buffer` is a
        // valid archive of exactly `ArchivedNodeBody<Key, Value>` — `try_new`
        // ran the full bytecheck validation, `from_serialized` took the bytes
        // straight from this crate's serializer for that type. Buffers are
        // immutable and 16-byte aligned.
        unsafe { rkyv::access_unchecked::<ArchivedNodeBody<Key, Value>>(self.buffer.as_ref()) }
    }

    /// Interprets this node as an index node, returning an error if it's a
    /// segment.
    pub fn as_index(&self) -> Result<&ArchivedIndex<Key>, DialogSearchTreeError> {
        match self.body() {
            ArchivedNodeBody::Index(index) => Ok(index),
            ArchivedNodeBody::Segment(_) => Err(DialogSearchTreeError::Access(
                "Attempted to interpret a segment node as an index node".to_string(),
            )),
        }
    }

    /// Interprets this node as a segment node, returning an error if it's an
    /// index.
    pub fn as_segment(&self) -> Result<&ArchivedSegment<Key, Value>, DialogSearchTreeError> {
        match self.body() {
            ArchivedNodeBody::Segment(segment) => Ok(segment),
            ArchivedNodeBody::Index(_) => Err(DialogSearchTreeError::Access(
                "Attempted to interpret a index node as an segment node".to_string(),
            )),
        }
    }

    /// Finds the index of the child containing the given key.
    pub fn get_child_index(&self, key: &Key::Archived) -> Option<usize> {
        match self.body() {
            ArchivedNodeBody::Index(index) => index
                .links
                .binary_search_by(|link| Ord::cmp(&link.upper_bound, key))
                .ok(),
            ArchivedNodeBody::Segment(segment) => segment
                .entries
                .binary_search_by(|entry| Ord::cmp(&entry.key, key))
                .ok(),
        }
    }
}

/// An index node containing links to child nodes.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedIndex)]
pub struct PersistentIndex<Key> {
    /// The child node links stored in this index.
    pub links: Vec<Link<Key>>,
}

impl<Key> PersistentIndex<Key>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
{
    /// Creates a new [`PersistentIndex`] containing a single link.
    pub fn new(link: Link<Key>) -> Self {
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
    Index(PersistentIndex<Key>),
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
