use std::cmp::Ordering;

use crate::{ArchivedEntry, DialogSearchTreeError, Entry, Key, Link, SymmetryWith, Value};
use rkyv::{
    Archive, Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

/// An index node containing links to child nodes.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct Index<Key> {
    /// The child node links stored in this index.
    pub links: Vec<Link<Key>>,
}

impl<Key> Index<Key>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
{
    /// Creates a new [`Index`] containing a single link.
    pub fn new(link: Link<Key>) -> Self {
        Self { links: vec![link] }
    }
}

impl<Key> ArchivedIndex<Key>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
{
    /// Returns the upper bound key of the last link in this index.
    pub fn upper_bound(&self) -> Option<&Key::Archived> {
        self.links.last().map(|link| &link.upper_bound)
    }
}

/// A leaf segment containing key-value entries.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct Segment<Key, Value> {
    /// The key-value entries stored in this segment.
    pub entries: Vec<Entry<Key, Value>>,
}

impl<Key, Value> Segment<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value,
{
    /// Creates a new [`Segment`] containing a single entry.
    pub fn new(entry: Entry<Key, Value>) -> Self {
        Self {
            entries: vec![entry],
        }
    }
}

impl<Key, Value> ArchivedSegment<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value,
{
    /// Returns the key of the last entry in this segment.
    pub fn upper_bound(&self) -> Option<&Key::Archived> {
        self.entries.last().map(|entry| &entry.key)
    }
}

/// The body of a tree node, either an index or a leaf segment.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum NodeBody<Key, Value> {
    /// An index node containing links to child nodes.
    Index(Index<Key>),
    /// A leaf segment containing key-value entries.
    Segment(Segment<Key, Value>),
}

impl<Key, Value> NodeBody<Key, Value>
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
    pub fn as_bytes(&self) -> Result<Vec<u8>, DialogSearchTreeError> {
        rkyv::to_bytes(self)
            .map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))
            .map(|bytes| bytes.to_vec())
    }
}

impl<Key, Value> TryFrom<Vec<Link<Key>>> for NodeBody<Key, Value> {
    type Error = DialogSearchTreeError;

    fn try_from(links: Vec<Link<Key>>) -> Result<Self, Self::Error> {
        if links.is_empty() {
            return Err(DialogSearchTreeError::Node(
                "Attempted to create an index from zero links".into(),
            ));
        }
        Ok(NodeBody::Index(Index { links }))
    }
}

impl<Key, Value> TryFrom<Vec<Entry<Key, Value>>> for NodeBody<Key, Value> {
    type Error = DialogSearchTreeError;

    fn try_from(entries: Vec<Entry<Key, Value>>) -> Result<Self, Self::Error> {
        if entries.is_empty() {
            return Err(DialogSearchTreeError::Node(
                "Attempted to create an index from zero links".into(),
            ));
        }
        Ok(NodeBody::Segment(Segment { entries }))
    }
}

impl<Key, Value> ArchivedNodeBody<Key, Value>
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
{
    /// Returns the upper bound key of this node body.
    pub fn upper_bound(&self) -> Result<&Key::Archived, DialogSearchTreeError> {
        match self {
            ArchivedNodeBody::Index(index) => index.upper_bound(),
            ArchivedNodeBody::Segment(segment) => segment.upper_bound(),
        }
        .ok_or_else(|| DialogSearchTreeError::Node("Node was unexpectedly empty".into()))
    }

    /// Searches for an entry with the given key in this segment.
    ///
    /// Returns `Ok(None)` if this is an index node or if the key is not found.
    pub fn find_entry(
        &self,
        key: &Key,
    ) -> Result<Option<&ArchivedEntry<Key, Value>>, DialogSearchTreeError> {
        match self {
            ArchivedNodeBody::Index(_) => Err(DialogSearchTreeError::Access(
                "Attempted to find an entry in an index node".into(),
            )),
            ArchivedNodeBody::Segment(segment) => Ok(segment
                .entries
                .binary_search_by(|entry| entry.key.partial_cmp(key).unwrap_or(Ordering::Less))
                .map(|index| segment.entries.get(index))
                .ok()
                .and_then(|entry| entry)),
        }
    }
}
