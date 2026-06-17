use std::cmp::Ordering;

use rkyv::{
    Deserialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    ArchivedEntry, ArchivedIndex, ArchivedNodeBody, ArchivedSegment, DialogSearchTreeError, Key,
    SymmetryWith, Value,
};

impl<Key, Value> ArchivedIndex<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value,
{
    /// Returns the upper bound key of the last link in this index.
    pub fn upper_bound(&self) -> Option<&Key::Archived> {
        self.links.last().map(|link| &link.upper_bound)
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
            Self::Index(index) => index.upper_bound(),
            Self::Segment(segment) => segment.upper_bound(),
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
            Self::Index(_) => Err(DialogSearchTreeError::Access(
                "Attempted to find an entry in an index node".into(),
            )),
            Self::Segment(segment) => Ok(segment
                .entries
                .binary_search_by(|entry| entry.key.partial_cmp(key).unwrap_or(Ordering::Less))
                .map(|index| segment.entries.get(index))
                .ok()
                .and_then(|entry| entry)),
        }
    }
}
