use std::cmp::Ordering;

use crate::{ArchivedEntry, DialogSearchTreeError, Entry, Key, Link, Rank, Value};
use dialog_common::Blake3Hash;
use nonempty::NonEmpty;
use rkyv::{
    Archive, Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::{Fallible, Strategy},
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct Index<Key> {
    pub links: Vec<Link<Key>>,
}

impl<Key> Index<Key>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key>,
{
    pub fn new(link: Link<Key>) -> Self {
        Self { links: vec![link] }
    }
}

impl<Key> ArchivedIndex<Key>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key>,
{
    pub fn upper_bound(&self) -> Option<&Key::Archived> {
        self.links.last().map(|link| &link.upper_bound)
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct Segment<Key, Value> {
    pub entries: Vec<Entry<Key, Value>>,
}

impl<Key, Value> Segment<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key>,
    Value: self::Value,
{
    pub fn new(entry: Entry<Key, Value>) -> Self {
        Self {
            entries: vec![entry],
        }
    }
}

impl<Key, Value> ArchivedSegment<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key>,
    Value: self::Value,
{
    pub fn upper_bound(&self) -> Option<&Key::Archived> {
        self.entries.last().map(|entry| &entry.key)
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum NodeBody<Key, Value> {
    Index(Index<Key>),
    Segment(Segment<Key, Value>),
}

impl<Key, Value> NodeBody<Key, Value>
where
    Key: self::Key
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    Key::Archived: PartialOrd<Key> + PartialEq<Key>,
    Value: self::Value
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
{
    pub fn as_bytes(&self) -> Result<Vec<u8>, DialogSearchTreeError> {
        rkyv::to_bytes(self)
            .map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))
            .map(|bytes| bytes.to_vec())
    }
}

// impl<Key, Value> From<NodeBody<Key, Value>> for Link<Key>
// where
//     Key: self::Key
//         + for<'a> Serialize<
//             Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
//         >,
//     Value: self::Value
//         + for<'a> Serialize<
//             Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
//         >,
// {
//     fn from(value: NodeBody<Key, Value>) -> Self {
//         let hash = Blake3Hash::hash(value.as_bytes());
//     }
// }

impl<Key, Value> TryFrom<Vec<Link<Key>>> for NodeBody<Key, Value> {
    type Error = DialogSearchTreeError;

    fn try_from(links: Vec<Link<Key>>) -> Result<Self, Self::Error> {
        if links.len() == 0 {
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
        if entries.len() == 0 {
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
        + PartialEq<Key>,
    Value: self::Value,
{
    pub fn upper_bound(&self) -> Result<&Key::Archived, DialogSearchTreeError> {
        match self {
            ArchivedNodeBody::Index(index) => index.upper_bound(),
            ArchivedNodeBody::Segment(segment) => segment.upper_bound(),
        }
        .ok_or_else(|| DialogSearchTreeError::Node("Node was unexpectedly empty".into()))
    }

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

#[cfg(test)]
mod tests {
    use super::{ArchivedIndex, Index, Link};
    use dialog_common::Blake3Hash;

    #[test]
    pub fn it_make_index() {
        let index = Index::new(Link {
            upper_bound: [1, 2, 3u8],
            node: Blake3Hash::hash(&[1, 2, 3]),
        });

        let mut idx = rkyv::to_bytes::<rkyv::rancor::Error>(&index)
            .unwrap()
            .to_vec();

        let archived =
            rkyv::access_mut::<ArchivedIndex<[u8; 3]>, rkyv::rancor::Error>(&mut idx).unwrap();

        println!("{:?}", archived.links.get(0).unwrap().upper_bound);
    }
}
