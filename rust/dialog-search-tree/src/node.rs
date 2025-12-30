use dialog_common::Blake3Hash;
use rkyv::{
    Deserialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    ArchivedIndex, ArchivedNodeBody, ArchivedSegment, Buffer, DialogSearchTreeError, Key, Link,
    SymmetryWith, Value, into_owned,
};
use std::marker::PhantomData;

/// A tree node containing either entries (segment) or links to children
/// (index).
///
/// Nodes are content-addressed by their [`Blake3Hash`] and store serialized
/// data in a [`Buffer`].
#[derive(Clone, Debug)]
pub struct Node<Key, Value> {
    key: PhantomData<Key>,
    value: PhantomData<Value>,

    buffer: Buffer,
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
    pub fn as_index(&self) -> Result<&ArchivedIndex<Key>, DialogSearchTreeError> {
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
