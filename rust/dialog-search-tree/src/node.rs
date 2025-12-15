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
    Value, into_owned,
};
use std::{marker::PhantomData, sync::Arc};

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
        + PartialEq<Key>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    pub fn new(buffer: Buffer) -> Self {
        Self {
            buffer,
            key: PhantomData,
            value: PhantomData,
        }
    }

    pub fn hash(&self) -> &Blake3Hash {
        self.buffer.blake3_hash()
    }

    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    pub fn to_link(&self) -> Result<Link<Key>, DialogSearchTreeError> {
        let upper_bound: Key = self.body()?.upper_bound().and_then(into_owned)?;
        let self_hash = self.buffer.blake3_hash().clone();

        Ok(Link {
            upper_bound,
            node: self_hash,
        })
    }

    pub fn body(&self) -> Result<&ArchivedNodeBody<Key, Value>, DialogSearchTreeError> {
        rkyv::access::<_, rkyv::rancor::Error>(self.buffer.as_ref())
            .map_err(|error| DialogSearchTreeError::Access(format!("{error}")))
    }

    pub fn as_index(&self) -> Result<&ArchivedIndex<Key>, DialogSearchTreeError> {
        self.body().and_then(|body| match body {
            ArchivedNodeBody::Index(index) => Ok(index),
            ArchivedNodeBody::Segment(_) => Err(DialogSearchTreeError::Access(format!(
                "Attempted to interpret a segment node as an index node"
            ))),
        })
    }

    pub fn as_segment(&self) -> Result<&ArchivedSegment<Key, Value>, DialogSearchTreeError> {
        self.body().and_then(|body| match body {
            ArchivedNodeBody::Segment(segment) => Ok(segment),
            ArchivedNodeBody::Index(_) => Err(DialogSearchTreeError::Access(format!(
                "Attempted to interpret a index node as an segment node"
            ))),
        })
    }
}
