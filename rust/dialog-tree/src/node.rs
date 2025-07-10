use std::collections::BTreeMap;

use bytes::Bytes;
use dialog_common::{Blake3Hash, Blake3Hashed};
use dialog_encoding::{decode, encode};
// use dialog_storage::StorageBackend;
// use futures_util::stream::Once;
// use nonempty::NonEmpty;
use once_cell::sync::OnceCell;
use zerocopy::TryFromBytes;

use crate::{DialogTreeError, KeyBuffer, ValueBuffer};

mod link;
pub use link::*;

mod entry;
pub use entry::*;

mod r#type;
pub use r#type::*;

mod body;
pub use body::*;

pub struct Node<'a, Key, Value>
where
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
{
    buffer: Bytes,
    body: OnceCell<NodeBody<'a, Key, Value>>,
    hash: OnceCell<Blake3Hash>,
    dirty: bool,
}

impl<'a, Key, Value> Blake3Hashed for Node<'a, Key, Value>
where
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
{
    fn hash(&self) -> &Blake3Hash {
        self.hash.get_or_init(|| Blake3Hash::hash(&self.buffer))
    }
}

impl<'a, Key, Value> Node<'a, Key, Value>
where
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
{
    pub fn new() -> Self {
        Node {
            buffer: Bytes::new(),
            body: OnceCell::with_value(NodeBody::Boundary),
            hash: OnceCell::new(),
            dirty: false,
        }
    }

    pub fn new_with_buffer(buffer: Bytes, dirty: bool) -> Self {
        Self {
            buffer,
            body: OnceCell::new(),
            hash: OnceCell::new(),
            dirty,
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }

    fn decode_body(&'a self) -> Result<NodeBody<'a, Key, Value>, DialogTreeError> {
        if self.buffer.len() == 0 {
            return Ok(NodeBody::Boundary);
        }

        match NodeType::try_ref_from_bytes(&self.buffer[0..1]) {
            Ok(NodeType::Branch) => {
                let branch = decode::<'a, _, _>(&self.buffer[1..])?;
                Ok(NodeBody::Branch {
                    branch,
                    child_cache: Default::default(),
                })
            }
            Ok(NodeType::Segment) => {
                let segment = decode::<'a, _, _>(&self.buffer[1..])?;
                Ok(NodeBody::Segment(segment))
            }
            Err(error) => {
                return Err(DialogTreeError::Node(format!(
                    "Could not determine node type: {}",
                    error
                )));
            }
        }
    }

    pub fn body(&'a self) -> Result<&'a NodeBody<'a, Key, Value>, DialogTreeError> {
        self.body.get_or_try_init(|| self.decode_body())
    }

    pub fn mutate<Mutator>(
        &'a self,
        mutator: Mutator,
    ) -> Result<Node<'a, Key, Value>, DialogTreeError>
    where
        Mutator: FnOnce(&'a NodeBody<'a, Key, Value>) -> NodeBody<'a, Key, Value>,
    {
        let body = self.body()?;
        let mutated_body = mutator(body);

        let mut next_buffer = vec![];
        match &mutated_body {
            NodeBody::Branch { branch, .. } => {
                next_buffer.push(u8::from(NodeType::Branch));
                encode(branch, &mut next_buffer)?;
            }
            NodeBody::Segment(segment) => {
                next_buffer.push(u8::from(NodeType::Segment));
                encode(segment, &mut next_buffer)?;
            }
            NodeBody::Boundary => (),
        }

        Ok(Node::new_with_buffer(Bytes::from(next_buffer), true))
    }
}
