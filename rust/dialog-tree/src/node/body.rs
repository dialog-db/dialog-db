mod branch;
use std::collections::BTreeMap;

pub use branch::*;

mod segment;
use dialog_common::Blake3Hash;
pub use segment::*;

use crate::{KeyBuffer, Node, ValueBuffer};

pub enum NodeBody<'a, Key, Value>
where
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
{
    Boundary,
    Branch {
        branch: Branch<'a, Key>,
        child_cache: BTreeMap<Blake3Hash, Node<'a, Key, Value>>,
    },
    Segment(Segment<'a, Key, Value>),
}
