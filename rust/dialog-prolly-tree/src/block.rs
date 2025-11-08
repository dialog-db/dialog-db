use dialog_storage::{DialogStorageError, HashType};
use nonempty::NonEmpty;
use serde::{Deserialize, Serialize};

use crate::{DialogProllyTreeError, Entry, KeyType, Reference, ValueType};

/// The serializable construct representing a [`Node`].
/// A [`Block`] is what is stored in a [`BlockStore`],
/// used to hydrate and store nodes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Block<Key, Value, Hash> {
    /// A block representing a Branch.
    Branch(NonEmpty<Reference<Key, Hash>>),
    /// A block representing a Segment.
    Segment(NonEmpty<Entry<Key, Value>>),
}

impl<Key, Value, Hash> Block<Key, Value, Hash>
where
    Key: KeyType,
    Value: ValueType,
    Hash: HashType,
{
    /// Create a new branch-type block.
    pub fn branch(data: NonEmpty<Reference<Key, Hash>>) -> Self {
        Block::Branch(data)
    }

    /// Create a new segment-type block.
    pub fn segment(data: NonEmpty<Entry<Key, Value>>) -> Self {
        Block::Segment(data)
    }

    /// Whether this block is a branch.
    pub fn is_branch(&self) -> bool {
        matches!(self, Block::Branch(_))
    }

    /// Whether this block is a segment.
    pub fn is_segment(&self) -> bool {
        !self.is_branch()
    }

    /// Get the upper bounds that this block represents.
    pub fn upper_bound(&self) -> &Key {
        match self {
            Block::Branch(data) => data.last().upper_bound(),
            Block::Segment(data) => &data.last().key,
        }
    }

    /// Get children data as [`Reference`]s.
    ///
    /// The result is an error if this [`Node`] is a segment.
    pub fn references(&self) -> Result<&NonEmpty<Reference<Key, Hash>>, DialogProllyTreeError> {
        match self {
            Block::Branch(data) => Ok(data),
            Block::Segment(_) => Err(DialogProllyTreeError::IncorrectTreeAccess(
                "Cannot read references from a segment".into(),
            )),
        }
    }

    /// Takes children data as [`Reference`]s.
    ///
    /// The result is an error if this [`Node`] is a segment.
    pub fn into_references(self) -> Result<NonEmpty<Reference<Key, Hash>>, DialogProllyTreeError> {
        match self {
            Block::Branch(data) => Ok(data),
            Block::Segment(_) => Err(DialogProllyTreeError::IncorrectTreeAccess(
                "Cannot take references from a segment".into(),
            )),
        }
    }

    /// Get children data as [`Entry`]s.
    ///
    /// The result is an error if this [`Node`] is a branch
    pub fn entries(&self) -> Result<&NonEmpty<Entry<Key, Value>>, DialogProllyTreeError> {
        match self {
            Block::Branch(_) => Err(DialogProllyTreeError::IncorrectTreeAccess(
                "Cannot read entries from a branch".into(),
            )),
            Block::Segment(data) => Ok(data),
        }
    }

    /// Take children data as [`Entry`]s.
    ///
    /// The result is an error if this [`Node`] is a branch
    pub fn into_entries(self) -> Result<NonEmpty<Entry<Key, Value>>, DialogProllyTreeError> {
        match self {
            Block::Branch(_) => Err(DialogProllyTreeError::IncorrectTreeAccess(
                "Cannot take entries from a branch".into(),
            )),
            Block::Segment(data) => Ok(data),
        }
    }
}

/// A [`BlockType`] contains variants that represent the kinds of blocks that
/// may occur within a tree structure (either a branch or a segment)
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum BlockType {
    /// A branch (non-leaf node)
    Branch = 0,
    /// A segment (leaf node)
    Segment = 1,
}

impl<Key, Value, Hash> From<&Block<Key, Value, Hash>> for BlockType
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType,
{
    fn from(value: &Block<Key, Value, Hash>) -> Self {
        match value {
            Block::Branch(_) => BlockType::Branch,
            Block::Segment(_) => BlockType::Segment,
        }
    }
}

impl From<BlockType> for u8 {
    fn from(value: BlockType) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for BlockType {
    type Error = DialogStorageError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => BlockType::Branch,
            1 => BlockType::Segment,
            _ => {
                return Err(DialogStorageError::DecodeFailed(format!(
                    "Byte does not represent a block type: {:x}",
                    value
                )));
            }
        })
    }
}
