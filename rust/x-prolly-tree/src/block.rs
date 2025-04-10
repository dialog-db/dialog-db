use nonempty::NonEmpty;
use x_storage::{HashType, XStorageError};

use crate::{Entry, KeyType, ReadFrom, Reader, Reference, ValueType, WriteInto, XProllyTreeError};

/// The serializable construct representing a [`Node`].
/// A [`Block`] is what is stored in a [`BlockStore`],
/// used to hydrate and store nodes.
#[derive(Clone, Debug, PartialEq)]
pub enum Block<const HASH_SIZE: usize, Key, Value, Hash>
where
    Key: KeyType,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
{
    /// A block representing a Branch.
    Branch(NonEmpty<Reference<HASH_SIZE, Key, Hash>>),
    /// A block representing a Segment.
    Segment(NonEmpty<Entry<Key, Value>>),
}

impl<const HASH_SIZE: usize, Key, Value, Hash> Block<HASH_SIZE, Key, Value, Hash>
where
    Key: KeyType,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
{
    /// Create a new branch-type block.
    pub fn branch(data: NonEmpty<Reference<HASH_SIZE, Key, Hash>>) -> Self {
        Block::Branch(data)
    }

    /// Create a new segment-type block.
    pub fn segment(data: NonEmpty<Entry<Key, Value>>) -> Self {
        Block::Segment(data)
    }

    /// Whether this block is a branch.
    pub fn is_branch(&self) -> bool {
        match self {
            Block::Branch(_) => true,
            _ => false,
        }
    }

    /// Whether this block is a segment.
    pub fn is_segment(&self) -> bool {
        !self.is_branch()
    }

    /// Get the upper bounds that this block represents.
    pub fn upper_bound(&self) -> &Key {
        match self {
            Block::Branch(data) => &data.last().upper_bound(),
            Block::Segment(data) => &data.last().key,
        }
    }

    /// Get children data as [`Reference`]s.
    ///
    /// The result is an error if this [`Node`] is a segment.
    pub fn references(
        &self,
    ) -> Result<&NonEmpty<Reference<HASH_SIZE, Key, Hash>>, XProllyTreeError> {
        match self {
            Block::Branch(data) => Ok(&data),
            Block::Segment(_) => Err(XProllyTreeError::IncorrectTreeAccess(
                "Cannot read references from a segment".into(),
            )),
        }
    }

    /// Takes children data as [`Reference`]s.
    ///
    /// The result is an error if this [`Node`] is a segment.
    pub fn into_references(
        self,
    ) -> Result<NonEmpty<Reference<HASH_SIZE, Key, Hash>>, XProllyTreeError> {
        match self {
            Block::Branch(data) => Ok(data),
            Block::Segment(_) => Err(XProllyTreeError::IncorrectTreeAccess(
                "Cannot take references from a segment".into(),
            )),
        }
    }

    /// Get children data as [`Entry`]s.
    ///
    /// The result is an error if this [`Node`] is a branch
    pub fn entries(&self) -> Result<&NonEmpty<Entry<Key, Value>>, XProllyTreeError> {
        match self {
            Block::Branch(_) => Err(XProllyTreeError::IncorrectTreeAccess(
                "Cannot read entries from a branch".into(),
            )),
            Block::Segment(data) => Ok(&data),
        }
    }

    /// Take children data as [`Entry`]s.
    ///
    /// The result is an error if this [`Node`] is a branch
    pub fn into_entries(self) -> Result<NonEmpty<Entry<Key, Value>>, XProllyTreeError> {
        match self {
            Block::Branch(_) => Err(XProllyTreeError::IncorrectTreeAccess(
                "Cannot take entries from a branch".into(),
            )),
            Block::Segment(data) => Ok(data),
        }
    }
}

/// A [`BlockType`] contains variants that represent the kinds of blocks that
/// may occur within a tree structure (either a branch or a segment)
#[repr(u8)]
#[derive(Clone, Copy)]
pub enum BlockType {
    /// A branch (non-leaf node)
    Branch = 0,
    /// A segment (leaf node)
    Segment = 1,
}

impl<const HASH_SIZE: usize, Key, Value, Hash> From<&Block<HASH_SIZE, Key, Value, Hash>>
    for BlockType
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
{
    fn from(value: &Block<HASH_SIZE, Key, Value, Hash>) -> Self {
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
    type Error = XStorageError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => BlockType::Branch,
            1 => BlockType::Segment,
            _ => {
                return Err(XStorageError::DecodeFailed(format!(
                    "Byte does not represent a block type: {:x}",
                    value
                )));
            }
        })
    }
}

impl WriteInto for BlockType {
    type Error = XStorageError;

    fn write_into(&self, writer: &mut crate::Writer) -> Result<(), Self::Error> {
        writer.write_u8(u8::from(*self))
    }
}

impl<'a> ReadFrom<'a> for BlockType {
    type Error = XStorageError;

    fn read_from<'r>(reader: &'r Reader<'a>) -> Result<BlockType, Self::Error>
    where
        'r: 'a,
    {
        reader.read_u8()?.try_into()
    }
}

// use x_storage::Storage;
