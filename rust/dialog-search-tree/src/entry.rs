use crate::{Key, SymmetryWith, Value};
use dialog_common::Blake3Hash;
use rkyv::{Archive, Deserialize, Serialize};

/// A key-value pair stored in the tree.
#[derive(Clone, Debug, Archive, Deserialize, Serialize)]
pub struct Entry<Key, Value> {
    /// The key for this entry.
    pub key: Key,
    /// The value associated with the key.
    pub value: Value,
}

impl<Key, Value> Entry<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value,
{
    /// Computes the [`Blake3Hash`] of the entry's key.
    pub fn key_hash(&self) -> Blake3Hash {
        Blake3Hash::hash(self.key.as_ref())
    }
}
