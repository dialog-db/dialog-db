use crate::{Key, Value};
use dialog_common::Blake3Hash;

/// A key-value pair stored in the tree.
#[derive(Clone, Debug)]
pub struct Entry<Key, Value> {
    /// The key for this entry.
    pub key: Key,
    /// The value associated with the key.
    pub value: Value,
}

impl<Key, Value> Entry<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
{
    /// Computes the [`Blake3Hash`] of the entry's key.
    pub fn key_hash(&self) -> Blake3Hash {
        Blake3Hash::hash(self.key.as_ref())
    }
}
