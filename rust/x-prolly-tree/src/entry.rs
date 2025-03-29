use crate::KeyType;

/// A key-value entry in a tree.
#[derive(Clone, Debug, PartialEq)]
pub struct Entry<Key, Value>
where
    Key: KeyType,
{
    /// The key in this key/value pair.
    pub key: Key,
    /// The value in this key/value pair.
    pub value: Value,
}

impl<Key, Value> Entry<Key, Value>
where
    Key: KeyType,
{
    /// Create a new [`Entry`].
    pub fn new(key: Key, value: Value) -> Self {
        Entry { key, value }
    }
}
