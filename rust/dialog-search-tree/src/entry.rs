use crate::{Key, Value};
use dialog_common::Blake3Hash;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Clone, Debug, Archive, Deserialize, Serialize)]
pub struct Entry<Key, Value> {
    pub key: Key,
    pub value: Value,
}

impl<Key, Value> Entry<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key>,
    Value: self::Value,
{
    pub fn key_hash(&self) -> Blake3Hash {
        Blake3Hash::hash(self.key.as_ref())
    }
}
