use dialog_common::ConditionalSync;
use serde::{Serialize, de::DeserializeOwned};

/// A key used to reference values in a [Tree] or [Node].
pub trait KeyType:
    std::fmt::Debug
    + AsRef<[u8]>
    + TryFrom<Vec<u8>>
    + ConditionalSync
    + Clone
    + PartialEq
    + Ord
    + Serialize
    + DeserializeOwned
{
}

impl KeyType for Vec<u8> {}

/// A value that may be stored within a [Tree]
pub trait ValueType:
    std::fmt::Debug + ConditionalSync + Clone + Serialize + DeserializeOwned
{
}

impl ValueType for Vec<u8> {}
