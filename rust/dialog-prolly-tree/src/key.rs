use dialog_common::ConditionalSync;
use serde::{Serialize, de::DeserializeOwned};

/// A key used to reference values in a [Tree] or [Node].
pub trait KeyType:
    std::fmt::Debug
    + TryFrom<Vec<u8>>
    + ConditionalSync
    + Clone
    + PartialEq
    + Ord
    + Serialize
    + DeserializeOwned
{
    /// Get the raw bytes of this [`KeyType`]
    fn bytes(&self) -> &[u8];
}

impl KeyType for Vec<u8> {
    fn bytes(&self) -> &[u8] {
        self.as_ref()
    }
}

/// A value that may be stored within a [Tree]
pub trait ValueType:
    std::fmt::Debug + ConditionalSync + Clone + Serialize + DeserializeOwned
{
    /// Compute a deterministic hash of this value for conflict resolution.
    /// Default implementation uses BLAKE3.
    fn hash(&self) -> [u8; 32]
    where
        Self: AsRef<[u8]>,
    {
        blake3::hash(self.as_ref()).into()
    }
}

impl ValueType for Vec<u8> {}
