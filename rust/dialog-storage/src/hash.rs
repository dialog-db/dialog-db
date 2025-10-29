use base58::ToBase58;
use dialog_common::ConditionalSync;
use serde::{Serialize, de::DeserializeOwned};

/// The representation of a common hash type (BLAKE3, in this case) that will
/// often be used as the [`KeyType`] for [`StorageBackend`]s and the
/// [`HashType`] for [`Encoder`]s.
pub type Blake3Hash = [u8; 32];

/// A trait that can be implemented for types that represent a hash. A blanket
/// "unchecked" implementation is provided for any type that matches
/// `AsRef<[u8]>` (this might be an antipattern; more investigation required).
pub trait HashType<const SIZE: usize>:
    Clone + AsRef<[u8]> + ConditionalSync + Serialize + DeserializeOwned + std::fmt::Debug + PartialEq
{
    /// Get the raw bytes of the hash
    fn bytes(&self) -> [u8; SIZE];

    /// Format the hash as a display string
    fn display(&self) -> String {
        format!("#{}...", self.bytes()[0..6].to_base58())
    }
}

impl<const SIZE: usize, T> HashType<SIZE> for T
where
    T: Clone
        + AsRef<[u8]>
        + ConditionalSync
        + Serialize
        + DeserializeOwned
        + std::fmt::Debug
        + PartialEq
        + Eq,
{
    fn bytes(&self) -> [u8; SIZE] {
        let mut bytes = [0u8; SIZE];
        bytes.copy_from_slice(&self.as_ref()[..SIZE.min(self.as_ref().len())]);
        bytes
    }
}
