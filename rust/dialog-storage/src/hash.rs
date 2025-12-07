use base58::ToBase58;
use dialog_common::ConditionalSync;
use serde::{Serialize, de::DeserializeOwned};

/// The representation of a common hash type (BLAKE3, in this case) that will
/// often be used as the [`KeyType`] for [`StorageBackend`]s and the
/// [`HashType`] for [`Encoder`]s.
pub type Blake3Hash = [u8; 32];

/// A trait that can be implemented for types that represent a hash.
pub trait HashType:
    Clone + AsRef<[u8]> + ConditionalSync + Serialize + DeserializeOwned + std::fmt::Debug + PartialEq
{
    /// The size of this hash type in bytes
    const SIZE: usize;

    /// Format the hash as a display string
    fn display(&self) -> String {
        format!("#{}...", self.as_ref()[0..6].to_base58())
    }
}

// Implement HashType for blake3 or any 32 byte hash
impl HashType for [u8; 32] {
    const SIZE: usize = 32;
}
