use base58::ToBase58;
use rkyv::{Archive, Deserialize, Serialize};

/// The size of a BLAKE3 hash in bytes.
///
/// BLAKE3 produces 256-bit (32-byte) hashes by default.
pub const BLAKE3_HASH_SIZE: usize = 32;

/// A BLAKE3 cryptographic hash.
///
/// This is a wrapper around a 32-byte array that represents a BLAKE3 hash digest.
/// BLAKE3 is a cryptographic hash function that is fast, secure, and provides
/// consistent output across different platforms.
///
/// # Examples
///
/// ```rust
/// use dialog_common::Blake3Hash;
///
/// let data = b"hello world";
/// let hash = Blake3Hash::hash(data);
/// ```
#[derive(
    Clone, Debug, Hash, Default, Archive, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
#[repr(transparent)]
pub struct Blake3Hash([u8; 32]);

impl Blake3Hash {
    /// Computes the BLAKE3 hash of the given bytes.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The input data to hash
    ///
    /// # Returns
    ///
    /// A `Blake3Hash` containing the 32-byte hash digest.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dialog_common::Blake3Hash;
    ///
    /// let data = b"hello world";
    /// let hash = Blake3Hash::hash(data);
    /// ```
    pub fn hash(bytes: &[u8]) -> Self {
        Self(blake3::hash(bytes).into())
    }

    pub fn hash_iter<'a, I>(bytes: I) -> Self
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let mut hasher = blake3::Hasher::new();
        for chunk in bytes {
            hasher.update(chunk);
        }
        Self(hasher.finalize().into())
    }

    pub fn bytes(&self) -> &[u8; BLAKE3_HASH_SIZE] {
        &self.0
    }

    pub fn matches(&self, bytes: &[u8]) -> bool {
        Self::hash(bytes) == *self
    }
}

impl std::fmt::Display for Blake3Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "blake3#{}", self.0.to_base58())
    }
}

pub const NULL_BLAKE3_HASH: &Blake3Hash = &Blake3Hash([0u8; 32]);
