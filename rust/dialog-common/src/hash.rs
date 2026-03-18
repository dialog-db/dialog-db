use base58::ToBase58;
use rkyv::Archive;
use thiserror::Error;
use zerocopy_derive::{FromBytes, Immutable, KnownLayout};

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
    Clone,
    Debug,
    Hash,
    Default,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    FromBytes,
    Immutable,
    KnownLayout,
)]
#[repr(transparent)]
pub struct Blake3Hash([u8; 32]);

impl ArchivedBlake3Hash {
    /// Returns the hash bytes as a 32-byte array reference.
    pub fn bytes(&self) -> &[u8; BLAKE3_HASH_SIZE] {
        &self.0
    }
}

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

    /// Returns the hash as a byte slice.
    pub fn as_bytes(&self) -> &[u8; BLAKE3_HASH_SIZE] {
        &self.0
    }

    /// Computes the BLAKE3 hash of multiple byte slices.
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

    /// Checks if the hash of the given bytes matches this hash.
    pub fn matches(&self, bytes: &[u8]) -> bool {
        Self::hash(bytes) == *self
    }
}

impl AsRef<[u8]> for Blake3Hash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; 32]> for Blake3Hash {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl TryFrom<Vec<u8>> for Blake3Hash {
    type Error = ConversionError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let len = bytes.len();
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| ConversionError::InvalidSize(len))?;
        Ok(Self(arr))
    }
}

impl std::fmt::Display for Blake3Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "blake3#{}", self.0.to_base58())
    }
}

impl<'a> From<&'a ArchivedBlake3Hash> for &'a Blake3Hash {
    fn from(value: &'a ArchivedBlake3Hash) -> Self {
        zerocopy::transmute_ref!(value.bytes())
    }
}

/// Error returned when trying to convert byte arry to a Blake3Hash.
#[derive(Error, Debug, Clone)]
pub enum ConversionError {
    /// Wrong number of bytes
    #[error("Expected {BLAKE3_HASH_SIZE} bytes, got {0}")]
    InvalidSize(usize),
}

/// A null hash consisting of all zero bytes.
///
/// This is used as a sentinel value to represent an empty or uninitialized state.
pub const NULL_BLAKE3_HASH: &Blake3Hash = &Blake3Hash([0u8; 32]);
