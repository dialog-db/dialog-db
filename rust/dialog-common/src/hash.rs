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
#[derive(Clone, Default, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

    /// Returns the hash as a byte slice.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for Blake3Hash {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl TryFrom<Vec<u8>> for Blake3Hash {
    type Error = Vec<u8>;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let arr: [u8; 32] = bytes.try_into()?;
        Ok(Self(arr))
    }
}
