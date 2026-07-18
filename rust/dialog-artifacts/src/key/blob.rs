//! Blob-index key: the `BLOB`-tagged key layout.
//!
//! The blob index is the fifth ordering carried in the artifact tree, after
//! the EAV / AEV / VAE indexes and the reserved history index. Its keys are
//! blob hashes under [`BLOB_KEY_TAG`]: the 32-byte hash sits immediately after
//! the tag byte so blobs sort contiguously by hash, and the remaining key
//! bytes are zero. Unlike the EAV/AEV/VAE views it carries no entity /
//! attribute / value fields — a blob is named by its hash alone.

use dialog_storage::Blake3Hash;

use crate::{Key, TAG_LENGTH};

/// Tag byte identifying blob-index keys (the fifth index).
pub const BLOB_KEY_TAG: u8 = 4;

/// Number of hash bytes carried in a blob key.
const BLOB_HASH_LENGTH: usize = 32;
/// Offset of the blob hash within the key (immediately after the tag).
const BLOB_HASH_OFFSET: usize = TAG_LENGTH;

/// A view over a [`Key`] in the blob index.
///
/// Layout: `BLOB_KEY_TAG ‖ blob_hash (32)`. Blob keys therefore occupy a
/// contiguous, hash-ordered range disjoint from every other index.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlobKey(pub Key);

impl BlobKey {
    /// Construct the blob-index key for a blob hash.
    pub fn new(hash: &Blake3Hash) -> Self {
        let mut bytes = Vec::with_capacity(TAG_LENGTH + BLOB_HASH_LENGTH);
        bytes.push(BLOB_KEY_TAG);
        bytes.extend_from_slice(hash);
        Self(Key::from(bytes))
    }

    /// The lowest key in the blob index (start of the `BLOB`-tag range): the
    /// tag byte alone, smaller than any hashed blob key.
    pub fn min() -> Self {
        Self(Key::from(vec![BLOB_KEY_TAG]))
    }

    /// The highest key in the blob index (end of the `BLOB`-tag range): the tag
    /// followed by an all-`0xFF` hash, larger than any real blob key.
    pub fn max() -> Self {
        let mut bytes = Vec::with_capacity(TAG_LENGTH + BLOB_HASH_LENGTH);
        bytes.push(BLOB_KEY_TAG);
        bytes.extend_from_slice(&[0xFFu8; BLOB_HASH_LENGTH]);
        Self(Key::from(bytes))
    }

    /// The blob hash carried by this key.
    pub fn blob_hash(&self) -> Blake3Hash {
        let bytes: &[u8] = self.0.as_ref();
        bytes[BLOB_HASH_OFFSET..BLOB_HASH_OFFSET + BLOB_HASH_LENGTH]
            .try_into()
            .expect("blob key always carries 32 hash bytes")
    }

    /// Convert into the generic tree [`Key`].
    pub fn into_key(self) -> Key {
        self.0
    }
}

impl From<&Blake3Hash> for BlobKey {
    fn from(hash: &Blake3Hash) -> Self {
        BlobKey::new(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_round_trips_the_blob_hash() {
        let hash: Blake3Hash = [7u8; 32];
        let key = BlobKey::new(&hash);
        assert_eq!(key.0.tag(), BLOB_KEY_TAG);
        assert_eq!(key.blob_hash(), hash);
    }

    #[test]
    fn it_brackets_the_blob_tag_range() {
        let hash: Blake3Hash = [7u8; 32];
        let key = BlobKey::new(&hash);
        assert!(BlobKey::min().0 <= key.0);
        assert!(key.0 <= BlobKey::max().0);
        // Disjoint from every lower-tagged index.
        assert!(BlobKey::min().0 > Key::max().set_tag(BLOB_KEY_TAG - 1));
    }

    #[test]
    fn it_orders_blobs_by_hash() {
        let lo = BlobKey::new(&[1u8; 32]);
        let hi = BlobKey::new(&[2u8; 32]);
        assert!(lo.0 < hi.0);
    }
}
