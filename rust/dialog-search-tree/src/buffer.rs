use std::sync::{Arc, OnceLock};

use dialog_common::Blake3Hash;

/// A reference-counted buffer with lazy hash computation.
///
/// Buffers store serialized node data with efficient cloning
/// and on-demand hash calculation.
#[derive(Clone, Debug)]
pub struct Buffer(Arc<(Vec<u8>, OnceLock<Blake3Hash>)>);

impl Buffer {
    /// Returns the [`Blake3Hash`] of this buffer's contents, computing it if
    /// necessary.
    pub fn blake3_hash(&self) -> &Blake3Hash {
        self.0.1.get_or_init(|| Blake3Hash::hash(&self.0.0))
    }

    /// Converts this [`Buffer`] into an owned `Vec<u8>`. This method will try
    /// to unwrap the interior smart pointer and pass back the bytes (rather than
    /// naively cloning them) in the case that there are no other strong references
    /// to them.
    pub fn into_vec(self) -> Vec<u8> {
        Arc::try_unwrap(self.0)
            .map(|(bytes, _)| bytes)
            .unwrap_or_else(|arc| arc.0.clone())
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self.0.0.as_ref()
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(value: Vec<u8>) -> Self {
        Self(Arc::new((value, OnceLock::new())))
    }
}
