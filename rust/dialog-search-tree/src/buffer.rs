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
