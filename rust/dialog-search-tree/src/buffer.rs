use std::sync::{Arc, OnceLock};

use dialog_common::Blake3Hash;
use rkyv::util::AlignedVec;

/// A reference-counted buffer with lazy hash computation.
///
/// Buffers store serialized node data with efficient cloning and on-demand
/// hash calculation. The bytes are held in an [`AlignedVec`] because node
/// bodies are accessed in place as rkyv archives, which requires the buffer
/// to satisfy the archived type's alignment. A plain `Vec<u8>` carries no
/// alignment guarantee at all: native allocators happen to return 8- or
/// 16-aligned blocks, masking the requirement, while the wasm allocator is
/// free to return odd addresses for align-1 allocations, which surfaced as
/// "unaligned pointer" errors when accessing nodes loaded from storage.
#[derive(Clone, Debug)]
pub struct Buffer(Arc<(AlignedVec, OnceLock<Blake3Hash>, OnceLock<()>)>);

impl Buffer {
    /// Returns the [`Blake3Hash`] of this buffer's contents, computing it if
    /// necessary.
    pub fn blake3_hash(&self) -> &Blake3Hash {
        self.0
            .1
            .get_or_init(|| Blake3Hash::hash(self.0.0.as_slice()))
    }

    /// Records that this buffer's contents passed archive validation, so
    /// subsequent accesses can skip the (linear) bytecheck pass. The marker
    /// is shared across clones, including copies handed out by the node
    /// cache and the delta. Buffers are immutable, so a single successful
    /// validation holds for the buffer's lifetime.
    pub(crate) fn mark_validated(&self) {
        let _ = self.0.2.set(());
    }

    /// Returns whether this buffer's contents have already passed archive
    /// validation.
    pub(crate) fn is_validated(&self) -> bool {
        self.0.2.get().is_some()
    }

    /// Converts this [`Buffer`] into an owned `Vec<u8>`.
    ///
    /// This always copies: the bytes live in an [`AlignedVec`], whose
    /// allocation cannot be handed over to a `Vec<u8>` (the two deallocate
    /// with different alignments).
    pub fn into_vec(self) -> Vec<u8> {
        self.0.0.as_slice().to_vec()
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self.0.0.as_slice()
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(value: Vec<u8>) -> Self {
        Self::from(value.as_slice())
    }
}

impl From<&[u8]> for Buffer {
    fn from(value: &[u8]) -> Self {
        let mut bytes = AlignedVec::with_capacity(value.len());
        bytes.extend_from_slice(value);
        Self(Arc::new((bytes, OnceLock::new(), OnceLock::new())))
    }
}

impl From<AlignedVec> for Buffer {
    fn from(value: AlignedVec) -> Self {
        Self(Arc::new((value, OnceLock::new(), OnceLock::new())))
    }
}

#[cfg(test)]
mod tests {
    use super::Buffer;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[dialog_common::test]
    fn it_aligns_bytes_from_arbitrary_sources() {
        // Node bodies are accessed in place as rkyv archives, so every
        // buffer must satisfy rkyv's alignment regardless of where the
        // bytes came from (storage backends return plain `Vec<u8>` with no
        // alignment guarantee). Empty buffers are exempt: they hold a
        // dangling (never dereferenced) pointer.
        let empty = Buffer::from(Vec::new());
        assert_eq!(empty.as_ref(), &[] as &[u8]);

        for len in [1usize, 3, 7, 64, 4096] {
            let bytes = vec![0xABu8; len];
            let buffer = Buffer::from(bytes.clone());
            assert_eq!(buffer.as_ref(), &bytes[..]);
            assert_eq!(
                buffer.as_ref().as_ptr() as usize % 16,
                0,
                "buffer of length {len} must be 16-byte aligned"
            );
        }
    }
}
