use std::fmt::{Formatter, Result as FmtResult};
use std::sync::{Arc, OnceLock};

use rkyv::util::AlignedVec;
use serde::de::{Error as DeserializeError, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Blake3Hash;

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
pub struct Buffer(Arc<(AlignedVec, OnceLock<Blake3Hash>)>);

impl Buffer {
    /// Returns the [`Blake3Hash`] of this buffer's contents, computing it if
    /// necessary.
    pub fn blake3_hash(&self) -> &Blake3Hash {
        self.0
            .1
            .get_or_init(|| Blake3Hash::hash(self.0.0.as_slice()))
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

/// Buffers compare by content (with a pointer-equality fast path for
/// clones sharing the same allocation).
impl PartialEq for Buffer {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.as_ref() == other.as_ref()
    }
}

impl Eq for Buffer {}

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
        Self(Arc::new((bytes, OnceLock::new())))
    }
}

impl From<AlignedVec> for Buffer {
    fn from(value: AlignedVec) -> Self {
        Self(Arc::new((value, OnceLock::new())))
    }
}

/// Serializes as raw bytes. Only exercised when a buffer crosses a wire
/// boundary (e.g. a remote invocation); in-process effect dispatch passes
/// buffers by reference-counted handle without touching this.
impl Serialize for Buffer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.as_ref())
    }
}

/// Deserializes from raw bytes, realigning them (see the type docs).
impl<'de> Deserialize<'de> for Buffer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BufferVisitor;

        impl<'de> Visitor<'de> for BufferVisitor {
            type Value = Buffer;

            fn expecting(&self, formatter: &mut Formatter) -> FmtResult {
                formatter.write_str("a byte buffer")
            }

            fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Buffer, E>
            where
                E: DeserializeError,
            {
                Ok(Buffer::from(bytes))
            }

            fn visit_byte_buf<E>(self, bytes: Vec<u8>) -> Result<Buffer, E>
            where
                E: DeserializeError,
            {
                Ok(Buffer::from(bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Buffer, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(byte) = seq.next_element::<u8>()? {
                    bytes.push(byte);
                }
                Ok(Buffer::from(bytes))
            }
        }

        deserializer.deserialize_bytes(BufferVisitor)
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
