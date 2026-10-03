use std::any::Any;
use std::fmt::{Formatter, Result as FmtResult};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use rkyv::util::AlignedVec;
use serde::de::{Error as DeserializeError, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Blake3Hash;

/// A type-erased value memoized on a [`Buffer`]. A consumer decodes the
/// buffer's bytes once into some owned artifact and caches it here, keyed by
/// nothing but the buffer's identity; because a buffer is content-addressed and
/// immutable, that artifact never goes stale.
///
/// The `Send + Sync` are real (not `ConditionalSend`/`ConditionalSync`): a
/// type-erased trait object cannot combine the non-auto conditional markers,
/// and the memo must be shareable across the threads that share the node cache.
type Decoded = Arc<dyn Any + Send + Sync>;

/// The interior of a [`Buffer`]: the aligned bytes, the lazily-computed content
/// hash, a lazily-populated decode memo (see [`Buffer::decoded`]), and a touch
/// counter used to decide when memoizing a decode is worthwhile.
struct BufferInner {
    bytes: AlignedVec,
    hash: OnceLock<Blake3Hash>,
    decoded: OnceLock<Decoded>,
    touches: AtomicU32,
}

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
#[derive(Clone)]
pub struct Buffer(Arc<BufferInner>);

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Buffer")
            .field("len", &self.0.bytes.len())
            .finish()
    }
}

impl Buffer {
    /// Returns the [`Blake3Hash`] of this buffer's contents, computing it if
    /// necessary.
    pub fn blake3_hash(&self) -> &Blake3Hash {
        self.0
            .hash
            .get_or_init(|| Blake3Hash::hash(self.0.bytes.as_slice()))
    }

    /// Returns this buffer's already-memoized decode of type `T`, or `None` if
    /// none is memoized yet — without ever decoding. A caller that wants to
    /// populate the memo uses [`memoize_decode`](Self::memoize_decode).
    pub fn memoized<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync, // bare-send-ok: type-erased memo needs real auto-trait bounds
    {
        self.0
            .decoded
            .get()
            .and_then(|value| value.clone().downcast::<T>().ok())
    }

    /// Records a touch of this buffer and reports whether the caller should
    /// *memoize* a decode rather than compute it transiently.
    ///
    /// A buffer touched once (a single range scan visiting each leaf once) gains
    /// nothing from a cached decode and would only pay the materialization; a
    /// buffer touched repeatedly (a join re-selecting the same branch, landing
    /// on the same leaves once per outer binding) amortizes a memoized decode
    /// across every later touch. So the first touch returns `false` (decode
    /// transiently) and subsequent touches return `true` (memoize and reuse).
    pub fn should_memoize(&self) -> bool {
        self.0.touches.fetch_add(1, Ordering::Relaxed) >= 1
    }

    /// Memoizes a decode of type `T`, computing it via `decode` on the first
    /// call and reusing it thereafter.
    ///
    /// Decoding a node body (e.g. a columnar leaf's keys) is pure work over the
    /// immutable, content-addressed bytes, so the result is safe to memoize on
    /// the buffer: every clone of the buffer shares one `Arc` interior, so a
    /// value cached here is reused by every reader that holds the buffer — for
    /// instance every scan served the same node from a shared node cache — and
    /// never needs invalidation. `T` identifies the artifact (a buffer holds at
    /// most one memoized decode); returns `None` only if a different `T` was
    /// already memoized, which callers avoid by using one `T` per buffer role.
    pub fn memoize_decode<T, E>(
        &self,
        decode: impl FnOnce() -> Result<T, E>,
    ) -> Result<Option<Arc<T>>, E>
    where
        T: Any + Send + Sync, // bare-send-ok: type-erased memo needs real auto-trait bounds
    {
        // Populate the slot on the first call; ignore the race loser (both
        // computed the same pure value). `get_or_try_init` is unstable, so do
        // the fallible decode first and only memoize on success.
        if self.0.decoded.get().is_none() {
            let value: Decoded = Arc::new(decode()?);
            let _ = self.0.decoded.set(value);
        }
        Ok(self.memoized())
    }

    /// Converts this [`Buffer`] into an owned `Vec<u8>`.
    ///
    /// This always copies: the bytes live in an [`AlignedVec`], whose
    /// allocation cannot be handed over to a `Vec<u8>` (the two deallocate
    /// with different alignments).
    pub fn into_vec(self) -> Vec<u8> {
        self.0.bytes.as_slice().to_vec()
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
        self.0.bytes.as_slice()
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
        Self::from(bytes)
    }
}

impl From<AlignedVec> for Buffer {
    fn from(value: AlignedVec) -> Self {
        Self(Arc::new(BufferInner {
            bytes: value,
            hash: OnceLock::new(),
            decoded: OnceLock::new(),
            touches: AtomicU32::new(0),
        }))
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
