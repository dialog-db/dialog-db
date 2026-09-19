//! A store to answer with, for tests and for whoever is wiring a peer up
//! before they have a repository behind it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dialog_capability::{Capability, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::archive::ArchiveError;
use dialog_effects::blob::{BlobError, BlobReader, BlobSink, BlobSource, BlobWriter};
use dialog_effects::{archive, blob, memory};

/// A store that keeps blocks in memory and records nothing else.
///
/// Enough to answer the archive effects truthfully — a put is readable
/// by a later get, which is the property a sync test actually leans on —
/// and honest about the rest: the memory effects report that they are
/// not implemented rather than pretending to succeed.
#[derive(Debug, Default)]
pub struct Volatile {
    blocks: Mutex<HashMap<Blake3Hash, Vec<u8>>>,
    blobs: Arc<Mutex<HashMap<Blake3Hash, Vec<u8>>>>,
}

impl Volatile {
    /// How many blocks have been stored.
    pub fn len(&self) -> usize {
        self.blocks.lock().expect("not poisoned").len()
    }

    /// Whether anything has been stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bytes stored under `digest`, if any.
    pub fn get(&self, digest: &Blake3Hash) -> Option<Vec<u8>> {
        self.blocks
            .lock()
            .expect("not poisoned")
            .get(digest)
            .cloned()
    }

    /// The blob stored under `digest`, if any.
    pub fn blob(&self, digest: &Blake3Hash) -> Option<Vec<u8>> {
        self.blobs
            .lock()
            .expect("not poisoned")
            .get(digest)
            .cloned()
    }
}

/// Yields the whole (already ranged) blob as one chunk.
///
/// A store that holds blobs in a `HashMap` has nothing to stream from,
/// so it does not pretend to: the transport chunks what it is given, and
/// the peer on the other end cannot tell the difference.
struct WholeBlob(Option<Vec<u8>>);

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BlobSource for WholeBlob {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        Ok(self.0.take())
    }
}

/// Accumulates bytes while hashing, and commits on finish.
///
/// `expected` is the declared digest of an import, checked before
/// anything is stored; an ingest leaves it `None` and keeps whatever
/// hash the bytes turn out to have.
struct CollectBlob {
    blobs: Arc<Mutex<HashMap<Blake3Hash, Vec<u8>>>>,
    buffer: Vec<u8>,
    expected: Option<Blake3Hash>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BlobSink for CollectBlob {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<Blake3Hash, BlobError> {
        let CollectBlob {
            blobs,
            buffer,
            expected,
        } = *self;
        let digest = Blake3Hash::from(*blake3::hash(&buffer).as_bytes());

        if let Some(expected) = expected
            && digest != expected
        {
            return Err(BlobError::DigestMismatch {
                expected: format!("{expected:?}"),
                actual: format!("{digest:?}"),
            });
        }

        blobs
            .lock()
            .expect("not poisoned")
            .insert(digest.clone(), buffer);
        Ok(digest)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<blob::Read> for Volatile {
    async fn execute(&self, input: Capability<blob::Read>) -> Result<BlobReader, BlobError> {
        let blob::Read { digest, range } = &input.constraint;
        let bytes = self
            .blob(digest)
            .ok_or_else(|| BlobError::NotFound(format!("{digest:?}")))?;

        let bytes = match range {
            None => bytes,
            Some(range) => {
                let from = (range.offset as usize).min(bytes.len());
                let to = match range.length {
                    None => bytes.len(),
                    Some(length) => from.saturating_add(length as usize).min(bytes.len()),
                };
                bytes[from..to].to_vec()
            }
        };

        Ok(Box::new(WholeBlob(Some(bytes))))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<blob::Write> for Volatile {
    async fn execute(&self, _input: Capability<blob::Write>) -> Result<BlobWriter, BlobError> {
        Ok(Box::new(CollectBlob {
            blobs: self.blobs.clone(),
            buffer: Vec::new(),
            expected: None,
        }))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<blob::Import> for Volatile {
    async fn execute(&self, input: Capability<blob::Import>) -> Result<BlobWriter, BlobError> {
        Ok(Box::new(CollectBlob {
            blobs: self.blobs.clone(),
            buffer: Vec::new(),
            expected: Some(input.constraint.digest.clone()),
        }))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<archive::Put> for Volatile {
    async fn execute(&self, input: Capability<archive::Put>) -> Result<(), ArchiveError> {
        let block = &input.constraint.block;
        self.blocks
            .lock()
            .expect("not poisoned")
            .insert(block.blake3_hash().clone(), block.as_ref().to_vec());
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<archive::Get> for Volatile {
    async fn execute(
        &self,
        input: Capability<archive::Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        Ok(self.get(&input.constraint.digest))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<archive::Import> for Volatile {
    async fn execute(&self, input: Capability<archive::Import>) -> Result<(), ArchiveError> {
        let mut blocks = self.blocks.lock().expect("not poisoned");
        for block in &input.constraint.blocks {
            blocks.insert(block.blake3_hash().clone(), block.as_ref().to_vec());
        }
        Ok(())
    }
}

/// The memory effects are declined rather than faked: a cell store with
/// compare-and-swap is not something to approximate, and a test that
/// needs one should say so by using a real repository.
macro_rules! declines {
    ($effect:ty, $output:ty) => {
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl Provider<$effect> for Volatile {
            async fn execute(&self, _: Capability<$effect>) -> $output {
                Err(memory::MemoryError::Storage(
                    "this store keeps blocks only".into(),
                ))
            }
        }
    };
}

declines!(
    memory::Resolve,
    Result<Option<memory::Edition<Vec<u8>>>, memory::MemoryError>
);
declines!(memory::Publish, Result<memory::Version, memory::MemoryError>);
declines!(memory::Retract, Result<(), memory::MemoryError>);
