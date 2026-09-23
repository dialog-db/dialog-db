//! A store that holds every block, blob and cell in memory: the
//! smallest provider of the effects the service performs, and the shape
//! an embedder's own provider takes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use base58::ToBase58;
use dialog_capability::{Capability, Policy, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::archive::prelude::{GetExt, PutExt};
use dialog_effects::archive::{self, ArchiveError, Catalog};
use dialog_effects::blob::prelude::{BlobImportExt as _, BlobReadExt as _};
use dialog_effects::blob::{self, BlobError, BlobReader, BlobSink, BlobSource, BlobWriter};
use dialog_effects::memory::prelude::{PublishExt, RetractExt};
use dialog_effects::memory::{self, Cell, Edition, MemoryError, Space, Version};

/// How many bytes a blob read yields per chunk, small enough that a
/// test blob streams as several.
const CHUNK: usize = 4 * 1024;

/// Blocks by `(subject, catalog, digest)`, blobs by `(subject,
/// digest)`, cells by `(subject, space, cell)` with the version they
/// stand at.
#[derive(Debug, Default)]
struct Inner {
    blocks: HashMap<(String, String, Blake3Hash), Vec<u8>>,
    blobs: HashMap<(String, Blake3Hash), Vec<u8>>,
    cells: HashMap<(String, String, String), (Vec<u8>, Version)>,
    editions: u64,
}

/// An in-memory provider of the archive and memory effects.
///
/// Versions are text, `v1`, `v2` and so on, as the `ETag`s they travel
/// as. A publish with no version expects the cell to be empty and a
/// publish or retract with one expects the cell to stand at exactly
/// that version, which is the compare-and-swap an object store gives
/// through `If-None-Match: *` and `If-Match`.
#[derive(Debug, Clone, Default)]
pub struct MemoryStore {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryStore {
    /// How many blocks the store holds.
    pub fn blocks(&self) -> usize {
        self.inner.lock().expect("store lock").blocks.len()
    }

    /// How many cells the store holds.
    pub fn cells(&self) -> usize {
        self.inner.lock().expect("store lock").cells.len()
    }

    /// How many blobs the store holds.
    pub fn blobs(&self) -> usize {
        self.inner.lock().expect("store lock").blobs.len()
    }
}

fn block_key<V, Fx>(capability: &Capability<Fx>, digest: Blake3Hash) -> (String, String, Blake3Hash)
where
    V: dialog_effects::Method,
    V::Of: dialog_capability::Constraint,
    Fx: Policy<Of = dialog_effects::archive::Block<V>>,
{
    (
        capability.subject().to_string(),
        Catalog::<V>::of(capability).catalog.clone(),
        digest,
    )
}

fn cell_key<V, Fx>(capability: &Capability<Fx>) -> (String, String, String)
where
    V: dialog_effects::Method,
    V::Of: dialog_capability::Constraint,
    Fx: Policy<Of = Cell<V>>,
{
    (
        capability.subject().to_string(),
        Space::<V>::of(capability).space.clone(),
        Cell::<V>::of(capability).cell.clone(),
    )
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<archive::Get> for MemoryStore {
    async fn execute(
        &self,
        capability: Capability<archive::Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        let key = block_key(&capability, capability.digest().clone());
        Ok(self
            .inner
            .lock()
            .expect("store lock")
            .blocks
            .get(&key)
            .cloned())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<archive::Put> for MemoryStore {
    async fn execute(&self, capability: Capability<archive::Put>) -> Result<(), ArchiveError> {
        let content = capability.content().to_vec();
        let key = block_key(&capability, Blake3Hash::hash(&content));
        self.inner
            .lock()
            .expect("store lock")
            .blocks
            .insert(key, content);
        Ok(())
    }
}

/// A blob's bytes, or the range of them a read asked for, yielded in
/// [`CHUNK`]-sized pieces.
struct Chunks {
    bytes: Vec<u8>,
    at: usize,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BlobSource for Chunks {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        if self.at >= self.bytes.len() {
            return Ok(None);
        }
        let end = (self.at + CHUNK).min(self.bytes.len());
        let chunk = self.bytes[self.at..end].to_vec();
        self.at = end;
        Ok(Some(chunk))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<blob::Read> for MemoryStore {
    async fn execute(&self, capability: Capability<blob::Read>) -> Result<BlobReader, BlobError> {
        let digest = capability.digest().clone();
        let key = (capability.subject().to_string(), digest.clone());
        let bytes = self
            .inner
            .lock()
            .expect("store lock")
            .blobs
            .get(&key)
            .cloned()
            .ok_or_else(|| BlobError::NotFound(digest.as_bytes().to_base58()))?;
        let bytes = match capability.range() {
            Some(range) => {
                let start = (range.offset as usize).min(bytes.len());
                let end = range.length.map_or(bytes.len(), |length| {
                    (start + length as usize).min(bytes.len())
                });
                bytes[start..end].to_vec()
            }
            None => bytes,
        };
        Ok(Box::new(Chunks { bytes, at: 0 }))
    }
}

/// Gathers an import's bytes and files them under the declared digest
/// once they have been checked against it.
struct Importing {
    store: MemoryStore,
    key: (String, Blake3Hash),
    buffer: Vec<u8>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BlobSink for Importing {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<Blake3Hash, BlobError> {
        let Importing { store, key, buffer } = *self;
        let hash = Blake3Hash::hash(&buffer);
        if hash != key.1 {
            return Err(BlobError::DigestMismatch {
                expected: key.1.as_bytes().to_base58(),
                actual: hash.as_bytes().to_base58(),
            });
        }
        store
            .inner
            .lock()
            .expect("store lock")
            .blobs
            .insert(key, buffer);
        Ok(hash)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<blob::Import> for MemoryStore {
    async fn execute(&self, capability: Capability<blob::Import>) -> Result<BlobWriter, BlobError> {
        Ok(Box::new(Importing {
            store: self.clone(),
            key: (
                capability.subject().to_string(),
                capability.digest().clone(),
            ),
            buffer: Vec::new(),
        }))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<memory::Resolve> for MemoryStore {
    async fn execute(
        &self,
        capability: Capability<memory::Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        let key = cell_key(&capability);
        Ok(self
            .inner
            .lock()
            .expect("store lock")
            .cells
            .get(&key)
            .map(|(content, version)| Edition {
                content: content.clone(),
                version: version.clone(),
            }))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<memory::Publish> for MemoryStore {
    async fn execute(
        &self,
        capability: Capability<memory::Publish>,
    ) -> Result<Version, MemoryError> {
        let key = cell_key(&capability);
        let mut inner = self.inner.lock().expect("store lock");
        let current = inner.cells.get(&key).map(|(_, version)| version.clone());
        if current.as_ref() != capability.when() {
            return Err(MemoryError::VersionMismatch {
                expected: capability.when().cloned(),
                actual: current,
            });
        }
        inner.editions += 1;
        let version = Version::from(format!("v{}", inner.editions));
        inner
            .cells
            .insert(key, (capability.content().to_vec(), version.clone()));
        Ok(version)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<memory::Retract> for MemoryStore {
    async fn execute(&self, capability: Capability<memory::Retract>) -> Result<(), MemoryError> {
        let key = cell_key(&capability);
        let mut inner = self.inner.lock().expect("store lock");
        let current = inner.cells.get(&key).map(|(_, version)| version.clone());
        if current.as_ref() != Some(capability.when()) {
            return Err(MemoryError::VersionMismatch {
                expected: Some(capability.when().clone()),
                actual: current,
            });
        }
        inner.cells.remove(&key);
        Ok(())
    }
}
