//! A store that holds every block and cell in memory: the smallest
//! provider of the effects the service performs, and the shape an
//! embedder's own provider takes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dialog_capability::{Capability, Policy, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::archive::prelude::{GetExt, PutExt};
use dialog_effects::archive::{self, ArchiveError, Catalog};
use dialog_effects::memory::prelude::{PublishExt, RetractExt};
use dialog_effects::memory::{self, Cell, Edition, MemoryError, Space, Version};

/// Blocks by `(subject, catalog, digest)`, cells by `(subject, space,
/// cell)` with the version they stand at.
#[derive(Debug, Default)]
struct Inner {
    blocks: HashMap<(String, String, Blake3Hash), Vec<u8>>,
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
}

fn block_key<Fx>(capability: &Capability<Fx>, digest: Blake3Hash) -> (String, String, Blake3Hash)
where
    Fx: Policy<Of = Catalog>,
{
    (
        capability.subject().to_string(),
        Catalog::of(capability).catalog.clone(),
        digest,
    )
}

fn cell_key<Fx>(capability: &Capability<Fx>) -> (String, String, String)
where
    Fx: Policy<Of = Cell>,
{
    (
        capability.subject().to_string(),
        Space::of(capability).space.clone(),
        Cell::of(capability).cell.clone(),
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
