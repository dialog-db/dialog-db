//! A store to answer with, for tests and for whoever is wiring a peer up
//! before they have a repository behind it.

use std::collections::HashMap;
use std::sync::Mutex;

use dialog_capability::{Capability, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::archive::ArchiveError;
use dialog_effects::{archive, memory};

/// A store that keeps blocks in memory and records nothing else.
///
/// Enough to answer the archive effects truthfully — a put is readable
/// by a later get, which is the property a sync test actually leans on —
/// and honest about the rest: the memory effects report that they are
/// not implemented rather than pretending to succeed.
#[derive(Debug, Default)]
pub struct Volatile {
    blocks: Mutex<HashMap<Blake3Hash, Vec<u8>>>,
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
