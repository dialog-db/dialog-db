use std::sync::Arc;

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer};
use hashbrown::HashMap;
use parking_lot::RwLock;

use crate::{Delta, DialogSearchTreeError, LoadBlock};

/// Blocks held in memory under their content hash, loadable by the tree.
///
/// The tree's [`LoadBlock`] for tests and tools that have no archive: persist a
/// tree into a [`Delta`], [`flush`](Self::flush) the delta here, and read the
/// tree back through this. Clones share the same blocks.
#[derive(Clone, Default)]
pub struct MemoryBlocks {
    blocks: Arc<RwLock<HashMap<Blake3Hash, Buffer>>>,
}

impl MemoryBlocks {
    /// An empty set of blocks.
    pub fn new() -> Self {
        Self::default()
    }

    /// Keeps `block` under its content hash.
    pub fn store(&self, block: Buffer) {
        self.blocks
            .write()
            .insert(block.blake3_hash().clone(), block);
    }

    /// Keeps every block `delta` holds, emptying it.
    pub fn flush(&self, delta: &mut Delta<Blake3Hash, Buffer>) {
        let mut blocks = self.blocks.write();
        for (hash, block) in delta.flush() {
            blocks.insert(hash, block);
        }
    }

    /// The block stored under `hash`, if any.
    pub fn get(&self, hash: &Blake3Hash) -> Option<Buffer> {
        self.blocks.read().get(hash).cloned()
    }

    /// Keeps `bytes` under `hash` whether or not they hash to it, to stand
    /// in for a store that returns corrupt blocks.
    pub fn corrupt(&self, hash: Blake3Hash, bytes: Vec<u8>) {
        self.blocks.write().insert(hash, Buffer::from(bytes));
    }

    /// Drops the block stored under `hash`, to stand in for a store that
    /// does not hold it.
    pub fn forget(&self, hash: &Blake3Hash) {
        self.blocks.write().remove(hash);
    }

    /// How many blocks are stored.
    pub fn len(&self) -> usize {
        self.blocks.read().len()
    }

    /// Whether no block is stored.
    pub fn is_empty(&self) -> bool {
        self.blocks.read().is_empty()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<LoadBlock> for MemoryBlocks {
    async fn execute(
        &self,
        LoadBlock { hash }: LoadBlock,
    ) -> Result<Option<Buffer>, DialogSearchTreeError> {
        Ok(self.get(&hash))
    }
}
