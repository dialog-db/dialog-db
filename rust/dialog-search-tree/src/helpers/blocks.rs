use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer};
use parking_lot::Mutex;

use crate::{Delta, DialogSearchTreeError, Load, MemoryBlocks};

/// [`MemoryBlocks`] that journal every load, in order, while journaling is
/// on.
///
/// The tree specs switch the journal off while they build and inspect a
/// tree, so what it holds is exactly the loads the code under test made.
#[derive(Clone)]
pub struct JournaledBlocks {
    blocks: MemoryBlocks,
    loads: Arc<Mutex<Vec<Blake3Hash>>>,
    journaling: Arc<AtomicBool>,
}

impl Default for JournaledBlocks {
    fn default() -> Self {
        Self {
            blocks: MemoryBlocks::default(),
            loads: Arc::default(),
            journaling: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl JournaledBlocks {
    /// An empty set of blocks, journaling.
    pub fn new() -> Self {
        Self::default()
    }

    /// Journals loads of `blocks`, which other handles may share, into a
    /// journal of its own.
    pub fn over(blocks: MemoryBlocks) -> Self {
        Self {
            blocks,
            ..Self::default()
        }
    }

    /// The blocks underneath, which load without journaling.
    pub fn blocks(&self) -> &MemoryBlocks {
        &self.blocks
    }

    /// Keeps `block` under its content hash.
    pub fn store(&self, block: Buffer) {
        self.blocks.store(block);
    }

    /// Keeps every block `delta` holds, emptying it.
    pub fn flush(&self, delta: &mut Delta<Blake3Hash, Buffer>) {
        self.blocks.flush(delta);
    }

    /// The block stored under `hash`, if any, without journaling the read.
    pub fn get(&self, hash: &Blake3Hash) -> Option<Buffer> {
        self.blocks.get(hash)
    }

    /// Every load journaled since the last [`clear_journal`](Self::clear_journal).
    pub fn get_reads(&self) -> Vec<Blake3Hash> {
        self.loads.lock().clone()
    }

    /// How many loads were journaled.
    pub fn read_count(&self) -> usize {
        self.loads.lock().len()
    }

    /// Forgets every journaled load.
    pub fn clear_journal(&self) {
        self.loads.lock().clear();
    }

    /// Stops journaling loads.
    pub fn disable_journal(&self) {
        self.journaling.store(false, Ordering::SeqCst);
    }

    /// Resumes journaling loads.
    pub fn enable_journal(&self) {
        self.journaling.store(true, Ordering::SeqCst);
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Load> for JournaledBlocks {
    async fn execute(&self, hash: Blake3Hash) -> Result<Option<Buffer>, DialogSearchTreeError> {
        if self.journaling.load(Ordering::SeqCst) {
            self.loads.lock().push(hash.clone());
        }
        Ok(self.blocks.get(&hash))
    }
}
