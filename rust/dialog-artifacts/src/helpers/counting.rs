use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_common::{Blake3Hash, Buffer, Held, Holds};
use dialog_effects::archive::{self, ArchiveError, Get};
use dialog_effects::blob;
use dialog_search_tree::helpers::Blocks;

use crate::ArchiveDelta;

/// [`Blocks`] that count the reads reaching them, through both lanes,
/// and how many were in flight at once.
///
/// Every load yields once before it is answered, so loads polled
/// concurrently are genuinely in flight together and show up in
/// [`peak_reads_in_flight`](Self::peak_reads_in_flight).
#[derive(Clone, Default)]
pub struct CountingBlocks {
    blocks: Blocks,
    reads: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl CountingBlocks {
    /// An empty set of blocks, counting.
    pub fn new() -> Self {
        Self::default()
    }

    /// Keeps every node and spilled value `delta` staged, emptying it,
    /// without counting.
    pub fn flush(&self, delta: &mut ArchiveDelta) {
        delta.flush_into(&self.blocks);
    }

    /// How many loads reached these blocks.
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    /// The most loads ever in flight at once since the last
    /// [`reset`](Self::reset).
    pub fn peak_reads_in_flight(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    /// Forgets every load counted so far.
    pub fn reset(&self) {
        self.reads.store(0, Ordering::SeqCst);
        self.peak.store(0, Ordering::SeqCst);
    }

    async fn load(&self, hash: &Blake3Hash) -> Option<Buffer> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        let mut yielded = false;
        poll_fn(|context| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                context.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
        let block = self.blocks.get(hash);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        block
    }
}

/// Holds nothing, so every node read is a counted read. Wrap it in a
/// [`Holding`](dialog_search_tree::helpers::Holding) to keep the nodes read.
impl Holds for CountingBlocks {
    fn held(&self, _key: &str) -> Option<Held> {
        None
    }

    fn hold(&self, _key: String, _handle: Held) {}
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::Load> for CountingBlocks {
    async fn execute(&self, load: archive::Load) -> Result<Option<Buffer>, ArchiveError> {
        Ok(self.load(&load.digest).await)
    }
}

/// A local-only read of a block counts as a read too.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Get> for CountingBlocks {
    async fn execute(&self, get: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        use dialog_effects::archive::prelude::GetExt as _;
        Ok(self
            .load(get.digest())
            .await
            .map(|block| block.as_ref().to_vec()))
    }
}

/// A spilled value is kept as a block, and its read is counted alike.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<blob::Load> for CountingBlocks {
    async fn execute(&self, load: blob::Load) -> Result<Option<Buffer>, ArchiveError> {
        Ok(self.load(&load.digest).await)
    }
}
