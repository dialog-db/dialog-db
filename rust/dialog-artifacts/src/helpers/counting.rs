use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::{DialogSearchTreeError, LoadBlock, MemoryBlocks};

use crate::{ArchiveDelta, DialogArtifactsError, LoadBlob};

/// [`MemoryBlocks`] that count the loads reaching them, through both lanes,
/// and how many were in flight at once.
///
/// Every load yields once before it is answered, so loads polled
/// concurrently are genuinely in flight together and show up in
/// [`peak_reads_in_flight`](Self::peak_reads_in_flight).
#[derive(Clone, Default)]
pub struct CountingBlocks {
    blocks: MemoryBlocks,
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<LoadBlock> for CountingBlocks {
    async fn execute(
        &self,
        LoadBlock { hash }: LoadBlock,
    ) -> Result<Option<Buffer>, DialogSearchTreeError> {
        Ok(self.load(&hash).await)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<LoadBlob> for CountingBlocks {
    async fn execute(
        &self,
        LoadBlob { hash }: LoadBlob,
    ) -> Result<Option<Buffer>, DialogArtifactsError> {
        Ok(self.load(&hash).await)
    }
}
