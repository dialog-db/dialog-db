use async_trait::async_trait;
use dialog_common::ConditionalSync;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::StorageBackend;

/// A [MeasuredStorageBackend] acts as a proxy over a [StorageBackend]
/// implementation that measures reads and writes.
#[derive(Clone)]
pub struct MeasuredStorageBackend<Backend>
where
    Backend: StorageBackend,
{
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    backend: Backend,
}

impl<Backend> MeasuredStorageBackend<Backend>
where
    Backend: StorageBackend,
{
    /// Wrap the provided [StorageBackend] so that reads and writes to it may be
    /// measured.
    pub fn new(backend: Backend) -> Self {
        Self {
            reads: Arc::new(AtomicUsize::default()),
            writes: Arc::new(AtomicUsize::default()),
            backend,
        }
    }

    /// The aggregate number of reads from the wrapped [StorageBackend]
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }

    /// The aggregate number of writes to the wrapped [StorageBackend]
    pub fn writes(&self) -> usize {
        self.writes.load(Ordering::Relaxed)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> StorageBackend for MeasuredStorageBackend<Backend>
where
    Backend: StorageBackend + ConditionalSync,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Error = Backend::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.backend.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.backend.get(key).await
    }
}
