use async_trait::async_trait;
use dialog_common::ConditionalSync;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::{StorageBackend};

/// A [MeasuredStorageBackend] acts as a proxy over a [StorageBackend]
/// implementation that measures reads and writes.
#[derive(Clone)]
pub struct MeasuredStorage<Backend>
where
    Backend: StorageBackend,
{
    reads: Arc<AtomicUsize>,
    read_bytes: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    write_bytes: Arc<AtomicUsize>,
    backend: Backend,
}

impl<Backend> MeasuredStorage<Backend>
where
    Backend: StorageBackend,
{
    /// Wrap the provided [StorageBackend] so that reads and writes to it may be
    /// measured.
    pub fn new(backend: Backend) -> Self {
        Self {
            reads: Arc::new(AtomicUsize::default()),
            read_bytes: Arc::new(AtomicUsize::default()),
            writes: Arc::new(AtomicUsize::default()),
            write_bytes: Arc::new(AtomicUsize::default()),
            backend,
        }
    }

    /// The aggregate number of reads from the wrapped [StorageBackend]
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }

    /// The total bytes read from the wrapped [StorageBackend]
    pub fn read_bytes(&self) -> usize {
        self.read_bytes.load(Ordering::Relaxed)
    }

    /// The aggregate number of writes to the wrapped [StorageBackend]
    pub fn writes(&self) -> usize {
        self.writes.load(Ordering::Relaxed)
    }

    /// The total bytes written to the wrapped [StorageBackend]
    pub fn write_bytes(&self) -> usize {
        self.write_bytes.load(Ordering::Relaxed)
    }
}

/// Trait for types that can report their byte length.
pub trait Measurable {
    /// Returns the byte length of this value.
    fn byte_len(&self) -> usize;
}

impl Measurable for Vec<u8> {
    fn byte_len(&self) -> usize {
        self.len()
    }
}

impl Measurable for [u8] {
    fn byte_len(&self) -> usize {
        self.len()
    }
}

impl<const SIZE: usize> Measurable for [u8; SIZE] {
    fn byte_len(&self) -> usize {
        SIZE
    }
}

/// A resource wrapper that measures reload and replace operations

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> StorageBackend for MeasuredStorage<Backend>
where
    Backend: StorageBackend + ConditionalSync,
    Backend::Key: Measurable + ConditionalSync,
    Backend::Value: Measurable + ConditionalSync + Clone,
    Backend::Error: ConditionalSync,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Error = Backend::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.write_bytes
            .fetch_add(key.byte_len() + value.byte_len(), Ordering::Relaxed);
        self.backend.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        self.reads.fetch_add(1, Ordering::Relaxed);

        let value = self.backend.get(key).await?;

        self.read_bytes.fetch_add(
            value
                .as_ref()
                .map(|value| value.byte_len())
                .unwrap_or_default(),
            Ordering::Relaxed,
        );

        Ok(value)
    }

}
