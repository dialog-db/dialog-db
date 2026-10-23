//! An operator that reports every archive block moved through it.
//!
//! The repository reads and writes blocks through archive effects: `Get`
//! for a read, `Put` and `Import` for writes. A value that spilled out of
//! its key (a revision record, a large fact value) is a blob, read through
//! the blob `Read` effect as a stream. [`Metered`] wraps an operator and
//! hands each block's bytes, and each blob's, to a [`Meter`] on the way
//! through, so a measurement sees exactly what a commit or query moved,
//! beneath every cache the branch keeps. Every other effect passes through
//! untouched.

use std::any::Any;
use std::mem::replace;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use dialog_capability::{Capability, Command, Provider};
use dialog_common::{ConditionalSend, ConditionalSync, Held, Holds};
use dialog_effects::archive::prelude::{ImportExt as _, PutExt as _};
use dialog_effects::archive::{ArchiveError, Get, Import, Put};
use dialog_effects::blob::{BlobError, BlobReader, BlobSource, Read as BlobRead};

/// Observes the archive blocks and blobs an operator moves.
pub trait Meter: ConditionalSync {
    /// A block was written.
    fn wrote(&self, block: &[u8]);
    /// A block was read.
    fn read(&self, block: &[u8]);
    /// A blob read was opened: a spilled value fetched by its reference.
    /// Its bytes follow through [`read_blob_chunk`](Self::read_blob_chunk)
    /// as the caller drains the stream.
    fn read_blob(&self) {}
    /// A chunk of a blob being read.
    fn read_blob_chunk(&self, _chunk: &[u8]) {}
}

/// Counts blocks and bytes in each direction, and the blobs read. Clones
/// share the counts.
#[derive(Clone, Default, Debug)]
pub struct Tally {
    writes: Arc<AtomicUsize>,
    write_bytes: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
    read_bytes: Arc<AtomicUsize>,
    blob_reads: Arc<AtomicUsize>,
    blob_read_bytes: Arc<AtomicUsize>,
}

impl Tally {
    /// Blobs read: spilled values fetched by reference.
    pub fn blob_reads(&self) -> usize {
        self.blob_reads.load(Ordering::Relaxed)
    }

    /// Bytes of blobs read.
    pub fn blob_read_bytes(&self) -> usize {
        self.blob_read_bytes.load(Ordering::Relaxed)
    }

    /// Blocks written.
    pub fn writes(&self) -> usize {
        self.writes.load(Ordering::Relaxed)
    }

    /// Bytes written.
    pub fn write_bytes(&self) -> usize {
        self.write_bytes.load(Ordering::Relaxed)
    }

    /// Blocks read.
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }

    /// Bytes read.
    pub fn read_bytes(&self) -> usize {
        self.read_bytes.load(Ordering::Relaxed)
    }
}

impl Meter for Tally {
    fn wrote(&self, block: &[u8]) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.write_bytes.fetch_add(block.len(), Ordering::Relaxed);
    }

    fn read(&self, block: &[u8]) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.read_bytes.fetch_add(block.len(), Ordering::Relaxed);
    }

    fn read_blob(&self) {
        self.blob_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn read_blob_chunk(&self, chunk: &[u8]) {
        self.blob_read_bytes
            .fetch_add(chunk.len(), Ordering::Relaxed);
    }
}

/// A blob stream whose chunks `meter` sees as the reader drains them.
struct MeteredSource<M> {
    inner: BlobReader,
    meter: M,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<M: Meter + ConditionalSend> BlobSource for MeteredSource<M> {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        let chunk = self.inner.next().await?;
        if let Some(chunk) = &chunk {
            self.meter.read_blob_chunk(chunk);
        }
        Ok(chunk)
    }
}

/// An operator whose archive block traffic `meter` observes.
#[derive(Clone)]
pub struct Metered<Env, M = Tally> {
    inner: Env,
    meter: M,
}

impl<Env, M> Metered<Env, M> {
    /// Meters `inner`'s archive traffic with `meter`.
    pub fn new(inner: Env, meter: M) -> Self {
        Self { inner, meter }
    }

    /// The meter observing this operator.
    pub fn meter(&self) -> &M {
        &self.meter
    }
}

impl<Env: Holds, M> Holds for Metered<Env, M> {
    fn held(&self, key: &str) -> Option<Held> {
        self.inner.held(key)
    }

    fn hold(&self, key: String, handle: Held) {
        self.inner.hold(key, handle)
    }

    fn held_or(&self, key: &str, make: &dyn Fn() -> Held) -> Held {
        self.inner.held_or(key, make)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<C, Env, M> Provider<C> for Metered<Env, M>
where
    C: Command + 'static,
    C::Input: ConditionalSend + 'static,
    C::Output: 'static,
    Env: Provider<C> + ConditionalSync,
    M: Meter + Clone + ConditionalSend + 'static,
{
    async fn execute(&self, input: C::Input) -> C::Output {
        // Archive effects are told apart by type: the blanket impl is what
        // lets every other effect pass through, so the few this meters are
        // recognized at run time rather than by their own impls.
        let input_of = &input as &dyn Any;
        if let Some(put) = input_of.downcast_ref::<Capability<Put>>() {
            self.meter.wrote(put.content());
        } else if let Some(import) = input_of.downcast_ref::<Capability<Import>>() {
            for block in import.blocks() {
                self.meter.wrote(block.as_ref());
            }
        }
        let reads = input_of.is::<Capability<Get>>();
        let reads_blob = input_of.is::<Capability<BlobRead>>();
        let mut output = self.inner.execute(input).await;
        if reads
            && let Some(Ok(Some(block))) =
                (&output as &dyn Any).downcast_ref::<Result<Option<Vec<u8>>, ArchiveError>>()
        {
            self.meter.read(block);
        }
        // A blob's bytes never travel in the effect: its output is a stream
        // the caller drains, so the stream is wrapped to meter what it
        // yields.
        if reads_blob
            && let Some(Ok(reader)) =
                (&mut output as &mut dyn Any).downcast_mut::<Result<BlobReader, BlobError>>()
        {
            self.meter.read_blob();
            let inner = replace(reader, Box::new(Exhausted));
            *reader = Box::new(MeteredSource {
                inner,
                meter: self.meter.clone(),
            });
        }
        output
    }
}

/// A blob stream with nothing in it: the placeholder a reader is swapped
/// out against while it is wrapped.
struct Exhausted;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSource for Exhausted {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        Ok(None)
    }
}
