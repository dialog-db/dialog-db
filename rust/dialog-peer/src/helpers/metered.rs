//! An operator that reports every archive block moved through it.
//!
//! The repository reads and writes blocks through archive effects: `Get`
//! (or the `Load` that reads through it) for a read, `Put` and `Import`
//! for writes. [`Metered`] wraps an operator
//! and hands each block's bytes to a [`Meter`] on the way through, so a
//! measurement sees exactly what a commit or query moved, beneath every
//! cache the branch keeps. Every other effect passes through untouched.

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use dialog_capability::{Capability, Command, Provider};
use dialog_common::{Buffer, ConditionalSend, ConditionalSync, Held, Holds};
use dialog_effects::archive::prelude::{ImportExt as _, PutExt as _};
use dialog_effects::archive::{ArchiveError, Get, Import, Load, Put};
use dialog_effects::blob::Import as BlobImport;
use dialog_effects::blob::prelude::BlobImportExt as _;

/// Observes the archive blocks an operator moves.
pub trait Meter: ConditionalSend + ConditionalSync {
    /// A block was written.
    fn wrote(&self, block: &[u8]);
    /// A block was read.
    fn read(&self, block: &[u8]);
    /// A blob of `size` bytes was imported under `digest` (a spilled value
    /// or a revision record a commit writes beside the tree's nodes). The
    /// default ignores it, so a meter of tree blocks alone stays as it was.
    fn imported_blob(&self, _digest: &dialog_common::Blake3Hash, _size: u64) {}
}

/// Counts blocks and bytes in each direction. Clones share the counts.
#[derive(Clone, Default, Debug)]
pub struct Tally {
    writes: Arc<AtomicUsize>,
    write_bytes: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
    read_bytes: Arc<AtomicUsize>,
}

impl Tally {
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
    M: Meter,
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
        } else if let Some(import) = input_of.downcast_ref::<Capability<BlobImport>>() {
            self.meter.imported_blob(import.digest(), import.size());
        }
        // A block is read by a `Get`, or by the `Load` a reader performs to
        // have it from wherever the env reaches it: the env answers that
        // beneath this wrapper, so the load is metered as its read.
        let reads = input_of.is::<Capability<Get>>() || input_of.is::<Load>();
        let output = self.inner.execute(input).await;
        if reads {
            let output_of = &output as &dyn Any;
            if let Some(Ok(Some(block))) =
                output_of.downcast_ref::<Result<Option<Vec<u8>>, ArchiveError>>()
            {
                self.meter.read(block);
            } else if let Some(Ok(Some(block))) =
                output_of.downcast_ref::<Result<Option<Buffer>, ArchiveError>>()
            {
                self.meter.read(block.as_ref());
            }
        }
        output
    }
}
