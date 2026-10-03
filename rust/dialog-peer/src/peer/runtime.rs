//! The runtime state peers share: the hydration scheduler and the
//! speculative-fetch queue.
//!
//! Both coordinate work across everything performing through them, so
//! they belong to the environment rather than to one identity: a worker
//! built from a peer shares its parent's, and two peers built over one
//! storage share one by being given the same handle. A fresh handle is
//! the default for a peer built on its own.

use std::sync::Arc;

use dialog_artifacts::PreloadQueue;
use dialog_network::HydrationScheduler;

/// The shared runtime: cheap to clone, every clone is the same state.
#[derive(Clone, Debug, Default)]
pub struct Runtime {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Remote hydrations, joined by digest and admitted per site by
    /// priority across every peer performing through this runtime.
    hydration: HydrationScheduler,
    /// The ambient speculative-fetch queue `Preload` hints land in.
    speculation: Arc<PreloadQueue>,
}

impl Runtime {
    /// A fresh runtime, sharing nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// The scheduler every remote block read goes through: where a
    /// site's window is set (`set_window`) and its traffic is read back
    /// (`tally`).
    pub fn hydration(&self) -> &HydrationScheduler {
        &self.inner.hydration
    }

    /// The speculative-fetch queue.
    pub(crate) fn speculation(&self) -> &Arc<PreloadQueue> {
        &self.inner.speculation
    }
}
