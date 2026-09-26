//! The operator's own speculative-replication providers: [`Preload`]
//! hints land in the ambient [`PreloadQueue`], and [`Speculation`]
//! hands the queue to whichever driven evaluation stream is about to
//! pop from it.
//!
//! The queue is one more piece of env-owned state next to the hydration
//! flight: hints from any path — queries, subscriptions, transaction
//! queries — enqueue here, and any borrower driving an evaluation
//! executes them, so cross-query warming needs no per-path wiring. The
//! operator owns descriptions only; fetch futures exist exclusively
//! inside some `.perform` borrowing it.

use std::sync::Arc;

use dialog_artifacts::{Preload, PreloadQueue, PreloadRequest, Speculation};
use dialog_capability::Provider;
use dialog_common::{ConditionalSend, ConditionalSync};

use crate::Operator;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Preload> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: ConditionalSync,
{
    async fn execute(&self, request: PreloadRequest) -> bool {
        self.speculation.preload(request)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Speculation> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: ConditionalSync,
{
    async fn execute(&self, (): ()) -> Arc<PreloadQueue> {
        self.speculation.clone()
    }
}
