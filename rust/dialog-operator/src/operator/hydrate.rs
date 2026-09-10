//! The operator's own [`Hydrate`] provider: on-demand replication as an
//! effect of the environment.
//!
//! The caller ([`NetworkedIndex`](dialog_repository::NetworkedIndex))
//! resolves the routing per call and performs [`Hydrate`], borrowing the
//! operator for exactly the duration of the perform. The sharing lives
//! here: concurrent hydrations of one digest anywhere in the process —
//! queries, subscriptions, transaction queries, pull — join one shared
//! fetch-and-write-back through the operator's digest-keyed flight.
//!
//! The shared future is built inside this impl from the operator's own
//! Arc-backed internals (a self handle cloned by the env's own
//! implementation, not handed to any component): the same move the Fs
//! transport's `Get` makes with its transport-level `Flight`, one layer
//! up. The flight holds the work weakly, so the strong shared futures
//! live only in active joiners — work makes progress exactly while some
//! `.perform` drives it (every joiner polls the shared future itself,
//! preserving the co-driving liveness rule), drops with its last
//! joiner, and can never keep the operator alive through its own field.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_repository::{Hydrate, HydrationRequest, RemoteSite, hydrate};

use crate::Operator;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Hydrate> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: Provider<Get>
        + Provider<Put>
        + Provider<Fork<RemoteSite, Get>>
        + Clone
        + ConditionalSend
        + ConditionalSync
        + 'static,
{
    async fn execute(
        &self,
        request: HydrationRequest,
    ) -> Result<Option<Arc<Vec<u8>>>, ArchiveError> {
        let digest = request.digest.clone();
        let tag = format!("{digest:?}");
        let tag = &tag[..16.min(tag.len())];
        // The origin names the consumer whose read demanded this block:
        // the construction site of the NetworkedIndex that missed.
        let file = request.origin.file();
        let file = file
            .rsplit('/')
            .next()
            .unwrap_or(file)
            .strip_suffix(".rs")
            .unwrap_or(file);
        // A stable label when the consumer set one; the construction
        // site otherwise. Line numbers move with every edit, which has
        // made probe output actively misleading across refactors.
        let tag = match request.label {
            Some(label) => format!("{tag} [{label}]"),
            None => format!("{tag} via {file}:{line}", line = request.origin.line()),
        };
        // Measured, not inferred: if this never exceeds 1 the reads are
        // genuinely serial; if it climbs they are overlapping.
        static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
        let depth = IN_FLIGHT.fetch_add(1, Ordering::Relaxed) + 1;
        dialog_common::probe(&format!("hydrate start {tag} inflight={depth}"));
        // Hydration is content-addressed, so every joiner's answer is
        // identical regardless of whose route runs; errors are shared
        // as their rendering and never cached, so retry semantics are
        // unchanged.
        let outcome = self
            .hydration
            .join(digest, move || {
                let env = self.clone();
                async move {
                    hydrate(&env, request)
                        .await
                        .map_err(|error| error.to_string())
                }
            })
            .await;
        let depth = IN_FLIGHT.fetch_sub(1, Ordering::Relaxed) - 1;
        dialog_common::probe(&format!("hydrate done {tag} inflight={depth}"));
        outcome.map_err(ArchiveError::Storage)
    }
}
