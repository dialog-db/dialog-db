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
        outcome.map_err(ArchiveError::Storage)
    }
}
