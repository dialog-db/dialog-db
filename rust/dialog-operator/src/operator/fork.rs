//! Fork dispatch provider for Operator.
//!
//! Calls [`Fork::authorize`] to produce a [`ForkInvocation`], then
//! delegates execution to the network layer. The site's own fork
//! wrapper fetches identity from the env via `authority::Identify`.

use crate::Operator;
use crate::network::Network;

use dialog_capability::SiteFork;
use dialog_capability::access::AuthorizeError;
use dialog_capability::{Effect, Provider};
use dialog_capability::{Fork, ForkInvocation, Site};
use dialog_common::{ConditionalSend, ConditionalSync};

/// Helper trait for effect outputs that can absorb authorization errors.
///
/// All our effects return `Result<T, E>` where `E: From<AuthorizeError>`.
/// Enables converting authorization failures into effect-specific errors
/// (e.g., `AuthorizeError` -> `ArchiveError::Authorization`).
trait FromAuthError {
    fn from_auth_error(e: AuthorizeError) -> Self;
}

impl<T, E: From<AuthorizeError>> FromAuthError for Result<T, E> {
    fn from_auth_error(e: AuthorizeError) -> Self {
        Err(E::from(e))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<A, At, Fx> Provider<Fork<At, Fx>> for Operator<A>
where
    // Operator's storage provider type
    A: Clone + ConditionalSend + ConditionalSync + 'static,
    At: Site,
    // Site's own fork wrapper carries the Authorize impl that fetches
    // identity from the env and produces a ForkInvocation.
    At::Fork<Fx>: SiteFork<Self, Site = At, Effect = Fx> + ConditionalSend,
    Fx: Effect + 'static,
    // Needed to flatten AuthorizeError into effect error via FromAuthError
    Fx::Output: FromAuthError,
    // Required by async_trait for Send futures
    Fork<At, Fx>: ConditionalSend,
    ForkInvocation<At, Fx>: ConditionalSend,
    // Network dispatches the authorized invocation to the site provider
    Network: Provider<ForkInvocation<At, Fx>> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Fork<At, Fx>) -> Fx::Output {
        match input.authorize(self).await {
            Ok(invocation) => invocation.perform(&self.network).await,
            Err(e) => FromAuthError::from_auth_error(e),
        }
    }
}
