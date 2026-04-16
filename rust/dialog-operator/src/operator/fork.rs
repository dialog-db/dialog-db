//! Fork dispatch provider for Operator.
//!
//! Uses [`Fork::claim`] to bundle the issuer, then [`Acquire::perform`]
//! to authorize and obtain a [`ForkInvocation`] for network execution.

use crate::Operator;
use crate::network::Network;

use dialog_capability::access::AuthorizeError;
use dialog_capability::fork::Acquire;
use dialog_capability::{Effect, Provider, SiteIssuer};
use dialog_capability::{Fork, ForkInvocation, Site};
use dialog_common::{ConditionalSend, ConditionalSync};

/// Helper trait for effect outputs that can absorb authorization errors.
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
    A: Clone + ConditionalSend + ConditionalSync + 'static,
    At: Site,
    At::Claim<Fx>: Acquire<Self, Site = At, Effect = Fx> + ConditionalSend,
    Fx: Effect + 'static,
    Fx::Output: FromAuthError,
    Fork<At, Fx>: ConditionalSend,
    ForkInvocation<At, Fx>: ConditionalSend,
    Network: Provider<ForkInvocation<At, Fx>> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Fork<At, Fx>) -> Fx::Output {
        let issuer = SiteIssuer {
            operator: self.authority.operator_did(),
            profile: self.authority.profile_did(),
        };

        match input.claim(issuer).perform(self).await {
            Ok(invocation) => invocation.perform(&self.network).await,
            Err(e) => FromAuthError::from_auth_error(e),
        }
    }
}
