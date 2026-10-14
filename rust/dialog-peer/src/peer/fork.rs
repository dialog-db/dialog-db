//! Fork dispatch provider for [`Peer`].
//!
//! Calls [`Fork::authorize`] to produce a [`ForkInvocation`], then
//! delegates execution to the network layer. The site's own fork
//! wrapper fetches identity from the env via `authority::Identify`.
//!
//! A remote that refuses the invocation because a certificate in its
//! chain was revoked names that certificate. The peer sets it aside (see
//! [`Peer::refuse`](super::Peer)) and authorizes the fork again: the
//! proof walk admits the first retained grant that verifies, in content
//! hash order, so a holder that retained a fresh grant beside the revoked
//! one was presenting whichever the order put first. Retrying through the
//! walk with the refused certificate excluded presents the other. When no
//! other route exists the refusal stands, and it is the one reported: it
//! names the authority that was withdrawn, where the walk's own failure
//! would only say the key holds nothing.

use super::{Mode, Peer};
use dialog_capability::access::{AuthorizationFailure, AuthorizeError};
use dialog_capability::{Effect, Fork, ForkInvocation, Provider, Site, SiteFork};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_network::Network;
use ipld_core::cid::Cid;

/// Helper trait for effect outputs that can absorb authorization errors
/// and report the ones they carry.
///
/// All our effects return `Result<T, E>` where `E: From<AuthorizeError>`
/// and `E: AuthorizationFailure`. Enables converting authorization
/// failures into effect-specific errors (e.g., `AuthorizeError` ->
/// `ArchiveError::Authorization`) and reading them back out of an output.
trait ForkOutput {
    fn from_auth_error(e: AuthorizeError) -> Self;

    /// The certificate a remote refused as revoked, when this output is
    /// such a refusal and the remote named it.
    fn refused_certificate(&self) -> Option<Cid>;
}

impl<T, E> ForkOutput for Result<T, E>
where
    E: From<AuthorizeError> + AuthorizationFailure,
{
    fn from_auth_error(e: AuthorizeError) -> Self {
        Err(E::from(e))
    }

    fn refused_certificate(&self) -> Option<Cid> {
        let Err(error) = self else {
            return None;
        };
        let AuthorizeError::Revoked {
            certificate: Some(certificate),
            ..
        } = error.authorization_failure()?
        else {
            return None;
        };
        certificate.parse().ok()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<A, At, Fx, M: Mode> Provider<Fork<At, Fx>> for Peer<A, M>
where
    // The peer's storage provider type
    A: Clone + ConditionalSend + ConditionalSync + 'static,
    At: Site,
    // Site's own fork wrapper carries the Authorize impl that fetches
    // identity from the env and produces a ForkInvocation.
    At::Fork<Fx>: SiteFork<Self, Site = At, Effect = Fx> + ConditionalSend,
    Fx: Effect + 'static,
    // Needed to flatten AuthorizeError into effect error, and to read a
    // revocation refusal back out of the output
    Fx::Output: ForkOutput,
    // Required by async_trait for Send futures; Clone so a refused fork
    // can be authorized again through another grant
    Fork<At, Fx>: Clone + ConditionalSend,
    ForkInvocation<At, Fx>: ConditionalSend,
    // Network dispatches the authorized invocation to the site provider
    Network: Provider<ForkInvocation<At, Fx>> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Fork<At, Fx>) -> Fx::Output {
        let mut output = match input.clone().authorize(self).await {
            Ok(invocation) => invocation.perform(self.network()).await,
            Err(e) => return ForkOutput::from_auth_error(e),
        };
        // Each round sets aside one more certificate the remote named, so
        // the loop ends when the remote stops naming new ones: either the
        // invocation goes through, or the refusal repeats one already set
        // aside, or no chain proves the claim without it.
        while let Some(certificate) = output.refused_certificate() {
            if !self.refuse(certificate) {
                break;
            }
            tracing::debug!(
                %certificate,
                "remote refused a revoked certificate; proving through another grant"
            );
            let Ok(invocation) = input.clone().authorize(self).await else {
                break;
            };
            output = invocation.perform(self.network()).await;
        }
        output
    }
}
