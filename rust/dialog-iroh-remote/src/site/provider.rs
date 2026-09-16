//! Signing an invocation for a peer, and sending it.
//!
//! Two halves of one exchange. [`SiteFork::authorize`] mints the
//! container — the same proving step a UCAN site runs, plus the payload
//! packed beside the invocation it commits to — and the
//! [`Provider`] hands it to the [`Channel`](crate::channel::Channel) and
//! reads the answer back.

use dialog_capability::access::{
    Access, Authorization as _, AuthorizeError, FromCapability, Protocol, Recourse, TimeRange,
};
use dialog_capability::{
    Ability, Authorize as AuthorizeEffect, Capability, Constraint, Effect, ForkInvocation,
    Provider, SiteFork, Subject,
};
use dialog_common::time::{self, UNIX_EPOCH};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::Rejection;
use dialog_effects::authority::{self, OperatorExt as _};
use dialog_ucan::Ucan;
use dialog_ucan_core::container::Container;
use dialog_ucan_core::container::bundle::InvocationBundle;
use serde::de::DeserializeOwned;

use crate::carries::Carries;
use crate::site::{Iroh, IrohAuthorization, IrohFork};
use crate::wire::{Refusal, Response, decode};

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx, Env> SiteFork<Env> for IrohFork<Fx>
where
    Fx: Effect + Carries + Clone + ConditionalSend + ConditionalSync + 'static,
    Fx::Of: Constraint<Capability: ConditionalSend + ConditionalSync>,
    Capability<Fx>: Ability + ConditionalSend + ConditionalSync,
    Env: Provider<AuthorizeEffect<Ucan>> + Provider<authority::Identify> + ConditionalSync,
{
    type Site = Iroh;
    type Effect = Fx;

    async fn authorize(self, env: &Env) -> Result<ForkInvocation<Iroh, Fx>, AuthorizeError> {
        let identity =
            authority::Identify
                .perform(env)
                .await
                .map_err(|error| AuthorizeError::Malformed {
                    detail: error.to_string(),
                })?;
        let profile = identity.profile().clone();
        let operator = identity.did();

        // `from_capability` is `Scope::invoke`, which projects payload
        // fields through `Attenuate` — so the block this signs for
        // becomes a digest and a checksum and does not go into the
        // arguments. What ships it is `Carries`, below.
        let scope = <Ucan as Protocol>::Access::from_capability(self.0.capability());

        // Ask for a chain good at the instant of presentation rather
        // than an unbounded one, for the reason `dialog-remote-ucan-s3`
        // records: an unbounded request is covered by every window,
        // including one that closed yesterday, and the responder does
        // check its clock.
        let at = time::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_secs())
            .unwrap_or_default();
        let authorization = Subject::from(profile)
            .attenuate(Access)
            .invoke(
                AuthorizeEffect::<Ucan>::new(operator, scope).during(TimeRange {
                    not_before: Some(at),
                    expiration: Some(at),
                }),
            )
            .perform(env)
            .await?;

        let invocation = authorization.invoke().await?;
        let blocks = dialog_capability::Policy::of(self.0.capability()).blocks();
        let bundle = InvocationBundle::from_chain(invocation.chain(), blocks).map_err(|error| {
            AuthorizeError::Malformed {
                detail: format!("could not assemble the request: {error}"),
            }
        })?;
        let container =
            Container::from(&bundle)
                .into_bytes()
                .map_err(|error| AuthorizeError::Malformed {
                    detail: format!("could not encode the request: {error}"),
                })?;

        Ok(self.0.attest(IrohAuthorization::new(container)))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx, T, E> Provider<ForkInvocation<Iroh, Fx>> for Iroh
where
    Fx: Effect<Output = Result<T, E>> + 'static,
    Fx::Of: Constraint<Capability: ConditionalSend + ConditionalSync>,
    Capability<Fx>: ConditionalSend + ConditionalSync,
    T: ConditionalSend,
    E: From<AuthorizeError> + From<Rejection> + ConditionalSend,
    Result<T, E>: DeserializeOwned,
{
    async fn execute(&self, invocation: ForkInvocation<Iroh, Fx>) -> Result<T, E> {
        let ForkInvocation {
            address,
            authorization,
            ..
        } = invocation;

        let answer = match self
            .channel()
            .exchange(&address, authorization.into_bytes())
            .await
        {
            Ok(answer) => answer,
            // The peer never answered, so nothing is known about the
            // request: retryable as it stands.
            Err(error) => {
                return Err(E::from(Rejection::Unavailable {
                    reason: error.to_string(),
                }));
            }
        };

        match decode::<Response>("response", &answer) {
            Ok(Response::Performed(output)) => match decode::<Result<T, E>>("output", &output) {
                Ok(outcome) => outcome,
                Err(error) => Err(E::from(Rejection::Unclassified {
                    detail: format!("the peer's answer did not fit the command: {error}"),
                })),
            },
            Ok(Response::Refused(refusal)) => Err(refused(refusal)),
            Err(error) => Err(E::from(Rejection::Unclassified {
                detail: format!("the peer did not answer in this protocol: {error}"),
            })),
        }
    }
}

/// A peer's refusal, in the caller's own vocabulary.
///
/// Only [`Refusal::Unauthorized`] is an access decision, and it is the
/// one a caller acts on differently — by fetching a fresh proof rather
/// than retrying. The rest are this exchange failing, not authority
/// being absent, so folding them into an access error would tell a
/// caller to go looking for a delegation it already holds.
fn refused<E: From<AuthorizeError> + From<Rejection>>(refusal: Refusal) -> E {
    match refusal {
        Refusal::Unauthorized(reason) => E::from(AuthorizeError::Declined {
            // Presenting the same proof again gets the same answer; a
            // caller that wants in needs different authority, not a
            // retry.
            recourse: Recourse::None,
            reason,
        }),
        Refusal::UnknownCommand(command) => E::from(Rejection::Unclassified {
            detail: format!("the peer does not serve {command}"),
        }),
        Refusal::Malformed(detail) => E::from(Rejection::Unclassified {
            detail: format!("the peer could not read the request: {detail}"),
        }),
        Refusal::Internal(reason) => E::from(Rejection::Unavailable { reason }),
    }
}
