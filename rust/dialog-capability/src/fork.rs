//! Fork — remote execution of capabilities via site-specific providers.
//!
//! A [`Fork`] pairs a [`Capability`] with a site address, orchestrating
//! authorization and dispatch via the environment.
//!
//! [`ForkInvocation`] is the input to `Provider<Fork<S, Fx>>` — it carries
//! the address and authorization needed for execution.

use crate::access::{Authorization, AuthorizeError, Protocol};
use crate::authority::{Identify, Sign};
use crate::command::Command;
use crate::effect::Effect;
use crate::site::Site;
use crate::storage::{Get, List};
use crate::{Ability, Capability, Constraint, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use std::marker::PhantomData;

/// The data bundle passed to `Provider<Fork<S, Fx>>`.
///
/// Carries address and authorization — everything needed
/// for a site-specific provider to execute the operation.
/// Credentials (if any) live on the address itself.
pub struct ForkInvocation<S: Site, Fx: Effect> {
    /// The site address (endpoint info + credentials for building requests).
    pub address: S::Address,
    /// The authorized capability with format-specific proof.
    pub authorization: Authorization<Fx, S::Protocol>,
}

/// Fork pairs a capability with a site address for remote execution.
///
/// Created by `.fork(&address)` on a capability chain. Use `acquire` to
/// authorize and build a `ForkInvocation`, or `perform` to do both in one step.
///
/// Also serves as the `Command` type for `Provider<Fork<S, Fx>>` bounds,
/// with `S` first so that impls like `Provider<Fork<S3, Fx>>` satisfy
/// the orphan rule.
pub struct Fork<S: Site, Fx: Effect> {
    capability: Capability<Fx>,
    address: S::Address,
    _site: PhantomData<S>,
}

impl<S: Site, Fx: Effect> Command for Fork<S, Fx>
where
    Fx::Of: Constraint,
{
    type Input = ForkInvocation<S, Fx>;
    type Output = Fx::Output;
}

impl<S, Fx> Fork<S, Fx>
where
    S: Site,
    Fx: Effect,
    Fx::Of: Constraint,
{
    /// Create a Fork from a capability and a site address.
    pub fn new(capability: Capability<Fx>, address: S::Address) -> Self {
        Self {
            capability,
            address,
            _site: PhantomData,
        }
    }

    /// Authorize the capability and build a `ForkInvocation`.
    ///
    /// Delegates to the site's [`Protocol::authorize`].
    pub async fn acquire<Env>(self, env: &Env) -> Result<ForkInvocation<S, Fx>, AuthorizeError>
    where
        Fx: Clone + ConditionalSend + 'static,
        Capability<Fx>: Ability + Clone + ConditionalSend + ConditionalSync,
        S::Protocol: Protocol,
        Env: Provider<Identify> + Provider<Sign> + Provider<List> + Provider<Get> + ConditionalSync,
    {
        let authorization = S::Protocol::authorize(env, self.capability).await?;

        Ok(ForkInvocation {
            address: self.address,
            authorization,
        })
    }

    /// Authorize and execute in one step.
    pub async fn perform<Env>(self, env: &Env) -> Result<Fx::Output, AuthorizeError>
    where
        Fx: Clone + ConditionalSend + 'static,
        Capability<Fx>: Ability + Clone + ConditionalSend + ConditionalSync,
        S::Protocol: Protocol,
        Env: Provider<Identify>
            + Provider<Sign>
            + Provider<List>
            + Provider<Get>
            + Provider<Fork<S, Fx>>
            + ConditionalSync,
    {
        let invocation = self.acquire(env).await?;
        Ok(invocation.perform(env).await)
    }
}

impl<S, Fx> ForkInvocation<S, Fx>
where
    S: Site,
    Fx: Effect,
    Fx::Of: Constraint,
{
    /// Execute this fork invocation against a provider.
    pub async fn perform<P>(self, provider: &P) -> Fx::Output
    where
        P: Provider<Fork<S, Fx>>,
    {
        provider.execute(self).await
    }
}

/// Error during fork execution.
#[derive(Debug, thiserror::Error)]
pub enum ForkError {
    /// Authorization was denied.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),
}
