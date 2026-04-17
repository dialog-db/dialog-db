//! Fork — remote execution of capabilities at a site address.
//!
//! A [`Fork`] pairs a [`Capability`] with a site address for remote execution.
//! The Operator builds a [`ForkInvocation`] by attaching protocol-specific
//! authorization, then the site provider executes it.

use crate::access::AuthorizeError;
use crate::command::Command;
use crate::effect::Effect;
use crate::site::Site;
use crate::{Ability, Capability, Constraint, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};

/// Fork pairs a capability with a site address for remote execution.
///
/// Created by [`Capability::fork`]. Call [`.perform(&env)`](Fork::perform)
/// to authorize and execute. The Operator builds the authorization
/// material and delegates to the site provider via [`ForkInvocation`].
pub struct Fork<S: Site, Fx: Effect> {
    capability: Capability<Fx>,
    address: S::Address,
}

/// A fork with authorization attached, ready for site-level execution.
///
/// Created by the Operator's `Provider<Fork<S, Fx>>` impl after building
/// protocol-specific authorization. The site provider receives this and
/// uses the authorization to execute the request.
///
/// For capability-based sites (UCAN), authorization is the verified proof
/// chain. The site provider calls `.invoke()` to produce the signed
/// invocation for the access service.
///
/// For credential-based sites (S3), authorization is the credentials.
/// The site provider uses them to sign the HTTP request.
pub struct ForkInvocation<S: Site, Fx: Effect> {
    /// The capability being executed.
    pub capability: Capability<Fx>,
    /// The site address.
    pub address: S::Address,
    /// Authorization material produced by the Operator.
    pub authorization: S::Authorization,
}

impl<S: Site, Fx: Effect> Command for Fork<S, Fx>
where
    Fx::Of: Constraint,
{
    type Input = Self;
    type Output = Fx::Output;
}

impl<S: Site, Fx: Effect> Command for ForkInvocation<S, Fx>
where
    Fx::Of: Constraint,
{
    type Input = Self;
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
        }
    }

    /// The capability being forked.
    pub fn capability(&self) -> &Capability<Fx> {
        &self.capability
    }

    /// The site address.
    pub fn address(&self) -> &S::Address {
        &self.address
    }

    /// Consume and return the parts.
    pub fn into_parts(self) -> (Capability<Fx>, S::Address) {
        (self.capability, self.address)
    }

    /// Execute the fork against a provider.
    ///
    /// The provider (typically the Operator) builds protocol-specific
    /// authorization and delegates to the site provider.
    ///
    /// Authorization errors are folded into the effect's error type via
    /// `From<AuthorizeError>`, so callers get a single `Result`.
    pub async fn perform<Env>(self, env: &Env) -> Fx::Output
    where
        Fx: ConditionalSend + 'static,
        Capability<Fx>: Ability + ConditionalSend + ConditionalSync,
        Env: Provider<Fork<S, Fx>> + ConditionalSync,
    {
        env.execute(self).await
    }
}

impl<S, Fx> ForkInvocation<S, Fx>
where
    S: Site,
    Fx: Effect,
    Fx::Of: Constraint,
{
    /// Create a new fork invocation with authorization.
    pub fn new(
        capability: Capability<Fx>,
        address: S::Address,
        authorization: S::Authorization,
    ) -> Self {
        Self {
            capability,
            address,
            authorization,
        }
    }

    /// Execute this invocation against a provider.
    pub async fn perform<Env>(self, env: &Env) -> Fx::Output
    where
        Self: ConditionalSend,
        Env: Provider<ForkInvocation<S, Fx>> + dialog_common::ConditionalSync,
    {
        env.execute(self).await
    }
}

impl<S, Fx> Fork<S, Fx>
where
    S: Site,
    Fx: Effect,
    Fx::Of: Constraint,
{
    /// Authorize this fork against an environment, producing a
    /// [`ForkInvocation`] ready for execution.
    ///
    /// Internally this converts the generic `Fork<S, Fx>` into the
    /// site specific fork type (`S::Fork<Fx>`) and delegates to its
    /// [`Authorize`] impl. The site's wrapper knows how to fetch
    /// authorization material from the env.
    pub async fn authorize<Env>(self, env: &Env) -> Result<ForkInvocation<S, Fx>, AuthorizeError>
    where
        S::Fork<Fx>: Authorize<Env, Site = S, Effect = Fx>,
    {
        let fork: S::Fork<Fx> = self.into();
        fork.authorize(env).await
    }
}

/// Trait for site-specific fork wrappers that can authorize against an env.
///
/// Implemented by each site's fork type (e.g., `S3Fork`, `UcanFork`).
/// The wrapper knows how to obtain authorization material from the
/// environment (credentials, signed delegations, etc.) and bundle it
/// with the capability + address into a [`ForkInvocation`].
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Authorize<Env> {
    /// The site type this fork authorizes for.
    type Site: Site;
    /// The effect type this fork was created for.
    type Effect: Effect;

    /// Authorize and produce a [`ForkInvocation`].
    async fn authorize(
        self,
        env: &Env,
    ) -> Result<ForkInvocation<Self::Site, Self::Effect>, AuthorizeError>;
}

/// Error during fork execution.
#[derive(Debug, thiserror::Error)]
pub enum ForkError {
    /// Authorization was denied.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),
}
