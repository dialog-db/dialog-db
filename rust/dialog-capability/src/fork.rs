//! Fork — remote execution of capabilities at a site address.
//!
//! A [`Fork`] pairs a [`Capability`] with a site address for remote execution.
//! The Operator builds a [`ForkInvocation`] by attaching protocol-specific
//! authorization, then the site provider executes it.

use crate::access::{AuthorizeError, Protocol};
use crate::command::Command;
use crate::effect::Effect;
use crate::site::Site;
use crate::{Ability, Capability, Constraint, Provider};
use dialog_common::ConditionalSend;
use std::marker::PhantomData;

/// Fork pairs a capability with a site address for remote execution.
///
/// Created by [`Capability::fork`]. Call [`.perform(&env)`](Fork::perform)
/// to authorize and execute. The Operator builds the authorization
/// material and delegates to the site provider via [`ForkInvocation`].
pub struct Fork<S: Site, Fx: Effect> {
    capability: Capability<Fx>,
    address: S::Address,
    _site: PhantomData<S>,
}

/// A fork with protocol-specific invocation attached.
///
/// Created by the Operator's `Provider<Fork<S, Fx>>` impl after building
/// the signed invocation. The site provider receives this and
/// executes the authorized request.
pub struct ForkInvocation<S: Site, Fx: Effect> {
    /// The capability being executed.
    pub capability: Capability<Fx>,
    /// The site address.
    pub address: S::Address,
    /// Protocol-specific invocation (e.g., signed UCAN chain for UCAN sites, () for S3).
    pub invocation: <S::Protocol as Protocol>::Invocation,
}

impl<S: Site, Fx: Effect> Command for Fork<S, Fx>
where
    Fx::Of: Constraint,
{
    type Input = Self;
    type Output = Result<Fx::Output, AuthorizeError>;
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
            _site: PhantomData,
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
    pub async fn perform<Env>(self, env: &Env) -> Result<Fx::Output, AuthorizeError>
    where
        Fx: ConditionalSend + 'static,
        Capability<Fx>: Ability + ConditionalSend + dialog_common::ConditionalSync,
        Env: Provider<Fork<S, Fx>> + dialog_common::ConditionalSync,
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
        invocation: <S::Protocol as Protocol>::Invocation,
    ) -> Self {
        Self {
            capability,
            address,
            invocation,
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

/// Error during fork execution.
#[derive(Debug, thiserror::Error)]
pub enum ForkError {
    /// Authorization was denied.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),
}
