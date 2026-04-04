//! Fork — remote execution of capabilities at a site address.
//!
//! A [`Fork`] pairs a [`Capability`] with a site address for remote execution.
//! The `Provider<Fork<S, Fx>>` implementation handles authorization internally.

use crate::access::AuthorizeError;
use crate::command::Command;
use crate::effect::Effect;
use crate::site::Site;
use crate::{Ability, Capability, Constraint, Provider};
use dialog_common::ConditionalSend;
use std::marker::PhantomData;

/// Fork pairs a capability with a site address for remote execution.
///
/// Used as a [`Command`]: the provider handles authorization
/// and dispatch internally. Callers only need `Provider<Fork<S, Fx>>`.
pub struct Fork<S: Site, Fx: Effect> {
    capability: Capability<Fx>,
    address: S::Address,
    _site: PhantomData<S>,
}

impl<S: Site, Fx: Effect> Command for Fork<S, Fx>
where
    Fx::Of: Constraint,
{
    type Input = Self;
    type Output = Result<Fx::Output, AuthorizeError>;
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
    pub async fn perform<Env>(self, env: &Env) -> Result<Fx::Output, AuthorizeError>
    where
        Fx: ConditionalSend + 'static,
        Capability<Fx>: Ability + ConditionalSend + dialog_common::ConditionalSync,
        Env: Provider<Fork<S, Fx>> + dialog_common::ConditionalSync,
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
