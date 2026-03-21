//! Fork — remote execution of capabilities via site-specific providers.
//!
//! A [`Fork`] pairs a [`Capability`] with a site address, orchestrating
//! authorization, credential resolution, and dispatch via the environment.
//!
//! [`ForkInvocation`] is the input to `Provider<Fork<S, Fx>>` — it carries
//! the address, credentials, and authorization needed for execution.

use crate::command::Command;
use crate::credential::Authorization;
use crate::credential::{
    Addressable, Authorize, AuthorizeError, Credential, CredentialError, Get, Profile,
};
use crate::effect::Effect;
use crate::site::Site;
use crate::{Capability, Constraint, Provider, Subject};
use dialog_common::ConditionalSend;
use std::marker::PhantomData;

/// The data bundle passed to `Provider<Fork<S, Fx>>`.
///
/// Carries address, credentials, and authorization — everything needed
/// for a site-specific provider to execute the operation.
pub struct ForkInvocation<S: Site, Fx: Effect> {
    /// The site address (endpoint info for building requests).
    pub address: S::Address,
    /// The resolved credentials for this site.
    pub credentials: S::Credentials,
    /// The authorized capability with format-specific proof.
    pub authorization: Authorization<Fx, S::Format>,
}

/// Fork is the Command type used in `Provider<Fork<S, Fx>>` bounds.
///
/// `S` appears first so that impls like `Provider<Fork<S3, Fx>>` satisfy
/// the orphan rule (local type `S3` before uncovered `Fx`).
pub struct Fork<S: Site, Fx: Effect> {
    /// The capability to execute remotely.
    pub capability: Capability<Fx>,
    /// The target site address.
    pub address: S::Address,
    _site: PhantomData<S>,
}

impl<S: Site, Fx: Effect> Command for Fork<S, Fx>
where
    Fx::Of: Constraint,
{
    type Input = ForkInvocation<S, Fx>;
    type Output = Fx::Output;
}

impl<Fx: Effect> Capability<Fx> {
    /// Fork this capability to a remote site for execution.
    pub fn fork<S: Site>(self, address: &S::Address) -> Fork<S, Fx> {
        Fork {
            capability: self,
            address: address.clone(),
            _site: PhantomData,
        }
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

    /// Credential resolution failed.
    #[error(transparent)]
    Credential(#[from] CredentialError),
}

impl<S, Fx> Fork<S, Fx>
where
    Fx: Effect,
    Fx::Of: Constraint,
    S: Site,
{
    /// Authorize, resolve credentials, build ForkInvocation, and execute.
    ///
    /// Dispatches through `Provider<Fork<S, Fx>>`, which allows each site
    /// type to define its own routing through the environment.
    pub async fn perform<Env>(self, env: &Env) -> Result<Fx::Output, ForkError>
    where
        Env: Provider<Authorize<Fx, S::Format>>
            + Provider<Get<S::Credentials>>
            + Provider<Fork<S, Fx>>,
        Capability<Fx>: ConditionalSend,
        Authorize<Fx, S::Format>: ConditionalSend + 'static,
        Get<S::Credentials>: ConditionalSend + 'static,
    {
        let subject = self.capability.subject().clone();

        // Step 1: Authorize for this site's format
        let authorized = Subject::from(subject.clone())
            .attenuate(Credential)
            .attenuate(Profile::default())
            .invoke(Authorize::<Fx, S::Format>::new(self.capability))
            .perform(env)
            .await?;

        // Step 2: Resolve credentials for this site
        let credentials = Subject::from(subject)
            .attenuate(Credential)
            .attenuate(Profile::default())
            .invoke(Get {
                address: self.address.credential_address(),
            })
            .perform(env)
            .await?;

        // Step 3: Build ForkInvocation and dispatch
        let invocation = ForkInvocation {
            address: self.address,
            credentials,
            authorization: authorized,
        };
        Ok(<Env as Provider<Fork<S, Fx>>>::execute(env, invocation).await)
    }
}
