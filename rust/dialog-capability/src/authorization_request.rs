//! Authorization request — pairs a site with a capability chain.
//!
//! [`AuthorizationRequest`] replaces the old `Claim` type. It carries a
//! reference to a [`Site`] alongside a [`Capability`] chain, enabling the
//! `.acquire()` method to run the Authorize → Redeem pipeline.

use crate::{
    Ability, Authorization, Capability, Constraint, Did, Effect, Policy, Provider,
    credential::{AcquireError, Authorize, AuthorizeError, Redeem},
    site::{Local, RemoteSite},
};
#[allow(unused_imports)]
use dialog_common::ConditionalSend;
use dialog_common::ConditionalSync;

/// A capability chain paired with a site reference for authorization.
///
/// Created via [`.at(&site)`](crate::Capability::at) on a [`Capability`] or
/// [`Subject`](crate::Subject). Mirrors [`Capability`]'s chain-building
/// methods (`.attenuate()`, `.invoke()`) while carrying a reference to the
/// site. When the chain reaches an [`Effect`], call `.acquire()` to
/// authorize and get an [`Authorization`] ready for execution.
///
/// # Example
///
/// ```no_run
/// # use dialog_capability::*;
/// # use dialog_capability::site::Local;
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Debug, Clone, Serialize, Deserialize)] struct Storage;
/// # impl Attenuation for Storage { type Of = Subject; }
/// # #[derive(Debug, Clone, Serialize, Deserialize)] struct Store { name: String }
/// # impl Policy for Store { type Of = Storage; }
/// # #[derive(Debug, Clone, Serialize, Deserialize)] struct Get { key: Vec<u8> }
/// # impl Effect for Get { type Of = Store; type Output = Option<Vec<u8>>; }
/// # async fn example(env: impl Provider<credential::Authorize<Get, Local>> + Sync + Send) -> Result<(), Box<dyn std::error::Error>> {
/// let authorization = Subject::from(did!("key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi"))
///     .at(&Local)
///     .attenuate(Storage)
///     .attenuate(Store { name: "index".into() })
///     .invoke(Get { key: b"my-key".to_vec() })
///     .acquire(&env)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct AuthorizationRequest<'a, S: ?Sized, T: Constraint> {
    site: &'a S,
    capability: Capability<T>,
}

impl<'a, S: ?Sized, T: Constraint> AuthorizationRequest<'a, S, T> {
    /// Create a new authorization request from a site and capability.
    pub fn new(site: &'a S, capability: Capability<T>) -> Self {
        Self { site, capability }
    }

    /// Get the inner capability.
    pub fn capability(&self) -> &Capability<T> {
        &self.capability
    }

    /// Get the site.
    pub fn site(&self) -> &S {
        self.site
    }

    /// Consume and return the inner capability.
    pub fn into_capability(self) -> Capability<T> {
        self.capability
    }

    /// Attenuate this request with a policy or attenuation.
    pub fn attenuate<U>(self, value: U) -> AuthorizationRequest<'a, S, U>
    where
        U: Policy<Of = T>,
        T::Capability: Ability,
    {
        AuthorizationRequest {
            site: self.site,
            capability: self.capability.attenuate(value),
        }
    }

    /// Invoke an effect on this request.
    pub fn invoke<Fx>(self, fx: Fx) -> AuthorizationRequest<'a, S, Fx>
    where
        Fx: Effect<Of = T>,
        T::Capability: Ability,
    {
        AuthorizationRequest {
            site: self.site,
            capability: self.capability.invoke(fx),
        }
    }
}

/// Remote sites: Authorize → Redeem pipeline.
impl<'a, S, Fx> AuthorizationRequest<'a, S, Fx>
where
    Fx: Effect,
    S: RemoteSite,
{
    /// Authorize the capability via the two-step Authorize → Redeem pipeline.
    ///
    /// Returns an [`Authorization<Fx, S::Access>`] ready for execution via
    /// [`Authorization::perform`].
    pub async fn acquire<Env>(self, env: &Env) -> Result<Authorization<Fx, S::Access>, AcquireError>
    where
        Env: Provider<Authorize<Fx, S>>
            + Provider<Redeem<Fx, S>>
            + ConditionalSend
            + ConditionalSync,
    {
        let authorize = Authorize::<Fx, S> {
            site: self.site.clone(),
            capability: self.capability,
        };
        let authorization = <Env as Provider<Authorize<Fx, S>>>::execute(env, authorize).await?;

        let redeem = Redeem::<Fx, S> {
            authorization,
            site: self.site.clone(),
        };
        let result = <Env as Provider<Redeem<Fx, S>>>::execute(env, redeem).await?;
        Ok(result)
    }

    /// Authorize and execute the capability in one step.
    ///
    /// Combines [`acquire`](Self::acquire) and [`Authorization::perform`] into
    /// a single call.
    pub async fn perform<Env, T, E>(self, env: &Env) -> Result<T, E>
    where
        Env: Provider<Authorize<Fx, S>>
            + Provider<Redeem<Fx, S>>
            + Provider<Authorization<Fx, S::Access>>
            + ConditionalSend
            + ConditionalSync,
        Fx: Effect<Output = Result<T, E>>,
        E: From<AcquireError>,
    {
        let authorization = self.acquire(env).await?;
        authorization.perform(env).await
    }
}

/// Local: operator access check only (no Redeem step).
impl<'a, Fx> AuthorizationRequest<'a, Local, Fx>
where
    Fx: Effect,
{
    /// Authorize the capability for local execution.
    ///
    /// Only runs the Authorize step — no Redeem is needed for local sites.
    pub async fn acquire<Env>(self, env: &Env) -> Result<Authorization<Fx, Local>, AuthorizeError>
    where
        Env: Provider<Authorize<Fx, Local>> + ConditionalSend + ConditionalSync,
    {
        let authorize = Authorize::<Fx, Local> {
            site: Local,
            capability: self.capability,
        };
        <Env as Provider<Authorize<Fx, Local>>>::execute(env, authorize).await
    }

    /// Authorize and execute the capability in one step (local).
    pub async fn perform<Env, T, E>(self, env: &Env) -> Result<T, E>
    where
        Env: Provider<Authorize<Fx, Local>>
            + Provider<Authorization<Fx, Local>>
            + ConditionalSend
            + ConditionalSync,
        Fx: Effect<Output = Result<T, E>>,
        E: From<AuthorizeError>,
    {
        let authorization = self.acquire(env).await?;
        authorization.perform(env).await
    }
}

impl<'a, S: ?Sized, T: Constraint> AuthorizationRequest<'a, S, T>
where
    T::Capability: Ability,
{
    /// Get the subject DID from the capability chain.
    pub fn subject(&self) -> &Did {
        self.capability.subject()
    }

    /// Get the ability path from the capability chain.
    pub fn ability(&self) -> String {
        self.capability.ability()
    }
}
