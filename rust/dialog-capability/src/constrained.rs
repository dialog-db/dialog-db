use crate::credential::{self, Addressable};
use crate::fork::ForkInvocation;
use crate::site::Site;
use crate::{
    Ability, Capability, Constraint, Did, Effect, Here, Policy, Provider, Selector, Subject, There,
};
use dialog_common::ConditionalSend;

/// A capability chain element - constraint applied to a parent capability.
///
/// Build capability chains by constraining from a Subject:
///
/// ```
/// use dialog_capability::{Subject, Policy, Capability, Attenuation, did};
/// use serde::{Serialize, Deserialize};
///
/// // Define an attenuation type
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct Storage;
///
/// impl Attenuation for Storage {
///     type Of = Subject;
/// }
///
/// // Build a capability chain
/// let cap: Capability<Storage> = Subject::from(did!("key:zSpace"))
///     .attenuate(Storage);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(bound(deserialize = ""))]
pub struct Constrained<P: Policy, Of: Ability> {
    /// The policy/ability being added.
    pub constraint: P,
    /// The parent capability.
    pub capability: Of,
}

impl<P: Policy, Of: Ability> Constrained<P, Of> {
    /// Extend this capability with another policy/ability.
    pub fn attenuate<T>(self, value: T) -> Constrained<T, Self>
    where
        T: Policy,
    {
        Constrained {
            constraint: value,
            capability: self,
        }
    }

    /// Extract a policy or ability from this chain.
    pub fn policy<T, Index>(&self) -> &T
    where
        Self: Selector<T, Index>,
    {
        self.select()
    }

    /// Get the subject DID from the capability chain.
    pub fn subject(&self) -> &Did {
        Ability::subject(self)
    }

    /// Get the ability path (e.g., `/storage/get`).
    pub fn ability(&self) -> String {
        Ability::ability(self)
    }

    /// Add an effect to create an invocation capability.
    pub fn invoke<Fx: Effect<Of = P>>(self, fx: Fx) -> Constrained<Fx, Self> {
        Constrained {
            constraint: fx,
            capability: self,
        }
    }
}

/// Implementation for effect capabilities.
///
/// When a Constrained's constraint is an Effect, we can perform it.
impl<Fx, Of> Constrained<Fx, Of>
where
    Fx: Effect,
    Fx::Of: Constraint,
    Of: Ability,
{
    /// Perform the invocation directly without authorization verification.
    ///
    /// Use this when the provider trusts the caller (e.g., local execution).
    /// For operations that require authorization, use `acquire` first.
    pub async fn perform<Env>(self, env: &Env) -> Fx::Output
    where
        Self: Into<Capability<Fx>>,
        Env: Provider<Fx>,
    {
        env.execute(self.into()).await
    }

    /// Authorize this capability for a specific site.
    ///
    /// Builds a `Capability<credential::Authorize<Fx, S::Format>>` from this
    /// capability and executes it via the environment's authorization provider.
    pub async fn acquire<S, Env>(
        self,
        env: &Env,
    ) -> Result<credential::Authorization<Fx, S::Format>, credential::AuthorizeError>
    where
        Self: Into<Capability<Fx>>,
        Fx: Clone,
        S: Site,
        Capability<Fx>: Ability + ConditionalSend,
        credential::Authorize<Fx, S::Format>: ConditionalSend + 'static,
        Env: Provider<credential::Authorize<Fx, S::Format>>,
    {
        let capability: Capability<Fx> = self.into();
        let authorize_cap = build_authorize_cap::<Fx, S::Format>(capability);
        <Env as Provider<credential::Authorize<Fx, S::Format>>>::execute(env, authorize_cap).await
    }

    /// Attach a site address to this invocation capability, returning a
    /// `SiteInvocation` that can be authorized and executed.
    pub fn fork<S: Site>(self, address: &S::Address) -> SiteInvocation<Fx, S>
    where
        Self: Into<Capability<Fx>>,
    {
        SiteInvocation::new(self.into(), address.clone())
    }
}

/// An invocation capability paired with a site address for remote execution.
///
/// Created by `.fork(address)`. Call `acquire` to authorize and
/// build the site-specific `ForkInvocation`.
pub struct SiteInvocation<Fx, S>
where
    Fx: Effect,
    Fx::Of: Constraint,
    S: Site,
{
    capability: Capability<Fx>,
    address: S::Address,
}

impl<Fx, S> SiteInvocation<Fx, S>
where
    Fx: Effect,
    Fx::Of: Constraint,
    S: Site,
{
    /// Create a new SiteInvocation from a capability and a site address.
    pub fn new(capability: Capability<Fx>, address: S::Address) -> Self {
        Self {
            capability,
            address,
        }
    }

    /// Authorize the capability and build a `ForkInvocation`.
    ///
    /// 1. Authorizes via `Provider<credential::Authorize<Fx, S::Format>>`
    /// 2. Looks up credentials via `Provider<credential::Get<S::Credentials>>`
    /// 3. Builds `ForkInvocation { address, credentials, authorization }`
    pub async fn acquire<Env>(
        self,
        env: &Env,
    ) -> Result<ForkInvocation<S, Fx>, credential::AuthorizeError>
    where
        Fx: Clone,
        Capability<Fx>: Ability + Clone + ConditionalSend,
        credential::Authorize<Fx, S::Format>: ConditionalSend + 'static,
        credential::Get<S::Credentials>: ConditionalSend + 'static,
        Env: Provider<credential::Authorize<Fx, S::Format>>
            + Provider<credential::Get<S::Credentials>>,
    {
        // Step 1: Authorize
        let authorize_cap = build_authorize_cap::<Fx, S::Format>(self.capability.clone());
        let authorization =
            <Env as Provider<credential::Authorize<Fx, S::Format>>>::execute(env, authorize_cap)
                .await?;

        // Step 2: Lookup credentials
        let get_cap = build_get_cap::<S::Credentials>(
            self.capability.subject().clone(),
            self.address.credential_address(),
        );
        let credentials: S::Credentials =
            <Env as Provider<credential::Get<S::Credentials>>>::execute(env, get_cap)
                .await
                .map_err(|e: credential::CredentialError| {
                    credential::AuthorizeError::Configuration(e.to_string())
                })?;

        // Step 3: Build ForkInvocation
        Ok(ForkInvocation {
            address: self.address,
            credentials,
            authorization,
        })
    }
}

/// Build a `Capability<credential::Authorize<Fx, F>>` from a `Capability<Fx>`.
///
/// Constructs the full credential chain: Subject -> Credential -> Profile -> Authorize<Fx, F>.
fn build_authorize_cap<Fx, F>(
    capability: Capability<Fx>,
) -> Capability<credential::Authorize<Fx, F>>
where
    Fx: Effect,
    Fx::Of: Constraint,
    F: credential::AuthorizationFormat,
    Capability<Fx>: Ability + ConditionalSend,
    credential::Authorize<Fx, F>: ConditionalSend + 'static,
{
    let did = capability.subject().clone();
    Subject::from(did)
        .attenuate(credential::Credential)
        .attenuate(credential::Profile::default())
        .invoke(credential::Authorize::<Fx, F>::new(capability))
}

/// Build a `Capability<credential::Get<C>>` for looking up credentials.
fn build_get_cap<C>(did: Did, address: credential::Address<C>) -> Capability<credential::Get<C>>
where
    C: serde::Serialize + serde::de::DeserializeOwned + ConditionalSend + 'static,
{
    Subject::from(did)
        .attenuate(credential::Credential)
        .attenuate(credential::Profile::default())
        .invoke(credential::Get { address })
}

// Selector Implementations

/// Select the head constraint from a Constrained.
impl<T: Policy, Tail: Ability> Selector<T, Here> for Constrained<T, Tail> {
    fn select(&self) -> &T {
        &self.constraint
    }
}

/// Recursively select from the tail of a Constrained.
impl<Head: Policy, Tail: Ability, T, Index> Selector<T, There<Index>> for Constrained<Head, Tail>
where
    Tail: Selector<T, Index>,
{
    fn select(&self) -> &T {
        self.capability.select()
    }
}
