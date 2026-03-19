use crate::{
    Ability, Access, Authorized, Capability, Constraint, Did, Effect, Policy, Provider, credential,
};
use dialog_common::{ConditionalSend, ConditionalSync};

/// A capability chain being built with credentials attached.
///
/// Created by [`Access::claim`]. Mirrors [`Capability`]'s chain-building
/// methods (`.attenuate()`, `.invoke()`) while carrying a reference to the
/// credentials. When the chain reaches an [`Effect`], call `.acquire()` to
/// authorize and get an [`Authorized`] capability ready for execution.
///
/// # Example
///
/// ```no_run
/// # use dialog_capability::*;
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Debug, Clone, Serialize, Deserialize)] struct Storage;
/// # impl Attenuation for Storage { type Of = Subject; }
/// # #[derive(Debug, Clone, Serialize, Deserialize)] struct Store { name: String }
/// # impl Policy for Store { type Of = Storage; }
/// # #[derive(Debug, Clone, Serialize, Deserialize)] struct Get { key: Vec<u8> }
/// # impl Effect for Get { type Of = Store; type Output = Option<Vec<u8>>; }
/// # async fn example<A: Access<Get>>(credentials: A, session: impl Provider<credential::Identify> + Provider<credential::Sign> + Sync + Send) -> Result<(), Box<dyn std::error::Error>> where A::Error: 'static {
/// let authorized = credentials
///     .claim(did!("key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi"))
///     .attenuate(Storage)
///     .attenuate(Store { name: "index".into() })
///     .invoke(Get { key: b"my-key".to_vec() })
///     .acquire(&session)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct Claim<'a, A: ?Sized, T: Constraint> {
    access: &'a A,
    capability: Capability<T>,
}

impl<'a, A: ?Sized, T: Constraint> Claim<'a, A, T> {
    /// Create a new claim from an access provider and capability.
    pub fn new(access: &'a A, capability: Capability<T>) -> Self {
        Self { access, capability }
    }

    /// Get the inner capability.
    pub fn capability(&self) -> &Capability<T> {
        &self.capability
    }

    /// Get the access provider.
    pub fn access(&self) -> &A {
        self.access
    }

    /// Consume and return the inner capability.
    pub fn into_capability(self) -> Capability<T> {
        self.capability
    }

    /// Attenuate this claim with a policy or attenuation.
    pub fn attenuate<U>(self, value: U) -> Claim<'a, A, U>
    where
        U: Policy<Of = T>,
        T::Capability: Ability,
    {
        Claim {
            access: self.access,
            capability: self.capability.attenuate(value),
        }
    }

    /// Invoke an effect on this claim.
    pub fn invoke<Fx>(self, fx: Fx) -> Claim<'a, A, Fx>
    where
        Fx: Effect<Of = T>,
        T::Capability: Ability,
    {
        Claim {
            access: self.access,
            capability: self.capability.invoke(fx),
        }
    }
}

impl<'a, A, Fx> Claim<'a, A, Fx>
where
    Fx: Effect,
    A: Access<Fx>,
{
    /// Authorize the capability, returning an [`Authorized`] ready for execution.
    pub async fn acquire<Env>(self, env: &Env) -> Result<Authorized<Fx, A::Authorization>, A::Error>
    where
        Env: Provider<credential::Identify>
            + Provider<credential::Sign>
            + ConditionalSend
            + ConditionalSync,
    {
        self.access.authorize(self.capability, env).await
    }

    /// Authorize and execute the capability in one step.
    ///
    /// Combines [`acquire`](Self::acquire) and [`Authorized::perform`] into a
    /// single call. Requires the environment to provide both credential effects
    /// (for authorization) and effect execution (for the actual operation).
    ///
    /// The effect's output must be a `Result` whose error type can absorb
    /// authorization errors via `From<A::Error>`.
    pub async fn perform<Env, T, E>(self, env: &Env) -> Result<T, E>
    where
        Env: Provider<credential::Identify>
            + Provider<credential::Sign>
            + Provider<Authorized<Fx, A::Authorization>>
            + ConditionalSend
            + ConditionalSync,
        Fx: Effect<Output = Result<T, E>>,
        E: From<A::Error>,
    {
        let authorized = self.acquire(env).await?;
        authorized.perform(env).await
    }
}

impl<'a, A: ?Sized, T: Constraint> Claim<'a, A, T>
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
