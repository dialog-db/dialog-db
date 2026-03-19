use crate::{
    Ability, Authorization, Capability, Constraint, Did, Effect, Policy, Provider,
    credential::{AcquireError, Authorize, Redeem, Remote},
};
use dialog_common::{ConditionalSend, ConditionalSync};

/// A capability chain being built with a remote resource attached.
///
/// Created via [`Remote::claim`]. Mirrors [`Capability`]'s chain-building
/// methods (`.attenuate()`, `.invoke()`) while carrying a reference to the
/// resource. When the chain reaches an [`Effect`], call `.acquire()` to
/// authorize and get an [`Authorization`] ready for execution.
///
/// # Example
///
/// ```no_run
/// # use dialog_capability::*;
/// # use dialog_capability::credential::Remote;
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Debug, Clone, Serialize, Deserialize)] struct Storage;
/// # impl Attenuation for Storage { type Of = Subject; }
/// # #[derive(Debug, Clone, Serialize, Deserialize)] struct Store { name: String }
/// # impl Policy for Store { type Of = Storage; }
/// # #[derive(Debug, Clone, Serialize, Deserialize)] struct Get { key: Vec<u8> }
/// # impl Effect for Get { type Of = Store; type Output = Option<Vec<u8>>; }
/// # async fn example<R: Remote>(resource: &R, env: impl Provider<credential::Authorize<Get, R>> + Provider<credential::Redeem<Get, R>> + Sync + Send) -> Result<(), Box<dyn std::error::Error>> {
/// let authorization = resource
///     .claim(did!("key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi"))
///     .attenuate(Storage)
///     .attenuate(Store { name: "index".into() })
///     .invoke(Get { key: b"my-key".to_vec() })
///     .acquire(&env)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct Claim<'a, R: ?Sized, T: Constraint> {
    resource: &'a R,
    capability: Capability<T>,
}

impl<'a, R: ?Sized, T: Constraint> Claim<'a, R, T> {
    /// Create a new claim from a remote resource and capability.
    pub fn new(resource: &'a R, capability: Capability<T>) -> Self {
        Self {
            resource,
            capability,
        }
    }

    /// Get the inner capability.
    pub fn capability(&self) -> &Capability<T> {
        &self.capability
    }

    /// Get the remote resource.
    pub fn resource(&self) -> &R {
        self.resource
    }

    /// Consume and return the inner capability.
    pub fn into_capability(self) -> Capability<T> {
        self.capability
    }

    /// Attenuate this claim with a policy or attenuation.
    pub fn attenuate<U>(self, value: U) -> Claim<'a, R, U>
    where
        U: Policy<Of = T>,
        T::Capability: Ability,
    {
        Claim {
            resource: self.resource,
            capability: self.capability.attenuate(value),
        }
    }

    /// Invoke an effect on this claim.
    pub fn invoke<Fx>(self, fx: Fx) -> Claim<'a, R, Fx>
    where
        Fx: Effect<Of = T>,
        T::Capability: Ability,
    {
        Claim {
            resource: self.resource,
            capability: self.capability.invoke(fx),
        }
    }
}

impl<'a, R, Fx> Claim<'a, R, Fx>
where
    Fx: Effect,
    R: Remote,
{
    /// Authorize the capability via the two-step Authorize → Redeem pipeline.
    ///
    /// Returns an [`Authorization<Fx, R::Access>`] ready for execution via
    /// [`Authorization::perform`].
    pub async fn acquire<Env>(self, env: &Env) -> Result<Authorization<Fx, R::Access>, AcquireError>
    where
        Env: Provider<Authorize<Fx, R>>
            + Provider<Redeem<Fx, R>>
            + ConditionalSend
            + ConditionalSync,
    {
        let authorize = Authorize::<Fx, R> {
            authorization: self.resource.authorization().clone(),
            address: self.resource.address().clone(),
            capability: self.capability,
        };
        let authorization = <Env as Provider<Authorize<Fx, R>>>::execute(env, authorize).await?;

        let redeem = Redeem::<Fx, R> {
            authorization,
            address: self.resource.address().clone(),
        };
        let result = <Env as Provider<Redeem<Fx, R>>>::execute(env, redeem).await?;
        Ok(result)
    }

    /// Authorize and execute the capability in one step.
    ///
    /// Combines [`acquire`](Self::acquire) and [`Authorization::perform`] into
    /// a single call. Requires the environment to provide both authorization
    /// effects and effect execution.
    ///
    /// The effect's output must be a `Result` whose error type can absorb
    /// acquire errors via `From<AcquireError>`.
    pub async fn perform<Env, T, E>(self, env: &Env) -> Result<T, E>
    where
        Env: Provider<Authorize<Fx, R>>
            + Provider<Redeem<Fx, R>>
            + Provider<Authorization<Fx, R::Access>>
            + ConditionalSend
            + ConditionalSync,
        Fx: Effect<Output = Result<T, E>>,
        E: From<AcquireError>,
    {
        let authorization = self.acquire(env).await?;
        authorization.perform(env).await
    }
}

impl<'a, R: ?Sized, T: Constraint> Claim<'a, R, T>
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
