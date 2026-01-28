use crate::{
    Ability, Capability, Constraint, Did, Effect, Here, Policy, Provider, Selector, There,
};

/// A capability chain element - constraint applied to a parent capability.
///
/// Build capability chains by constraining from a Subject:
///
/// ```
/// use dialog_capability::{Subject, Policy, Capability, Attenuation};
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
/// let cap: Capability<Storage> = Subject::from("did:key:zSpace")
///     .attenuate(Storage);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

    /// Collect all parameters from this capability chain into a new map.
    ///
    /// This walks the capability chain and collects parameters from each
    /// constraint.
    #[cfg(feature = "ucan")]
    pub fn parameters(&self) -> super::settings::Parameters {
        let mut parameters = super::settings::Parameters::new();
        Ability::parametrize(self, &mut parameters);
        parameters
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
    pub async fn perform<Env>(self, env: &mut Env) -> Fx::Output
    where
        Self: Into<Capability<Fx>>,
        Env: Provider<Fx>,
    {
        env.execute(self.into()).await
    }
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
