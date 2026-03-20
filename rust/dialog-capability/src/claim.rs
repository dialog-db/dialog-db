//! Backward-compatible claim type.
//!
//! [`Claim`] is preserved for backward compatibility with existing code
//! that uses `Remote::claim()`. New code should use
//! [`AuthorizationRequest`](crate::AuthorizationRequest) via `.at(&site)`.

use crate::{Ability, Capability, Constraint, Did, Effect, Policy};

/// A capability chain being built with an attached context.
///
/// This is a backward-compatible alias. New code should use
/// [`AuthorizationRequest`](crate::AuthorizationRequest) via `.at(&site)`.
pub struct Claim<'a, R: ?Sized, T: Constraint> {
    resource: &'a R,
    capability: Capability<T>,
}

impl<'a, R: ?Sized, T: Constraint> Claim<'a, R, T> {
    /// Create a new claim from a resource and capability.
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

    /// Get the resource.
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
