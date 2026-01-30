use crate::{Ability, Authorization, Did};

/// A delegation granting authority over a capability.
///
/// Contains:
/// - `issuer` - who created this delegation
/// - `audience` - who this delegation is for
/// - `capability` - the capability being delegated
/// - `authorization` - proof of authority (signature, UCAN chain, etc.)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Delegation<C: Ability, A: Authorization> {
    /// Who issued this delegation.
    pub issuer: Did,
    /// Who this delegation is for.
    pub audience: Did,
    /// The capability being delegated.
    pub capability: C,
    /// The authorization proof.
    pub authorization: A,
}

impl<C: Ability, A: Authorization> Delegation<C, A> {
    /// Create a new delegation.
    pub fn new(issuer: Did, audience: Did, capability: C, authorization: A) -> Self {
        Self {
            issuer,
            audience,
            capability,
            authorization,
        }
    }

    /// Get the issuer DID.
    pub fn issuer(&self) -> &Did {
        &self.issuer
    }

    /// Get the audience DID.
    pub fn audience(&self) -> &Did {
        &self.audience
    }

    /// Get the capability.
    pub fn capability(&self) -> &C {
        &self.capability
    }

    /// Get the authorization.
    pub fn authorization(&self) -> &A {
        &self.authorization
    }
}
