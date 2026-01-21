//! Claim type for capability authorization queries.
//!
//! A `Claim` wraps a capability with an audience (who needs authorization).
//! It represents a query to an `Access` for authorization.

use super::ability::Ability;
use super::subject::Did;

/// A capability claim requesting authorization.
///
/// Wraps a capability with the audience (who needs authorization).
/// Used as a query to `Access` stores.
#[derive(Debug, Clone)]
pub struct Claim<C> {
    /// The capability being claimed.
    pub capability: C,
    /// Who this claim is for (the audience).
    pub audience: Did,
}

impl<C> Claim<C> {
    /// Create a new claim.
    pub fn new(capability: C, audience: Did) -> Self {
        Self {
            capability,
            audience,
        }
    }

    /// Get the capability.
    pub fn capability(&self) -> &C {
        &self.capability
    }

    /// Consume the claim and return the capability.
    pub fn into_capability(self) -> C {
        self.capability
    }

    /// Get the audience DID.
    pub fn audience(&self) -> &Did {
        &self.audience
    }
}

impl<C: Ability> Claim<C> {
    /// Get the subject DID from the capability.
    pub fn subject(&self) -> &Did {
        self.capability.subject()
    }

    /// Get the command path from the capability.
    pub fn command(&self) -> String {
        self.capability.command()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::{Attenuation, Capability, Subject};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestAbility;

    impl Attenuation for TestAbility {
        type Of = Subject;
    }

    #[test]
    fn it_creates_claim_with_accessors() {
        let cap = Subject::from("did:key:zSubject").attenuate(TestAbility);
        let claim = Claim::new(cap.clone(), "did:key:zAudience".into());

        assert_eq!(claim.audience(), "did:key:zAudience");
        assert_eq!(claim.subject(), "did:key:zSubject");
        assert_eq!(claim.command(), "/testability");
    }

    #[test]
    fn it_provides_capability_reference() {
        let cap = Subject::from("did:key:zSubject").attenuate(TestAbility);
        let claim = Claim::new(cap, "did:key:zAudience".into());

        let retrieved = claim.capability();
        assert_eq!(retrieved.subject(), "did:key:zSubject");
    }
}
