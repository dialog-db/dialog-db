pub use dialog_varsig::Did;
pub use dialog_varsig::did;

use crate::{Capability, Constrained, Effect, Policy};

/// The subject (resource) - anchors the capability chain.
///
/// A `Subject` wraps a DID that identifies the resource owner. It is the
/// root of all capability chains - every chain starts with a Subject.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Subject(Did);

impl Subject {
    /// Get the DID.
    pub fn did(&self) -> &Did {
        &self.0
    }
}

impl std::fmt::Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Did> for Subject {
    fn from(did: Did) -> Self {
        Self(did)
    }
}

/// DID used to represent "any subject" in delegation scope.
pub const ANY_SUBJECT: &str = "did:_:_";

impl Subject {
    /// Create a wildcard subject representing "any resource".
    ///
    /// Used in delegation to grant unrestricted access across all subjects.
    pub fn any() -> Self {
        Self::from(ANY_SUBJECT.parse::<Did>().expect("valid wildcard DID"))
    }

    /// Whether this is the wildcard "any" subject.
    pub fn is_any(&self) -> bool {
        self.did().as_ref() == ANY_SUBJECT
    }

    /// Start building a capability chain from this subject.
    pub fn attenuate<T>(self, value: T) -> Capability<T>
    where
        T: Policy<Of = Self>,
    {
        Capability::new(Constrained {
            constraint: value,
            capability: self,
        })
    }

    /// Create an invocable capability directly on this subject.
    pub fn invoke<Fx>(self, fx: Fx) -> Capability<Fx>
    where
        Fx: Effect<Of = Self>,
    {
        Capability::new(Constrained {
            constraint: fx,
            capability: self,
        })
    }
}
