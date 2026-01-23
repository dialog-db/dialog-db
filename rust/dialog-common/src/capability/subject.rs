//! Subject type - the root anchor of capability chains.
//!
//! The `Subject` represents the resource owner (identified by a DID) and serves
//! as the starting point for building capability chains.

use super::constrained::Constrained;
use super::interface::Capability;
use super::policy::Policy;

/// A DID (Decentralized Identifier).
pub type Did = String;

/// The subject (resource) - anchors the capability chain.
///
/// A `Subject` wraps a DID that identifies the resource owner. It is the
/// root of all capability chains - every chain starts with a Subject.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Subject(pub Did);

impl From<&str> for Subject {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for Subject {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Subject {
    /// Start building a capability chain from this subject.
    pub fn attenuate<T>(self, value: T) -> Capability<T>
    where
        T: Policy<Of = Self>,
    {
        Capability(Constrained {
            constraint: value,
            capability: self,
        })
    }
}
