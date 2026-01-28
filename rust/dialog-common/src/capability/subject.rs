//! Subject type - the root anchor of capability chains.
//!
//! The `Subject` represents the resource owner (identified by a DID) and serves
//! as the starting point for building capability chains.

use super::{Capability, Constrained, Policy};

/// A DID (Decentralized Identifier).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize)]
pub struct Did(pub String);

impl std::fmt::Display for Did {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Did {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for Did {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&Did> for Did {
    fn from(value: &Did) -> Self {
        value.clone()
    }
}

impl AsRef<str> for Did {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for Did {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for Did {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl std::ops::Deref for Did {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<String> for Did {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

#[cfg(feature = "ucan")]
impl From<ucan::did::Ed25519Did> for Did {
    fn from(value: ucan::did::Ed25519Did) -> Self {
        Self(value.to_string())
    }
}

#[cfg(feature = "ucan")]
impl From<&ucan::did::Ed25519Did> for Did {
    fn from(value: &ucan::did::Ed25519Did) -> Self {
        Self(value.to_string())
    }
}

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
        Self(value.into())
    }
}

impl From<Did> for Subject {
    fn from(value: Did) -> Self {
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
