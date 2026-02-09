pub use varsig::Did;
pub use varsig::did;

use crate::{Capability, Constrained, Policy};

/// The subject (resource) - anchors the capability chain.
///
/// A `Subject` wraps a DID that identifies the resource owner. It is the
/// root of all capability chains - every chain starts with a Subject.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Subject(pub Did);

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
