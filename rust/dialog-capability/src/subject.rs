pub use dialog_varsig::Did;
pub use dialog_varsig::did;

use crate::site::Site;
use crate::{AuthorizationRequest, Capability, Constrained, Policy};

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

    /// Attach a site to this subject, creating an [`AuthorizationRequest`].
    pub fn at<S: Site>(self, site: &S) -> AuthorizationRequest<'_, S, Self> {
        AuthorizationRequest::new(site, Capability::<Self>::new(self))
    }
}
