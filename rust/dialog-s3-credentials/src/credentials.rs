use super::s3;
#[cfg(feature = "ucan")]
use super::ucan;
use crate::capability::AuthorizedRequest;
use crate::s3::provider::S3Permit;
use dialog_capability::credential;

/// Unified credentials enum supporting multiple authorization backends.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum Credentials {
    /// Direct S3 credentials (public or private).
    S3(s3::Credentials),
    /// UCAN-based authorization via external access service.
    #[cfg(feature = "ucan")]
    Ucan(ucan::Credentials),
}

impl std::hash::Hash for Credentials {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::S3(c) => c.hash(state),
            #[cfg(feature = "ucan")]
            Self::Ucan(c) => c.hash(state),
        }
    }
}

impl From<s3::Credentials> for Credentials {
    fn from(credentials: s3::Credentials) -> Self {
        Self::S3(credentials)
    }
}

#[cfg(feature = "ucan")]
impl From<ucan::Credentials> for Credentials {
    fn from(credentials: ucan::Credentials) -> Self {
        Self::Ucan(credentials)
    }
}

/// Unified proof type for the Credentials enum.
#[derive(Debug)]
pub enum UnifiedProof {
    /// S3 permit (credentials ready to presign).
    S3(S3Permit),
    /// UCAN invocation (signed, ready to POST).
    #[cfg(feature = "ucan")]
    Ucan(ucan::UcanInvocation),
}

impl credential::Remote for Credentials {
    type Authorization = Credentials;
    type Permit = UnifiedProof;
    type Access = AuthorizedRequest;
    type Address = Credentials;

    fn address(&self) -> &Credentials {
        self
    }

    fn authorization(&self) -> &Credentials {
        self
    }
}
