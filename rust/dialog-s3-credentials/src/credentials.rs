use super::Address;
use super::s3;
#[cfg(feature = "ucan")]
use super::ucan;

/// Unified remote site configuration supporting multiple authorization backends.
///
/// Each variant carries the address (endpoint/bucket/region) and any
/// authentication material needed for that backend.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum Credentials {
    /// Direct S3 access: address + optional SigV4 credentials.
    S3(Address, Option<s3::S3Credentials>),
    /// UCAN-based authorization via external access service.
    #[cfg(feature = "ucan")]
    Ucan(ucan::Credentials),
}

impl std::hash::Hash for Credentials {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::S3(addr, c) => {
                addr.hash(state);
                c.hash(state);
            }
            #[cfg(feature = "ucan")]
            Self::Ucan(c) => c.hash(state),
        }
    }
}
