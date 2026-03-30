//! Serializable remote address configuration.
//!
//! [`RemoteAddress`] carries the address (endpoint/bucket/region) for the
//! remote backend. Credentials are stored separately in the credential store.

use std::hash::{Hash, Hasher};
use std::mem;

use dialog_remote_s3::Address;

/// Serializable remote address configuration.
///
/// Carries the address (endpoint/bucket/region) for the remote backend.
/// Credentials are stored separately in the credential store and resolved
/// via `Provider<credential::Retrieve<...>>` during remote operations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum RemoteAddress {
    /// Direct S3 access: address (endpoint/region/bucket).
    S3(Address),
    /// UCAN-based authorization via external access service.
    #[cfg(feature = "ucan")]
    Ucan(dialog_remote_ucan_s3::UcanAddress),
}

impl Hash for RemoteAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
        match self {
            Self::S3(addr) => {
                addr.hash(state);
            }
            #[cfg(feature = "ucan")]
            Self::Ucan(c) => c.hash(state),
        }
    }
}
