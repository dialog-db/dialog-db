//! Serializable remote address configuration.
//!
//! [`RemoteAddress`] carries both the address (endpoint/bucket/region) and
//! authentication material needed for the remote backend.

use std::hash::{Hash, Hasher};
use std::mem;

use dialog_remote_s3::Address;
use dialog_remote_s3::s3::S3Credentials;

/// Serializable remote address configuration.
///
/// Carries both the address (endpoint/bucket/region) and authentication
/// material needed for the remote backend.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum RemoteAddress {
    /// Direct S3 access: address + optional SigV4 credentials.
    S3(Address, Option<S3Credentials>),
    /// UCAN-based authorization via external access service.
    #[cfg(feature = "ucan")]
    Ucan(dialog_remote_ucan_s3::Credentials),
}

impl Hash for RemoteAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
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
