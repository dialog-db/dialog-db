//! Remote address types.
//!
//! [`SiteAddress`] is the connection info (endpoint/credentials).
//! [`RemoteAddress`] pairs it with a subject DID to identify a specific
//! remote repository.

use std::hash::{Hash, Hasher};
use std::mem;

use dialog_capability::Did;
use dialog_remote_s3::Address;

/// Connection info for a remote site.
///
/// Carries the address (endpoint/bucket/region) for the remote backend.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SiteAddress {
    /// Direct S3 access.
    S3(Address),
    /// UCAN-based authorization via external access service.
    #[cfg(feature = "ucan")]
    Ucan(dialog_remote_ucan_s3::UcanAddress),
}

impl From<Address> for SiteAddress {
    fn from(addr: Address) -> Self {
        Self::S3(addr)
    }
}

#[cfg(feature = "ucan")]
impl From<dialog_remote_ucan_s3::UcanAddress> for SiteAddress {
    fn from(addr: dialog_remote_ucan_s3::UcanAddress) -> Self {
        Self::Ucan(addr)
    }
}

impl Hash for SiteAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
        match self {
            Self::S3(addr) => addr.hash(state),
            #[cfg(feature = "ucan")]
            Self::Ucan(c) => c.hash(state),
        }
    }
}

/// A remote repository address — connection info plus subject DID.
///
/// This is what gets stored in the `remote/{name}/address` cell.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct RemoteAddress {
    /// How to connect to the remote.
    pub address: SiteAddress,
    /// Which repository (subject DID) at that site.
    pub subject: Did,
}

impl RemoteAddress {
    /// Create a new remote address.
    pub fn new(address: SiteAddress, subject: Did) -> Self {
        Self { address, subject }
    }

    /// The site connection info.
    pub fn site(&self) -> &SiteAddress {
        &self.address
    }

    /// The subject DID of the remote repository.
    pub fn subject(&self) -> &Did {
        &self.subject
    }
}
