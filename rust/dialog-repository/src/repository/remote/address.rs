//! Remote address types.
//!
//! Re-exports [`NetworkAddress`] and [`Network`] from dialog-operator.
//! [`RemoteAddress`] pairs a site address with a subject DID to identify
//! a specific remote repository.

use dialog_capability::Did;

pub use dialog_operator::network::Network as RemoteSite;
pub use dialog_operator::network::NetworkAddress as SiteAddress;

/// A remote repository address -- connection info plus subject DID.
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
