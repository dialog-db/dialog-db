//! The site address: the access service endpoint.

use dialog_capability::{SiteAddress, SiteId};
use dialog_remote_ucan_s3::UcanAddress as PermitAddress;
use serde::{Deserialize, Serialize};

use crate::site::UcanSite;

/// The address of an access service: its endpoint URL.
///
/// The same shape, field for field, as the permit-based remote's
/// address, so it encodes to the same bytes: a directory row written by
/// either reads back as either, and the variant a network address files
/// it under does not change.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UcanAddress {
    /// The access service endpoint URL.
    pub endpoint: String,
}

impl UcanAddress {
    /// An address for the access service at `endpoint`.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    /// The access service endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The same endpoint as the permit-based remote addresses it, for
    /// the exchanges that go through permits.
    pub(crate) fn permits(&self) -> PermitAddress {
        PermitAddress::new(self.endpoint.clone())
    }
}

impl SiteAddress for UcanAddress {
    type Site = UcanSite;
}

impl From<UcanAddress> for SiteId {
    fn from(address: UcanAddress) -> Self {
        address.endpoint.into()
    }
}

impl From<PermitAddress> for UcanAddress {
    fn from(address: PermitAddress) -> Self {
        Self::new(address.endpoint)
    }
}

impl From<UcanAddress> for PermitAddress {
    fn from(address: UcanAddress) -> Self {
        address.permits()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    /// The two address types are one wire shape: whichever wrote a row,
    /// the other reads it.
    #[dialog_common::test]
    fn it_encodes_exactly_as_the_permit_based_address() {
        let ours = UcanAddress::new("https://access.example/ucan/");
        let theirs = PermitAddress::new("https://access.example/ucan/");
        let ours_bytes = serde_ipld_dagcbor::to_vec(&ours).unwrap();
        let theirs_bytes = serde_ipld_dagcbor::to_vec(&theirs).unwrap();
        assert_eq!(ours_bytes, theirs_bytes);

        let read_back: UcanAddress = serde_ipld_dagcbor::from_slice(&theirs_bytes).unwrap();
        assert_eq!(read_back, ours);
        let read_theirs: PermitAddress = serde_ipld_dagcbor::from_slice(&ours_bytes).unwrap();
        assert_eq!(read_theirs.endpoint(), ours.endpoint());
    }
}
