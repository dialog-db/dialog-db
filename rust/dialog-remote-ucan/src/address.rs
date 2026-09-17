//! The site address: the access service endpoint, and the exchange the
//! service is spoken to with.

use dialog_capability::{SiteAddress, SiteId};
use dialog_remote_ucan_s3::UcanAddress as PermitAddress;
use serde::{Deserialize, Serialize};

use crate::site::UcanSite;

/// How the site talks to the service at an address.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Exchange {
    /// The invocation rides in `Authorization` and the service performs
    /// the operation in that request. The default, and what an address
    /// that names no exchange means.
    #[default]
    Direct,
    /// The invocation is redeemed for a permit, which the site performs
    /// itself: the exchange the permit-based site speaks.
    Permit,
}

impl Exchange {
    fn is_direct(&self) -> bool {
        matches!(self, Exchange::Direct)
    }
}

/// The address of an access service: its endpoint URL, and the
/// exchange to speak there.
///
/// The exchange is left out of the encoding when it is the default, so
/// an address written before there was a choice reads back as the
/// direct exchange, and encodes to the same bytes it always did.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UcanAddress {
    /// The access service endpoint URL.
    pub endpoint: String,
    /// The exchange to speak at the endpoint.
    #[serde(default, skip_serializing_if = "Exchange::is_direct")]
    pub exchange: Exchange,
}

impl UcanAddress {
    /// An address for the access service at `endpoint`, spoken to
    /// directly.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            exchange: Exchange::Direct,
        }
    }

    /// The same address, spoken to with `exchange`.
    pub fn with_exchange(mut self, exchange: Exchange) -> Self {
        self.exchange = exchange;
        self
    }

    /// The access service endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The exchange to speak at the endpoint.
    pub fn exchange(&self) -> Exchange {
        self.exchange
    }

    /// The same endpoint as the permit-based site addresses it.
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

    /// An address that names no exchange is one wire shape with the
    /// permit-based address: whichever wrote a row, the other reads it,
    /// and it means the direct exchange here.
    #[dialog_common::test]
    fn it_encodes_exactly_as_the_permit_based_address_by_default() {
        let ours = UcanAddress::new("https://access.example/ucan/");
        let theirs = PermitAddress::new("https://access.example/ucan/");
        let ours_bytes = serde_ipld_dagcbor::to_vec(&ours).unwrap();
        let theirs_bytes = serde_ipld_dagcbor::to_vec(&theirs).unwrap();
        assert_eq!(ours_bytes, theirs_bytes);

        let read_back: UcanAddress = serde_ipld_dagcbor::from_slice(&theirs_bytes).unwrap();
        assert_eq!(read_back, ours);
        assert_eq!(read_back.exchange(), Exchange::Direct);
        let read_theirs: PermitAddress = serde_ipld_dagcbor::from_slice(&ours_bytes).unwrap();
        assert_eq!(read_theirs.endpoint(), ours.endpoint());
    }

    #[dialog_common::test]
    fn it_carries_the_permit_exchange_when_asked_for() {
        let address =
            UcanAddress::new("https://access.example/ucan/").with_exchange(Exchange::Permit);
        let bytes = serde_ipld_dagcbor::to_vec(&address).unwrap();
        let read_back: UcanAddress = serde_ipld_dagcbor::from_slice(&bytes).unwrap();
        assert_eq!(read_back, address);
        assert_eq!(read_back.exchange(), Exchange::Permit);
        assert_ne!(
            bytes,
            serde_ipld_dagcbor::to_vec(&UcanAddress::new("https://access.example/ucan/")).unwrap(),
            "the exchange is on the wire when it is not the default"
        );
    }
}
