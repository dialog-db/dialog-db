//! The site address: the access service endpoint, and the exchange the
//! service is spoken to with.

use dialog_capability::{SiteAddress, SiteId};
use dialog_did_web::Service;
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
    /// The service's socket, where one connection carries invocations
    /// both ways, which is what lets it answer a watch as a cell changes.
    /// Left out of the encoding when the service has none, so an address
    /// written before there was a socket encodes as it always did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket: Option<String>,
}

impl UcanAddress {
    /// An address for the access service at `endpoint`, spoken to
    /// directly.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            exchange: Exchange::Direct,
            socket: None,
        }
    }

    /// The same address, with the service's socket at `socket`.
    pub fn with_socket(mut self, socket: impl Into<String>) -> Self {
        self.socket = Some(socket.into());
        self
    }

    /// The service's socket, if it has one.
    pub fn socket(&self) -> Option<&str> {
        self.socket.as_deref()
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

/// The kind of a DID document's service entry that names an access
/// service's endpoint.
pub const ACCESS_SERVICE: &str = "UcanAccessService";

/// The kind of a DID document's service entry that names an access
/// service's socket.
pub const ACCESS_SOCKET: &str = "UcanAccessSocket";

impl UcanAddress {
    /// The address a DID document's services name for its access service:
    /// the endpoint of its [`ACCESS_SERVICE`] entry, and the socket of its
    /// [`ACCESS_SOCKET`] entry when it has one. `None` when the services
    /// name no access service.
    pub fn from_services(services: &[Service]) -> Option<Self> {
        let endpoint = services
            .iter()
            .find(|service| service.is(ACCESS_SERVICE))
            .and_then(Service::url)?;
        let address = Self::new(endpoint);
        Some(
            match services
                .iter()
                .find(|service| service.is(ACCESS_SOCKET))
                .and_then(Service::url)
            {
                Some(socket) => address.with_socket(socket),
                None => address,
            },
        )
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

    /// A socket is carried when the address names one, and an address
    /// that names none encodes exactly as before there was a socket.
    #[dialog_common::test]
    fn it_carries_the_socket_when_the_service_has_one() {
        let plain = UcanAddress::new("https://access.example/ucan/");
        let address = plain.clone().with_socket("wss://access.example/ucan/");
        let bytes = serde_ipld_dagcbor::to_vec(&address).unwrap();
        let read_back: UcanAddress = serde_ipld_dagcbor::from_slice(&bytes).unwrap();
        assert_eq!(read_back.socket(), Some("wss://access.example/ucan/"));
        assert_eq!(read_back, address);
        let theirs = PermitAddress::new("https://access.example/ucan/");
        assert_eq!(
            serde_ipld_dagcbor::to_vec(&plain).unwrap(),
            serde_ipld_dagcbor::to_vec(&theirs).unwrap(),
        );
    }

    fn service(kind: &str, endpoint: serde_json::Value) -> Service {
        Service {
            id: None,
            kinds: vec![kind.to_string()],
            endpoint,
        }
    }

    /// A DID document's services name an access service by its endpoint,
    /// with its socket when it has one.
    #[dialog_common::test]
    fn it_reads_the_address_the_services_name() {
        let endpoint = service(ACCESS_SERVICE, "https://access.example/ucan/".into());
        let socket = service(ACCESS_SOCKET, "wss://access.example/ucan/".into());
        let other = service("Other", "https://elsewhere.example/".into());

        assert_eq!(
            UcanAddress::from_services(&[other.clone(), endpoint.clone(), socket]),
            Some(
                UcanAddress::new("https://access.example/ucan/")
                    .with_socket("wss://access.example/ucan/")
            )
        );
        assert_eq!(
            UcanAddress::from_services(&[endpoint]),
            Some(UcanAddress::new("https://access.example/ucan/")),
            "no socket entry, no socket"
        );
        assert_eq!(UcanAddress::from_services(&[other]), None);
        assert_eq!(
            UcanAddress::from_services(&[service(ACCESS_SERVICE, serde_json::json!({}))]),
            None,
            "an endpoint that is not a URL names no address"
        );
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
