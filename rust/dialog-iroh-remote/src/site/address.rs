//! Naming a peer.

use dialog_capability::{SiteAddress, SiteId};
use iroh_base::{EndpointAddr, EndpointId};
use serde::{Deserialize, Serialize};

/// Where a peer is, and who it is.
///
/// Wraps iroh's [`EndpointAddr`] — an endpoint id plus however many
/// routes are known — because that is already the shape iroh dials and
/// already serializes. A route set may be empty: iroh resolves an
/// endpoint id through its address lookups, and a stored route is a hint
/// rather than a requirement.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IrohAddress(EndpointAddr);

impl IrohAddress {
    /// Address a peer by everything known about it.
    pub fn new(addr: impl Into<EndpointAddr>) -> Self {
        Self(addr.into())
    }

    /// The peer's identity.
    pub fn endpoint(&self) -> &EndpointId {
        &self.0.id
    }

    /// Everything known about where the peer is.
    pub fn addr(&self) -> &EndpointAddr {
        &self.0
    }
}

impl SiteAddress for IrohAddress {
    type Site = crate::site::Iroh;
}

impl From<EndpointAddr> for IrohAddress {
    fn from(addr: EndpointAddr) -> Self {
        Self(addr)
    }
}

impl std::fmt::Display for IrohAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.id.fmt(f)
    }
}

/// The credential-store key is the endpoint id alone, never the routes.
///
/// iroh's own guidance is to cache endpoint ids and let it resolve
/// current dialing details, because "the dialing information in a ticket
/// (especially IP addresses) can become outdated as network conditions
/// change". Keying on the whole address would orphan a credential every
/// time a peer moved — and the routes are not identity in any case: the
/// endpoint id is a public key, and iroh's TLS refuses anything that
/// cannot prove it.
impl From<IrohAddress> for SiteId {
    fn from(address: IrohAddress) -> Self {
        SiteId::from(address.0.id.to_string())
    }
}
