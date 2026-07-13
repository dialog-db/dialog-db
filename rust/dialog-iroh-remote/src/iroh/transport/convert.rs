//! Conversions between [`IrohAddress`] and live iroh types.

use crate::{IrohAddress, IrohRemoteError};
use iroh::{EndpointAddr, EndpointId, RelayUrl};

impl IrohAddress {
    /// Parse the address's endpoint id into an [`EndpointId`].
    pub fn endpoint_id(&self) -> Result<EndpointId, IrohRemoteError> {
        self.endpoint()
            .parse::<EndpointId>()
            .map_err(|e| IrohRemoteError::Address(format!("invalid endpoint id: {e}")))
    }

    /// Resolve the address into a dialable [`EndpointAddr`], carrying any
    /// relay/direct hints it holds. Hints that fail to parse are skipped —
    /// the endpoint id is the identity; hints are best-effort.
    pub fn endpoint_addr(&self) -> Result<EndpointAddr, IrohRemoteError> {
        let mut addr = EndpointAddr::new(self.endpoint_id()?);
        if let Some(relay) = self.relay_url()
            && let Ok(url) = relay.parse::<RelayUrl>()
        {
            addr = addr.with_relay_url(url);
        }
        for direct in self.direct_addresses() {
            if let Ok(socket) = direct.parse::<std::net::SocketAddr>() {
                addr = addr.with_ip_addr(socket);
            }
        }
        Ok(addr)
    }
}

impl From<&EndpointAddr> for IrohAddress {
    fn from(addr: &EndpointAddr) -> Self {
        let mut address = IrohAddress::new(addr.id.to_string());
        if let Some(relay) = addr.relay_urls().next() {
            address = address.with_relay_url(relay.to_string());
        }
        for socket in addr.ip_addrs() {
            address = address.with_direct_address(socket.to_string());
        }
        address
    }
}

impl From<EndpointAddr> for IrohAddress {
    fn from(addr: EndpointAddr) -> Self {
        Self::from(&addr)
    }
}
