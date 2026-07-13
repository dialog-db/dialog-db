//! Iroh remote address type.
//!
//! An [`IrohAddress`] names a peer by its endpoint id — the z-base-32
//! rendering of the ed25519 public key of its iroh endpoint — optionally
//! carrying a relay URL and direct socket addresses as dialing hints.
//!
//! The address is deliberately pure data (strings): it serializes into the
//! repository's remote cell, hashes into a stable [`SiteId`], and compiles
//! on every target including wasm, where the iroh dependency itself is
//! absent. Conversions to and from live iroh types are native-only and live
//! in the transport module.

use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use super::Iroh;
use dialog_capability::{SiteAddress, SiteId};
use serde::{Deserialize, Serialize};

/// Address of an iroh-powered remote peer: the peer's endpoint id plus
/// optional dialing hints.
///
/// With a discovery service configured (the default n0 preset), the
/// endpoint id alone suffices — `"<endpoint-id>".parse()` produces a usable
/// address. Relay and direct-address hints let peers connect without any
/// external discovery, and are refreshed opportunistically from gossip
/// `Have` announcements.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IrohAddress {
    /// The peer's endpoint id: z-base-32 ed25519 public key.
    endpoint: String,
    /// Optional home relay URL of the peer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    relay_url: Option<String>,
    /// Optional direct socket addresses of the peer.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    direct_addresses: BTreeSet<String>,
}

impl IrohAddress {
    /// Construct an address for the peer with the given endpoint id.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            relay_url: None,
            direct_addresses: BTreeSet::new(),
        }
    }

    /// Add the peer's home relay URL as a dialing hint.
    pub fn with_relay_url(mut self, relay_url: impl Into<String>) -> Self {
        self.relay_url = Some(relay_url.into());
        self
    }

    /// Add a direct socket address (e.g. `"192.168.1.10:4433"`) as a
    /// dialing hint.
    pub fn with_direct_address(mut self, address: impl Into<String>) -> Self {
        self.direct_addresses.insert(address.into());
        self
    }

    /// The peer's endpoint id (z-base-32 ed25519 public key).
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The peer's home relay URL hint, if any.
    pub fn relay_url(&self) -> Option<&str> {
        self.relay_url.as_deref()
    }

    /// The peer's direct socket address hints.
    pub fn direct_addresses(&self) -> impl Iterator<Item = &str> {
        self.direct_addresses.iter().map(String::as_str)
    }
}

impl SiteAddress for IrohAddress {
    type Site = Iroh;
}

impl From<IrohAddress> for SiteId {
    fn from(address: IrohAddress) -> Self {
        format!("iroh:{}", address.endpoint).into()
    }
}

impl Display for IrohAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "iroh:{}", self.endpoint)
    }
}

/// Error parsing an [`IrohAddress`] from a string.
#[derive(Debug, thiserror::Error)]
#[error("invalid iroh address: {0}")]
pub struct IrohAddressParseError(pub String);

impl FromStr for IrohAddress {
    type Err = IrohAddressParseError;

    /// Parse a bare endpoint id, optionally prefixed with `iroh:`.
    ///
    /// The id is validated shallowly (non-empty, no whitespace); full key
    /// validation happens on native targets when the address is dialed.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let id = s.strip_prefix("iroh:").unwrap_or(s).trim();
        if id.is_empty() {
            return Err(IrohAddressParseError("empty endpoint id".into()));
        }
        if id.chars().any(|c| c.is_whitespace()) {
            return Err(IrohAddressParseError(format!(
                "endpoint id contains whitespace: {id:?}"
            )));
        }
        Ok(Self::new(id))
    }
}
