//! Naming a peer.

use base58::{FromBase58, ToBase58};
use dialog_capability::{SiteAddress, SiteId};
use dialog_varsig::did::Did;
use iroh_base::{EndpointAddr, EndpointId, TransportAddr};
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

/// The multicodec tag for an ed25519 public key, which is what a
/// `did:key` puts in front of the bytes.
const ED25519_MULTICODEC: [u8; 2] = [0xed, 0x01];

impl IrohAddress {
    /// This peer as a `did:key`.
    ///
    /// Not a translation into dialog's vocabulary — the same bytes, said
    /// the way the rest of the system says them. An iroh endpoint id is
    /// an ed25519 public key, `did:key:z…` is an ed25519 public key
    /// under a multicodec tag, and every other principal dialog handles
    /// is already written that way. A peer that printed itself in
    /// iroh's base32 instead would be the one identifier in the system
    /// a user could not paste where a DID is expected.
    ///
    /// Routes are deliberately absent. They are iroh's to resolve, and
    /// they change; identity does not. What this returns stays true for
    /// the life of the key.
    pub fn did(&self) -> Did {
        let mut tagged = Vec::with_capacity(34);
        tagged.extend_from_slice(&ED25519_MULTICODEC);
        tagged.extend_from_slice(self.0.id.as_bytes());
        format!("did:key:z{}", tagged.to_base58())
            .parse()
            .expect("a did:key built from 34 bytes parses")
    }

    /// This peer as a URI: its `did:key`, and any routes as hints.
    ///
    /// A bare `did:key` is the whole address whenever something can
    /// resolve it — iroh's address lookups do, pkarr included, and in a
    /// browser too. Identity is stable and routes are not, so the
    /// identifier that outlives a peer moving is the one worth writing
    /// down.
    ///
    /// But resolution needs a network, and the case this exists for is
    /// two processes on one machine with no internet: there is no lookup
    /// to answer, and the route cannot be derived, because a WebRTC
    /// route carries a DTLS fingerprint and candidates that no public
    /// key implies. So routes ride as `?route=` hints when there are
    /// any. A reader that ignores them still has a peer; a reader
    /// offline still has a way to reach it.
    pub fn to_uri(&self) -> String {
        let did = self.did().to_string();
        if self.0.addrs.is_empty() {
            return did;
        }
        let hints = self
            .0
            .addrs
            .iter()
            .map(|addr| addr.to_string())
            .fold(
                form_urlencoded::Serializer::new(String::new()),
                |mut q, addr| {
                    q.append_pair("route", &addr);
                    q
                },
            )
            .finish();
        format!("{did}?{hints}")
    }

    /// Read a peer from [`Self::to_uri`], or from a bare `did:key`.
    ///
    /// A hint that cannot be parsed is refused rather than dropped: a
    /// silently ignored route turns "cannot reach this peer offline"
    /// into a timeout with no cause, which is the failure this whole
    /// form exists to avoid.
    pub fn parse_uri(uri: &str) -> Result<Self, DidError> {
        let (did, query) = match uri.split_once('?') {
            Some((did, query)) => (did, Some(query)),
            None => (uri, None),
        };

        let mut address = Self::parse_did(did)?;
        let Some(query) = query else {
            return Ok(address);
        };

        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            if key != "route" {
                continue;
            }
            address.0.addrs.insert(parse_route(&value)?);
        }
        Ok(address)
    }

    /// Read a peer from its `did:key`, with no routes.
    ///
    /// The result dials by endpoint id alone, which is what iroh's own
    /// guidance asks for: routes go stale as a peer moves, and iroh
    /// resolves current ones through its address lookups.
    pub fn parse_did(did: &str) -> Result<Self, DidError> {
        let base58 = did.strip_prefix("did:key:z").ok_or(DidError::NotADidKey)?;
        let tagged = base58.from_base58().map_err(|_| DidError::NotBase58)?;
        let tagged = <[u8; 34]>::try_from(tagged.as_slice()).map_err(|_| DidError::WrongLength)?;
        if tagged[..2] != ED25519_MULTICODEC {
            return Err(DidError::NotEd25519);
        }
        let key = <[u8; 32]>::try_from(&tagged[2..]).expect("34 less 2 is 32");
        let id = EndpointId::from_bytes(&key).map_err(|_| DidError::NotAKey)?;
        Ok(Self(EndpointAddr::from(id)))
    }
}

/// Why a string is not a peer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DidError {
    /// Some other kind of identifier entirely.
    #[error("a peer is named by a did:key, and this is not one")]
    NotADidKey,
    /// The part after `z` is not base58btc.
    #[error("the key is not base58btc")]
    NotBase58,
    /// Right shape, wrong size.
    #[error("a did:key for ed25519 carries 34 bytes")]
    WrongLength,
    /// A `did:key` naming some other algorithm. iroh endpoints are
    /// ed25519 and nothing else, so this cannot be dialed.
    #[error("this did:key is not ed25519, so it names no iroh endpoint")]
    NotEd25519,
    /// Correctly encoded, but not a point on the curve.
    #[error("these bytes are not an ed25519 public key")]
    NotAKey,
    /// A `?route=` hint in a form nothing can dial.
    #[error("{0} is not a route this understands")]
    BadRoute(String),
}

/// The counterpart of [`TransportAddr`]'s `Display`, which iroh has and
/// has no parser for.
fn parse_route(route: &str) -> Result<TransportAddr, DidError> {
    let (kind, rest) = route
        .split_once(':')
        .ok_or_else(|| DidError::BadRoute(route.to_owned()))?;
    match kind {
        "ip" => rest
            .parse()
            .map(TransportAddr::Ip)
            .map_err(|_| DidError::BadRoute(route.to_owned())),
        "relay" => rest
            .parse()
            .map(TransportAddr::Relay)
            .map_err(|_| DidError::BadRoute(route.to_owned())),
        "custom" => rest
            .parse()
            .map(TransportAddr::Custom)
            .map_err(|_| DidError::BadRoute(route.to_owned())),
        _ => Err(DidError::BadRoute(route.to_owned())),
    }
}

impl std::str::FromStr for IrohAddress {
    type Err = DidError;

    fn from_str(uri: &str) -> Result<Self, Self::Err> {
        Self::parse_uri(uri)
    }
}

/// Prints as [`Self::to_uri`]: the identity, and the routes when it has
/// any, so what a peer prints is what another can be given.
impl std::fmt::Display for IrohAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_uri())
    }
}

/// The credential-store key is the identity alone, never the routes.
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
        SiteId::from(address.did().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh_base::SecretKey;
    use std::str::FromStr;

    /// The point of the `did:key` form is that it is *dialog's*
    /// encoding, not a lookalike. So this checks it against the encoder
    /// dialog already uses for an ed25519 principal rather than against
    /// itself — a round trip through my own two functions would agree
    /// with any consistent mistake.
    #[dialog_common::test]
    fn a_peer_is_a_did_key_dialog_would_recognise() {
        let secret = SecretKey::generate();
        let peer = IrohAddress::from(EndpointAddr::from(secret.public()));

        let dialogs = dialog_credentials::Ed25519Verifier::from(
            ed25519_dalek::VerifyingKey::from_bytes(secret.public().as_bytes())
                .expect("an iroh endpoint id is a verifying key"),
        );

        assert_eq!(
            peer.did().to_string(),
            dialog_varsig::Principal::did(&dialogs).to_string(),
            "the same key, said the same way"
        );
    }

    #[dialog_common::test]
    fn a_did_key_reads_back_as_the_peer_it_names() {
        let id = SecretKey::generate().public();
        let peer = IrohAddress::from(EndpointAddr::from(id));

        let read = IrohAddress::from_str(&peer.to_string()).expect("its own rendering parses");
        assert_eq!(read.endpoint(), &id);
        assert!(
            read.addr().addrs.is_empty(),
            "a did names a peer, not a route to one"
        );
    }

    /// A `did:key` for some other algorithm is a valid DID and not a
    /// peer. Refusing it by name beats failing later at a dial that
    /// could never have worked.
    #[dialog_common::test]
    fn a_did_key_for_another_algorithm_is_refused() {
        // 0xec 0x01 is x25519, not ed25519.
        let mut tagged = vec![0xec, 0x01];
        tagged.extend_from_slice(&[7u8; 32]);
        let did = format!("did:key:z{}", tagged.to_base58());

        assert_eq!(IrohAddress::from_str(&did), Err(DidError::NotEd25519));
    }

    /// The case the hints exist for: two processes on one machine with
    /// no internet, where nothing can resolve a `did:key` and the WebRTC
    /// route carries a fingerprint no public key implies.
    #[dialog_common::test]
    fn a_local_route_survives_being_written_down_and_read_back() {
        let id = SecretKey::generate().public();
        // What a WebRTC transport registers: an opaque blob under a
        // transport id, which is where the candidates and the DTLS
        // fingerprint live.
        let route = super::parse_route("custom:1f_00aabbcc").expect("a custom route parses");
        let mut addr = EndpointAddr::from(id);
        addr.addrs.insert(route.clone());
        let peer = IrohAddress::from(addr);

        let uri = peer.to_uri();
        assert!(uri.starts_with("did:key:z"), "identity leads: {uri}");
        assert!(uri.contains("route="), "and the route rides along: {uri}");

        let read = IrohAddress::from_str(&uri).expect("its own rendering parses");
        assert_eq!(read.endpoint(), &id);
        assert!(
            read.addr().addrs.contains(&route),
            "the offline route came back intact"
        );
    }

    /// A relay URL carries `/` and can carry a query of its own, so the
    /// encoding has to survive being put inside one.
    #[dialog_common::test]
    fn a_route_that_looks_like_a_url_survives_the_query() {
        let id = SecretKey::generate().public();
        let route = super::parse_route("relay:https://relay.example.com/?region=eu")
            .expect("a relay route parses");
        let mut addr = EndpointAddr::from(id);
        addr.addrs.insert(route.clone());

        let peer = IrohAddress::from(addr);
        let read = IrohAddress::from_str(&peer.to_uri()).expect("round trip");
        assert!(read.addr().addrs.contains(&route));
    }

    /// Dropping a hint silently would turn "unreachable offline" into a
    /// timeout with no cause.
    #[dialog_common::test]
    fn a_route_nothing_can_dial_is_refused_rather_than_ignored() {
        let id = SecretKey::generate().public();
        let did = IrohAddress::from(EndpointAddr::from(id)).did();

        let error = IrohAddress::from_str(&format!("{did}?route=carrier-pigeon%3Anorth"))
            .expect_err("an unknown route kind is refused");
        assert!(matches!(error, DidError::BadRoute(_)), "got {error:?}");
    }

    /// Credentials are keyed on who a peer is, so they must not move
    /// when it does.
    #[dialog_common::test]
    fn the_site_id_ignores_routes() {
        let id = SecretKey::generate().public();
        let bare = IrohAddress::from(EndpointAddr::from(id));

        let mut moved = EndpointAddr::from(id);
        moved
            .addrs
            .insert(super::parse_route("ip:127.0.0.1:4433").expect("an ip route"));
        let moved = IrohAddress::from(moved);

        assert_eq!(
            SiteId::from(bare).to_string(),
            SiteId::from(moved).to_string(),
            "a peer that gained a route is the same peer"
        );
    }

    #[dialog_common::test]
    fn something_that_is_not_a_did_is_not_a_peer() {
        assert_eq!(
            IrohAddress::from_str("https://example.com"),
            Err(DidError::NotADidKey)
        );
    }
}
