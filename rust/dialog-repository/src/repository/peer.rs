//! Peers, identified by where they are reached.
//!
//! A peer is whoever holds replicas. A remote service has no key of its
//! own that a replica could name, so its DID is derived from its
//! address, and every replica that names the same service converges on
//! the same peer:
//!
//! - a service reached over HTTP (S3, UCAN) is `did:web` of the
//!   endpoint's origin;
//! - a directory on the local filesystem is the `did:key` of the
//!   Ed25519 key seeded by the Blake3 hash of its file URI.

use dialog_common::Blake3Hash;
use dialog_credentials::Ed25519Verifier;
use dialog_effects::storage::{Directory, Location};
use dialog_varsig::{Did, Principal};
use thiserror::Error;
use url::Url;

use crate::SiteAddress;
use crate::schema::{Peer, PeerAddress, peer};

/// Why an address does not name a peer.
#[derive(Debug, Error)]
pub enum PeerError {
    /// The endpoint is not a URL with a host to derive a `did:web` from.
    #[error("Endpoint {endpoint} has no origin to name a peer by")]
    NoOrigin {
        /// The endpoint as given.
        endpoint: String,
    },

    /// The address could not be encoded or decoded.
    #[error("Peer address could not be encoded: {0}")]
    Encoding(String),
}

impl Peer {
    /// The peer reached at `address`, known locally as `name`.
    pub fn at(name: impl Into<String>, address: &SiteAddress) -> Result<Self, PeerError> {
        Ok(Self::new(&did(address)?, name))
    }
}

impl PeerAddress {
    /// Record that `peer` is reached at `address`.
    pub fn new(peer: &Peer, address: &SiteAddress) -> Result<Self, PeerError> {
        let bytes = serde_ipld_dagcbor::to_vec(address)
            .map_err(|error| PeerError::Encoding(error.to_string()))?;
        Ok(Self {
            this: peer.this.clone(),
            address: peer::Address(bytes),
        })
    }

    /// The address this records.
    pub fn site(&self) -> Result<SiteAddress, PeerError> {
        serde_ipld_dagcbor::from_slice(&self.address.0)
            .map_err(|error| PeerError::Encoding(error.to_string()))
    }
}

/// The DID of the peer reached at `address`.
pub fn did(address: &SiteAddress) -> Result<Did, PeerError> {
    match address {
        SiteAddress::S3(address) => web(address.endpoint()),
        SiteAddress::Ucan(address) => {
            let endpoint = Url::parse(&address.endpoint).map_err(|_| PeerError::NoOrigin {
                endpoint: address.endpoint.clone(),
            })?;
            web(&endpoint)
        }
        SiteAddress::Fs(address) => Ok(key(address.location())),
    }
}

/// `did:web` of the endpoint's origin: its host, with a non-default
/// port percent-encoded after it as the `did:web` method requires.
fn web(endpoint: &Url) -> Result<Did, PeerError> {
    let no_origin = || PeerError::NoOrigin {
        endpoint: endpoint.to_string(),
    };
    let host = endpoint.host_str().ok_or_else(no_origin)?;
    let did = match endpoint.port() {
        Some(port) => format!("did:web:{host}%3A{port}"),
        None => format!("did:web:{host}"),
    };
    did.parse().map_err(|_| no_origin())
}

/// `did:key` of the Ed25519 key seeded by the Blake3 hash of the
/// location's file URI.
fn key(location: &Location) -> Did {
    let seed = Blake3Hash::hash(file_uri(location).as_bytes());
    let key = ed25519_dalek::SigningKey::from_bytes(seed.as_bytes());
    Ed25519Verifier::from(key).did()
}

/// The file URI naming a location: an absolute directory as a `file://`
/// URL, and a platform directory by its role, since where it resolves
/// differs by device.
fn file_uri(location: &Location) -> String {
    let Location { directory, name } = location;
    match directory {
        Directory::At(path) => format!("file://{}/{name}", path.trim_end_matches('/')),
        Directory::Profile => format!("file:profile/{name}"),
        Directory::Current => format!("file:current/{name}"),
        Directory::Temp => format!("file:temp/{name}"),
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::did;
    use crate::SiteAddress;
    use crate::schema::{Peer, PeerAddress};
    use dialog_effects::storage::{Directory, Location};
    use dialog_remote_s3::Address;
    use dialog_remote_ucan::UcanAddress;
    use dialog_varsig::did;

    /// A UCAN service is the `did:web` of its origin; the path it is
    /// served under does not change which service it is.
    #[dialog_common::test]
    fn it_names_a_ucan_service_by_its_origin() -> anyhow::Result<()> {
        let address = SiteAddress::from(UcanAddress::new("https://tonk.network/ucan/"));
        assert_eq!(did(&address)?, did!("web:tonk.network"));
        Ok(())
    }

    /// A non-default port is part of the origin, percent-encoded as
    /// `did:web` requires.
    #[dialog_common::test]
    fn it_keeps_a_non_default_port() -> anyhow::Result<()> {
        let address = SiteAddress::from(UcanAddress::new("http://localhost:8787/ucan"));
        assert_eq!(did(&address)?, did!("web:localhost%3A8787"));
        Ok(())
    }

    /// An S3 bucket is the `did:web` of its endpoint's origin, which for
    /// a virtual-hosted endpoint carries the bucket: two buckets on one
    /// host are two peers.
    #[dialog_common::test]
    fn it_names_an_s3_bucket_by_its_origin() -> anyhow::Result<()> {
        let address = SiteAddress::from(
            Address::builder("https://s3.us-east-1.amazonaws.com")
                .region("us-east-1")
                .bucket("my-bucket")
                .build()?,
        );
        assert_eq!(
            did(&address)?,
            did!("web:my-bucket.s3.us-east-1.amazonaws.com")
        );
        Ok(())
    }

    /// A directory is a `did:key`, the same one every time it is named,
    /// and a different one for a different directory.
    #[dialog_common::test]
    fn it_names_a_directory_by_a_key_seeded_from_it() -> anyhow::Result<()> {
        let at = |name: &str| {
            SiteAddress::Fs(dialog_remote_fs::FsAddress::new(Location::new(
                Directory::At("/var/dialog".into()),
                name,
            )))
        };
        let first = did(&at("backup"))?;
        assert!(first.as_str().starts_with("did:key:z6Mk"), "{first}");
        assert_eq!(first, did(&at("backup"))?);
        assert_ne!(first, did(&at("other"))?);
        Ok(())
    }

    /// An address round-trips through the fact that records it.
    #[dialog_common::test]
    fn it_recovers_the_address_a_peer_is_reached_at() -> anyhow::Result<()> {
        let address = SiteAddress::from(UcanAddress::new("https://tonk.network/ucan/"));
        let peer = Peer::at("origin", &address)?;
        assert_eq!(PeerAddress::new(&peer, &address)?.site()?, address);
        Ok(())
    }
}
