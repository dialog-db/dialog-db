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

use dialog_capability::{Provider, Subject};
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_credentials::Ed25519Verifier;
use dialog_effects::MethodExt as _;
use dialog_effects::authority::{AuthorityError, Identify, OperatorExt as _};
use dialog_effects::memory::Resolve;
use dialog_effects::peer::prelude::*;
use dialog_effects::peer::{self as peer_fx, PeerAddress};
use dialog_effects::storage::Location;
use dialog_varsig::{Did, Principal as _};
use thiserror::Error;
use url::Url;

use crate::schema::DidExt as _;
use crate::{AddAddressError, ConnectError, ConnectedBranch, ConnectedReplica, SiteAddress};
use dialog_artifacts::Entity;

pub mod contacts;

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

/// Encode a site address as the host records it for a contact.
pub fn peer_address(address: &SiteAddress) -> Result<PeerAddress, PeerError> {
    serde_ipld_dagcbor::to_vec(address)
        .map(PeerAddress)
        .map_err(|error| PeerError::Encoding(error.to_string()))
}

/// Decode a contact's recorded address.
pub fn site_address(address: &PeerAddress) -> Result<SiteAddress, PeerError> {
    serde_ipld_dagcbor::from_slice(&address.0)
        .map_err(|error| PeerError::Encoding(error.to_string()))
}

/// How a peer is picked out: by the name the host knows it by, or by its
/// entity -- its DID.
///
/// Only an entity can pick out a peer the host has not recorded. A name
/// is something a peer is given, making it a contact, so a name finds a
/// peer that has one but cannot conjure one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum By {
    /// The peer the host knows by this name.
    Name(String),
    /// The peer with this entity.
    Entity(Entity),
}

impl From<&str> for By {
    fn from(name: &str) -> Self {
        Self::Name(name.to_string())
    }
}

impl From<String> for By {
    fn from(name: String) -> Self {
        Self::Name(name)
    }
}

impl From<Entity> for By {
    fn from(entity: Entity) -> Self {
        Self::Entity(entity)
    }
}

impl From<Did> for By {
    fn from(did: Did) -> Self {
        Self::Entity(did.this())
    }
}

impl From<&Did> for By {
    fn from(did: &Did) -> Self {
        Self::Entity(did.this())
    }
}

/// The environment contact commands run against: one that knows its host
/// and keeps the host's contacts.
pub trait PeersEnv:
    Provider<Identify>
    + Provider<peer_fx::AddAddress>
    + Provider<peer_fx::SetName>
    + Provider<peer_fx::Find>
    + Provider<peer_fx::Connect>
    + ConditionalSync
{
}

impl<T> PeersEnv for T where
    T: Provider<Identify>
        + Provider<peer_fx::AddAddress>
        + Provider<peer_fx::SetName>
        + Provider<peer_fx::Find>
        + Provider<peer_fx::Connect>
        + ConditionalSync
{
}

/// A contact of the host the command runs on, picked out by name or by
/// entity. The host is whoever the environment acts for, found when a
/// command is performed.
pub fn contact(by: impl Into<By>) -> ContactReference {
    ContactReference { by: by.into() }
}

/// A reference to a contact. Nothing is read until a command on it is
/// performed.
#[derive(Debug, Clone)]
pub struct ContactReference {
    by: By,
}

impl ContactReference {
    /// Record that the peer is reached at `address`.
    ///
    /// A peer picked out by entity is recorded if it is new; one picked
    /// out by name must already have that name.
    pub fn add_address(self, address: impl Into<SiteAddress>) -> AddAddress {
        AddAddress {
            contact: self,
            address: address.into(),
            name: None,
        }
    }

    /// Connect to the peer, to reach the repositories it holds.
    pub fn connect(self) -> ContactConnection {
        ContactConnection { contact: self }
    }
}

/// Command to add an address to a contact. Created by
/// [`ContactReference::add_address`].
pub struct AddAddress {
    contact: ContactReference,
    address: SiteAddress,
    name: Option<String>,
}

impl AddAddress {
    /// Also give the peer this name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Record the address, and the name if one was given, answering the
    /// peer's entity.
    pub async fn perform<Env: PeersEnv>(self, env: &Env) -> Result<Entity, AddAddressError> {
        let host = host(env).await?;
        let peer = match self.contact.by {
            By::Entity(entity) => entity,
            By::Name(name) => find(&host, name, env).await?,
        };
        host.clone()
            .writer()
            .peers()
            .add_address(peer.clone(), peer_address(&self.address)?)
            .perform(env)
            .await?;
        if let Some(name) = self.name {
            host.writer()
                .peers()
                .set_name(peer.clone(), name)
                .perform(env)
                .await?;
        }
        Ok(peer)
    }
}

/// A contact being connected to. Created by [`ContactReference::connect`].
#[derive(Debug, Clone)]
pub struct ContactConnection {
    contact: ContactReference,
}

impl ContactConnection {
    /// The peer's replica of the repository `subject`.
    pub fn repository(self, subject: impl Into<Did>) -> PeerReplica {
        PeerReplica {
            contact: self.contact,
            subject: subject.into(),
        }
    }
}

/// A repository's replica at a peer, not yet connected to. Created by
/// [`ContactConnection::repository`].
#[derive(Debug, Clone)]
pub struct PeerReplica {
    contact: ContactReference,
    subject: Did,
}

impl PeerReplica {
    /// A branch of the replica, by name.
    pub fn branch(self, name: impl Into<String>) -> PeerBranch {
        PeerBranch {
            replica: self,
            name: name.into(),
        }
    }

    /// Connect to the peer holding the replica.
    pub fn open(self) -> OpenPeerReplica {
        OpenPeerReplica { replica: self }
    }
}

/// Command to connect to a replica at a peer. Created by
/// [`PeerReplica::open`].
pub struct OpenPeerReplica {
    replica: PeerReplica,
}

impl OpenPeerReplica {
    /// Connect to the peer, answering the replica there.
    pub async fn perform<Env: PeersEnv>(self, env: &Env) -> Result<ConnectedReplica, ConnectError> {
        let PeerReplica { contact, subject } = self.replica;
        connect(&contact.by, subject, env).await
    }
}

/// A branch of a replica at a peer. Created by [`PeerReplica::branch`].
#[derive(Debug, Clone)]
pub struct PeerBranch {
    replica: PeerReplica,
    name: String,
}

impl PeerBranch {
    /// Connect to the peer and open the branch there.
    pub fn open(self) -> OpenPeerBranch {
        OpenPeerBranch { branch: self }
    }
}

/// Command to open a branch of a replica at a peer. Created by
/// [`PeerBranch::open`].
pub struct OpenPeerBranch {
    branch: PeerBranch,
}

impl OpenPeerBranch {
    /// Connect to the peer, and open the branch there from its local
    /// cache; nothing crosses the network until it is fetched.
    pub async fn perform<Env>(self, env: &Env) -> Result<ConnectedBranch, ConnectError>
    where
        Env: PeersEnv + Provider<Resolve>,
    {
        let PeerBranch { replica, name } = self.branch;
        let remote = replica.open().perform(env).await?;
        Ok(remote.branch(name).open().perform(env).await?)
    }
}

/// The subject of the host `env` acts for: its home.
pub(crate) async fn host<Env: Provider<Identify> + ConditionalSync>(
    env: &Env,
) -> Result<Subject, AuthorityError> {
    Ok(Subject::from(
        Identify.perform(env).await?.profile().clone(),
    ))
}

/// The replica of `subject` at the peer `by` picks out, on the host's
/// connection to it.
pub(crate) async fn connect<Env: PeersEnv>(
    by: &By,
    subject: Did,
    env: &Env,
) -> Result<ConnectedReplica, ConnectError> {
    let host = host(env).await?;
    let (peer, name) = match by {
        By::Entity(entity) => (entity.clone(), None),
        By::Name(name) => (find(&host, name.clone(), env).await?, Some(name.clone())),
    };
    let connection = host
        .clone()
        .reader()
        .peers()
        .connect(peer)
        .perform(env)
        .await?;
    Ok(ConnectedReplica::connected(
        host,
        &connection,
        name,
        subject,
    )?)
}

/// The one contact the host knows by `name`.
async fn find<Env: PeersEnv>(
    host: &Subject,
    name: String,
    env: &Env,
) -> Result<Entity, ConnectError> {
    let mut peers = host
        .clone()
        .reader()
        .peers()
        .find(name.clone())
        .perform(env)
        .await?;
    peers.sort();
    peers.dedup();
    match peers.len() {
        0 => Err(ConnectError::NotFound { name }),
        1 => Ok(peers.remove(0)),
        _ => Err(ConnectError::Ambiguous { name }),
    }
}

/// The DID of the peer reached at `address`, derived from it: the one
/// place a peer's identity is not given but worked out, for remotes that
/// never had one.
pub fn peer_did(address: &SiteAddress) -> Result<Did, PeerError> {
    match address {
        // A virtual-hosted endpoint names the bucket in its host, so its
        // origin is the store. A path-style one shares its host with every
        // other bucket there, so the bucket joins the DID as a path
        // segment: two buckets are two stores, and two peers.
        SiteAddress::S3(address) if address.path_style() => {
            web_at(address.endpoint(), Some(address.bucket()))
        }
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
    web_at(endpoint, None)
}

/// `did:web` of the endpoint's origin, with `path` appended as a segment
/// when given.
fn web_at(endpoint: &Url, path: Option<&str>) -> Result<Did, PeerError> {
    let no_origin = || PeerError::NoOrigin {
        endpoint: endpoint.to_string(),
    };
    let host = endpoint.host_str().ok_or_else(no_origin)?;
    let mut did = match endpoint.port() {
        Some(port) => format!("did:web:{host}%3A{port}"),
        None => format!("did:web:{host}"),
    };
    if let Some(path) = path {
        did.push(':');
        did.push_str(path);
    }
    did.parse().map_err(|_| no_origin())
}

/// `did:key` of the Ed25519 key seeded by the Blake3 hash of the
/// location's file URI.
fn key(location: &Location) -> Did {
    let seed = Blake3Hash::hash(location.uri().as_bytes());
    let key = ed25519_dalek::SigningKey::from_bytes(seed.as_bytes());
    Ed25519Verifier::from(key).did()
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{contact, peer_address, peer_did, site_address};
    use crate::helpers::test_repo;
    use crate::schema::{self, DidExt as _};
    use crate::{AddAddressError, Branch, REGISTRY, RepositoryMemoryExt as _, SiteAddress};
    use dialog_artifacts::Entity;
    use dialog_capability::Subject;
    use dialog_effects::MethodExt as _;
    use dialog_effects::peer::PeerAddress;
    use dialog_effects::peer::prelude::*;
    use dialog_effects::storage::{Directory, Location};
    use dialog_peer::Peer as LocalPeer;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::{Output as _, Query, Term};
    use dialog_remote_fs::FsAddress;
    use dialog_remote_s3::Address;
    use dialog_remote_ucan::UcanAddress;
    use dialog_storage::provider::storage::VolatileSpace;
    use dialog_varsig::did;

    /// A UCAN service is the `did:web` of its origin; the path it is
    /// served under does not change which service it is.
    #[dialog_common::test]
    fn it_names_a_ucan_service_by_its_origin() -> anyhow::Result<()> {
        let address = SiteAddress::from(UcanAddress::new("https://tonk.network/ucan/"));
        assert_eq!(peer_did(&address)?, did!("web:tonk.network"));
        Ok(())
    }

    /// A non-default port is part of the origin, percent-encoded as
    /// `did:web` requires.
    #[dialog_common::test]
    fn it_keeps_a_non_default_port() -> anyhow::Result<()> {
        let address = SiteAddress::from(UcanAddress::new("http://localhost:8787/ucan"));
        assert_eq!(peer_did(&address)?, did!("web:localhost%3A8787"));
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
            peer_did(&address)?,
            did!("web:my-bucket.s3.us-east-1.amazonaws.com")
        );
        Ok(())
    }

    /// A path-style S3 endpoint shares its host with every bucket on it,
    /// so the bucket is part of the peer: two buckets are two peers.
    #[dialog_common::test]
    fn it_names_path_style_buckets_apart() -> anyhow::Result<()> {
        let bucket = |name: &str| -> anyhow::Result<SiteAddress> {
            Ok(SiteAddress::from(
                Address::builder("http://127.0.0.1:9000")
                    .region("us-east-1")
                    .bucket(name)
                    .path_style(true)
                    .build()?,
            ))
        };
        assert_eq!(
            peer_did(&bucket("alpha")?)?,
            did!("web:127.0.0.1%3A9000:alpha")
        );
        assert_ne!(peer_did(&bucket("alpha")?)?, peer_did(&bucket("beta")?)?);
        Ok(())
    }

    /// A directory is a `did:key`, the same one every time it is named,
    /// and a different one for a different directory.
    #[dialog_common::test]
    fn it_names_a_directory_by_a_key_seeded_from_it() -> anyhow::Result<()> {
        let at = |name: &str| {
            SiteAddress::Fs(FsAddress::new(Location::new(
                Directory::At("/var/dialog".into()),
                name,
            )))
        };
        let first = peer_did(&at("backup"))?;
        assert!(first.as_str().starts_with("did:key:z6Mk"), "{first}");
        assert_eq!(first, peer_did(&at("backup"))?);
        assert_ne!(first, peer_did(&at("other"))?);
        Ok(())
    }

    /// An address round-trips through the encoding a contact records it
    /// in.
    #[dialog_common::test]
    fn it_recovers_the_address_a_peer_is_reached_at() -> anyhow::Result<()> {
        let address = SiteAddress::from(UcanAddress::new("https://tonk.network/ucan/"));
        assert_eq!(site_address(&peer_address(&address)?)?, address);
        Ok(())
    }

    /// The names and addresses `branch` records for peers.
    async fn recorded(
        operator: &LocalPeer<VolatileSpace>,
        branch: &Branch,
    ) -> anyhow::Result<(Vec<schema::Contact>, Vec<(Entity, SiteAddress)>)> {
        let names: Vec<schema::Contact> = branch
            .query()
            .select(Query::<schema::Contact> {
                this: Term::var("this"),
                name: Term::var("name"),
            })
            .perform(operator)
            .try_vec()
            .await?;
        let mut addresses = Vec::new();
        for row in branch
            .query()
            .select(Query::<schema::PeerAddress> {
                this: Term::var("this"),
                address: Term::var("address"),
            })
            .perform(operator)
            .try_vec()
            .await?
        {
            let row: schema::PeerAddress = row;
            addresses.push((row.this.clone(), site_address(&PeerAddress(row.address.0))?));
        }
        addresses.sort_by_key(|(this, site)| (this.clone(), format!("{site:?}")));
        Ok((names, addresses))
    }

    /// The contacts the host `operator` acts for knows by `name`.
    async fn found(operator: &LocalPeer<VolatileSpace>, name: &str) -> anyhow::Result<Vec<Entity>> {
        let host = super::host(operator).await?;
        Ok(host.reader().peers().find(name).perform(operator).await?)
    }

    /// Where the host `operator` acts for reaches `peer`, in order.
    async fn reached(
        operator: &LocalPeer<VolatileSpace>,
        peer: &Entity,
    ) -> anyhow::Result<Vec<SiteAddress>> {
        let host = super::host(operator).await?;
        let connection = host
            .reader()
            .peers()
            .connect(peer.clone())
            .perform(operator)
            .await?;
        let mut addresses = connection
            .addresses()
            .iter()
            .map(site_address)
            .collect::<Result<Vec<_>, _>>()?;
        addresses.sort_by_key(|site| format!("{site:?}"));
        Ok(addresses)
    }

    /// A peer picked out by its DID is recorded at the address, and
    /// is named if a name is given; by that name it is then found again
    /// and a second address is added to the same peer. The host keeps
    /// them in its own state, not in any repository's registry.
    #[dialog_common::test]
    async fn it_adds_addresses_to_a_peer_by_did_then_by_name() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let origin = did!("web:tonk.network");
        let first = SiteAddress::from(UcanAddress::new("https://tonk.network/ucan/"));
        let second = SiteAddress::from(UcanAddress::new("https://backup.tonk.network/ucan/"));

        let entity = contact(&origin)
            .add_address(first.clone())
            .name("origin")
            .perform(&operator)
            .await?;
        assert_eq!(entity, origin.this());

        let again = contact("origin")
            .add_address(second.clone())
            .perform(&operator)
            .await?;
        assert_eq!(again, entity, "the name finds the same peer");

        assert_eq!(found(&operator, "origin").await?, vec![entity.clone()]);
        let mut expected = vec![first, second];
        expected.sort_by_key(|site| format!("{site:?}"));
        assert_eq!(reached(&operator, &entity).await?, expected);

        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .open()
            .perform(&operator)
            .await?;
        let (names, addresses) = recorded(&operator, &registry).await?;
        assert!(
            names.is_empty() && addresses.is_empty(),
            "contacts are the host's, not the repository's"
        );
        Ok(())
    }

    /// A name finds a peer but cannot make one: an unknown name is
    /// refused and nothing is recorded.
    #[dialog_common::test]
    async fn it_refuses_an_unknown_name() -> anyhow::Result<()> {
        let (operator, _) = test_session_with_peer().await;

        let refused = contact("origin")
            .add_address(UcanAddress::new("https://tonk.network/ucan/"))
            .perform(&operator)
            .await;
        assert!(
            matches!(refused, Err(AddAddressError::NotFound { ref name }) if name == "origin"),
            "{refused:?}"
        );
        assert!(found(&operator, "origin").await?.is_empty());
        Ok(())
    }

    /// A name two peers share picks out neither.
    #[dialog_common::test]
    async fn it_refuses_an_ambiguous_name() -> anyhow::Result<()> {
        let (operator, _) = test_session_with_peer().await;
        for (did, endpoint) in [
            (did!("web:one.example"), "https://one.example/"),
            (did!("web:two.example"), "https://two.example/"),
        ] {
            contact(did)
                .add_address(UcanAddress::new(endpoint))
                .name("shared")
                .perform(&operator)
                .await?;
        }

        let refused = contact("shared")
            .add_address(UcanAddress::new("https://three.example/"))
            .perform(&operator)
            .await;
        assert!(
            matches!(refused, Err(AddAddressError::Ambiguous { ref name }) if name == "shared"),
            "{refused:?}"
        );
        Ok(())
    }
}
