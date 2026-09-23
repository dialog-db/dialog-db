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

use crate::registry::{RegistryEnv, apply};
use crate::schema::{Branch as BranchConcept, DidExt as _, Peer, PeerAddress, peer};
use crate::{
    AddAddressError, Branch, ConnectError, ConnectedBranch, ConnectedReplica, Repository,
    RepositoryMemoryExt as _, SiteAddress,
};
use dialog_artifacts::{Changes, Entity};
use dialog_capability::Subject;
use dialog_query::{Output as _, Query, Statement as _, Term};

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
        Ok(Self::new(&peer_did(address)?, name))
    }
}

impl PeerAddress {
    /// Record that the peer with entity `peer` is reached at `address`.
    pub fn new(peer: &Entity, address: &SiteAddress) -> Result<Self, PeerError> {
        let bytes = serde_ipld_dagcbor::to_vec(address)
            .map_err(|error| PeerError::Encoding(error.to_string()))?;
        Ok(Self {
            this: peer.clone(),
            address: peer::Address(bytes),
        })
    }

    /// The address this records.
    pub fn site(&self) -> Result<SiteAddress, PeerError> {
        serde_ipld_dagcbor::from_slice(&self.address.0)
            .map_err(|error| PeerError::Encoding(error.to_string()))
    }
}

/// How a peer is picked out: by the name it is known by locally, or by
/// its entity -- its DID.
///
/// Only an entity can make a new peer. A name is something a peer is
/// given, so it can find a peer that has one but cannot conjure one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum By {
    /// The peer known locally by this name.
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

impl<C: Principal> Repository<C> {
    /// A peer of this repository, picked out by name or by entity.
    pub fn peer(&self, by: impl Into<By>) -> PeerReference {
        PeerReference {
            subject: self.subject(),
            by: by.into(),
        }
    }
}

/// A reference to a peer. Nothing is read until a command on it is
/// performed.
#[derive(Debug, Clone)]
pub struct PeerReference {
    subject: Subject,
    by: By,
}

impl PeerReference {
    /// Record that the peer is reached at `address`.
    ///
    /// A peer picked out by entity is recorded if it is new; one picked
    /// out by name must already have that name.
    pub fn add_address(self, address: impl Into<SiteAddress>) -> AddAddress {
        AddAddress {
            peer: self,
            address: address.into(),
            name: None,
        }
    }
}

/// Command to add an address to a peer. Created by
/// [`PeerReference::add_address`].
pub struct AddAddress {
    peer: PeerReference,
    address: SiteAddress,
    name: Option<String>,
}

impl AddAddress {
    /// Also give the peer this local name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Record the address, and the name if one was given, answering the
    /// peer's entity.
    pub async fn perform<Env: RegistryEnv>(self, env: &Env) -> Result<Entity, AddAddressError> {
        let registry = self.peer.subject.registry().open().perform(env).await?;
        let this = match self.peer.by {
            By::Entity(entity) => entity,
            By::Name(name) => named(&registry, name, env).await?,
        };

        let mut changes = Changes::new();
        if let Some(name) = self.name {
            Peer {
                this: this.clone(),
                name: peer::Name(name),
            }
            .assert(&mut changes);
        }
        PeerAddress::new(&this, &self.address)?.assert(&mut changes);
        apply(&registry, changes, env).await?;

        Ok(this)
    }
}

impl PeerReference {
    /// Connect to the peer, to reach the repositories it holds. Nothing
    /// is read until a branch there is opened.
    pub fn connect(self) -> PeerConnection {
        PeerConnection { peer: self }
    }
}

/// A peer being connected to. Created by [`PeerReference::connect`].
#[derive(Debug, Clone)]
pub struct PeerConnection {
    peer: PeerReference,
}

impl PeerConnection {
    /// The peer's replica of the repository `subject`.
    pub fn repository(self, subject: impl Into<Did>) -> PeerRepository {
        PeerRepository {
            peer: self.peer,
            subject: subject.into(),
        }
    }
}

/// A repository held at a peer, not yet connected to. Created by
/// [`PeerConnection::repository`].
#[derive(Debug, Clone)]
pub struct PeerRepository {
    peer: PeerReference,
    subject: Did,
}

impl PeerRepository {
    /// A branch of the repository, picked out by name or by entity.
    pub fn branch(self, by: impl Into<By>) -> PeerBranch {
        PeerBranch {
            repository: self,
            by: by.into(),
        }
    }

    /// Connect: find the peer and the addresses it is reached at.
    pub fn open(self) -> OpenPeerRepository {
        OpenPeerRepository { repository: self }
    }
}

/// Command to connect to a repository held at a peer. Created by
/// [`PeerRepository::open`].
pub struct OpenPeerRepository {
    repository: PeerRepository,
}

impl OpenPeerRepository {
    /// Find the peer and its addresses, answering the repository there.
    pub async fn perform<Env: RegistryEnv>(
        self,
        env: &Env,
    ) -> Result<ConnectedReplica, ConnectError> {
        let PeerRepository { peer, subject } = self.repository;
        let registry = peer.subject.registry().open().perform(env).await?;
        connect(&registry, &peer.subject, &peer.by, subject, env).await
    }
}

/// A branch of a repository held at a peer. Created by
/// [`PeerRepository::branch`].
#[derive(Debug, Clone)]
pub struct PeerBranch {
    repository: PeerRepository,
    by: By,
}

impl PeerBranch {
    /// Connect to the peer and open the branch there.
    pub fn open(self) -> OpenPeerBranch {
        OpenPeerBranch { branch: self }
    }
}

/// Command to open a branch of a repository held at a peer. Created by
/// [`PeerBranch::open`].
pub struct OpenPeerBranch {
    branch: PeerBranch,
}

impl OpenPeerBranch {
    /// Find the peer and its addresses, and open the branch there from
    /// its local cache; nothing crosses the network until it is fetched.
    pub async fn perform<Env: RegistryEnv>(
        self,
        env: &Env,
    ) -> Result<ConnectedBranch, ConnectError> {
        let PeerBranch { repository, by } = self.branch;
        let PeerRepository { peer, subject } = repository;
        let registry = peer.subject.registry().open().perform(env).await?;
        let remote = connect(&registry, &peer.subject, &peer.by, subject, env).await?;
        let name = match by {
            By::Name(name) => name,
            By::Entity(entity) => branch_name(&registry, &entity, env).await?,
        };
        Ok(remote.branch(name).open().perform(env).await?)
    }
}

/// The repository `subject` held at the peer `by` picks out, reached at
/// the addresses recorded for it, with its state cached under `host`.
pub(crate) async fn connect<Env: RegistryEnv>(
    registry: &Branch,
    host: &Subject,
    by: &By,
    subject: Did,
    env: &Env,
) -> Result<ConnectedReplica, ConnectError> {
    let peer = match by {
        By::Entity(entity) => entity.clone(),
        By::Name(name) => named(registry, name.clone(), env).await?,
    };
    reach(registry, host, peer, subject, env).await
}

/// The repository `subject` held at `peer`, reached at the addresses
/// recorded for it.
pub(crate) async fn reach<Env: RegistryEnv>(
    registry: &Branch,
    host: &Subject,
    peer: Entity,
    subject: Did,
    env: &Env,
) -> Result<ConnectedReplica, ConnectError> {
    let query = |error: dialog_query::EvaluationError| ConnectError::Query(error.to_string());

    let names: Vec<Peer> = Box::pin(
        registry
            .query()
            .select(Query::<Peer> {
                this: peer.clone().into(),
                name: Term::var("name"),
            })
            .perform(env)
            .try_vec(),
    )
    .await
    .map_err(query)?;
    let name = names.into_iter().next().map(|row| row.name.0);

    let rows: Vec<PeerAddress> = Box::pin(
        registry
            .query()
            .select(Query::<PeerAddress> {
                this: peer.clone().into(),
                address: Term::var("address"),
            })
            .perform(env)
            .try_vec(),
    )
    .await
    .map_err(query)?;
    let addresses = rows
        .iter()
        .map(PeerAddress::site)
        .collect::<Result<Vec<_>, _>>()?;

    if addresses.is_empty() {
        return Err(ConnectError::Unreachable {
            peer: name.unwrap_or_else(|| peer.to_string()),
        });
    }

    Ok(ConnectedReplica::new(
        host.clone(),
        peer,
        name,
        addresses,
        subject,
    ))
}

/// The name recorded for the branch with entity `branch`.
pub(crate) async fn branch_name<Env: RegistryEnv>(
    registry: &Branch,
    branch: &Entity,
    env: &Env,
) -> Result<String, ConnectError> {
    let rows: Vec<BranchConcept> = Box::pin(
        registry
            .query()
            .select(Query::<BranchConcept> {
                this: branch.clone().into(),
                name: Term::var("name"),
                replica: Term::var("replica"),
            })
            .perform(env)
            .try_vec(),
    )
    .await
    .map_err(|error| ConnectError::Query(error.to_string()))?;
    rows.into_iter()
        .next()
        .map(|row| row.name.0)
        .ok_or_else(|| ConnectError::UnknownBranch {
            branch: branch.to_string(),
        })
}

/// The one peer recorded under `name`.
async fn named<Env: RegistryEnv>(
    registry: &Branch,
    name: String,
    env: &Env,
) -> Result<Entity, ConnectError> {
    let rows: Vec<Peer> = Box::pin(
        registry
            .query()
            .select(Query::<Peer> {
                this: Term::var("this"),
                name: name.clone().into(),
            })
            .perform(env)
            .try_vec(),
    )
    .await
    .map_err(|error| ConnectError::Query(error.to_string()))?;

    let mut peers: Vec<Entity> = rows.into_iter().map(|row| row.this).collect();
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

    use super::peer_did;
    use crate::helpers::test_repo;
    use crate::schema::{DidExt as _, Peer, PeerAddress};
    use crate::{AddAddressError, REGISTRY, Repository, RepositoryMemoryExt as _, SiteAddress};
    use dialog_artifacts::Entity;
    use dialog_capability::Subject;
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

    /// An address round-trips through the fact that records it.
    #[dialog_common::test]
    fn it_recovers_the_address_a_peer_is_reached_at() -> anyhow::Result<()> {
        let address = SiteAddress::from(UcanAddress::new("https://tonk.network/ucan/"));
        let peer = Peer::at("origin", &address)?;
        assert_eq!(PeerAddress::new(&peer.this, &address)?.site()?, address);
        Ok(())
    }

    /// Which peers the registry holds, and at which addresses.
    async fn recorded(
        operator: &LocalPeer<VolatileSpace>,
        repo: &Repository,
    ) -> anyhow::Result<(Vec<Peer>, Vec<(Entity, SiteAddress)>)> {
        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .open()
            .perform(operator)
            .await?;
        let peers: Vec<Peer> = registry
            .query()
            .select(Query::<Peer> {
                this: Term::var("this"),
                name: Term::var("name"),
            })
            .perform(operator)
            .try_vec()
            .await?;
        let mut addresses = Vec::new();
        for row in registry
            .query()
            .select(Query::<PeerAddress> {
                this: Term::var("this"),
                address: Term::var("address"),
            })
            .perform(operator)
            .try_vec()
            .await?
        {
            let row: PeerAddress = row;
            addresses.push((row.this.clone(), row.site()?));
        }
        addresses.sort_by_key(|(this, site)| (this.clone(), format!("{site:?}")));
        Ok((peers, addresses))
    }

    /// A peer picked out by its DID is recorded with the address, and
    /// named if a name is given; by that name it is then found again and
    /// a second address is added to the same peer.
    #[dialog_common::test]
    async fn it_adds_addresses_to_a_peer_by_did_then_by_name() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let origin = did!("web:tonk.network");
        let first = SiteAddress::from(UcanAddress::new("https://tonk.network/ucan/"));
        let second = SiteAddress::from(UcanAddress::new("https://backup.tonk.network/ucan/"));

        let entity = repo
            .peer(&origin)
            .add_address(first.clone())
            .name("origin")
            .perform(&operator)
            .await?;
        assert_eq!(entity, origin.this());

        let again = repo
            .peer("origin")
            .add_address(second.clone())
            .perform(&operator)
            .await?;
        assert_eq!(again, entity, "the name finds the same peer");

        let (peers, addresses) = recorded(&operator, &repo).await?;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].name.0, "origin");
        let mut expected = vec![(entity.clone(), first), (entity, second)];
        expected.sort_by_key(|(this, site)| (this.clone(), format!("{site:?}")));
        assert_eq!(addresses, expected);
        Ok(())
    }

    /// A name finds a peer but cannot make one: an unknown name is
    /// refused and nothing is recorded.
    #[dialog_common::test]
    async fn it_refuses_an_unknown_name() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let refused = repo
            .peer("origin")
            .add_address(UcanAddress::new("https://tonk.network/ucan/"))
            .perform(&operator)
            .await;
        assert!(
            matches!(refused, Err(AddAddressError::NotFound { ref name }) if name == "origin"),
            "{refused:?}"
        );
        let (peers, addresses) = recorded(&operator, &repo).await?;
        assert!(peers.is_empty() && addresses.is_empty());
        Ok(())
    }

    /// A name two peers share picks out neither.
    #[dialog_common::test]
    async fn it_refuses_an_ambiguous_name() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        for (did, endpoint) in [
            (did!("web:one.example"), "https://one.example/"),
            (did!("web:two.example"), "https://two.example/"),
        ] {
            repo.peer(did)
                .add_address(UcanAddress::new(endpoint))
                .name("shared")
                .perform(&operator)
                .await?;
        }

        let refused = repo
            .peer("shared")
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
