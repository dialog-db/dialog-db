//! Peers, identified by where they are reached.
//!
//! A peer is whoever holds replicas. A remote service has no key of its
//! own that a replica could name, so its DID is derived from its
//! address, and every replica that names the same service converges on
//! the same peer:
//!
//! - a UCAN service is `did:web` of its endpoint's origin, the DID its
//!   host publishes a document for, whatever path the service is served
//!   under;
//! - an S3 bucket is `did:web` of its endpoint's origin, with the bucket
//!   joining as a path segment when the endpoint is path-style;
//! - a directory on the local filesystem is the `did:key` of the
//!   Ed25519 key seeded by the Blake3 hash of its file URI, one for one
//!   directory however its path is spelled.
//!
//! A peer reached over iroh is the exception: it does have a key, its
//! endpoint id, so its DID is that key's `did:key` and nothing is
//! derived.

use dialog_capability::{Provider, Subject};
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_credentials::Ed25519Verifier;
use dialog_effects::MethodExt as _;
use dialog_effects::authority::{AuthorityError, Identify, OperatorExt as _};
use dialog_effects::memory::Resolve;
use dialog_effects::peer::prelude::*;
use dialog_effects::peer::{self as peer_fx, PeerAddress};
use dialog_effects::storage::{Directory, Location};
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
///
/// Private to the crate: an application names a peer by the DID it was
/// given and records the addresses that DID resolves to. Only the upgrade,
/// carrying remotes that were recorded by address alone, has nothing but
/// an address to name a peer by.
pub(crate) fn peer_did(address: &SiteAddress) -> Result<Did, PeerError> {
    match address {
        // A virtual-hosted endpoint names the bucket in its host, so its
        // origin is the store. A path-style one shares its host with every
        // other bucket there, so the bucket joins the DID as a path
        // segment: two buckets are two stores, and two peers.
        SiteAddress::S3(address) if address.path_style() => {
            web(address.endpoint(), [address.bucket()])
        }
        SiteAddress::S3(address) => web(address.endpoint(), []),
        // A UCAN service is the peer its origin names: `did:web` resolves
        // a bare origin to `/.well-known/did.json`, the document the host
        // publishes, while a path would name a document under that path
        // nobody serves. The path is where the service is reached, and
        // stays in the address.
        SiteAddress::Ucan(address) => {
            let endpoint = Url::parse(&address.endpoint).map_err(|_| PeerError::NoOrigin {
                endpoint: address.endpoint.clone(),
            })?;
            web(&endpoint, [])
        }
        SiteAddress::Fs(address) => Ok(key(address.location())),
        // An iroh peer is its key: the endpoint id is the peer's own
        // Ed25519 public key, so the address already carries the DID
        // and its routes are only hints for reaching it.
        SiteAddress::Iroh(address) => Ok(address.did()),
    }
}

/// `did:web` of the endpoint's origin, with `path` appended as segments.
///
/// `did:web` names an HTTPS origin: `did:web:h` is `https://h`, and a
/// port other than 443 follows the host percent-encoded, as the method
/// requires. A plain HTTP endpoint is the same host reached another
/// way, so it carries its port even when that is the default 80: `http://h`
/// is `did:web:h%3A80`, and `http://h` and `https://h` are two peers. An
/// explicit port names one service whichever scheme reaches it, since a
/// port serves one protocol. Each path segment is percent-encoded to
/// the characters a DID allows.
fn web<'a>(endpoint: &Url, path: impl IntoIterator<Item = &'a str>) -> Result<Did, PeerError> {
    let no_origin = || PeerError::NoOrigin {
        endpoint: endpoint.to_string(),
    };
    let host = endpoint.host_str().ok_or_else(no_origin)?;
    let port = match (endpoint.scheme(), endpoint.port_or_known_default()) {
        ("https", Some(443)) => None,
        ("https" | "http", Some(port)) => Some(port),
        _ => return Err(no_origin()),
    };
    let mut did = match port {
        Some(port) => format!("did:web:{host}%3A{port}"),
        None => format!("did:web:{host}"),
    };
    for segment in path {
        did.push(':');
        did.push_str(&encode(segment));
    }
    did.parse().map_err(|_| no_origin())
}

/// `segment` with every byte outside a DID's unreserved characters
/// percent-encoded.
fn encode(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-' | b'_' => {
                encoded.push(byte as char)
            }
            byte => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

/// `did:key` of the Ed25519 key seeded by the Blake3 hash of the
/// location's canonical file URI: the one [`file_uri`] derives, which
/// names one directory however its path is spelled, not the URI a
/// peer's records carry ([`Location::uri`]), which keeps the spelling.
fn key(location: &Location) -> Did {
    let seed = Blake3Hash::hash(file_uri(location).as_bytes());
    let key = ed25519_dalek::SigningKey::from_bytes(seed.as_bytes());
    Ed25519Verifier::from(key).did()
}

/// The file URI naming a location: one for one directory, however its
/// path is spelled.
///
/// Natively the directory is resolved to where it is on this device --
/// a platform directory to the path it stands for, as the filesystem
/// storage lays it out, and a relative one against the working
/// directory -- and the path is normalized lexically: `.` and `..`
/// folded, slashes single and none trailing. So the same role on two
/// devices is two peers, and one directory named by role or by path is
/// one peer. A platform directory that cannot be resolved is named by
/// its role instead.
///
/// On the web there is no path to resolve: a platform directory is
/// named by its role and `At` by its key, normalized the same way, so
/// these identities are per origin rather than per directory.
fn file_uri(location: &Location) -> String {
    let Location { directory, name } = location;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(path) = resolve(directory) {
        return format!("file://{}/{name}", normalize(&path));
    }
    match directory {
        Directory::At(path) => format!("file://{}/{name}", normalize(path)),
        Directory::Profile => format!("file:profile/{name}"),
        Directory::Current => format!("file:current/{name}"),
        Directory::Temp => format!("file:temp/{name}"),
    }
}

/// Where `directory` is on this device, if that can be told: the same
/// places the filesystem storage resolves it to.
#[cfg(not(target_arch = "wasm32"))]
fn resolve(directory: &Directory) -> Option<String> {
    use std::env;
    use std::path::PathBuf;
    let path = match directory {
        Directory::Profile => dirs::data_dir()?.join("dialog"),
        Directory::Current => env::current_dir().ok()?,
        Directory::Temp => env::temp_dir(),
        Directory::At(path) => {
            let path = PathBuf::from(path);
            if path.is_absolute() {
                path
            } else {
                env::current_dir().ok()?.join(path)
            }
        }
    };
    Some(path.to_string_lossy().into_owned())
}

/// `path` with `.` and `..` folded, slashes single and none trailing,
/// keeping whether it is absolute.
fn normalize(path: &str) -> String {
    let mut segments: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            segment => segments.push(segment),
        }
    }
    let joined = segments.join("/");
    if path.starts_with('/') {
        format!("/{joined}")
    } else {
        joined
    }
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
    use dialog_peer_iroh::site::IrohAddress;
    use dialog_query::{Output as _, Query, Term};
    use dialog_remote_fs::FsAddress;
    use dialog_remote_s3::Address;
    use dialog_remote_ucan::UcanAddress;
    use dialog_storage::provider::storage::VolatileSpace;
    use dialog_varsig::did;

    /// A UCAN service is the `did:web` of its origin, the DID whose
    /// document the host publishes at `/.well-known/did.json`, whatever
    /// path the service is served under. The DIDs are pinned: a peer's
    /// identity is derived from its address, so changing the derivation
    /// changes every peer on record.
    #[dialog_common::test]
    fn it_names_a_ucan_service_by_its_origin() -> anyhow::Result<()> {
        let ucan = |endpoint: &str| SiteAddress::from(UcanAddress::new(endpoint));
        assert_eq!(
            peer_did(&ucan("https://tonk.network/ucan/"))?,
            did!("web:tonk.network")
        );
        assert_eq!(
            peer_did(&ucan("https://tonk.network/"))?,
            did!("web:tonk.network")
        );
        assert_eq!(
            peer_did(&ucan("https://tonk.network/sync/v2/"))?,
            did!("web:tonk.network")
        );
        assert_eq!(
            peer_did(&ucan("https://tonk.network/a/"))?,
            peer_did(&ucan("https://tonk.network/b/"))?,
            "two paths on one origin are one peer"
        );
        Ok(())
    }

    /// A non-default port is part of the origin, percent-encoded as
    /// `did:web` requires. `did:web` names an HTTPS origin, so a plain
    /// HTTP one carries its port even when it is the default: `http://h`
    /// and `https://h` are two peers.
    #[dialog_common::test]
    fn it_tells_schemes_and_ports_apart() -> anyhow::Result<()> {
        let ucan = |endpoint: &str| SiteAddress::from(UcanAddress::new(endpoint));
        assert_eq!(
            peer_did(&ucan("http://localhost:8787/ucan"))?,
            did!("web:localhost%3A8787")
        );
        assert_eq!(
            peer_did(&ucan("https://tonk.network:8443/"))?,
            did!("web:tonk.network%3A8443")
        );
        assert_eq!(
            peer_did(&ucan("http://tonk.network/"))?,
            did!("web:tonk.network%3A80")
        );
        assert_eq!(
            peer_did(&ucan("https://tonk.network:443/"))?,
            did!("web:tonk.network")
        );
        assert_ne!(
            peer_did(&ucan("http://tonk.network/"))?,
            peer_did(&ucan("https://tonk.network/"))?
        );
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

    /// A directory is a `did:key`, the same one however its path is
    /// spelled -- with a trailing slash, doubled slashes, or `.` and
    /// `..` in it -- and a different one for a different directory. The
    /// DID is pinned: changing the derivation changes every peer on
    /// record.
    #[dialog_common::test]
    fn it_names_a_directory_by_a_key_seeded_from_it() -> anyhow::Result<()> {
        let at = |path: &str, name: &str| {
            SiteAddress::Fs(FsAddress::new(Location::new(
                Directory::At(path.into()),
                name,
            )))
        };
        let backup = peer_did(&at("/var/dialog", "backup"))?;
        assert_eq!(
            backup,
            did!("key:z6MkvLsmuzEuydpgVzeoXG1pvKm7R9uTCsrhy25KcJYefBNY"),
            "the pinned DID of file:///var/dialog/backup"
        );
        for spelling in [
            "/var/dialog/",
            "/var//dialog",
            "/var/./dialog",
            "/var/tmp/../dialog",
        ] {
            assert_eq!(peer_did(&at(spelling, "backup"))?, backup, "{spelling}");
        }
        assert_ne!(backup, peer_did(&at("/var/dialog", "other"))?);
        assert_ne!(backup, peer_did(&at("/var/dialog2", "backup"))?);
        Ok(())
    }

    /// A platform directory names the directory it resolves to on this
    /// device, so the same role on two devices is two peers, and the
    /// same directory named by role or by path is one.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    fn it_names_a_platform_directory_by_where_it_resolves() -> anyhow::Result<()> {
        use std::env;
        use std::path::PathBuf;
        let fs = |directory: Directory, name: &str| {
            SiteAddress::Fs(FsAddress::new(Location::new(directory, name)))
        };
        let at = |path: PathBuf| Directory::At(path.to_string_lossy().into_owned());
        assert_eq!(
            peer_did(&fs(Directory::Current, "backup"))?,
            peer_did(&fs(at(env::current_dir()?), "backup"))?
        );
        assert_eq!(
            peer_did(&fs(Directory::Temp, "backup"))?,
            peer_did(&fs(at(env::temp_dir()), "backup"))?
        );
        assert_eq!(
            peer_did(&fs(Directory::At("relative/dir".into()), "backup"))?,
            peer_did(&fs(at(env::current_dir()?.join("relative/dir")), "backup"))?
        );
        assert_ne!(
            peer_did(&fs(Directory::Current, "backup"))?,
            peer_did(&fs(Directory::Temp, "backup"))?
        );
        Ok(())
    }

    /// A peer reached over iroh is named by its own key, not by anything
    /// derived from where it is: the address carries the `did:key`, and
    /// it survives the encoding a contact records it in.
    #[dialog_common::test]
    async fn it_names_an_iroh_peer_by_its_key() -> anyhow::Result<()> {
        let (_, peer) = test_session_with_peer().await;
        let did = peer.did();
        let address = SiteAddress::from(IrohAddress::parse_did(did.as_ref())?);

        assert_eq!(peer_did(&address)?, did);
        assert_eq!(site_address(&peer_address(&address)?)?, address);
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
        operator: &LocalPeer<VolatileSpace, impl dialog_peer::Mode>,
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
    async fn found(
        operator: &LocalPeer<VolatileSpace, impl dialog_peer::Mode>,
        name: &str,
    ) -> anyhow::Result<Vec<Entity>> {
        let host = super::host(operator).await?;
        Ok(host.reader().peers().find(name).perform(operator).await?)
    }

    /// Where the host `operator` acts for reaches `peer`, in order.
    async fn reached(
        operator: &LocalPeer<VolatileSpace, impl dialog_peer::Mode>,
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
