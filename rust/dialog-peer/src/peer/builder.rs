//! [`PeerBuilder`]: opens a [`Peer`].

use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;

use dialog_capability::{Ability, Capability, Constraint, Subject, did};
use dialog_common::Holdings;
use dialog_credentials::{Ed25519Signer, Signer, SignerCredential};
use dialog_effects::storage::{self as storage_fx, Directory, Location, LocationExt as _};
use dialog_identity::access::Claim;
use dialog_network::Network;
use dialog_repository::{ACCESS_BRANCH, Repository};
use dialog_storage::provider::storage::Storage;
use dialog_ucan::{Scope, UcanCertificate};
use dialog_ucan_core::{DelegationBuilder, time::Timestamp};
use dialog_varsig::{Did, Principal as _};

use std::sync::OnceLock;

use parking_lot::Mutex;

use super::{Grant, Inner, Peer, PeerSpace, Runtime};

/// A builder slot before it is filled.
#[derive(Debug, Clone, Copy, Default)]
pub struct Unset;

/// The key a peer acts with: supplied, or derived from another
/// credential for a context at open.
///
/// Derivation is deterministic per `(credential, context)`, so a grant
/// issued to the derived DID is reusable across runs; random bytes make a
/// disposable key. It runs when the peer opens, through
/// [`SignerCredential::derive`].
#[derive(Clone)]
pub enum PeerKey {
    /// Act as this credential.
    Supplied(Box<SignerCredential>),
    /// Derive the key from `from` and `context` at open.
    Derived {
        /// The credential to derive from.
        from: Box<SignerCredential>,
        /// The derivation context.
        context: Vec<u8>,
    },
}

impl fmt::Debug for PeerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Supplied(credential) => {
                f.debug_tuple("Supplied").field(&credential.did()).finish()
            }
            Self::Derived { from, context } => f
                .debug_struct("Derived")
                .field("from", &from.did())
                .field("context", context)
                .finish(),
        }
    }
}

impl From<SignerCredential> for PeerKey {
    fn from(credential: SignerCredential) -> Self {
        Self::Supplied(Box::new(credential))
    }
}

impl From<Signer> for PeerKey {
    fn from(signer: Signer) -> Self {
        SignerCredential::from(signer).into()
    }
}

impl From<Ed25519Signer> for PeerKey {
    fn from(signer: Ed25519Signer) -> Self {
        SignerCredential::from(signer).into()
    }
}

impl PeerKey {
    async fn resolve(self) -> Result<SignerCredential, PeerError> {
        match self {
            Self::Supplied(credential) => Ok(*credential),
            Self::Derived { from, context } => from
                .derive(&context)
                .await
                .map_err(|error| PeerError::Key(error.to_string())),
        }
    }
}

/// A scope some issuer grants the peer at open.
///
/// Made from a capability (claimed by the builder's default issuer, the
/// parent of a [worker](Peer::worker)), from a [`Claim`] that names its
/// issuer and carries its window, or from a pre-minted
/// [`UcanCertificate`] whose audience is the peer's key.
#[derive(Clone)]
pub struct Allowance {
    kind: AllowanceKind,
}

impl fmt::Debug for Allowance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            AllowanceKind::Scope {
                scope,
                issuer,
                not_before,
                expiration,
                unbounded,
            } => f
                .debug_struct("Allowance")
                .field("scope", scope)
                .field("issuer", &issuer.as_ref().map(|issuer| issuer.did()))
                .field("not_before", not_before)
                .field("expiration", expiration)
                .field("unbounded", unbounded)
                .finish(),
            AllowanceKind::Certificate(certificate) => f
                .debug_tuple("Allowance")
                .field(certificate.0.issuer())
                .finish(),
        }
    }
}

#[derive(Clone)]
enum AllowanceKind {
    Scope {
        scope: Scope,
        issuer: Option<SignerCredential>,
        not_before: Option<Timestamp>,
        expiration: Option<Timestamp>,
        /// Whether a missing expiration is deliberate: set by
        /// [`PeerBuilder::allow`], never by [`PeerBuilder::grant`].
        unbounded: bool,
    },
    Certificate(UcanCertificate),
}

impl Allowance {
    fn unbounded(mut self) -> Self {
        if let AllowanceKind::Scope { unbounded, .. } = &mut self.kind {
            *unbounded = true;
        }
        self
    }
}

impl<T> From<Capability<T>> for Allowance
where
    T: Constraint,
    Capability<T>: Ability,
{
    fn from(capability: Capability<T>) -> Self {
        Allowance {
            kind: AllowanceKind::Scope {
                scope: Scope::from(&capability),
                issuer: None,
                not_before: None,
                expiration: None,
                unbounded: false,
            },
        }
    }
}

impl From<Subject> for Allowance {
    fn from(subject: Subject) -> Self {
        Capability::from(subject).into()
    }
}

impl<C> From<Claim<'_, C>> for Allowance
where
    C: Constraint,
    Capability<C>: Ability,
{
    fn from(claim: Claim<'_, C>) -> Self {
        Allowance {
            kind: AllowanceKind::Scope {
                scope: Scope::from(claim.capability()),
                issuer: Some(claim.by().clone()),
                not_before: claim.activation(),
                expiration: claim.expiration(),
                unbounded: false,
            },
        }
    }
}

impl From<UcanCertificate> for Allowance {
    fn from(certificate: UcanCertificate) -> Self {
        Allowance {
            kind: AllowanceKind::Certificate(certificate),
        }
    }
}

/// Builder for a [`Peer`]. Created by [`Peer::open`] or [`Peer::worker`].
///
/// `K` is the key slot and `St` the storage slot, [`Unset`] until
/// [`credential`](Self::credential) and [`storage`](Self::storage) fill
/// them; only a builder with both can be awaited.
pub struct PeerBuilder<K = Unset, St = Unset> {
    home: Did,
    key: K,
    storage: St,
    location: Option<Location>,
    directory: Option<Directory>,
    network: Network,
    runtime: Runtime,
    branch: Option<String>,
    issuer: Option<SignerCredential>,
    allowed: Vec<Allowance>,
}

impl PeerBuilder {
    pub(crate) fn new(home: Did) -> Self {
        Self {
            home,
            key: Unset,
            storage: Unset,
            location: None,
            directory: None,
            network: Network::default(),
            runtime: Runtime::default(),
            branch: Some(ACCESS_BRANCH.to_string()),
            issuer: None,
            allowed: Vec::new(),
        }
    }
}

impl<S: Clone> PeerBuilder<PeerKey, Storage<S>> {
    /// A builder pre-filled from `peer`: its home, storage, network,
    /// runtime, base directory and state branch, with `peer` as the
    /// issuer of bare-capability grants.
    pub(crate) fn from_peer(peer: &Peer<S>, key: PeerKey) -> Self {
        Self {
            home: peer.home().clone(),
            key,
            storage: peer.storage().clone(),
            location: None,
            directory: Some(peer.directory().clone()),
            network: peer.network().clone(),
            runtime: peer.runtime().clone(),
            branch: peer.branch().map(str::to_string),
            issuer: Some(peer.credential().clone()),
            allowed: Vec::new(),
        }
    }
}

impl<K, St> PeerBuilder<K, St> {
    /// Where the home repository's space lives. Mounted at open when it
    /// is not already, and checked to be the space `home` names.
    ///
    /// Not needed when the space is already mounted in the storage, as it
    /// is after `OpenCredential` loaded the credential from it, or for a
    /// worker of a peer that mounted it.
    pub fn at(mut self, location: Location) -> Self {
        self.location = Some(location);
        self
    }

    /// The directory space names resolve against, until the registry
    /// resolves them by fact. Defaults to the [`at`](Self::at) location's
    /// directory, else the current directory.
    pub fn base(mut self, directory: Directory) -> Self {
        self.directory = Some(directory);
        self
    }

    /// The network fork invocations dispatch through.
    pub fn network(mut self, network: Network) -> Self {
        self.network = network;
        self
    }

    /// The runtime this peer performs through, to share a scheduler and
    /// speculation queue with other peers over the same storage.
    pub fn runtime(mut self, runtime: Runtime) -> Self {
        self.runtime = runtime;
        self
    }

    /// The branch of the home repository that holds this peer's own
    /// state: where it finds delegations and retains them. Defaults to
    /// `main`.
    pub fn branch(mut self, name: impl Into<String>) -> Self {
        self.branch = Some(name.into());
        self
    }

    /// No state branch: the peer proves from its in-memory grants and
    /// self-issued authority only, and retains nothing. For a disposable
    /// worker, whose retained state would outlive its key.
    pub fn ephemeral(mut self) -> Self {
        self.branch = None;
        self
    }

    /// The credential bare-capability grants are claimed by, when the
    /// builder was not started from a peer.
    pub fn issuer(mut self, credential: impl Into<SignerCredential>) -> Self {
        self.issuer = Some(credential.into());
        self
    }

    /// Grant a bounded scope: a [`Claim`] with an expiration, or a
    /// pre-minted certificate. A claim without an expiration is refused
    /// at open; use [`allow`](Self::allow) for the deliberate unbounded
    /// case.
    pub fn grant(mut self, allowance: impl Into<Allowance>) -> Self {
        self.allowed.push(allowance.into());
        self
    }

    /// Allow a scope without a time bound, minted at open. The unbounded
    /// grant is the deliberate case, so it has its own name.
    pub fn allow(mut self, allowance: impl Into<Allowance>) -> Self {
        self.allowed.push(allowance.into().unbounded());
        self
    }
}

impl<St> PeerBuilder<Unset, St> {
    /// The key this peer acts with: a credential, a bare signer, or a
    /// [`PeerKey`] to derive at open.
    pub fn credential<K: Into<PeerKey>>(self, key: K) -> PeerBuilder<PeerKey, St> {
        PeerBuilder {
            home: self.home,
            key: key.into(),
            storage: self.storage,
            location: self.location,
            directory: self.directory,
            network: self.network,
            runtime: self.runtime,
            branch: self.branch,
            issuer: self.issuer,
            allowed: self.allowed,
        }
    }
}

impl<K> PeerBuilder<K, Unset> {
    /// The storage the peer's spaces are mounted in. Fixes the space type.
    pub fn storage<S: Clone>(self, storage: Storage<S>) -> PeerBuilder<K, Storage<S>> {
        PeerBuilder {
            home: self.home,
            key: self.key,
            storage,
            location: self.location,
            directory: self.directory,
            network: self.network,
            runtime: self.runtime,
            branch: self.branch,
            issuer: self.issuer,
            allowed: self.allowed,
        }
    }
}

impl<S: PeerSpace> PeerBuilder<PeerKey, Storage<S>> {
    /// Open the peer: resolve its key, mount its home space when told
    /// where, mint its grants, and open its state branch.
    ///
    /// Every allowance becomes a delegation to the peer's key held **in
    /// memory**. Nothing is persisted: a derived key re-mints identical
    /// authority on every open, and persisting it would only accumulate
    /// (one immortal certificate per session was exactly the field
    /// pathology).
    pub async fn build(self) -> Result<Peer<S>, PeerError> {
        let credential = self.key.resolve().await?;

        if let Some(location) = &self.location {
            let mounted = Subject::from(did!("local:storage"))
                .attenuate(storage_fx::Storage)
                .attenuate(location.clone())
                .load()
                .perform(&self.storage)
                .await
                .map_err(|error| PeerError::Open(error.to_string()))?;
            if mounted.did() != self.home {
                return Err(PeerError::Home(format!(
                    "the space at {location:?} is {}, not the home {}",
                    mounted.did(),
                    self.home
                )));
            }
        }

        let directory = self
            .directory
            .or_else(|| {
                self.location
                    .as_ref()
                    .map(|location| location.directory.clone())
            })
            .unwrap_or(Directory::Current);

        let audience = credential.did();
        let mut grants = Vec::with_capacity(self.allowed.len());
        for allowance in self.allowed {
            let grant = match allowance.kind {
                AllowanceKind::Certificate(certificate) => Grant {
                    issuer: certificate.0.issuer().clone(),
                    certificate,
                },
                AllowanceKind::Scope {
                    scope,
                    issuer,
                    not_before,
                    expiration,
                    unbounded,
                } => {
                    let issuer = match issuer.or_else(|| self.issuer.clone()) {
                        Some(issuer) => issuer,
                        None => {
                            return Err(PeerError::Issuer(format!(
                                "no issuer for the grant of {scope:?}: claim it by a credential"
                            )));
                        }
                    };
                    if expiration.is_none() && !unbounded {
                        return Err(PeerError::Unbounded(format!(
                            "the grant of {scope:?} has no expiration; use allow for an unbounded grant"
                        )));
                    }
                    let mut builder = DelegationBuilder::new()
                        .issuer(issuer.signer().clone())
                        .audience(&audience)
                        .subject(scope.subject.clone())
                        .command(scope.command.segments().clone())
                        .policy(scope.policy());
                    if let Some(not_before) = not_before {
                        builder = builder.not_before(not_before);
                    }
                    if let Some(expiration) = expiration {
                        builder = builder.expiration(expiration);
                    }
                    let delegation = builder
                        .try_build()
                        .await
                        .map_err(|e| PeerError::Delegation(format!("{e:?}")))?;
                    Grant {
                        issuer: issuer.did(),
                        certificate: UcanCertificate(delegation),
                    }
                }
            };
            grants.push(grant);
        }

        let peer = Peer::assemble(
            self.storage,
            Inner {
                credential,
                home: self.home.clone(),
                directory,
                network: self.network,
                runtime: self.runtime,
                branch: self.branch.clone(),
                state: OnceLock::new(),
                chains: Mutex::default(),
                grants,
                holdings: Holdings::default(),
                connections: Mutex::default(),
            },
        );

        if let Some(name) = self.branch {
            let branch = Repository::from(self.home)
                .branch(name)
                .open()
                .perform(&peer)
                .await
                .map_err(|error| PeerError::Delegation(format!("{error}")))?;
            peer.attach_state(branch);
        }

        Ok(peer)
    }
}

/// The future an awaited builder runs.
#[cfg(not(target_arch = "wasm32"))]
pub type OpenFuture<S> = Pin<Box<dyn Future<Output = Result<Peer<S>, PeerError>> + Send>>;
/// The future an awaited builder runs (single-threaded wasm form).
#[cfg(target_arch = "wasm32")]
pub type OpenFuture<S> = Pin<Box<dyn Future<Output = Result<Peer<S>, PeerError>>>>;

impl<S: PeerSpace> IntoFuture for PeerBuilder<PeerKey, Storage<S>> {
    type Output = Result<Peer<S>, PeerError>;
    type IntoFuture = OpenFuture<S>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.build())
    }
}

/// Errors that can occur when opening a peer.
#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    /// Key derivation or generation failed.
    #[error("Key error: {0}")]
    Key(String),
    /// Mounting the home space failed.
    #[error("Open error: {0}")]
    Open(String),
    /// The space at the given location is not the home repository.
    #[error("Home error: {0}")]
    Home(String),
    /// A grant named no issuer.
    #[error("Grant error: {0}")]
    Issuer(String),
    /// A grant has no expiration and was not marked unbounded.
    #[error("Grant error: {0}")]
    Unbounded(String),
    /// Minting a grant or opening the state branch failed.
    #[error("Delegation error: {0}")]
    Delegation(String),
}
