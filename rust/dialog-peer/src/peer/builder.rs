//! [`PeerBuilder`]: opens a [`Peer`].

use std::any::Any;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use dialog_capability::access::{Access, Prove};
use dialog_capability::{Ability, Capability, Constraint, Subject, did};
use dialog_common::{Held, Holdings};
use dialog_credentials::{Credential, Ed25519Signer, Signer, SignerCredential, Verifier};
use dialog_effects::storage::{self as storage_fx, Directory, Location, LocationExt as _};
use dialog_identity::access::{Access as Accessor, Claim};
use dialog_network::Network;
use dialog_repository::BranchReference;
use dialog_storage::provider::storage::Storage;
use dialog_ucan::{Scope, Ucan, UcanCertificate};
use dialog_ucan_core::subject::Subject as UcanSubject;
use dialog_ucan_core::{DelegationBuilder, time::Timestamp};
use dialog_varsig::{Did, Principal as _};

use parking_lot::Mutex;

use super::secret::SiteSecrets;
use super::upgrade::Step;
use super::{Grant, Inner, Local, Mode, Peer, PeerSpace, Runtime, Session};

/// A peer built from the branch that holds its state: the same builder
/// [`Peer::new`] starts, with this branch as its [space](PeerBuilder::space).
///
/// ```no_run
/// # use dialog_peer::{Allowance, BranchPeerExt as _};
/// # use dialog_repository::Repository;
/// # use dialog_varsig::{Did, Principal as _};
/// # async fn example(
/// #     credential: dialog_credentials::SignerCredential,
/// #     storage: dialog_storage::provider::storage::Storage<dialog_storage::provider::storage::VolatileSpace>,
/// #     granted: Allowance,
/// # ) -> anyhow::Result<()> {
/// let peer = Repository::from(credential.did())
///     .branch("profile")
///     .peer(credential.clone())
///     .with(storage)
///     .grant(granted)
///     .await?;
/// # let _ = peer;
/// # Ok(())
/// # }
/// ```
pub trait BranchPeerExt {
    /// Start building the peer acting with `credential` whose state this
    /// branch holds.
    fn peer(self, credential: impl Into<SignerCredential>) -> PeerBuilder<PeerKey>;
}

impl BranchPeerExt for BranchReference {
    fn peer(self, credential: impl Into<SignerCredential>) -> PeerBuilder<PeerKey> {
        Peer::new(credential).space(self)
    }
}

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
/// peer of a [session](Peer::session)), from a [`Claim`] that names its
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
    /// A grant from `system` of the storage it owns: mounting spaces in
    /// it. What a peer is given to open spaces in a storage
    /// [owned by](Storage::owned_by) `system`, for as long as the peer is
    /// open.
    pub fn storage(system: &SignerCredential) -> Self {
        Allowance::from(
            Accessor::new(system).claim(Subject::from(system.did()).attenuate(storage_fx::Storage)),
        )
        .unbounded()
    }

    fn unbounded(mut self) -> Self {
        if let AllowanceKind::Scope { unbounded, .. } = &mut self.kind {
            *unbounded = true;
        }
        self
    }

    /// Whether this allowance so much as names the storage of `system`:
    /// a scope or certificate over that subject whose command covers
    /// `storage`. Not a proof, a first look before anything is mounted.
    fn names_storage_of(&self, system: &Did) -> bool {
        let covers = |segments: &[String]| {
            segments.is_empty() || segments.first().map(String::as_str) == Some("storage")
        };
        match &self.kind {
            AllowanceKind::Scope { scope, .. } => {
                matches!(&scope.subject, UcanSubject::Specific(subject) if subject == system)
                    && covers(scope.command.segments())
            }
            AllowanceKind::Certificate(certificate) => {
                let subject = match certificate.0.subject() {
                    UcanSubject::Any => true,
                    UcanSubject::Specific(subject) => subject == system,
                };
                subject && covers(&certificate.0.command().0)
            }
        }
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

/// Builder for a [`Peer`]. Created by [`Peer::new`], [`Peer::operator`]
/// or [`Peer::session`].
///
/// `K` is the key slot and `St` the storage slot, [`Unset`] until
/// [`credential`](Self::credential) and [`with`](Self::with) a storage
/// fill them; only a builder with both can be awaited. `M` is the mode of the
/// peer it builds: [`Local`] acting with the peer's own key, or
/// [`Session`] acting with a separate operator key.
pub struct PeerBuilder<K = Unset, St = Unset, M = Local> {
    state: Option<BranchReference>,
    key: K,
    storage: St,
    location: Option<Location>,
    directory: Option<Directory>,
    network: Network,
    runtime: Runtime,
    issuer: Option<SignerCredential>,
    allowed: Vec<Allowance>,
    /// Certificates addressed to someone above this peer in its chain:
    /// a session's peer's own grants, which its proofs pass through.
    held: Vec<Grant>,
    /// Steps bringing the peer's records up to date, run when it opens:
    /// each a `Step<S>` for the storage's space, type-erased until the
    /// storage is known.
    steps: Vec<Held>,
    /// The live peer a session is built from, asked for its site secrets
    /// (see [`Inner::sites`]).
    sites: Option<Arc<dyn SiteSecrets>>,
    mode: PhantomData<M>,
}

impl<M> PeerBuilder<Unset, Unset, M> {
    pub(crate) fn new() -> Self {
        Self {
            state: None,
            key: Unset,
            storage: Unset,
            location: None,
            directory: None,
            network: Network::default(),
            runtime: Runtime::default(),
            issuer: None,
            allowed: Vec::new(),
            held: Vec::new(),
            steps: Vec::new(),
            sites: None,
            mode: PhantomData,
        }
    }
}

impl<S: PeerSpace> PeerBuilder<PeerKey, Storage<S>, Session> {
    /// A session builder pre-filled from `peer`: its storage, network,
    /// runtime and base directory, with `peer` as the issuer of
    /// bare-capability grants. Its state branch is not assumed to be the
    /// peer's: it is given one with [`space`](Self::space).
    pub(crate) fn from_peer(peer: &Peer<S, Local>, key: PeerKey) -> Self {
        Self {
            state: None,
            key,
            storage: peer.storage().clone(),
            location: None,
            directory: Some(peer.directory().clone()),
            network: peer.network().clone(),
            runtime: peer.runtime().clone(),
            issuer: Some(peer.credential().clone()),
            allowed: Vec::new(),
            // The peer's grants from the storage's system: links a
            // session's chains to the storage pass through, which it can
            // extend but not use alone.
            held: peer
                .grants()
                .iter()
                .filter(|grant| &grant.issuer == peer.system())
                .cloned()
                .collect(),
            steps: Vec::new(),
            sites: Some(Arc::new(peer.clone())),
            mode: PhantomData,
        }
    }
}

impl<K, St, M> PeerBuilder<K, St, M> {
    /// Where the home repository's space lives: mounted at open, and
    /// created holding the home's public identity when it is not there
    /// yet, then checked to be the space the home names.
    ///
    /// Not needed when the space is already mounted in the storage, as it
    /// is for a session of a peer that mounted it.
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

    /// Build the peer with `provider`: the [`Storage`] its spaces are
    /// mounted in, or the [`Network`] it reaches other peers through.
    pub fn with<T>(self, provider: T) -> <Self as With<T>>::Output
    where
        Self: With<T>,
    {
        With::with(self, provider)
    }

    /// The runtime this peer performs through, to share a scheduler and
    /// speculation queue with other peers over the same storage.
    pub fn runtime(mut self, runtime: Runtime) -> Self {
        self.runtime = runtime;
        self
    }

    /// The space the peer keeps its records in: `branch`, where it finds
    /// delegations and retains them, records its spaces and contacts, and
    /// keeps its sealed secrets. Its repository is
    /// the peer's home. Required: a peer is never assumed to keep its
    /// records anywhere in particular, and a session is not assumed to
    /// share its peer's.
    pub fn space(mut self, branch: impl Into<BranchReference>) -> Self {
        self.state = Some(branch.into());
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

impl<St> PeerBuilder<PeerKey, St, Local> {
    /// Act with `operator` instead of the peer's own key: a session of
    /// the peer. Grants given as bare capabilities are claimed by the
    /// peer, and the peer's key is not kept by what this builds.
    pub fn operator(self, operator: impl Into<PeerKey>) -> PeerBuilder<PeerKey, St, Session> {
        PeerBuilder {
            state: self.state,
            key: operator.into(),
            storage: self.storage,
            location: self.location,
            directory: self.directory,
            network: self.network,
            runtime: self.runtime,
            issuer: self.issuer,
            allowed: self.allowed,
            held: self.held,
            steps: self.steps,
            sites: self.sites,
            mode: PhantomData,
        }
    }

    /// Act with a key derived from the peer's for `context`: a session of
    /// the peer, as [`operator`](Self::operator) with a derived key.
    pub fn session(self, context: impl AsRef<[u8]>) -> PeerBuilder<PeerKey, St, Session> {
        let from = match &self.key {
            PeerKey::Supplied(credential) => credential.clone(),
            PeerKey::Derived { from, .. } => from.clone(),
        };
        self.operator(PeerKey::Derived {
            from,
            context: context.as_ref().to_vec(),
        })
    }
}

impl<St> PeerBuilder<Unset, St, Session> {
    /// The key the session acts with.
    pub fn operator(self, operator: impl Into<PeerKey>) -> PeerBuilder<PeerKey, St, Session> {
        self.credential(operator)
    }
}

impl<St, M> PeerBuilder<Unset, St, M> {
    /// The key this peer acts with: a credential, a bare signer, or a
    /// [`PeerKey`] to derive at open.
    pub fn credential<K: Into<PeerKey>>(self, key: K) -> PeerBuilder<PeerKey, St, M> {
        PeerBuilder {
            state: self.state,
            key: key.into(),
            storage: self.storage,
            location: self.location,
            directory: self.directory,
            network: self.network,
            runtime: self.runtime,
            issuer: self.issuer,
            allowed: self.allowed,
            held: self.held,
            steps: self.steps,
            sites: self.sites,
            mode: PhantomData,
        }
    }
}

/// A provider a peer is built with. See [`PeerBuilder::with`].
pub trait With<T> {
    /// The builder with the provider in place.
    type Output;
    /// Put `provider` in place.
    fn with(self, provider: T) -> Self::Output;
}

/// The storage the peer's spaces are mounted in. Fixes the space type.
impl<K, M, S: Clone> With<Storage<S>> for PeerBuilder<K, Unset, M> {
    type Output = PeerBuilder<K, Storage<S>, M>;

    fn with(self, storage: Storage<S>) -> Self::Output {
        PeerBuilder {
            state: self.state,
            key: self.key,
            storage,
            location: self.location,
            directory: self.directory,
            network: self.network,
            runtime: self.runtime,
            issuer: self.issuer,
            allowed: self.allowed,
            held: self.held,
            steps: self.steps,
            sites: self.sites,
            mode: PhantomData,
        }
    }
}

/// The network fork invocations dispatch through.
impl<K, St, M> With<Network> for PeerBuilder<K, St, M> {
    type Output = Self;

    fn with(mut self, network: Network) -> Self {
        self.network = network;
        self
    }
}

impl<S: PeerSpace, M: Mode> PeerBuilder<PeerKey, Storage<S>, M> {
    /// Register `step`, run when the peer opens if its version is above
    /// the one the peer's records are at. See
    /// [`upgrade`](super::upgrade). A session runs no step.
    pub fn upgrade(mut self, step: Step<S>) -> Self {
        self.steps.push(Arc::new(step));
        self
    }

    /// Open the peer: resolve its key, mount its home space when told
    /// where, mint its grants, and open its state branch.
    ///
    /// Every allowance becomes a delegation to the peer's key held **in
    /// memory**. Nothing is persisted: a derived key re-mints identical
    /// authority on every open, and persisting it would only accumulate
    /// (one immortal certificate per session was exactly the field
    /// pathology). That includes the storage's grant: a peer, or a
    /// session through its peer, holds it in memory, and a session
    /// without a handle on its peer proves the storage from a delegation
    /// someone asserted in its state.
    pub async fn build(self) -> Result<Peer<S, M>, PeerError> {
        let Some(reference) = self.state else {
            return Err(PeerError::State(
                "no state branch: give the peer one with `space`".to_string(),
            ));
        };
        let home = reference.of().clone();
        let credential = self.key.resolve().await?;

        // Mounting a space takes the authority of the system the storage
        // belongs to. Nothing grants it implicitly: the peer holds a grant
        // from that system in memory, or proves one through its state.
        let Some(system) = self.storage.system().cloned() else {
            return Err(PeerError::Storage(
                "the storage belongs to no system: give it one with `Storage::owned_by`"
                    .to_string(),
            ));
        };
        // A peer acting as itself is refused before it mounts anything
        // when no grant it was given so much as names the storage: the
        // proof below is what admits it, but a refused peer must not
        // leave its home mounted in the storage it was refused.
        if M::HOLDS_KEYS
            && !self
                .allowed
                .iter()
                .any(|allowance| allowance.names_storage_of(&system))
        {
            return Err(PeerError::Storage(format!(
                "nothing grants {} the storage of {system}: grant it with `Allowance::storage`",
                credential.did()
            )));
        }

        if let Some(location) = &self.location {
            let at = Subject::from(did!("local:storage"))
                .attenuate(storage_fx::Storage)
                .attenuate(location.clone());
            // The home space holds the home's public identity only, like
            // every space: created from the home DID's verifier when it is
            // not there yet.
            let mounted = match at.clone().load().perform(&self.storage).await {
                Ok(mounted) => mounted,
                Err(storage_fx::StorageError::NotFound(_)) => {
                    let identity: Verifier = home.to_string().parse().map_err(|_| {
                        PeerError::Home(format!("the home {home} names no public key"))
                    })?;
                    at.create(Credential::from(identity))
                        .perform(&self.storage)
                        .await
                        .map_err(|error| PeerError::Open(error.to_string()))?
                }
                Err(error) => return Err(PeerError::Open(error.to_string())),
            };
            if mounted.did() != home {
                return Err(PeerError::Home(format!(
                    "the space at {location:?} is {}, not the home {home}",
                    mounted.did(),
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
                AllowanceKind::Certificate(certificate) => {
                    // A certificate is this peer's authority only when it
                    // was issued to this peer's key, its issuer signed
                    // it, and it holds now.
                    if *certificate.0.audience() != audience {
                        return Err(PeerError::Certificate(format!(
                            "the certificate from {} is issued to {}, not to {audience}",
                            certificate.0.issuer(),
                            certificate.0.audience()
                        )));
                    }
                    let now = Timestamp::now();
                    if let Some(expiration) = certificate.0.expiration()
                        && expiration <= now
                    {
                        return Err(PeerError::Certificate(format!(
                            "the certificate from {} expired at {}",
                            certificate.0.issuer(),
                            expiration.to_unix()
                        )));
                    }
                    if let Some(not_before) = certificate.0.not_before()
                        && not_before > now
                    {
                        return Err(PeerError::Certificate(format!(
                            "the certificate from {} is not valid before {}",
                            certificate.0.issuer(),
                            not_before.to_unix()
                        )));
                    }
                    // The structure names an issuer; only the signature
                    // says the issuer stands behind it. Checked as the
                    // delegation walk checks a retained envelope: a local
                    // did:key parse, no I/O.
                    if let Err(error) = certificate
                        .0
                        .verify_signature(&dialog_credentials::DidKeyResolver)
                        .await
                    {
                        return Err(PeerError::Certificate(format!(
                            "the certificate from {} does not verify: {error}",
                            certificate.0.issuer()
                        )));
                    }
                    Grant {
                        issuer: certificate.0.issuer().clone(),
                        certificate,
                    }
                }
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

        let state = reference
            .open()
            .perform(&self.storage)
            .await
            .map_err(|error| PeerError::State(error.to_string()))?;

        let peer = Peer::assemble(
            self.storage,
            Inner {
                credential,
                system: system.clone(),
                held: self.held,
                home,
                directory,
                network: self.network,
                runtime: self.runtime,
                state,
                chains: Mutex::default(),
                grants,
                holdings: Holdings::default(),
                connections: Mutex::default(),
                sites: self.sites,
            },
        );

        // A peer acting as itself must be granted the storage it is built
        // on. A session need not: it may be scoped to other work, and one
        // without the storage's authority is refused when it mounts a
        // space.
        if M::HOLDS_KEYS {
            let storage =
                Scope::from(&Subject::from(system.clone()).attenuate(storage_fx::Storage));
            Subject::from(peer.did())
                .attenuate(Access)
                .invoke(Prove::<Ucan>::new(peer.did(), storage))
                .perform(&peer)
                .await
                .map_err(|error| {
                    PeerError::Storage(format!(
                        "nothing grants {} the storage of {system}: grant it with `Allowance::storage`: {error}",
                        peer.did()
                    ))
                })?;
        }

        // A peer acting as itself brings its records up to date. A
        // session runs no step: it writes nothing to its peer's space.
        if let Some(local) = (&peer as &dyn Any).downcast_ref::<Peer<S, Local>>() {
            let steps: Vec<&Step<S>> = self
                .steps
                .iter()
                .filter_map(|step| step.downcast_ref::<Step<S>>())
                .collect();
            local.upgrade(&steps).await?;
        }

        Ok(peer)
    }
}

/// The future an awaited builder runs.
#[cfg(not(target_arch = "wasm32"))]
pub type OpenFuture<S, M = Local> =
    Pin<Box<dyn Future<Output = Result<Peer<S, M>, PeerError>> + Send>>;
/// The future an awaited builder runs (single-threaded wasm form).
#[cfg(target_arch = "wasm32")]
pub type OpenFuture<S, M = Local> = Pin<Box<dyn Future<Output = Result<Peer<S, M>, PeerError>>>>;

impl<S: PeerSpace, M: Mode> IntoFuture for PeerBuilder<PeerKey, Storage<S>, M> {
    type Output = Result<Peer<S, M>, PeerError>;
    type IntoFuture = OpenFuture<S, M>;

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

    /// A certificate given as a grant is not this peer's authority.
    #[error("Certificate error: {0}")]
    Certificate(String),

    /// The peer holds no grant over the storage it is built on.
    #[error("Storage error: {0}")]
    Storage(String),

    /// The peer was given no state branch, or it could not be opened.
    #[error("State error: {0}")]
    State(String),

    /// A step bringing the peer's records up to date failed, or its
    /// version could not be read or recorded.
    #[error("Upgrade error: {0}")]
    Upgrade(String),
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::{test_grant, test_peer, test_state, test_storage, unique_name};
    use anyhow::Result;
    use dialog_storage::provider::storage::VolatileSpace;
    use dialog_ucan_core::Delegation;
    use dialog_ucan_core::subject::Subject as UcanSubject;
    use dialog_varsig::AnySignature;

    /// The context the certificate tests derive their session key for.
    const CONTEXT: &[u8] = b"certificate";

    /// A fixture pinning the whole derivation: a fixed peer seed and a
    /// fixed context derive one fixed session DID, on every platform.
    ///
    /// The other tests show the derivation is stable within a run, which
    /// a randomized derivation would also pass on any platform whose
    /// Ed25519 does not hedge its nonce. This pins the value itself, so
    /// it fails for a change anywhere in the chain that produces it:
    /// `dialog_credentials`' known-answer vector pins the derived secret;
    /// this pins what that secret becomes. If the derivation changes on
    /// purpose, bump the derivation context there and record the new DID
    /// deliberately. See `notes/operator-derivation.md`.
    #[dialog_common::test]
    async fn it_derives_a_fixed_operator_did_from_a_fixed_seed() -> Result<()> {
        // RFC 8032 test vector 1, used here only as a stable arbitrary seed.
        const PEER_SEED: [u8; 32] = [
            0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec,
            0x2c, 0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03,
            0x1c, 0xae, 0x7f, 0x60,
        ];
        const FIXTURE: &[u8] = b"fixture";
        const EXPECTED_SESSION_DID: &str =
            "did:key:z6MkgAajey1H5u8MLHYnN7YUPd8Pjcvi4MhBtUqqgaRFJbJe";

        let credential = SignerCredential::from(Ed25519Signer::import(&PEER_SEED).await?);
        let derived = credential.derive(FIXTURE).await?;
        assert_eq!(derived.did().to_string(), EXPECTED_SESSION_DID);

        // The session a peer opens for the context acts with that key.
        let peer = Peer::new(credential.clone())
            .at(Location::profile(unique_name("fixed")))
            .space(test_state(&credential.did()))
            .with(test_storage().await)
            .grant(test_grant().await)
            .build()
            .await?;
        let session = peer
            .session(FIXTURE)
            .space(peer.state())
            .allow(Subject::any())
            .await?;
        assert_eq!(session.did().to_string(), EXPECTED_SESSION_DID);
        Ok(())
    }

    /// A delegation from a fresh space to the key `peer.session(CONTEXT)`
    /// derives, valid from `not_before` on.
    async fn delegation_to_session(
        peer: &Peer<VolatileSpace>,
        not_before: Option<Timestamp>,
    ) -> Result<Delegation<AnySignature>> {
        let space = Ed25519Signer::generate().await?;
        let audience = peer.credential().derive(CONTEXT).await?.did();
        let mut builder = DelegationBuilder::new()
            .issuer(Signer::from(space.clone()))
            .audience(&audience)
            .subject(UcanSubject::Specific(space.did()))
            .command(vec!["storage".to_string()]);
        if let Some(not_before) = not_before {
            builder = builder.not_before(not_before);
        }
        builder
            .try_build()
            .await
            .map_err(|error| anyhow::anyhow!("{error:?}"))
    }

    /// A certificate whose envelope signature does not verify against
    /// its issuer is refused at build, before it can become a grant.
    #[dialog_common::test]
    async fn it_refuses_a_certificate_whose_signature_does_not_verify() -> Result<()> {
        let peer = test_peer().await;
        let delegation = delegation_to_session(&peer, None).await?;

        // The envelope encodes as `[signature, payload]`: a two-element
        // array whose first element is the 64-byte signature. Flip one
        // byte inside it, leaving the payload the issuer signed intact.
        let mut bytes = delegation.encoded().to_vec();
        assert_eq!(&bytes[..3], &[0x82, 0x58, 0x40], "the envelope layout");
        bytes[3 + 20] ^= 0xff;
        let tampered: Delegation<AnySignature> = serde_ipld_dagcbor::from_slice(&bytes)?;
        assert_eq!(tampered.issuer(), delegation.issuer());

        let Err(refused) = peer
            .session(CONTEXT)
            .space(peer.state())
            .grant(UcanCertificate(tampered))
            .await
        else {
            panic!("a tampered certificate is refused");
        };
        assert!(matches!(refused, PeerError::Certificate(_)), "{refused:?}");

        // The untampered certificate is the peer's authority.
        peer.session(CONTEXT)
            .space(peer.state())
            .grant(UcanCertificate(delegation))
            .await?;
        Ok(())
    }

    /// A certificate that is not valid yet is refused at build, as an
    /// expired one is.
    #[dialog_common::test]
    async fn it_refuses_a_certificate_not_valid_yet() -> Result<()> {
        let peer = test_peer().await;
        let in_an_hour = Timestamp::try_from(i128::from(Timestamp::now().to_unix()) + 3600)?;
        let delegation = delegation_to_session(&peer, Some(in_an_hour)).await?;

        let Err(refused) = peer
            .session(CONTEXT)
            .space(peer.state())
            .grant(UcanCertificate(delegation))
            .await
        else {
            panic!("a certificate not valid yet is refused");
        };
        assert!(matches!(refused, PeerError::Certificate(_)), "{refused:?}");
        Ok(())
    }
}
