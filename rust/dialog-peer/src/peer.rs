//! Peer — the runtime capability environment: a peer's replicas over a
//! storage and a network, acted on with one key.
//!
//! Every handle represents a peer: the owner of the replicas it opens and
//! commits to, with its own state in a branch of the repository its key
//! names. What differs is the key a handle acts with, which its mode
//! records ([`Local`] or [`Session`]):
//!
//! - [`Peer::new`] builds a [`Local`] handle acting with the peer's own
//!   key. It can do anything the peer can, including granting and opening
//!   sessions.
//! - [`Peer::session`], or [`operator`](PeerBuilder::operator) on the
//!   builder, builds a [`Session`]: the same peer, acting with a separate
//!   key within what the peer granted it. It keeps no copy of the peer's
//!   key, so it cannot sign as the peer. Every session commits under its
//!   own origin, so sessions of one peer never collide.
//!
//! Either is the environment every `perform` takes. See
//! `notes/peer-and-session.md`.
//!
//! ```no_run
//! # use dialog_capability::Subject;
//! # use dialog_identity::OpenCredential;
//! # use dialog_peer::Peer;
//! # use dialog_storage::provider::storage::{Storage, VolatileSpace};
//! # async fn example() -> anyhow::Result<()> {
//! let storage = Storage::<VolatileSpace>::volatile();
//! let credential = OpenCredential::open("alice").perform(&storage).await?;
//! let alice = Peer::new(credential).storage(storage).await?;
//!
//! let job = alice.session(b"refactor").allow(Subject::any()).await?;
//! # let _ = job;
//! # Ok(())
//! # }
//! ```

pub(crate) mod access;
mod branch;
mod builder;
mod contact;
mod fork;
mod hydrate;
mod mode;
mod open;
mod preload;
mod runtime;
mod space;
#[cfg(test)]
mod test;

pub use builder::{Allowance, OpenFuture, PeerBuilder, PeerError, PeerKey, Unset};
pub use mode::{Local, Mode, Session};
pub use open::OpenPeer;
pub use runtime::Runtime;

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use dialog_capability::access::AuthorizeError;
use dialog_capability::identity::Entity;
use dialog_capability::{Capability, Fork, Provider};
use dialog_common::{ConditionalSend, ConditionalSync, Held, Holdings, Holds};
use dialog_credentials::{Credential, SignerCredential};
use dialog_effects::authority::{Attest, Identify, Operator as AuthOperator};
use dialog_effects::credential::{CredentialError, Secret};
use dialog_effects::peer::PeerConnection;
use dialog_effects::storage::{Directory, Location};
use dialog_effects::{archive, blob, credential, memory};
use dialog_identity::access::Access;
use dialog_identity::{Authority, CredentialHandle, SpaceHandle};
use dialog_network::{HydrationScheduler, Network};
use dialog_repository::{
    Branch, By, ContactConnection, ContactReference, RemoteSite, ReplicaReference, contact,
};
use dialog_storage::provider::space::SpaceProvider;
use dialog_storage::provider::storage::Storage;
use dialog_storage::resource::Resource;
use dialog_ucan::UcanCertificate;
use dialog_varsig::{Did, Principal};
use parking_lot::Mutex;

use access::ChainCache;

/// The space provider bound a peer's storage must satisfy for the peer
/// to provide every effect, including the remote forks, and to mount its
/// home at a location.
pub trait PeerSpace:
    SpaceProvider
    + Resource<Location, Error: fmt::Display>
    + Provider<blob::Read>
    + Provider<blob::Write>
    + Provider<blob::Import>
    + Clone
    + ConditionalSend
    + ConditionalSync
    + 'static
{
}

impl<T> PeerSpace for T where
    T: SpaceProvider
        + Resource<Location, Error: fmt::Display>
        + Provider<blob::Read>
        + Provider<blob::Write>
        + Provider<blob::Import>
        + Clone
        + ConditionalSend
        + ConditionalSync
        + 'static
{
}

/// A boxed effect dispatch: one remote fork effect's input to its output.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type ReachFuture<Output> = Pin<Box<dyn Future<Output = Output> + Send>>;
/// A boxed effect dispatch (single-threaded wasm form).
#[cfg(target_arch = "wasm32")]
pub(crate) type ReachFuture<Output> = Pin<Box<dyn Future<Output = Output>>>;

/// One remote fork effect the authorization walk may dispatch.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type ReachFn<Fx> =
    Box<dyn Fn(Fx) -> ReachFuture<<Fx as dialog_capability::Command>::Output> + Send + Sync>;
/// One remote fork effect (single-threaded wasm form).
#[cfg(target_arch = "wasm32")]
pub(crate) type ReachFn<Fx> =
    Box<dyn Fn(Fx) -> ReachFuture<<Fx as dialog_capability::Command>::Output>>;

/// The remote reach of the authorization walk: the fork effects a proof's
/// tree and envelope reads may dispatch to replicate content on demand,
/// exactly as any other read does.
///
/// Dyn-erased and installed at build time because naming the remote fork
/// providers as bounds on the `Prove` provider itself would close the
/// trait cycle authorization must not enter (Prove -> Fork -> Authorize
/// -> Prove); at the build site the concrete peer satisfies them without
/// any cycle. The peer clone captured inside these closures carries NO
/// reach of its own, so the proof that authorizes a fetch resolves from
/// what is already local — that is what bounds the recursion.
pub(crate) struct WalkReach {
    /// Remote block read for the walk's tree scans.
    pub(crate) get: ReachFn<Fork<RemoteSite, archive::Get>>,
    /// Remote head resolution for the walk's index store.
    pub(crate) resolve: ReachFn<Fork<RemoteSite, memory::Resolve>>,
    /// Remote envelope read for candidate admission.
    pub(crate) blob_read: ReachFn<Fork<RemoteSite, blob::Read>>,
}

/// A grant this peer holds: a delegation to its key, and who issued it.
#[derive(Clone)]
pub(crate) struct Grant {
    /// The issuer, whose own authority the walk proves before the grant
    /// completes the chain.
    pub(crate) issuer: Did,
    /// The delegation itself.
    pub(crate) certificate: UcanCertificate,
}

/// A peer: an acting key, the storage its spaces are mounted in, the
/// network its forks go through, and the branch of its home repository
/// that holds its own state.
///
/// Cheap to clone: every clone is a handle onto the same storage, runtime,
/// state branch and connections. Build one with [`Peer::new`]; open a
/// session of it with [`Peer::session`], or of a peer known only by its
/// DID with [`Peer::session_of`].
#[derive(Provider, Clone)]
pub struct Peer<S: Clone, M: Mode = Local> {
    #[provide(Identify, Attest)]
    /// Provider for authority effects (identity and attestation).
    authority: Authority,

    #[provide(
        archive::Get,
        archive::Put,
        archive::Import,
        blob::Read,
        blob::Write,
        blob::Import,
        credential::Save<Credential>,
        credential::Load<Secret>,
        credential::Save<Secret>,
        credential::Retract<Secret>,
        memory::Resolve,
        memory::Publish,
        memory::Retract,
        memory::List
    )]
    /// The storage — routes DID-based effects.
    storage: Storage<S>,

    inner: Arc<Inner>,

    /// The authorization walk's remote reach (see [`WalkReach`]).
    /// Deliberately EMPTY on the peer clone captured inside the reach
    /// closures — the proof that authorizes a fetch must resolve from
    /// what is already local, or the recursion would never bottom out.
    reach: Arc<OnceLock<WalkReach>>,

    /// Which key the handle acts with: the peer's own, or a session's.
    mode: PhantomData<M>,
}

pub(crate) struct Inner {
    /// The key this peer acts with.
    credential: SignerCredential,
    /// The repository holding this peer's own state, and the replica
    /// identity every entity it writes derives from. The peer's own DID
    /// for a peer acting as itself; the peer's for its session.
    home: Did,
    /// Base directory for resolving space names, until the registry
    /// resolves them by fact.
    directory: Directory,
    /// Network dispatch for fork invocations.
    network: Network,
    /// The shared runtime: hydration scheduler and speculation queue.
    runtime: Runtime,
    /// The branch of the home repository that holds this peer's state,
    /// or none for an ephemeral peer that keeps its grants in memory only.
    branch: Option<String>,
    /// That branch, opened. Proofs resolve from its `dialog.ucan/*` facts
    /// and retained delegations commit into it.
    state: OnceLock<Branch>,
    /// Resolved-chain cache: its keys carry the principal, and its epoch
    /// is the registry head.
    chains: Mutex<ChainCache>,
    /// The grants this peer holds: delegations to its key, minted in
    /// memory at build, one per allowance. Never persisted — a derived
    /// key re-mints identical authority on demand, and persisting it
    /// would only accumulate.
    grants: Vec<Grant>,
    /// Handles code running on this peer keeps warm between calls (each
    /// repository's branch registry, opened when the repository is),
    /// held type-erased, so the peer needs to know nothing of what it
    /// holds (see [`Holds`]).
    holdings: Holdings,
    /// The peers this one is connected to, so every sync with a peer
    /// shares what its connection has learned. A connection is dropped
    /// when the peer's addresses change.
    connections: Mutex<HashMap<Entity, PeerConnection>>,
}

impl<S: Clone, M: Mode> fmt::Debug for Peer<S, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Peer")
            .field("did", &self.did())
            .field("home", &self.inner.home)
            .field("branch", &self.inner.branch)
            .field("grants", &self.inner.grants.len())
            .finish_non_exhaustive()
    }
}

impl Peer<Unset> {
    /// Start building the peer `credential` is the key of, acting with
    /// that key. Its home is the repository the key names. Give it an
    /// [`operator`](PeerBuilder::operator) or a
    /// [`session`](PeerBuilder::session) to act with a separate key
    /// instead.
    // The builder is how a peer is made: `Peer::new(credential)` names
    // what is being built, and awaiting the builder opens it.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(credential: impl Into<SignerCredential>) -> PeerBuilder<PeerKey> {
        let credential = credential.into();
        PeerBuilder::<Unset, Unset, Local>::new(credential.did())
            .issuer(credential.clone())
            .credential(credential)
    }

    /// Connect to the peer `peer` picks out, by its DID or by the name
    /// this peer knows it by, to reach the replicas it holds.
    ///
    /// Performed against a peer, the connection is found through that
    /// peer's contacts: `Peer::connect(did).repository(subject)
    /// .branch("main").open().perform(&peer)` opens a branch of the
    /// remote peer's replica, whose fetches and publishes reach the remote
    /// peer at whichever of its addresses answers.
    pub fn connect(peer: impl Into<By>) -> ContactConnection {
        contact(peer).connect()
    }

    /// Start building a session of the peer `peer`, when its key is not
    /// at hand: an [`operator`](PeerBuilder::operator) key acting under
    /// grants the peer issued, given as pre-minted certificates.
    pub fn session_of(peer: impl Into<Did>) -> PeerBuilder<Unset, Unset, Session> {
        PeerBuilder::new(peer.into())
    }
}

/// A key is handed only to a handle that holds keys: the peer acting as
/// itself. A session is refused, whichever key it asks for.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<credential::Load<Credential>> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<credential::Load<Credential>> + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Load<Credential>>,
    ) -> Result<Credential, CredentialError> {
        if !M::HOLDS_KEYS {
            return Err(CredentialError::Withheld(format!(
                "the key of {} is not handed to a session",
                input.subject()
            )));
        }
        input.perform(&self.storage).await
    }
}

impl<S: Clone, M: Mode> Peer<S, M> {
    /// The peer's DID: the key it acts with.
    pub fn did(&self) -> Did {
        self.inner.credential.did()
    }

    /// The repository holding this peer's own state, and the replica
    /// identity it writes under.
    pub fn home(&self) -> &Did {
        &self.inner.home
    }

    /// The key this peer acts with.
    pub fn credential(&self) -> &SignerCredential {
        &self.inner.credential
    }

    /// Site secrets stored under the home DID: remote credentials,
    /// handoff material and the like, until sealed facts replace them.
    pub fn secrets(&self) -> CredentialHandle {
        CredentialHandle::new(self.inner.home.clone())
    }

    /// Access handle for claiming and delegating with the peer's key.
    pub fn access(&self) -> Access<'_> {
        Access::new(&self.inner.credential)
    }

    /// A handle to a named space the home repository's peer holds.
    ///
    /// The returned handle opens, loads, or creates a repository through
    /// this peer. Until the registry resolves names by fact, the name
    /// resolves against the peer's base directory.
    pub fn space(&self, name: impl Into<String>) -> SpaceHandle {
        SpaceHandle {
            peer: self.inner.home.clone(),
            name: name.into(),
        }
    }

    /// A repository this peer holds, by the name of its space or by its
    /// DID: the replica of it that belongs to the principal the peer acts
    /// for, its home. A session opens its peer's repositories this way.
    ///
    /// ```no_run
    /// # async fn example(peer: &dialog_peer::Peer<dialog_storage::provider::storage::VolatileSpace>) -> anyhow::Result<()> {
    /// let notes = peer.repository("notes").branch("main").open().perform(peer).await?;
    /// # let _ = notes;
    /// # Ok(())
    /// # }
    /// ```
    pub fn repository(&self, by: impl Into<By>) -> ReplicaReference {
        ReplicaReference::new(self.inner.home.clone(), by)
    }

    /// The network dispatch fork invocations go through.
    pub fn network(&self) -> &Network {
        &self.inner.network
    }

    /// The shared runtime this peer performs through.
    pub fn runtime(&self) -> &Runtime {
        &self.inner.runtime
    }

    /// The name of the state branch, or none for an ephemeral peer.
    pub fn branch(&self) -> Option<&str> {
        self.inner.branch.as_deref()
    }

    /// The scheduler every remote block read of this peer goes through:
    /// where a site's window is set (`set_window`) and its traffic is read
    /// back (`tally`).
    pub fn hydration(&self) -> &HydrationScheduler {
        self.inner.runtime.hydration()
    }

    /// A contact of this peer, picked out by name or by its DID: the
    /// peers it can reach, and where.
    pub fn contact(&self, by: impl Into<By>) -> ContactReference {
        contact(by)
    }

    /// The state branch, or an error for an ephemeral peer.
    pub fn state(&self) -> Result<&Branch, AuthorizeError> {
        self.inner
            .state
            .get()
            .ok_or_else(|| AuthorizeError::Malformed {
                detail: "the peer holds no state branch".to_string(),
            })
    }

    /// The state branch when there is one.
    pub(crate) fn state_opt(&self) -> Option<&Branch> {
        self.inner.state.get()
    }

    /// The state branch this peer serves proofs from and retains into.
    pub(crate) fn delegations(&self) -> Result<&Branch, AuthorizeError> {
        self.state()
    }

    pub(crate) fn directory(&self) -> &Directory {
        &self.inner.directory
    }

    pub(crate) fn connections(&self) -> &Mutex<HashMap<Entity, PeerConnection>> {
        &self.inner.connections
    }

    pub(crate) fn chains(&self) -> &Mutex<ChainCache> {
        &self.inner.chains
    }

    pub(crate) fn speculation(&self) -> &Arc<dialog_artifacts::PreloadQueue> {
        self.inner.runtime.speculation()
    }

    /// The grants this peer holds: the in-memory delegations to its key.
    pub(crate) fn grants(&self) -> &[Grant] {
        &self.inner.grants
    }

    pub(crate) fn authority(&self) -> &Authority {
        &self.authority
    }

    /// Build the authority chain for a given subject DID.
    pub fn build_authority(&self, subject: Did) -> Capability<AuthOperator> {
        self.authority.build_authority(subject)
    }
}

impl<S: Clone> Peer<S, Local> {
    /// The storage every space this peer holds is mounted in, keys and
    /// all: only the peer acting as itself is handed it.
    pub fn storage(&self) -> &Storage<S> {
        &self.storage
    }

    /// Start a session of this peer: the same peer, acting with a key
    /// derived from this one and `context`, sharing this peer's storage,
    /// network, runtime and state branch, within the grants the builder
    /// is given.
    ///
    /// Derivation is deterministic per `(peer, context)`, so a grant
    /// issued to the session's key is reusable across runs; random bytes
    /// make a disposable key. Grants given as bare capabilities are
    /// claimed by this peer.
    pub fn session(&self, context: impl AsRef<[u8]>) -> PeerBuilder<PeerKey, Storage<S>, Session> {
        PeerBuilder::from_peer(
            self,
            PeerKey::Derived {
                from: Box::new(self.inner.credential.clone()),
                context: context.as_ref().to_vec(),
            },
        )
    }
}

impl<S: PeerSpace, M: Mode> Peer<S, M> {
    /// Assemble a peer with its authority and grants, and install the
    /// walk's remote reach.
    ///
    /// The authorization walk's tree and envelope reads replicate content
    /// on demand through fork effects, like any other read. The captured
    /// peer clone carries NO reach of its own — the proof that authorizes
    /// such a fetch resolves from what is already local, which bounds the
    /// recursion a fork-inside-a-proof would otherwise open.
    pub(crate) fn assemble(storage: Storage<S>, inner: Inner) -> Self {
        let authority = Authority::new(
            "peer",
            inner.home.clone(),
            inner.credential.signer().clone(),
        );
        let peer = Peer {
            authority,
            storage,
            inner: Arc::new(inner),
            reach: Arc::new(OnceLock::new()),
            mode: PhantomData,
        };

        let anchor = Peer {
            reach: Arc::new(OnceLock::new()),
            ..peer.clone()
        };
        let reach = WalkReach {
            get: {
                let anchor = anchor.clone();
                Box::new(move |input| {
                    let anchor = anchor.clone();
                    Box::pin(async move {
                        Provider::<Fork<RemoteSite, archive::Get>>::execute(&anchor, input).await
                    })
                })
            },
            resolve: {
                let anchor = anchor.clone();
                Box::new(move |input| {
                    let anchor = anchor.clone();
                    Box::pin(async move {
                        Provider::<Fork<RemoteSite, memory::Resolve>>::execute(&anchor, input).await
                    })
                })
            },
            blob_read: {
                let anchor = anchor.clone();
                Box::new(move |input| {
                    let anchor = anchor.clone();
                    Box::pin(async move {
                        Provider::<Fork<RemoteSite, blob::Read>>::execute(&anchor, input).await
                    })
                })
            },
        };
        peer.reach
            .set(reach)
            .unwrap_or_else(|_| unreachable!("a freshly assembled peer has no reach yet"));

        peer
    }

    /// Wire the opened state branch. Called once by the builder.
    pub(crate) fn attach_state(&self, branch: Branch) {
        self.inner
            .state
            .set(branch)
            .unwrap_or_else(|_| unreachable!("a freshly built peer has no state branch yet"));
    }
}

impl<S: Clone, M: Mode> Holds for Peer<S, M> {
    fn held(&self, key: &str) -> Option<Held> {
        self.inner.holdings.held(key)
    }

    fn hold(&self, key: String, handle: Held) {
        self.inner.holdings.hold(key, handle)
    }
}

impl<S: Clone, M: Mode> Principal for Peer<S, M> {
    fn did(&self) -> Did {
        self.inner.credential.did()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::{open_peer, test_session_with_peer, unique_name};
    use crate::{ClaimExt as _, OpenCredential};
    use anyhow::Result;
    use dialog_capability::Subject;
    use dialog_capability::access::{Access, Proof as _, Prove, Retain};
    use dialog_capability::did;
    use dialog_credentials::Ed25519Signer;
    use dialog_credentials::{Credential, SignerCredential};
    use dialog_effects::credential::{self as credential_fx, prelude::*};
    use dialog_effects::storage::Location;
    use dialog_repository::{OpenReplicaBranchError, RepositoryAtExt as _, RepositoryExt as _};
    use dialog_storage::provider::storage::VolatileSpace;
    use dialog_ucan::{Parameters, Scope, Ucan, UcanCertificate, UcanDelegation};
    use dialog_ucan_core::command::Command as UcanCommand;
    use dialog_ucan_core::subject::Subject as UcanSubject;
    use dialog_ucan_core::time::Timestamp;
    use dialog_ucan_core::time::timestamp::{Duration, SystemTime};
    use dialog_ucan_core::{DelegationBuilder, DelegationChain};

    fn storage_scope(subject: &Did) -> Scope {
        Scope {
            subject: UcanSubject::Specific(subject.clone()),
            command: UcanCommand(vec!["storage".to_string()]),
            parameters: Parameters::default(),
        }
    }

    async fn retain(env: &impl Provider<Retain<Ucan>>, holder: &Did, space: &Ed25519Signer) {
        let delegation = DelegationBuilder::new()
            .issuer(dialog_credentials::Signer::from(space.clone()))
            .audience(holder)
            .subject(UcanSubject::Specific(space.did()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();
        Subject::from(holder.clone())
            .attenuate(Access)
            .invoke(Retain::<Ucan>::new(UcanDelegation::new(
                DelegationChain::new(delegation),
            )))
            .perform(env)
            .await
            .unwrap();
    }

    #[dialog_common::test]
    async fn it_opens_the_same_peer_twice() -> Result<()> {
        let storage = Storage::<VolatileSpace>::volatile();
        let location = Location::temp(unique_name("peer"));

        let first = open_peer(storage.clone(), location.clone()).await?;
        let second = open_peer(storage, location).await?;

        assert_eq!(first.did(), second.did());
        assert_eq!(first.home(), second.home());
        Ok(())
    }

    /// The credential is opened apart from the peer: create it once, load
    /// it after, and open the peer over whichever was found.
    #[dialog_common::test]
    async fn it_opens_over_a_separately_loaded_credential() -> Result<()> {
        let storage = Storage::<VolatileSpace>::volatile();
        let location = Location::temp(unique_name("created"));

        let created = OpenCredential::create(location.name.clone())
            .at(location.directory.clone())
            .perform(&storage)
            .await?;
        let loaded = OpenCredential::load(location.name.clone())
            .at(location.directory.clone())
            .perform(&storage)
            .await?;
        assert_eq!(created.did(), loaded.did());

        let peer = Peer::new(loaded)
            .storage(storage.clone())
            .at(location.clone())
            .await?;
        assert_eq!(peer.did(), created.did());
        assert_eq!(*peer.home(), created.did());
        Ok(())
    }

    /// The home named by the builder must be the space at `at`.
    #[dialog_common::test]
    async fn it_refuses_a_home_that_is_not_at_the_location() -> Result<()> {
        let storage = Storage::<VolatileSpace>::volatile();
        let location = Location::temp(unique_name("elsewhere"));
        let credential = OpenCredential::open(location.name.clone())
            .at(location.directory.clone())
            .perform(&storage)
            .await?;
        let other = Ed25519Signer::generate().await?;

        let result = Peer::session_of(other.did())
            .operator(credential)
            .storage(storage)
            .at(location)
            .await;
        assert!(matches!(result, Err(PeerError::Home(_))));
        Ok(())
    }

    /// Workers of one peer share its replica identity: the home DID.
    /// Their own keys differ per context and are stable per context.
    #[dialog_common::test]
    async fn it_derives_workers_deterministically_per_context() -> Result<()> {
        let peer = open_peer(
            Storage::<VolatileSpace>::volatile(),
            Location::temp(unique_name("workers")),
        )
        .await?;

        let a = peer.session(b"a").await?;
        let again = peer.session(b"a").await?;
        let b = peer.session(b"b").await?;

        assert_eq!(a.did(), again.did());
        assert_ne!(a.did(), b.did());
        assert_eq!(a.home(), peer.home());
        assert_eq!(b.home(), peer.home());
        assert_ne!(a.did(), peer.did());
        Ok(())
    }

    /// A worker is built without a handle on its parent: the parent's
    /// credential derives the key and claims the grant, and the builder
    /// takes the rest.
    #[dialog_common::test]
    async fn it_builds_a_worker_from_the_credential_alone() -> Result<()> {
        let storage = Storage::<VolatileSpace>::volatile();
        let location = Location::temp(unique_name("standalone"));
        let credential = OpenCredential::open(location.name.clone())
            .at(location.directory.clone())
            .perform(&storage)
            .await?;

        let worker = Peer::session_of(credential.did())
            .operator(credential.derive(b"worker").await?)
            .storage(storage)
            .allow(Subject::any().claim(&credential))
            .await?;

        assert_eq!(*worker.home(), credential.did());
        assert_ne!(worker.did(), credential.did());

        // The powerline grant reaches the home subject in one link.
        let proof = Subject::from(worker.home().clone())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(
                worker.did(),
                storage_scope(worker.home()),
            ))
            .perform(&worker)
            .await?;
        assert_eq!(proof.proofs().len(), 1);
        Ok(())
    }

    /// A supplied signer is the worker's key; the parent still grants it.
    #[dialog_common::test]
    async fn it_builds_a_worker_over_a_supplied_signer() -> Result<()> {
        let peer = open_peer(
            Storage::<VolatileSpace>::volatile(),
            Location::temp(unique_name("supplied")),
        )
        .await?;
        let agent = Ed25519Signer::generate().await?;

        let worker = Peer::session_of(peer.home().clone())
            .operator(agent.clone())
            .storage(peer.storage().clone())
            .allow(Subject::any().claim(peer.credential()))
            .await?;

        assert_eq!(worker.did(), agent.did());

        let proof = Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(worker.did(), storage_scope(&peer.did())))
            .perform(&worker)
            .await?;
        assert_eq!(proof.proofs().len(), 1);
        Ok(())
    }

    /// A grant without an expiration is refused unless allowed as such;
    /// a bare capability with no issuer to claim it is refused too.
    #[dialog_common::test]
    async fn it_refuses_an_unbounded_grant_and_an_unclaimed_one() -> Result<()> {
        let peer = open_peer(
            Storage::<VolatileSpace>::volatile(),
            Location::temp(unique_name("bounds")),
        )
        .await?;

        let unbounded = peer
            .session(b"unbounded")
            .grant(Subject::any().claim(peer.credential()))
            .await;
        assert!(matches!(unbounded, Err(PeerError::Unbounded(_))));

        let expiration = Timestamp::new(SystemTime::now() + Duration::from_secs(3600))?;
        let bounded = peer
            .session(b"bounded")
            .grant(Subject::any().claim(peer.credential()).expires(expiration))
            .await;
        assert!(bounded.is_ok());

        let unclaimed = Peer::session_of(peer.home().clone())
            .operator(peer.credential().derive(b"unclaimed").await?)
            .storage(peer.storage().clone())
            .allow(Subject::any())
            .await;
        assert!(matches!(unclaimed, Err(PeerError::Issuer(_))));
        Ok(())
    }

    /// The root peer is the unconstrained environment: it opens
    /// repositories and proves for itself with no grants.
    #[dialog_common::test]
    async fn it_performs_as_the_root_peer() -> Result<()> {
        let peer = open_peer(
            Storage::<VolatileSpace>::volatile(),
            Location::temp(unique_name("self")),
        )
        .await?;

        let repo = peer
            .space(unique_name("repo"))
            .open()
            .perform(&peer)
            .await?;
        assert!(!repo.did().to_string().is_empty());

        let space = Ed25519Signer::generate().await?;
        retain(&peer, &peer.did(), &space).await;

        let proof = Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(peer.did(), storage_scope(&space.did())))
            .perform(&peer)
            .await?;
        assert_eq!(proof.proofs().len(), 1, "space -> peer, no worker link");
        Ok(())
    }

    /// The state branch is the home's: a worker proves from it and
    /// retains into it, and a second worker sees what the first retained.
    #[dialog_common::test]
    async fn it_shares_the_state_branch_across_workers() -> Result<()> {
        let peer = open_peer(
            Storage::<VolatileSpace>::volatile(),
            Location::temp(unique_name("shared")),
        )
        .await?;
        let space = Ed25519Signer::generate().await?;

        let first = peer.session(b"first").allow(Subject::any()).await?;
        retain(&first, &peer.did(), &space).await;

        let second = peer.session(b"second").allow(Subject::any()).await?;
        let proof = Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(
                second.did(),
                storage_scope(&space.did()),
            ))
            .perform(&second)
            .await?;
        assert_eq!(proof.proofs().len(), 2, "space -> peer ++ peer -> worker");
        Ok(())
    }

    /// An ephemeral worker proves from its grants and self-issued
    /// authority alone: it retains nothing and reads no branch.
    #[dialog_common::test]
    async fn it_keeps_an_ephemeral_worker_in_memory() -> Result<()> {
        let peer = open_peer(
            Storage::<VolatileSpace>::volatile(),
            Location::temp(unique_name("ephemeral")),
        )
        .await?;
        let space = Ed25519Signer::generate().await?;
        retain(&peer, &peer.did(), &space).await;

        let worker = peer
            .session(b"ephemeral")
            .ephemeral()
            .allow(Subject::any())
            .await?;
        assert!(worker.state().is_err());

        // Its own subject through the grant: no branch needed.
        let own = Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(worker.did(), storage_scope(&peer.did())))
            .perform(&worker)
            .await?;
        assert_eq!(own.proofs().len(), 1);

        // What the peer retained is in a branch the worker does not read.
        let retained = Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(
                worker.did(),
                storage_scope(&space.did()),
            ))
            .perform(&worker)
            .await;
        assert!(retained.is_err());
        Ok(())
    }

    /// The state branch is chosen per peer: a peer on `account/x` proves
    /// from and retains into that branch, and `main` holds nothing.
    #[dialog_common::test]
    async fn it_proves_from_the_named_branch() -> Result<()> {
        let storage = Storage::<VolatileSpace>::volatile();
        let location = Location::temp(unique_name("named-branch"));
        let credential = OpenCredential::open(location.name.clone())
            .at(location.directory.clone())
            .perform(&storage)
            .await?;
        let peer = Peer::new(credential.clone())
            .storage(storage.clone())
            .branch("account/test")
            .await?;
        let space = Ed25519Signer::generate().await?;
        retain(&peer, &peer.did(), &space).await;

        let proof = Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(peer.did(), storage_scope(&space.did())))
            .perform(&peer)
            .await?;
        assert_eq!(proof.proofs().len(), 1);

        let on_main = Peer::new(credential).storage(storage).await?;
        assert!(on_main.state()?.revision().is_none(), "main holds nothing");
        let refused = Subject::from(on_main.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(
                on_main.did(),
                storage_scope(&space.did()),
            ))
            .perform(&on_main)
            .await;
        assert!(refused.is_err());
        Ok(())
    }

    /// A worker opens its parent's repository by the name of the
    /// parent's space for it, or by the repository's DID, and gets the
    /// parent's replica of the branch.
    #[dialog_common::test]
    async fn it_opens_a_parents_branch_by_repository_name_or_did() -> Result<()> {
        let (worker, peer) = test_session_with_peer().await;
        let name = unique_name("notes");
        let created = peer.space(name.clone()).create().perform(&worker).await?;

        let by_name = peer
            .repository(name.as_str())
            .branch("home")
            .open()
            .perform(&worker)
            .await?;
        assert_eq!(by_name.subject().did(), &created.did());
        assert_eq!(by_name.name(), "home");

        let by_did = peer
            .repository(created.did())
            .branch("home")
            .open()
            .perform(&worker)
            .await?;
        assert_eq!(by_did.subject().did(), &created.did());
        Ok(())
    }

    /// Opening a branch of a repository that has no space by that name is
    /// refused rather than creating one.
    #[dialog_common::test]
    async fn it_does_not_create_a_repository_to_open_a_branch_in() -> Result<()> {
        let (worker, peer) = test_session_with_peer().await;
        let opened = peer
            .repository(unique_name("missing").as_str())
            .branch("home")
            .open()
            .perform(&worker)
            .await;
        assert!(
            matches!(opened, Err(OpenReplicaBranchError::Load(_))),
            "{opened:?}"
        );
        Ok(())
    }

    /// Naming another peer's replica is refused rather than opened under
    /// the replica of the peer the worker acts for.
    #[dialog_common::test]
    async fn it_refuses_to_open_another_peers_replica() -> Result<()> {
        let (worker, _) = test_session_with_peer().await;
        let opened = did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")
            .repository("notes")
            .branch("home")
            .open()
            .perform(&worker)
            .await;
        assert!(
            matches!(opened, Err(OpenReplicaBranchError::Foreign { .. })),
            "{opened:?}"
        );
        Ok(())
    }

    /// A peer built from its credential acts with that key; a session of
    /// it acts with a key derived for its context, while representing the
    /// same peer. Deriving the session from the builder or from an open
    /// peer gives the same key.
    #[dialog_common::test]
    async fn it_builds_a_peer_and_its_sessions() -> Result<()> {
        let storage = Storage::<VolatileSpace>::volatile();
        let credential = OpenCredential::open(unique_name("alice"))
            .perform(&storage)
            .await?;

        let alice = Peer::new(credential.clone())
            .storage(storage.clone())
            .await?;
        assert_eq!(alice.did(), credential.did());
        assert_eq!(*alice.home(), credential.did());

        let session: Peer<VolatileSpace, Session> =
            alice.session(b"worker").allow(Subject::any()).await?;
        assert_ne!(session.did(), credential.did());
        assert_eq!(*session.home(), credential.did());

        let built: Peer<VolatileSpace, Session> = Peer::new(credential.clone())
            .session(b"worker")
            .storage(storage)
            .allow(Subject::any())
            .await?;
        assert_eq!(built.did(), session.did());
        assert_eq!(*built.home(), credential.did());
        Ok(())
    }

    /// A certificate over every subject, from `issuer` to `audience`.
    async fn certificate(
        issuer: &SignerCredential,
        audience: &Did,
        expiration: Timestamp,
    ) -> UcanCertificate {
        UcanCertificate(
            DelegationBuilder::new()
                .issuer(issuer.signer().clone())
                .audience(audience)
                .subject(UcanSubject::Any)
                .command(vec![])
                .expiration(expiration)
                .try_build()
                .await
                .unwrap(),
        )
    }

    fn in_an_hour() -> Timestamp {
        Timestamp::new(SystemTime::now() + Duration::from_secs(3600)).unwrap()
    }

    /// A session acts under certificates issued to its own key. One
    /// issued to another key is not its authority, and is refused when
    /// the session is built rather than carried as though it were.
    #[dialog_common::test]
    async fn it_refuses_a_certificate_issued_to_another_key() -> Result<()> {
        let peer = open_peer(
            Storage::<VolatileSpace>::volatile(),
            Location::temp(unique_name("audience")),
        )
        .await?;
        let agent = Ed25519Signer::generate().await?;
        let other = Ed25519Signer::generate().await?;

        let granted = certificate(peer.credential(), &other.did(), in_an_hour()).await;
        let session = Peer::session_of(peer.home().clone())
            .operator(agent)
            .storage(peer.storage().clone())
            .grant(granted)
            .await;
        assert!(session.is_err(), "the certificate is another key's");
        Ok(())
    }

    /// A certificate that has already expired grants nothing, and is
    /// refused when the session is built.
    #[dialog_common::test]
    async fn it_refuses_an_expired_certificate() -> Result<()> {
        let peer = open_peer(
            Storage::<VolatileSpace>::volatile(),
            Location::temp(unique_name("expired")),
        )
        .await?;
        let agent = Ed25519Signer::generate().await?;

        let expired = Timestamp::new(SystemTime::now() - Duration::from_secs(60))?;
        let granted = certificate(peer.credential(), &agent.did(), expired).await;
        let session = Peer::session_of(peer.home().clone())
            .operator(agent)
            .storage(peer.storage().clone())
            .grant(granted)
            .await;
        assert!(session.is_err(), "the certificate expired");
        Ok(())
    }

    /// A session holding a grant scoped to one subject and a grant over
    /// every subject proves a claim on a third subject by the second: the
    /// first does not cover the claim, whatever its command.
    #[dialog_common::test]
    async fn it_proves_by_the_grant_that_covers_the_subject() -> Result<()> {
        let peer = open_peer(
            Storage::<VolatileSpace>::volatile(),
            Location::temp(unique_name("grants")),
        )
        .await?;
        let elsewhere = Ed25519Signer::generate().await?;

        let session = peer
            .session(b"grants")
            .allow(Subject::from(elsewhere.did()).claim(peer.credential()))
            .allow(Subject::any().claim(peer.credential()))
            .await?;

        let proof = Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(
                session.did(),
                storage_scope(&peer.did()),
            ))
            .perform(&session)
            .await?;
        let last = proof.proofs().last().expect("the session's grant");
        assert_eq!(
            *last.0.subject(),
            UcanSubject::Any,
            "the grant scoped to another subject was used"
        );
        Ok(())
    }

    /// A session acts with its own key. The peer's key is not handed to
    /// it, and neither is the key of a repository it loads: it reads and
    /// writes the repository under its grants, never as the repository.
    #[dialog_common::test]
    async fn it_withholds_keys_from_a_session() -> Result<()> {
        let peer = open_peer(
            Storage::<VolatileSpace>::volatile(),
            Location::temp(unique_name("keys")),
        )
        .await?;
        let session = peer
            .session(b"keys")
            .allow(Subject::any().claim(peer.credential()))
            .await?;

        let key = Subject::from(peer.did())
            .credential()
            .key(credential_fx::SELF)
            .load()
            .perform(&session)
            .await;
        assert!(key.is_err(), "the session was handed the peer's key");

        let name = unique_name("repo");
        peer.space(name.clone()).open().perform(&peer).await?;
        let repository = peer.space(name).load().perform(&session).await?;
        assert!(
            matches!(repository.credential(), Credential::Verifier(_)),
            "the session was handed the repository's key"
        );
        Ok(())
    }
}
