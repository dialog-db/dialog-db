//! A peer: a site identified by a key, holding replicas of repositories.
//!
//! A [`Peer`] is what a [`Session`] inherits and can only narrow: the
//! persisted signer, the storage its spaces are mounted in, the network
//! dispatch, the branch of its own repository that serves as registry and
//! default proof source, and the process-wide runtime shared by every
//! session (the hydration scheduler, the preload queue, the resolved-chain
//! cache). See `notes/peer-and-session.md`.
//!
//! ```no_run
//! # use dialog_capability::Subject;
//! # use dialog_effects::storage::Location;
//! # use dialog_peer::Peer;
//! # use dialog_storage::provider::storage::{Storage, VolatileSpace};
//! # async fn example() -> anyhow::Result<()> {
//! let alice = Peer::new(Storage::<VolatileSpace>::volatile())
//!     .branch("main")
//!     .open(Location::profile("alice"))
//!     .await?;
//!
//! let job = alice
//!     .session(b"refactor")
//!     .allow(Subject::any())
//!     .build()
//!     .await?;
//! # let _ = job;
//! # Ok(())
//! # }
//! ```
//!
//! The peer itself is the unconstrained environment: performing against
//! it acts with the peer's own key and proves from its branch. A session
//! narrows that to a derived (or supplied) key and the grants it was
//! built with.

use std::fmt;
use std::sync::{Arc, OnceLock};

use dialog_capability::access::AuthorizeError;
use dialog_capability::{Command, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::SignerCredential;
use dialog_effects::storage::{Directory, Location};
use dialog_identity::access::Access;
use dialog_identity::{Authority, Profile, SpaceHandle};
use dialog_network::{HydrationScheduler, Network};
use dialog_repository::{ACCESS_BRANCH, Branch, Repository};
use dialog_storage::provider::storage::Storage;
use dialog_storage::resource::Resource;
use dialog_varsig::{Did, Principal};
use parking_lot::Mutex;

use crate::session::access::ChainCache;
use crate::{PeerError, PeerSpace, Session, SessionBuilder};

/// A site identified by a key, holding replicas.
///
/// Cheap to clone: every clone is a handle onto the same storage, runtime
/// and registry. Build one with [`Peer::new`]; derive sessions with
/// [`Peer::session`].
pub struct Peer<S: Clone> {
    inner: Arc<Inner<S>>,
}

impl<S: Clone> Clone for Peer<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S: Clone> fmt::Debug for Peer<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Peer")
            .field("did", &self.did())
            .field("branch", &self.inner.branch)
            .finish_non_exhaustive()
    }
}

struct Inner<S: Clone> {
    /// The persisted signer: the peer's identity.
    credential: SignerCredential,
    /// The pool every space this peer holds is mounted in.
    storage: Storage<S>,
    /// Base directory for resolving space names, until the registry
    /// resolves them by fact.
    directory: Directory,
    /// Network dispatch for fork invocations.
    network: Network,
    /// The branch of the peer's own repository that serves as registry
    /// and default proof source.
    branch: String,
    /// That branch, opened. Proofs resolve from its `dialog.ucan/*` facts
    /// and retained delegations commit into it.
    registry: OnceLock<Branch>,
    /// Resolved-chain cache, shared by every session of this peer: its
    /// keys carry the principal, and its epoch is the registry head.
    chains: Mutex<ChainCache>,
    /// Remote hydrations, joined by digest and admitted per site by
    /// priority across every session performing through this peer.
    hydration: HydrationScheduler,
    /// The ambient speculative-fetch queue `Preload` hints land in.
    speculation: Arc<dialog_artifacts::PreloadQueue>,
}

impl<S: Clone> Peer<S> {
    /// Start building a peer over `storage`.
    ///
    /// The storage decides persistence: a platform default persists, a
    /// [`Storage::volatile`] peer leaves nothing behind. Returns the
    /// builder rather than a peer because opening is async and fallible.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(storage: Storage<S>) -> PeerBuilder<S> {
        PeerBuilder {
            storage,
            network: Network::default(),
            directory: Directory::Current,
            branch: ACCESS_BRANCH.to_string(),
        }
    }

    /// The peer's DID.
    pub fn did(&self) -> Did {
        self.inner.credential.did()
    }

    /// The peer's signing credential.
    pub fn credential(&self) -> &SignerCredential {
        &self.inner.credential
    }

    /// The identity handle over the same credential, for APIs that still
    /// speak in profiles.
    pub fn profile(&self) -> Profile {
        Profile::try_from(dialog_credentials::Credential::Signer(
            self.inner.credential.clone(),
        ))
        .unwrap_or_else(|_| unreachable!("a signer credential is a profile"))
    }

    /// Access handle for claiming and delegating with the peer's key.
    pub fn access(&self) -> Access<'_> {
        Access::new(&self.inner.credential)
    }

    /// A handle to a named space this peer holds.
    ///
    /// The returned handle opens, loads, or creates a repository through
    /// a session of this peer. Until the registry resolves names by fact,
    /// the name resolves against the peer's base directory.
    pub fn space(&self, name: impl Into<String>) -> SpaceHandle {
        SpaceHandle {
            profile_did: self.did(),
            name: name.into(),
        }
    }

    /// The storage every space this peer holds is mounted in.
    pub fn storage(&self) -> &Storage<S> {
        &self.inner.storage
    }

    /// The network dispatch fork invocations go through.
    pub fn network(&self) -> &Network {
        &self.inner.network
    }

    /// The name of the branch that serves as registry and proof source.
    pub fn branch(&self) -> &str {
        &self.inner.branch
    }

    /// The scheduler every remote block read of this peer goes through:
    /// where a site's window is set (`set_window`) and its traffic is read
    /// back (`tally`).
    pub fn hydration(&self) -> &HydrationScheduler {
        &self.inner.hydration
    }

    /// The registry branch, or an error before [`PeerBuilder::open`] wired
    /// it (unreachable through the public API).
    pub fn registry(&self) -> Result<&Branch, AuthorizeError> {
        self.inner
            .registry
            .get()
            .ok_or_else(|| AuthorizeError::Malformed {
                detail: "peer registry branch is not wired".to_string(),
            })
    }

    /// The registry branch when it is wired.
    pub(crate) fn registry_opt(&self) -> Option<&Branch> {
        self.inner.registry.get()
    }

    pub(crate) fn directory(&self) -> &Directory {
        &self.inner.directory
    }

    pub(crate) fn chains(&self) -> &Mutex<ChainCache> {
        &self.inner.chains
    }

    pub(crate) fn speculation(&self) -> &Arc<dialog_artifacts::PreloadQueue> {
        &self.inner.speculation
    }

    /// Start a session derived from this peer's key with `context`.
    ///
    /// Derivation is deterministic per `(peer, context)`, so a grant
    /// issued to a derived DID is reusable across runs. Pass a random
    /// context for a disposable key.
    pub fn session(&self, context: impl Into<Vec<u8>>) -> SessionBuilder<S> {
        SessionBuilder::derive(self.clone(), context.into())
    }
}

impl<S: PeerSpace> Peer<S> {
    /// The unconstrained session: the peer acting with its own key and
    /// proving from its branch, with no grants to mint.
    pub fn as_session(&self) -> Session<S> {
        let signer = self.inner.credential.signer().clone();
        Session::new(
            self.clone(),
            Authority::new("peer", signer.clone(), signer),
            Arc::new(Vec::new()),
        )
    }
}

impl<S: Clone> Principal for Peer<S> {
    fn did(&self) -> Did {
        self.inner.credential.did()
    }
}

/// The peer's own repository: the one whose subject is the peer's key.
impl<S: Clone> From<&Peer<S>> for Repository<SignerCredential> {
    fn from(peer: &Peer<S>) -> Self {
        Repository::from(peer.inner.credential.clone())
    }
}

/// The peer is an environment: every effect its sessions provide, it
/// provides as the unconstrained session.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, C> Provider<C> for Peer<S>
where
    S: PeerSpace,
    C: Command + 'static,
    C::Input: ConditionalSend,
    Session<S>: Provider<C> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: C::Input) -> C::Output {
        Provider::<C>::execute(&self.as_session(), input).await
    }
}

/// Builder for a [`Peer`]. Created by [`Peer::new`].
pub struct PeerBuilder<S: Clone> {
    storage: Storage<S>,
    network: Network,
    directory: Directory,
    branch: String,
}

impl<S: Clone> PeerBuilder<S> {
    /// Set the network dispatch provider. Defaults to [`Network::default`].
    pub fn network(mut self, network: Network) -> Self {
        self.network = network;
        self
    }

    /// Set the base directory space names resolve against. Defaults to
    /// [`Directory::Current`].
    pub fn base(mut self, directory: Directory) -> Self {
        self.directory = directory;
        self
    }

    /// Name the branch of the peer's own repository that serves as
    /// registry and default proof source. Defaults to [`ACCESS_BRANCH`].
    pub fn branch(mut self, name: impl Into<String>) -> Self {
        self.branch = name.into();
        self
    }
}

impl<S> PeerBuilder<S>
where
    S: PeerSpace + Resource<Location>,
    S::Error: fmt::Display,
{
    /// Open the peer at `location`: load its credential, or generate and
    /// persist one if none is there.
    pub async fn open(self, location: Location) -> Result<Peer<S>, PeerError> {
        let profile = Profile::open(location.name.clone())
            .at(location.directory.clone())
            .perform(&self.storage)
            .await
            .map_err(|error| PeerError::Open(error.to_string()))?;
        self.attach(profile.signer().clone()).await
    }

    /// Load the peer at `location`, failing if no credential is there.
    pub async fn load(self, location: Location) -> Result<Peer<S>, PeerError> {
        let profile = Profile::load(location.name.clone())
            .at(location.directory.clone())
            .perform(&self.storage)
            .await
            .map_err(|error| PeerError::Open(error.to_string()))?;
        self.attach(profile.signer().clone()).await
    }
}

impl<S: PeerSpace> PeerBuilder<S> {
    /// Build the peer over a credential whose space is already mounted in
    /// the storage, then open its registry branch through the peer itself.
    ///
    /// For a credential opened some other way; [`open`](Self::open) is the
    /// ordinary path.
    pub async fn attach(self, credential: SignerCredential) -> Result<Peer<S>, PeerError> {
        let peer = Peer {
            inner: Arc::new(Inner {
                credential: credential.clone(),
                storage: self.storage,
                directory: self.directory,
                network: self.network,
                branch: self.branch,
                registry: OnceLock::new(),
                chains: Mutex::default(),
                hydration: HydrationScheduler::default(),
                speculation: Arc::default(),
            }),
        };

        let branch = Repository::from(credential)
            .branch(peer.inner.branch.clone())
            .open()
            .perform(&peer)
            .await
            .map_err(|error| PeerError::Delegation(format!("{error}")))?;
        peer.inner
            .registry
            .set(branch)
            .unwrap_or_else(|_| unreachable!("a freshly built peer has no registry yet"));

        Ok(peer)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::unique_name;
    use anyhow::Result;
    use dialog_capability::Subject;
    use dialog_capability::access::{Access, Proof as _, Prove, Retain};
    use dialog_credentials::Ed25519Signer;
    use dialog_repository::RepositoryExt as _;
    use dialog_storage::provider::storage::VolatileSpace;
    use dialog_ucan::{Parameters, Scope, Ucan, UcanDelegation};
    use dialog_ucan_core::command::Command as UcanCommand;
    use dialog_ucan_core::subject::Subject as UcanSubject;
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

        let first = Peer::new(storage.clone()).open(location.clone()).await?;
        let second = Peer::new(storage).open(location).await?;

        assert_eq!(first.did(), second.did());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_refuses_to_load_a_missing_peer() -> Result<()> {
        let storage = Storage::<VolatileSpace>::volatile();
        let result = Peer::new(storage)
            .load(Location::temp(unique_name("missing")))
            .await;
        assert!(matches!(result, Err(PeerError::Open(_))));
        Ok(())
    }

    /// Sessions of one peer share its replica identity: the peer DID.
    /// Their own keys differ per context and are stable per context.
    #[dialog_common::test]
    async fn it_derives_sessions_deterministically_per_context() -> Result<()> {
        let peer = Peer::new(Storage::<VolatileSpace>::volatile())
            .open(Location::temp(unique_name("sessions")))
            .await?;

        let a = peer.session(b"a").build().await?;
        let again = peer.session(b"a").build().await?;
        let b = peer.session(b"b").build().await?;

        assert_eq!(a.did(), again.did());
        assert_ne!(a.did(), b.did());
        assert_eq!(a.peer().did(), peer.did());
        assert_eq!(b.peer().did(), peer.did());
        Ok(())
    }

    /// A supplied credential is the session key; the peer still grants it.
    #[dialog_common::test]
    async fn it_builds_a_session_over_a_supplied_credential() -> Result<()> {
        let peer = Peer::new(Storage::<VolatileSpace>::volatile())
            .open(Location::temp(unique_name("supplied")))
            .await?;
        let agent = Ed25519Signer::generate().await?;

        let session = peer
            .session(b"ignored")
            .credential(SignerCredential::from(agent.clone()))
            .allow(Subject::any())
            .build()
            .await?;

        assert_eq!(session.did(), agent.did());

        // The powerline grant reaches the peer's own subject in one link.
        let proof = Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(
                session.did(),
                storage_scope(&peer.did()),
            ))
            .perform(&session)
            .await?;
        assert_eq!(proof.proofs().len(), 1);
        Ok(())
    }

    /// The peer is the unconstrained environment: it opens repositories
    /// and proves for itself without a session.
    #[dialog_common::test]
    async fn it_performs_as_the_peer_itself() -> Result<()> {
        let peer = Peer::new(Storage::<VolatileSpace>::volatile())
            .open(Location::temp(unique_name("self")))
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
        assert_eq!(proof.proofs().len(), 1, "space -> peer, no session link");
        Ok(())
    }

    /// The registry branch is the peer's: a session proves from it and
    /// retains into it, and a second session sees what the first retained.
    #[dialog_common::test]
    async fn it_shares_the_registry_across_sessions() -> Result<()> {
        let peer = Peer::new(Storage::<VolatileSpace>::volatile())
            .open(Location::temp(unique_name("shared")))
            .await?;
        let space = Ed25519Signer::generate().await?;

        let first = peer.session(b"first").allow(Subject::any()).build().await?;
        retain(&first, &peer.did(), &space).await;

        let second = peer
            .session(b"second")
            .allow(Subject::any())
            .build()
            .await?;
        let proof = Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(
                second.did(),
                storage_scope(&space.did()),
            ))
            .perform(&second)
            .await?;
        assert_eq!(proof.proofs().len(), 2, "space -> peer ++ peer -> session");
        Ok(())
    }

    /// The registry branch is chosen per peer: a peer on `account/x`
    /// proves from and retains into that branch, and `main` holds nothing.
    #[dialog_common::test]
    async fn it_proves_from_the_named_branch() -> Result<()> {
        let storage = Storage::<VolatileSpace>::volatile();
        let location = Location::temp(unique_name("named-branch"));
        let peer = Peer::new(storage.clone())
            .branch("account/test")
            .open(location.clone())
            .await?;
        let space = Ed25519Signer::generate().await?;
        retain(&peer, &peer.did(), &space).await;

        let proof = Subject::from(peer.did())
            .attenuate(Access)
            .invoke(Prove::<Ucan>::new(peer.did(), storage_scope(&space.did())))
            .perform(&peer)
            .await?;
        assert_eq!(proof.proofs().len(), 1);

        let on_main = Peer::new(storage).open(location).await?;
        assert!(
            on_main.registry()?.revision().is_none(),
            "main holds nothing"
        );
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
}
