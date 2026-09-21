//! Session — a constrained [`Peer`]: the environment every `perform` takes.
//!
//! Build one via [`Peer::session`], or perform against the [`Peer`] itself
//! for the unconstrained case.

pub(crate) mod access;
mod builder;
mod fork;
mod hydrate;
mod preload;
mod space;
#[cfg(test)]
mod test;

pub use builder::{PeerError, SessionBuilder};

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use dialog_repository::Branch;
use dialog_ucan::UcanCertificate;

use dialog_capability::access::AuthorizeError;
use dialog_capability::{Capability, Fork, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Credential;
use dialog_effects::authority::{Attest, Identify, Operator as AuthOperator};
use dialog_effects::credential::Secret;
use dialog_effects::{archive, blob, credential, memory};
use dialog_identity::Authority;
use dialog_repository::RemoteSite;
use dialog_storage::provider::space::SpaceProvider;
use dialog_storage::provider::storage::Storage;
use dialog_varsig::{Did, Principal};

use crate::Peer;

/// The space provider bound a peer's storage must satisfy for the peer
/// and its sessions to provide every effect, including the remote forks.
pub trait PeerSpace:
    SpaceProvider
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
/// -> Prove); at the build site the concrete session satisfies them
/// without any cycle. The session clone captured inside these closures
/// carries NO reach of its own, so the proof that authorizes a fetch
/// resolves from what is already local — that is what bounds the
/// recursion.
pub(crate) struct WalkReach {
    /// Remote block read for the walk's tree scans.
    pub(crate) get: ReachFn<dialog_capability::Fork<dialog_repository::RemoteSite, archive::Get>>,
    /// Remote head resolution for the walk's index store.
    pub(crate) resolve:
        ReachFn<dialog_capability::Fork<dialog_repository::RemoteSite, memory::Resolve>>,
    /// Remote envelope read for candidate admission.
    pub(crate) blob_read:
        ReachFn<dialog_capability::Fork<dialog_repository::RemoteSite, blob::Read>>,
}

/// A session: a [`Peer`] narrowed to one acting key and the grants it
/// was built with.
///
/// Composes:
/// - The peer: storage, network, registry branch and shared runtime.
/// - Authority credentials (the peer key and the session key).
/// - The session grants: peer-to-session delegations minted in memory
///   at build, one per allowed scope.
#[derive(Provider, Clone)]
pub struct Session<S: Clone> {
    /// The peer this session narrows.
    peer: Peer<S>,

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
        credential::Load<Credential>,
        credential::Save<Credential>,
        credential::Load<Secret>,
        credential::Save<Secret>,
        credential::Retract<Secret>,
        memory::Resolve,
        memory::Publish,
        memory::Retract
    )]
    /// The peer's storage — routes DID-based effects. A handle onto the
    /// same pool the peer holds, kept here so the derive can dispatch to
    /// it directly.
    storage: Storage<S>,

    /// The session grants: peer-to-session delegations minted in memory
    /// at build time, one per allowed scope. Never persisted — a derived
    /// session key re-mints identical authority on demand, and
    /// persisting it would only accumulate.
    grants: Arc<Vec<UcanCertificate>>,

    /// The authorization walk's remote reach (see [`WalkReach`]).
    /// Deliberately EMPTY on the session clone captured inside the reach
    /// closures — the proof that authorizes a fetch must resolve from
    /// what is already local, or the recursion would never bottom out.
    reach: Arc<OnceLock<WalkReach>>,
}

impl<S: Clone> Session<S> {
    /// The peer this session narrows.
    pub fn peer(&self) -> &Peer<S> {
        &self.peer
    }

    /// The peer's registry branch this session serves proofs from.
    pub(crate) fn delegations(&self) -> Result<&Branch, AuthorizeError> {
        self.peer.registry()
    }

    /// The session grants: the in-memory peer-to-session links.
    pub(crate) fn grants(&self) -> &[UcanCertificate] {
        &self.grants
    }

    /// The scheduler every remote block read of this session goes
    /// through: where a site's window is set (`set_window`) and its
    /// traffic is read back (`tally`).
    pub fn hydration(&self) -> &dialog_network::HydrationScheduler {
        self.peer.hydration()
    }

    /// The session's DID (the derived or supplied acting key).
    pub fn did(&self) -> Did {
        self.authority.operator_did()
    }

    /// Build the authority chain for a given subject DID.
    pub fn build_authority(&self, subject: Did) -> Capability<AuthOperator> {
        self.authority.build_authority(subject)
    }

    pub(crate) fn authority(&self) -> &Authority {
        &self.authority
    }
}

impl<S: PeerSpace> Session<S> {
    /// Assemble a session over `peer` with its authority and grants, and
    /// install the walk's remote reach.
    ///
    /// The authorization walk's tree and envelope reads replicate content
    /// on demand through fork effects, like any other read. The captured
    /// session clone carries NO reach of its own — the proof that
    /// authorizes such a fetch resolves from what is already local, which
    /// bounds the recursion a fork-inside-a-proof would otherwise open.
    pub(crate) fn new(
        peer: Peer<S>,
        authority: Authority,
        grants: Arc<Vec<UcanCertificate>>,
    ) -> Self {
        let session = Session {
            storage: peer.storage().clone(),
            peer,
            authority,
            grants,
            reach: Arc::new(OnceLock::new()),
        };

        let anchor = Session {
            reach: Arc::new(OnceLock::new()),
            ..session.clone()
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
        session
            .reach
            .set(reach)
            .unwrap_or_else(|_| unreachable!("a freshly built session has no reach yet"));

        session
    }
}

impl<S: Clone> Principal for Session<S> {
    fn did(&self) -> Did {
        self.authority.operator_did()
    }
}
