//! Operator — an operating environment built from a Profile.
//!
//! Build one via `Profile::derive()`.

mod access;
mod builder;
mod fork;
mod space;
#[cfg(test)]
mod test;

pub use builder::{DeriveOperator, OperatorBuilder, OperatorError};

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use dialog_repository::Branch;
use dialog_ucan::UcanCertificate;
use parking_lot::Mutex;

use dialog_capability::access::AuthorizeError;
use dialog_capability::{Capability, Provider};
use dialog_credentials::Credential;
use dialog_effects::authority::{Attest, Identify, Operator as AuthOperator};
use dialog_effects::credential::Secret;
use dialog_effects::storage as storage_fx;
use dialog_effects::{archive, blob, credential, memory};
use dialog_identity::Authority;
use dialog_network::Network;
use dialog_storage::provider::storage::Storage;
use dialog_varsig::{Did, Principal};

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
/// -> Prove); at the build site the concrete operator satisfies them
/// without any cycle. The operator clone captured inside these closures
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

/// An operating environment built from a [`Profile`](crate::profile::Profile).
///
/// Composes:
/// - Authority credentials (identity)
/// - [`Storage`] for DID-routed effects
/// - Base directory for resolving space names to storage locations
/// - Remote for fork invocations
#[derive(Provider, Clone)]
pub struct Operator<S: Clone> {
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
    /// Storage — routes DID-based effects.
    storage: Storage<S>,

    /// Base directory for resolving space names.
    directory: storage_fx::Directory,

    /// Network dispatch for fork invocations.
    network: Network,

    /// The session grants: profile-to-operator delegations minted in
    /// memory at build time, one per allowed scope. Never persisted — the
    /// operator key derives from the profile key, so any device holding
    /// the profile re-mints identical authority on demand.
    session: Arc<Vec<UcanCertificate>>,

    /// The profile repository's access branch, opened at build time.
    /// Proofs resolve from its `dialog.ucan/*` facts and retained
    /// delegations commit into it.
    delegations: Arc<OnceLock<Branch>>,

    /// Resolved-chain cache (see `operator/access.rs`).
    chains: Arc<Mutex<access::ChainCache>>,

    /// The authorization walk's remote reach (see [`WalkReach`]).
    /// Deliberately EMPTY on the operator clone captured inside the reach
    /// closures — the proof that authorizes a fetch must resolve from
    /// what is already local, or the recursion would never bottom out.
    reach: Arc<OnceLock<WalkReach>>,
}

impl<S: Clone> Operator<S> {
    /// The profile repository's access branch this operator serves proofs
    /// from, or an error before build wires it (unreachable through the
    /// public API).
    pub(crate) fn delegations(&self) -> Result<&Branch, AuthorizeError> {
        self.delegations
            .get()
            .ok_or_else(|| AuthorizeError::Malformed {
                detail: "operator access branch is not wired".to_string(),
            })
    }

    /// The operator's DID (the ephemeral/derived session key).
    pub fn did(&self) -> Did {
        self.authority.operator_did()
    }

    /// The profile's DID (the long-lived identity).
    pub fn profile_did(&self) -> Did {
        self.authority.profile_did()
    }

    /// Build the authority chain for a given subject DID.
    pub fn build_authority(&self, subject: Did) -> Capability<AuthOperator> {
        self.authority.build_authority(subject)
    }
}

impl<S: Clone> Principal for Operator<S> {
    fn did(&self) -> Did {
        self.authority.operator_did()
    }
}
