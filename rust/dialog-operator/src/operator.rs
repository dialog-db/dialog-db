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
