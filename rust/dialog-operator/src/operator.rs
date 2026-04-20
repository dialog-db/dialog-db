//! Operator — an operating environment built from a Profile.
//!
//! Build one via `Profile::derive()`.

mod access;
mod builder;
mod fork;
mod space;
#[cfg(test)]
mod test;

pub use builder::{OperatorBuilder, OperatorError};

use crate::Authority;
use crate::network::Network;
use dialog_capability::{Capability, Provider};
use dialog_credentials::Credential;
use dialog_effects::authority::{Identify, Operator as AuthOperator};
use dialog_effects::credential::Secret;
use dialog_effects::storage as storage_fx;
use dialog_effects::{archive, credential, memory};
use dialog_storage::provider::storage::Storage;
use dialog_varsig::{Did, Principal};

/// An operating environment built from a [`Profile`](crate::profile::Profile).
///
/// Composes:
/// - Authority credentials (identity)
/// - [`Storage`] for DID-routed effects
/// - Base directory for resolving space names to storage locations
/// - Remote for fork invocations
#[derive(Provider)]
pub struct Operator<S: Clone> {
    #[provide(Identify)]
    /// Provider for authority effects (identity).
    authority: Authority,

    #[provide(
        archive::Get,
        archive::Put,
        credential::Load<Credential>,
        credential::Save<Credential>,
        credential::Load<Secret>,
        credential::Save<Secret>,
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
}

impl<S: Clone> Operator<S> {
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
