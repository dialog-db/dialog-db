//! Test helpers for building delegation chains.
//!
//! These are gated behind the `helpers` feature so other crates' tests can
//! reuse them without re-implementing the wire format or signer setup.

use super::ContainerError;
use crate::DelegationBuilder;
use crate::delegation::Delegation;
use crate::subject::Subject;
use dialog_credentials::{Ed25519Signer, Signer};
use dialog_varsig::AnySignature;
use dialog_varsig::Principal;

/// Generate a new random signer.
///
/// Returns the algorithm-agnostic [`Signer`] (backed by a fresh ed25519 key),
/// so delegations built with it carry the agnostic signature type.
pub async fn generate_signer() -> Signer {
    Signer::from(
        Ed25519Signer::generate()
            .await
            .expect("Failed to generate signer"),
    )
}

/// Create a delegation from issuer to audience for a subject with the given command.
///
/// This is a convenience function for building simple delegations in tests.
pub async fn create_delegation(
    issuer: &Signer,
    audience: &impl Principal,
    subject: &impl Principal,
    command: &[&str],
) -> Result<Delegation<AnySignature>, ContainerError> {
    DelegationBuilder::new()
        .issuer(issuer.clone())
        .audience(audience)
        .subject(Subject::Specific(subject.did()))
        .command(command.iter().map(|&s| s.to_string()).collect())
        .try_build()
        .await
        .map_err(|e| ContainerError::Invocation(format!("Failed to build delegation: {:?}", e)))
}
