//! Test helpers for building delegation chains.
//!
//! These are gated behind the `helpers` feature so other crates' tests can
//! reuse them without re-implementing the wire format or signer setup.

use super::ContainerError;
use crate::DelegationBuilder;
use crate::delegation::Delegation;
use crate::subject::Subject;
use dialog_credentials::Ed25519Signer;
use dialog_varsig::Principal;
use dialog_varsig::eddsa::Ed25519Signature;

/// Generate a new random Ed25519 signer.
///
/// This is useful for creating space signers in tests.
pub async fn generate_signer() -> Ed25519Signer {
    Ed25519Signer::generate()
        .await
        .expect("Failed to generate signer")
}

/// Create a delegation from issuer to audience for a subject with the given command.
///
/// This is a convenience function for building simple delegations in tests.
pub async fn create_delegation(
    issuer: &Ed25519Signer,
    audience: &impl Principal,
    subject: &impl Principal,
    command: &[&str],
) -> Result<Delegation<Ed25519Signature>, ContainerError> {
    DelegationBuilder::new()
        .issuer(issuer.clone())
        .audience(audience)
        .subject(Subject::Specific(subject.did()))
        .command(command.iter().map(|&s| s.to_string()).collect())
        .try_build()
        .await
        .map_err(|e| ContainerError::Invocation(format!("Failed to build delegation: {:?}", e)))
}
