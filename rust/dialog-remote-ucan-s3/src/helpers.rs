//! UCAN test helpers.
//!
//! Provides test infrastructure including delegation helpers,
//! address types, and a local UCAN access server.

use dialog_credentials::Ed25519Signer;
use dialog_ucan_core::ContainerError;
use dialog_ucan_core::Delegation;
use dialog_ucan_core::DelegationBuilder;
use dialog_ucan_core::subject::Subject;
use dialog_varsig::Principal;
use dialog_varsig::eddsa::Ed25519Signature;
use serde::{Deserialize, Serialize};

#[cfg(all(not(target_arch = "wasm32"), feature = "helpers"))]
mod server;
#[cfg(all(not(target_arch = "wasm32"), feature = "helpers"))]
pub use server::*;

/// UCAN+S3 test server connection info.
///
/// Combines a UCAN access service endpoint with the backing S3 server details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UcanS3Address {
    /// URL of the UCAN access service.
    pub access_service_url: String,
    /// URL of the backing S3 server (for test verification).
    pub s3_endpoint: String,
    /// The bucket name.
    pub bucket: String,
    /// AWS access key ID (used by the access service).
    pub access_key_id: String,
    /// AWS secret access key (used by the access service).
    pub secret_access_key: String,
}

/// Generate a new random Ed25519 signer.
pub async fn generate_signer() -> Ed25519Signer {
    Ed25519Signer::generate()
        .await
        .expect("Failed to generate signer")
}

/// Create a delegation from issuer to audience for a subject with the given command.
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
