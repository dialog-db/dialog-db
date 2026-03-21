//! UCAN-based authorization for S3-compatible storage.
//!
//! This crate provides UCAN (User Controlled Authorization Networks) support:
//!
//! ## Client-side (making requests)
//!
//! - [`Credentials`] - Credentials that delegate to an external access service
//! - [`DelegationChain`] - Chain of delegations proving authority
//!
//! ## Server-side (handling requests)
//!
//! - [`UcanAuthorizer`] - Wraps credentials to handle UCAN invocations and authorize requests
//! - [`InvocationChain`] - Parsed UCAN container with invocation and delegation chain

mod authorization;
pub mod credentials;
mod provider;
pub mod site;

pub use authorization::UcanInvocation;
pub use credentials::{Credentials, authorize};
pub use provider::UcanAuthorizer;
pub use site::{UcanAddress, UcanCredentials, UcanFormat, UcanSite};

// Re-export container types from dialog-ucan
pub use dialog_ucan::{Container, ContainerError, DelegationChain, InvocationChain};

/// Test helpers for creating UCAN delegations.
/// Only available with the `helpers` feature.
#[cfg(any(test, feature = "helpers"))]
pub mod test_helpers {
    use dialog_credentials::Ed25519Signer;
    use dialog_ucan::ContainerError;
    use dialog_ucan::Delegation;
    use dialog_ucan::DelegationBuilder;
    use dialog_ucan::subject::Subject;
    use dialog_varsig::Principal;
    use dialog_varsig::eddsa::Ed25519Signature;

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
}
