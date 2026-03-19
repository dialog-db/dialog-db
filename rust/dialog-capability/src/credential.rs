//! Credential capability hierarchy.
//!
//! Provides identity and signing operations scoped to a repository subject.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//!   +-- Credential (ability: /credential)
//!         +-- Identify -> Effect -> Result<Did, CredentialError>
//!         +-- Sign { payload } -> Effect -> Result<Vec<u8>, CredentialError>
//! ```

pub use crate::{Attenuation, Capability, Did, Effect, Policy, Subject};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Root attenuation for credential operations.
///
/// Attaches to Subject and provides the `/credential` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Credential;

impl Attenuation for Credential {
    type Of = Subject;
}

/// Identify operation — returns the operator's DID.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Identify;

impl Effect for Identify {
    type Of = Credential;
    type Output = Result<Did, CredentialError>;
}

/// Sign operation — signs a payload using the operator's key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sign {
    /// The payload to sign.
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl Sign {
    /// Create a new Sign effect.
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            payload: payload.into(),
        }
    }
}

impl Effect for Sign {
    type Of = Credential;
    type Output = Result<Vec<u8>, CredentialError>;
}

/// Extension trait for `Capability<Sign>` to access its fields.
pub trait SignCapability {
    /// Get the payload to sign.
    fn payload(&self) -> &[u8];
}

impl SignCapability for Capability<Sign> {
    fn payload(&self) -> &[u8] {
        &Sign::of(self).payload
    }
}

/// Errors that can occur during credential operations.
#[derive(Debug, Error)]
pub enum CredentialError {
    /// The signing operation failed.
    #[error("Signing failed: {0}")]
    SigningFailed(String),

    /// No credentials available.
    #[error("No credentials available: {0}")]
    NotFound(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::did;

    #[test]
    fn it_builds_credential_claim_path() {
        let claim = Subject::from(did!("key:zSpace")).attenuate(Credential);

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/credential");
    }

    #[test]
    fn it_builds_identify_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .invoke(Identify);

        assert_eq!(claim.ability(), "/credential/identify");
    }

    #[test]
    fn it_builds_sign_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .invoke(Sign::new(b"hello"));

        assert_eq!(claim.ability(), "/credential/sign");
    }

    #[test]
    fn it_extracts_payload_from_sign() {
        let cap = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .invoke(Sign::new(b"payload"));

        assert_eq!(cap.payload(), b"payload");
    }
}
