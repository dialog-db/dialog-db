//! Credential capability hierarchy and remote resource authorization.
//!
//! Provides identity and signing operations scoped to a repository subject,
//! plus the [`Remote`] trait for credential types that require
//! multi-step authorization (authorize → redeem → perform).
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
use crate::{Authorization, Constraint, Invocation};
use dialog_common::ConditionalSend;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// Re-export Site types for consumers that use credential types
pub use crate::site::{Local, RemoteSite};

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

use crate::site::Site;

/// Request to authorize a capability against a site `S`.
///
/// This is the first step of the authorization pipeline. The environment
/// receives the site configuration and a capability, and produces an
/// `Authorization<Fx, S::Permit>`.
pub struct Authorize<Fx: Constraint, S: Site> {
    /// The site configuration.
    pub site: S,
    /// The capability to authorize.
    pub capability: Capability<Fx>,
}

impl<Fx: Constraint, S: Site> Invocation for Authorize<Fx, S> {
    type Input = Self;
    type Output = Result<Authorization<Fx, S::Permit>, AuthorizeError>;
}

/// Redeem a permit into final access.
///
/// This is the second step of the authorization pipeline. The environment
/// takes the intermediate permit and produces an `Authorization<Fx, S::Access>`.
pub struct Redeem<Fx: Constraint, S: Site> {
    /// The intermediate authorization (capability + permit).
    pub authorization: Authorization<Fx, S::Permit>,
    /// The site configuration.
    pub site: S,
}

impl<Fx: Constraint, S: Site> Invocation for Redeem<Fx, S> {
    type Input = Self;
    type Output = Result<Authorization<Fx, S::Access>, RedeemError>;
}

/// Import credential material into the environment's credential store.
pub struct Import<Material> {
    /// The credential material to import.
    pub material: Material,
}

impl<Material: ConditionalSend + 'static> Invocation for Import<Material> {
    type Input = Self;
    type Output = Result<(), CredentialError>;
}

/// Error during the authorize step.
#[derive(Debug, Error)]
pub enum AuthorizeError {
    /// Authorization was denied.
    #[error("Authorization denied: {0}")]
    Denied(String),

    /// Configuration error (e.g., missing delegation chain).
    #[error("Authorization configuration error: {0}")]
    Configuration(String),
}

/// Error during the redeem step.
#[derive(Debug, Error)]
pub enum RedeemError {
    /// The access service rejected the proof.
    #[error("Redeem failed: {0}")]
    Rejected(String),

    /// Transport or service error during redemption.
    #[error("Redeem service error: {0}")]
    Service(String),
}

/// Error during the full acquire pipeline (authorize + redeem).
#[derive(Debug, Error)]
pub enum AcquireError {
    /// Error during the authorize step.
    #[error(transparent)]
    Authorize(#[from] AuthorizeError),

    /// Error during the redeem step.
    #[error(transparent)]
    Redeem(#[from] RedeemError),
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
