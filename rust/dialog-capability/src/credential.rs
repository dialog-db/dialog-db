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

/// A remote resource requiring authorization to access.
///
/// Implemented by concrete credential types (e.g., `s3::Credentials`,
/// `ucan::Credentials`). The associated types track the authorization
/// lifecycle from authorization through permit to final access.
pub trait Remote: ConditionalSend + 'static {
    /// The authorization material (e.g., the credentials themselves for S3,
    /// a delegation chain for UCAN).
    type Authorization: Clone + ConditionalSend;

    /// Intermediate permit produced by `Authorize` (e.g., `S3Permit`,
    /// `UcanInvocation`). Redeemed into final `Access`.
    type Permit: ConditionalSend;

    /// Final access token produced by `Redeem` (e.g., `AuthorizedRequest`).
    type Access: ConditionalSend;

    /// Service address (e.g., S3 endpoint URL, access service URL).
    type Address: Clone + ConditionalSend;

    /// Get the service address.
    fn address(&self) -> &Self::Address;

    /// Get the authorization material.
    fn authorization(&self) -> &Self::Authorization;

    /// Start building a capability chain with these credentials.
    ///
    /// Returns a [`Claim`] rooted at the given subject. Use `.attenuate()`
    /// and `.invoke()` to build the chain, then `.acquire()` to authorize.
    fn claim(&self, subject: impl Into<Subject>) -> crate::Claim<'_, Self, Subject>
    where
        Self: Sized,
    {
        crate::Claim::new(self, Capability::new(subject.into()))
    }
}

/// Request to authorize a capability against a remote resource `R`.
///
/// This is the first step of the authorization pipeline. The environment
/// receives authorization material and a capability, and produces an
/// `Authorization<Fx, R::Permit>`.
pub struct Authorize<Fx: Constraint, R: Remote> {
    /// The authorization material from the resource.
    pub authorization: R::Authorization,
    /// The service address.
    pub address: R::Address,
    /// The capability to authorize.
    pub capability: Capability<Fx>,
}

impl<Fx: Constraint, R: Remote> Invocation for Authorize<Fx, R> {
    type Input = Self;
    type Output = Result<Authorization<Fx, R::Permit>, AuthorizeError>;
}

/// Redeem a permit into final access.
///
/// This is the second step of the authorization pipeline. The environment
/// takes the intermediate permit and produces an `Authorization<Fx, R::Access>`.
pub struct Redeem<Fx: Constraint, R: Remote> {
    /// The intermediate authorization (capability + permit).
    pub authorization: Authorization<Fx, R::Permit>,
    /// The service address.
    pub address: R::Address,
}

impl<Fx: Constraint, R: Remote> Invocation for Redeem<Fx, R> {
    type Input = Self;
    type Output = Result<Authorization<Fx, R::Access>, RedeemError>;
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
