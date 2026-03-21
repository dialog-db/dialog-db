//! Credential capability hierarchy and remote resource authorization.
//!
//! Provides identity and signing operations scoped to a repository subject,
//! plus the [`Authorize`] command for authorizing capabilities against
//! an access format.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (operator DID)
//! └── Credential (ability: /credential)
//!     └── Profile { profile: String }  (policy, scopes to named profile)
//!         ├── Identify -> Effect -> Result<Identity, CredentialError>
//!         ├── Sign { payload } -> Effect -> Result<Vec<u8>, CredentialError>
//!         └── Import<M> { material: M } -> Effect -> Result<(), CredentialError>
//! ```

use crate::Constraint;
use crate::access::Access;
use crate::authorization::Authorized;
pub use crate::{Attenuation, Capability, Did, Effect, Policy, Subject};
use dialog_common::ConditionalSend;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use crate::site::Local;

/// Root attenuation for credential operations.
///
/// Attaches to Subject and provides the `/credential` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Credential;

impl Attenuation for Credential {
    type Of = Subject;
}

/// Profile policy that scopes credential operations to a named profile.
///
/// A profile is a named user identity on a specific device (e.g. "default",
/// "work", "personal"), each with its own ed25519 keypair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Profile {
    /// The profile name.
    pub profile: String,
}

impl Profile {
    /// Create a new Profile policy.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            profile: name.into(),
        }
    }
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            profile: "default".to_string(),
        }
    }
}

impl Policy for Profile {
    type Of = Credential;
}

/// The active credential session: profile, operator, and optional account.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Identity {
    /// The profile's DID (device-local persistent identity).
    pub profile: Did,
    /// The operator's DID (ephemeral session key).
    pub operator: Did,
    /// The account's DID (optional cross-device recovery identity).
    pub account: Option<Did>,
}

/// Identify operation — returns the credential detail for the active session.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Identify;

impl Effect for Identify {
    type Of = Profile;
    type Output = Result<Identity, CredentialError>;
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
    type Of = Profile;
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

/// Request to authorize a capability for a given access format.
///
/// Parameterized by access format, not site. One provider covers ALL sites
/// sharing the same access format.
#[derive(Serialize, Deserialize)]
#[serde(bound(deserialize = ""))]
pub struct Authorize<Fx: Constraint, A: Access> {
    /// The capability to authorize.
    pub capability: Capability<Fx>,
    /// The access context (carries addressing info).
    pub access: A,
}

impl<Fx, A: Access> Effect for Authorize<Fx, A>
where
    Fx: Effect,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    A: ConditionalSend,
    Self: ConditionalSend + 'static,
{
    type Of = Profile;
    type Output = Result<Authorized<Fx, A>, AuthorizeError>;
}

/// Import credential material into the credential store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Import<Material: Serialize> {
    /// The credential material to import.
    pub material: Material,
}

impl<Material: Serialize + DeserializeOwned + ConditionalSend + 'static> Effect
    for Import<Material>
{
    type Of = Profile;
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
    fn it_builds_profile_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Profile::new("default"));

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/credential");
    }

    #[test]
    fn it_builds_identify_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Profile::new("default"))
            .invoke(Identify);

        assert_eq!(claim.ability(), "/credential/identify");
    }

    #[test]
    fn it_builds_sign_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Profile::new("default"))
            .invoke(Sign::new(b"hello"));

        assert_eq!(claim.ability(), "/credential/sign");
    }

    #[test]
    fn it_extracts_payload_from_sign() {
        let cap = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Profile::new("default"))
            .invoke(Sign::new(b"payload"));

        assert_eq!(cap.payload(), b"payload");
    }
}
