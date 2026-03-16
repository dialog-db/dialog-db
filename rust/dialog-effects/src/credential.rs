//! Credential capability hierarchy.
//!
//! Provides identity and signing operations scoped to an authority (the
//! principal on whose behalf changes are made).
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//!   └── Credential (ability: /credential)
//!         └── Authority { did: Did }
//!               ├── Identify → Effect → Result<Did, CredentialError>
//!               └── Sign { payload } → Effect → Result<Vec<u8>, CredentialError>
//! ```

pub use dialog_capability::{Attenuation, Capability, Did, Effect, Policy, Subject};
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

/// Authority policy that scopes operations to the principal on whose behalf
/// the operator acts.
///
/// In UCAN terms, this is whoever the subject directly delegated to.
/// The delegation chain may be longer (repo → authority → operator), but
/// the authority is the meaningful identity for authorship and scoping.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Authority {
    /// The authority's DID (typically an account).
    pub did: Did,
}

impl Authority {
    /// Create a new Authority policy.
    pub fn new(did: Did) -> Self {
        Self { did }
    }
}

impl Policy for Authority {
    type Of = Credential;
}

/// Identify operation — returns the authority's DID.
///
/// Used for memory scoping and revision authorship.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Identify;

impl Effect for Identify {
    type Of = Authority;
    type Output = Result<Did, CredentialError>;
}

/// Extension trait for `Capability<Identify>` to access its fields.
pub trait IdentifyCapability {
    /// Get the authority DID from the capability chain.
    fn authority(&self) -> &Did;
}

impl IdentifyCapability for Capability<Identify> {
    fn authority(&self) -> &Did {
        &Authority::of(self).did
    }
}

/// Sign operation — signs a payload using the operator's key,
/// on behalf of the authority.
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
    type Of = Authority;
    type Output = Result<Vec<u8>, CredentialError>;
}

/// Extension trait for `Capability<Sign>` to access its fields.
pub trait SignCapability {
    /// Get the authority DID from the capability chain.
    fn authority(&self) -> &Did;
    /// Get the payload to sign.
    fn payload(&self) -> &[u8];
}

impl SignCapability for Capability<Sign> {
    fn authority(&self) -> &Did {
        &Authority::of(self).did
    }

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

    /// No credentials available for the requested authority.
    #[error("No credentials for authority: {0}")]
    NotFound(Did),
}

/// Trait for types that can provide credential operations.
///
/// Implementors hold an operator keypair and know which authority (account)
/// the operator acts on behalf of.
///
/// Implementors should also implement `Provider<Identify>` and
/// `Provider<Sign>` to be usable as an environment's credential provider.
///
/// In the simplest case, the authority and operator are the same identity
/// (self-issued credentials with no delegation chain).
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Operator {
    /// Return the authority's DID (the account this operator acts on behalf of).
    fn identify(&self) -> Result<Did, CredentialError>;

    /// Sign a payload using the operator's key.
    async fn sign(&self, payload: &[u8]) -> Result<Vec<u8>, CredentialError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::did;

    #[test]
    fn it_builds_credential_claim_path() {
        let claim = Subject::from(did!("key:zSpace")).attenuate(Credential);

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/credential");
    }

    #[test]
    fn it_builds_authority_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Authority::new(did!("key:zAccount")));

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/credential");
    }

    #[test]
    fn it_builds_identify_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Authority::new(did!("key:zAccount")))
            .invoke(Identify);

        assert_eq!(claim.ability(), "/credential/identify");
    }

    #[test]
    fn it_builds_sign_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Authority::new(did!("key:zAccount")))
            .invoke(Sign::new(b"hello"));

        assert_eq!(claim.ability(), "/credential/sign");
    }

    #[test]
    fn it_extracts_authority_from_identify() {
        let cap = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Authority::new(did!("key:zAccount")))
            .invoke(Identify);

        assert_eq!(cap.authority(), &did!("key:zAccount"));
    }

    #[test]
    fn it_extracts_fields_from_sign() {
        let cap = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Authority::new(did!("key:zAccount")))
            .invoke(Sign::new(b"payload"));

        assert_eq!(cap.authority(), &did!("key:zAccount"));
        assert_eq!(cap.payload(), b"payload");
    }
}
