//! Credential capability hierarchy.
//!
//! Provides named credential discovery and registration.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject
//!   └── Credential (ability: /credential)
//!         └── Name { name: String }
//!               ├── Load → Effect → Result<Option<Identity>, CredentialError>
//!               └── Save(Identity) → Effect → Result<(), CredentialError>
//! ```

pub use dialog_capability::{Attenuation, Capability, Claim, Effect, Policy, Subject};
#[cfg(not(target_arch = "wasm32"))]
pub use dialog_credentials::credential::export::{SignerExport, VerifierExport};
pub use dialog_credentials::credential::{
    Credential as Identity, CredentialExport, CredentialExportError, SignerCredential,
    SignerCredentialExport, VerifierCredential, VerifierCredentialExport,
};
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

/// Name policy that scopes operations to a specific named credential.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Name {
    /// The credential name (e.g. "self", "s3-bucket").
    pub name: String,
}

impl Name {
    /// Create a new Name policy.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Policy for Name {
    type Of = Credential;
}

/// Load operation — reads the credential for a named entry.
///
/// Returns `None` if no credential with this name exists.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Claim)]
pub struct Load;

impl Effect for Load {
    type Of = Name;
    type Output = Result<Option<Identity>, CredentialError>;
}

/// Extension trait for `Capability<Load>` to access its fields.
pub trait LoadCapability {
    /// Get the credential name from the capability chain.
    fn name(&self) -> &str;
}

impl LoadCapability for Capability<Load> {
    fn name(&self) -> &str {
        &Name::of(self).name
    }
}

/// Save operation — stores a credential for a named entry.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Save {
    /// The credential to store.
    ///
    /// Mapped to its DID in the authorization claim so that private
    /// key material never appears in signed invocations.
    #[claim(into = dialog_capability::Did, rename = subject)]
    pub credential: Identity,
}

impl Save {
    /// Create a new Save effect.
    pub fn new(credential: Identity) -> Self {
        Self { credential }
    }
}

impl Effect for Save {
    type Of = Name;
    type Output = Result<(), CredentialError>;
}

/// Extension trait for `Capability<Save>` to access its fields.
pub trait SaveCapability {
    /// Get the credential name from the capability chain.
    fn name(&self) -> &str;
    /// Get the credential being saved.
    fn credential(&self) -> &Identity;
}

impl SaveCapability for Capability<Save> {
    fn name(&self) -> &str {
        &Name::of(self).name
    }

    fn credential(&self) -> &Identity {
        &Save::of(self).credential
    }
}

/// Error type for credential operations.
#[derive(Debug, Error)]
pub enum CredentialError {
    /// The requested credential was not found.
    #[error("Credential not found: {0}")]
    NotFound(String),

    /// Storage error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Credential data is corrupted.
    #[error("Corrupted credential: {0}")]
    Corrupted(String),
}

impl From<dialog_capability::storage::StorageError> for CredentialError {
    fn from(e: dialog_capability::storage::StorageError) -> Self {
        Self::Storage(e.to_string())
    }
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
    fn it_builds_load_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Name::new("self"))
            .invoke(Load);

        assert_eq!(claim.ability(), "/credential/load");
    }

    #[test]
    fn it_builds_save_claim_path() {
        let verifier: dialog_credentials::Ed25519Verifier =
            "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
                .parse()
                .unwrap();
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Name::new("self"))
            .invoke(Save::new(verifier.into()));

        assert_eq!(claim.ability(), "/credential/save");
    }
}
