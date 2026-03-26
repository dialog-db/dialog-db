//! Repository capability hierarchy.
//!
//! Provides named repository discovery and registration.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (environment DID)
//!   └── Repository (ability: /repository)
//!         └── Name { name: String }
//!               ├── Load → Effect → Result<Option<Credential>, RepositoryError>
//!               └── Save(Credential) → Effect → Result<(), RepositoryError>
//! ```

pub use dialog_capability::{Attenuation, Capability, Claim, Effect, Policy, Subject};
#[cfg(not(target_arch = "wasm32"))]
pub use dialog_credentials::credential::export::{SignerExport, VerifierExport};
pub use dialog_credentials::credential::{
    Credential, CredentialExport, CredentialExportError, SignerCredential, SignerCredentialExport,
    VerifierCredential, VerifierCredentialExport,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Root attenuation for repository operations.
///
/// Attaches to Subject and provides the `/repository` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Repository;

impl Attenuation for Repository {
    type Of = Subject;
}

/// Name policy that scopes operations to a specific named repository.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Name {
    /// The repository name (e.g. "home", "work").
    pub name: String,
}

impl Name {
    /// Create a new Name policy.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Policy for Name {
    type Of = Repository;
}

/// Load operation — reads the credential for a named repository.
///
/// Returns `None` if no repository with this name exists.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Claim)]
pub struct Load;

impl Effect for Load {
    type Of = Name;
    type Output = Result<Option<Credential>, RepositoryError>;
}

/// Extension trait for `Capability<Load>` to access its fields.
pub trait LoadCapability {
    /// Get the repository name from the capability chain.
    fn name(&self) -> &str;
}

impl LoadCapability for Capability<Load> {
    fn name(&self) -> &str {
        &Name::of(self).name
    }
}

/// Save operation — stores a credential for a named repository.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Save {
    /// The credential to store.
    ///
    /// Mapped to its DID in the authorization claim so that private
    /// key material never appears in signed invocations.
    #[claim(into = dialog_capability::Did, rename = subject)]
    pub credential: Credential,
}

impl Save {
    /// Create a new Save effect.
    pub fn new(credential: Credential) -> Self {
        Self { credential }
    }
}

impl Effect for Save {
    type Of = Name;
    type Output = Result<(), RepositoryError>;
}

/// Extension trait for `Capability<Save>` to access its fields.
pub trait SaveCapability {
    /// Get the repository name from the capability chain.
    fn name(&self) -> &str;
    /// Get the credential being saved.
    fn credential(&self) -> &Credential;
}

impl SaveCapability for Capability<Save> {
    fn name(&self) -> &str {
        &Name::of(self).name
    }

    fn credential(&self) -> &Credential {
        &Save::of(self).credential
    }
}

/// Error type for repository operations.
#[derive(Debug, Error)]
pub enum RepositoryError {
    /// Repository not found.
    #[error("Repository not found: {0}")]
    NotFound(String),

    /// Storage error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Repository data is corrupted.
    #[error("Corrupted repository: {0}")]
    Corrupted(String),
}
