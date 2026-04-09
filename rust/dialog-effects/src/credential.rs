//! Credential capability hierarchy.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (profile or repository DID)
//!   └── Credential (ability: /credential)
//!         └── Address { address: String }
//!             ├── Save { credential } → Effect → Result<(), CredentialError>
//!             └── Load → Effect → Result<Credential, CredentialError>
//! ```

pub mod prelude;

pub use dialog_capability::{
    Attenuation, Capability, Claim, Effect, Policy, StorageError, Subject,
};
pub use dialog_credentials;
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

/// Address for a credential store.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Name {
    /// The storage address path.
    pub name: String,
}

/// The default credential address for a space's own identity.
pub const SELF: &str = "self";

impl Name {
    /// Create a new credential address.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Policy for Name {
    type Of = Credential;
}

/// Save a credential to storage.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Save {
    /// The credential to save.
    pub credential: dialog_credentials::Credential,
}

impl Effect for Save {
    type Of = Name;
    type Output = Result<(), CredentialError>;
}

/// Load a credential from storage.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Load;

impl Effect for Load {
    type Of = Name;
    type Output = Result<dialog_credentials::Credential, CredentialError>;
}

/// Errors that can occur during credential operations.
#[derive(Debug, Error)]
pub enum CredentialError {
    /// Credential not found at the given address.
    #[error("Credential not found: {0}")]
    NotFound(String),

    /// Storage error while reading or writing a credential.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Credential data is corrupted or unreadable.
    #[error("Corrupted credential: {0}")]
    Corrupted(String),
}

impl From<StorageError> for CredentialError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e.to_string())
    }
}
