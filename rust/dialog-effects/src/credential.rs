//! Credential capability hierarchy.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (profile or repository DID)
//!   └── Credential (ability: /credential)
//!         ├── Key { address: String }
//!         │   ├── Save<Credential> → Result<(), CredentialError>
//!         │   └── Load<Credential> → Result<Credential, CredentialError>
//!         └── Site { address: String }
//!             ├── Save<Secret> → Result<(), CredentialError>
//!             └── Load<Secret> → Result<Secret, CredentialError>
//! ```

pub mod prelude;

pub use dialog_capability::{
    Attenuation, Capability, Claim, Effect, Policy, StorageError, Subject,
};
pub use dialog_credentials;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Opaque secret bytes for site credentials.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Secret(pub Vec<u8>);

impl From<Vec<u8>> for Secret {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<Secret> for Vec<u8> {
    fn from(secret: Secret) -> Self {
        secret.0
    }
}

/// Root attenuation for credential operations.
///
/// Attaches to Subject and provides the `/credential` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Credential;

impl Attenuation for Credential {
    type Of = Subject;
}

/// Key credential address (e.g., signing keypair).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Key {
    /// The credential address (e.g., "self").
    pub address: String,
}

/// The default key address for a space's own identity.
pub const SELF: &str = "self";

impl Key {
    /// Create a new key credential address.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
        }
    }
}

impl Attenuation for Key {
    type Of = Credential;
}

/// Site credential address (e.g., S3 access keys).
///
/// Keyed by URL so that the address naturally identifies the remote
/// service the credentials are for.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Site {
    /// The credential address (e.g., "https://s3.us-east-1.amazonaws.com/my-bucket").
    pub address: String,
}

impl Site {
    /// Create a new site credential address.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
        }
    }
}

impl Attenuation for Site {
    type Of = Credential;
}

/// Save a credential or secret to storage.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Save<T> {
    /// The value to save.
    pub credential: T,
}

impl<T> Save<T> {
    /// Create a new save effect.
    pub fn new(value: T) -> Self {
        Self { credential: value }
    }
}

impl Effect for Save<dialog_credentials::Credential> {
    type Of = Key;
    type Output = Result<(), CredentialError>;
}

impl Effect for Save<Secret> {
    type Of = Site;
    type Output = Result<(), CredentialError>;
}

/// Load a credential or secret from storage.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Load<T> {
    #[serde(skip)]
    _marker: std::marker::PhantomData<T>,
}

impl<T> Load<T> {
    /// Create a new load effect.
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> Default for Load<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl Effect for Load<dialog_credentials::Credential> {
    type Of = Key;
    type Output = Result<dialog_credentials::Credential, CredentialError>;
}

impl Effect for Load<Secret> {
    type Of = Site;
    type Output = Result<Secret, CredentialError>;
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
