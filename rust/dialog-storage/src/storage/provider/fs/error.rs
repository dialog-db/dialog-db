//! Error types for filesystem storage operations.

use dialog_capability::access::AuthorizeError;
use dialog_effects::credential::CredentialError;
use thiserror::Error;

/// Errors that can occur during filesystem operations.
#[derive(Debug, Error)]
pub enum FileSystemError {
    /// I/O operation failed.
    #[error("Filesystem I/O error: {0}")]
    Io(String),

    /// No file exists at the requested location.
    #[error("Not found: {0}")]
    NotFound(String),

    /// Lock acquisition failed.
    #[error("Lock error: {0}")]
    Lock(String),

    /// CAS condition failed.
    #[error("CAS condition failed: {0}")]
    Cas(String),

    /// Path containment violation (attempted to escape base directory).
    #[error("Containment violation: {0}")]
    Containment(String),
}

impl From<FileSystemError> for CredentialError {
    fn from(e: FileSystemError) -> Self {
        match e {
            FileSystemError::NotFound(location) => Self::NotFound(location),
            other => Self::Storage(other.to_string()),
        }
    }
}

impl From<FileSystemError> for AuthorizeError {
    fn from(e: FileSystemError) -> Self {
        Self::Configuration(e.to_string())
    }
}
