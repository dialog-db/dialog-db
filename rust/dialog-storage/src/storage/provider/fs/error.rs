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

    /// Lock acquisition failed.
    #[error("Lock error: {0}")]
    Lock(String),

    /// Path containment violation (attempted to escape base directory).
    #[error("Containment violation: {0}")]
    Containment(String),
}

impl From<FileSystemError> for CredentialError {
    fn from(e: FileSystemError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<FileSystemError> for AuthorizeError {
    fn from(e: FileSystemError) -> Self {
        Self::Malformed {
            detail: e.to_string(),
        }
    }
}
