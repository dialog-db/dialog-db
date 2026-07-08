//! Errors for blob operations.

use std::error::Error;

use thiserror::Error as ThisError;

use dialog_capability::access::AuthorizeError;
use dialog_capability::{
    DialogCapabilityAuthorizationError, DialogCapabilityPerformError, StorageError,
};

/// Errors that can occur during blob operations.
#[derive(Debug, ThisError)]
pub enum BlobError {
    /// The blob was not found.
    #[error("Blob not found: {0}")]
    NotFound(String),

    /// The written content did not hash to the declared digest.
    #[error("Blob digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch {
        /// The declared digest.
        expected: String,
        /// The digest computed from the written bytes.
        actual: String,
    },

    /// Authorization failed.
    #[error("Unauthorized error: {0}")]
    AuthorizationError(String),

    /// The operation failed during execution.
    #[error("Execution error: {0}")]
    ExecutionError(String),

    /// The storage backend failed.
    #[error("Storage error: {0}")]
    Storage(String),

    /// An I/O error occurred.
    #[error("IO error: {0}")]
    Io(String),
}

impl From<StorageError> for BlobError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<DialogCapabilityAuthorizationError> for BlobError {
    fn from(value: DialogCapabilityAuthorizationError) -> Self {
        BlobError::AuthorizationError(value.to_string())
    }
}

impl From<AuthorizeError> for BlobError {
    fn from(value: AuthorizeError) -> Self {
        BlobError::AuthorizationError(value.to_string())
    }
}

impl<E: Error> From<DialogCapabilityPerformError<E>> for BlobError {
    fn from(value: DialogCapabilityPerformError<E>) -> Self {
        match value {
            DialogCapabilityPerformError::Authorization(error) => {
                BlobError::AuthorizationError(error.to_string())
            }
            DialogCapabilityPerformError::Execution(error) => {
                BlobError::ExecutionError(error.to_string())
            }
        }
    }
}
