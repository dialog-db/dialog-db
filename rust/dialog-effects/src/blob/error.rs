//! Errors for blob operations.

use std::error::Error;

use thiserror::Error as ThisError;

use crate::Rejection;
use dialog_capability::access::AuthorizeError;
use dialog_capability::{DialogCapabilityPerformError, StorageError};

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

    /// The bytes held under a digest are not the size they were named at.
    #[error("Blob {digest} was named at {expected} bytes, {held} are held")]
    SizeMismatch {
        /// The blob's content hash.
        digest: String,
        /// The size the blob was named at.
        expected: u64,
        /// The size of the bytes actually held.
        held: u64,
    },

    /// The request was not authorized.
    ///
    /// Carries the decision itself rather than its rendering, so a
    /// caller can tell a withdrawn authority from a lapsed one.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),

    /// The request was not carried out, for a reason that is not an
    /// access decision.
    #[error(transparent)]
    Rejected(#[from] Rejection),

    /// The storage backend failed.
    #[error("Storage error: {0}")]
    Storage(String),
}

impl From<StorageError> for BlobError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl<E: Error> From<DialogCapabilityPerformError<E>> for BlobError {
    fn from(value: DialogCapabilityPerformError<E>) -> Self {
        match value {
            DialogCapabilityPerformError::Authorization(error) => error.into(),
            DialogCapabilityPerformError::Execution(error) => BlobError::Storage(error.to_string()),
        }
    }
}
