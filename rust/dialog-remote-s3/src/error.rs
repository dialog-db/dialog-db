//! Error types for S3 operations.

use thiserror::Error;

/// Error type for authorization operations.
#[derive(Debug, Error)]
pub enum AccessError {
    /// No delegation found for the subject.
    #[error("No delegation for subject: {0}")]
    NoDelegation(String),

    /// Failed to build the invocation.
    #[error("Invocation error: {0}")]
    Invocation(String),

    /// Access service returned an error.
    #[error("Access service error: {0}")]
    Service(String),

    /// Invalid configuration.
    #[error("Configuration error: {0}")]
    Configuration(String),
}

/// Errors that can occur when using the S3 storage backend.
#[derive(Error, Debug)]
pub enum S3StorageError {
    /// Failed to authorize the request (signing or credential issues).
    #[error("Authorization error: {0}")]
    AuthorizationError(String),

    /// Transport-level error (connection failed, timeout, network issues).
    #[error("Transport error: {0}")]
    TransportError(String),

    /// Service-level error (S3 returned an error response).
    #[error("Service error: {0}")]
    ServiceError(String),

    /// Error during serialization or deserialization of data.
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// CAS edition mismatch (concurrent modification detected).
    #[error("Edition mismatch: expected {expected:?}, got {actual:?}")]
    EditionMismatch {
        /// The expected edition.
        expected: Option<String>,
        /// The actual edition found.
        actual: Option<String>,
    },
}

impl From<reqwest::Error> for S3StorageError {
    fn from(error: reqwest::Error) -> Self {
        S3StorageError::TransportError(error.to_string())
    }
}

impl From<AccessError> for S3StorageError {
    fn from(error: AccessError) -> Self {
        S3StorageError::AuthorizationError(error.to_string())
    }
}
