//! Error types for S3 operations.

use dialog_effects::archive::ArchiveError;
use dialog_effects::memory::MemoryError;
use thiserror::Error;

/// Error type for S3 operations.
#[derive(Debug, Error)]
pub enum S3Error {
    /// Failed to authorize the request.
    #[error("Authorization error: {0}")]
    Authorization(String),

    /// Transport-level error (connection failed, timeout, network issues).
    #[error("Transport error: {0}")]
    Transport(String),

    /// Service-level error (S3 returned an error response).
    #[error("Service error: {0}")]
    Service(String),

    /// Invalid configuration.
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Error during serialization or deserialization of data.
    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<reqwest::Error> for S3Error {
    fn from(error: reqwest::Error) -> Self {
        S3Error::Transport(error.to_string())
    }
}

impl From<S3Error> for ArchiveError {
    fn from(error: S3Error) -> Self {
        ArchiveError::Io(error.to_string())
    }
}

impl From<S3Error> for MemoryError {
    fn from(error: S3Error) -> Self {
        MemoryError::Storage(error.to_string())
    }
}
