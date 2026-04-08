//! Error types for S3 access operations.

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
