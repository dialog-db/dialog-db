//! Error types for S3 operations.

use dialog_effects::archive::ArchiveError;
use dialog_effects::blob::BlobError;
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

impl From<S3Error> for BlobError {
    fn from(error: S3Error) -> Self {
        BlobError::Storage(error.to_string())
    }
}

/// Whether an error means the presented permit itself was rejected.
///
/// The S3 providers map an HTTP 401/403 to their error type's
/// authorization variant; everything else is either a semantic outcome
/// (a missing object, a CAS conflict) or a transport failure that
/// proves nothing about the permit. A permit cache uses this split to
/// decide when a cached permit must be dropped and redeemed afresh:
/// invalidating on any error would let a routine presence probe (a 404)
/// evict a perfectly reusable permit.
pub trait PermitRejection {
    /// `true` when the service rejected the permit this operation
    /// presented, so retrying with the same permit cannot succeed.
    fn is_permit_rejection(&self) -> bool;
}

impl PermitRejection for ArchiveError {
    fn is_permit_rejection(&self) -> bool {
        matches!(self, ArchiveError::AuthorizationError(_))
    }
}

impl PermitRejection for BlobError {
    fn is_permit_rejection(&self) -> bool {
        matches!(self, BlobError::AuthorizationError(_))
    }
}

impl PermitRejection for MemoryError {
    fn is_permit_rejection(&self) -> bool {
        matches!(self, MemoryError::Authorization(_))
    }
}

/// Error encoding or decoding [`S3Authorization`](crate::S3Authorization)
/// to/from a [`Secret`](dialog_effects::credential::Secret).
#[derive(Debug, Error)]
pub enum AuthorizationFormatError {
    /// Failed to serialize authorization to bytes.
    #[error("Failed to serialize S3 authorization: {0}")]
    Serialize(String),

    /// Failed to deserialize authorization from bytes.
    #[error("Failed to deserialize S3 authorization: {0}")]
    Deserialize(String),
}

impl From<AuthorizationFormatError> for S3Error {
    fn from(error: AuthorizationFormatError) -> Self {
        S3Error::Serialization(error.to_string())
    }
}

impl From<AuthorizationFormatError> for dialog_effects::credential::CredentialError {
    fn from(error: AuthorizationFormatError) -> Self {
        Self::Corrupted(error.to_string())
    }
}

impl From<AuthorizationFormatError> for dialog_capability::AuthorizeError {
    fn from(error: AuthorizationFormatError) -> Self {
        Self::Malformed(error.to_string())
    }
}
