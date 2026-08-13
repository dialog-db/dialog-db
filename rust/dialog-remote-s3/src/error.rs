//! Error types for S3 operations.

use dialog_capability::access::AuthorizeError;
use dialog_effects::archive::ArchiveError;
use dialog_effects::blob::BlobError;
use dialog_effects::memory::MemoryError;
use dialog_effects::service::Rejection;
use thiserror::Error;

/// Error type for S3 operations.
#[derive(Debug, Error)]
pub enum S3Error {
    /// The request was not authorized.
    ///
    /// Carries the access decision itself rather than its rendering, so
    /// a caller can tell a withdrawn authority from a lapsed one without
    /// parsing a message.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),

    /// Transport-level error (connection failed, timeout, network issues).
    #[error("Transport error: {0}")]
    Transport(String),

    /// The request was not carried out, for a reason that is not an
    /// access decision.
    #[error(transparent)]
    Rejected(#[from] Rejection),

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
        match error {
            S3Error::Authorization(error) => ArchiveError::Authorization(error),
            S3Error::Rejected(error) => ArchiveError::Rejected(error),
            error => ArchiveError::Storage(error.to_string()),
        }
    }
}

impl From<S3Error> for MemoryError {
    fn from(error: S3Error) -> Self {
        match error {
            S3Error::Authorization(error) => MemoryError::Authorization(error),
            S3Error::Rejected(error) => MemoryError::Rejected(error),
            error => MemoryError::Storage(error.to_string()),
        }
    }
}

impl From<S3Error> for BlobError {
    fn from(error: S3Error) -> Self {
        match error {
            S3Error::Authorization(error) => BlobError::Authorization(error),
            S3Error::Rejected(error) => BlobError::Rejected(error),
            error => BlobError::Storage(error.to_string()),
        }
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
        matches!(self, ArchiveError::Authorization(_))
    }
}

impl PermitRejection for BlobError {
    fn is_permit_rejection(&self) -> bool {
        matches!(self, BlobError::Authorization(_))
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
        Self::Malformed {
            detail: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use dialog_capability::access::AuthorizeError;
    use dialog_effects::blob::BlobError;

    use super::*;

    fn revoked() -> AuthorizeError {
        AuthorizeError::Revoked {
            subject: dialog_capability::did!(
                "did:key:z6MkrCD1csqtgdj8sRHYRPGLYcMFXAoDhkgvHNq2FML2xqCX"
            ),
        }
    }

    // Every hop from the transport to an effect error must carry the
    // reason itself. Stringifying it here is what made callers parse
    // messages -- and what made `is_permit_rejection` a substring test
    // in all but name.
    #[dialog_common::test]
    async fn it_preserves_the_reason_across_effect_errors() {
        let archive = ArchiveError::from(S3Error::Authorization(revoked()));
        let memory = MemoryError::from(S3Error::Authorization(revoked()));
        let blob = BlobError::from(S3Error::Authorization(revoked()));

        assert!(matches!(
            archive,
            ArchiveError::Authorization(AuthorizeError::Revoked { .. })
        ));
        assert!(matches!(
            memory,
            MemoryError::Authorization(AuthorizeError::Revoked { .. })
        ));
        assert!(matches!(
            blob,
            BlobError::Authorization(AuthorizeError::Revoked { .. })
        ));
    }

    // The one consumer that acts on the distinction.
    #[dialog_common::test]
    async fn it_recognizes_a_rejected_permit() {
        assert!(ArchiveError::Authorization(revoked()).is_permit_rejection());
        assert!(!ArchiveError::Storage("disk".into()).is_permit_rejection());
    }
}
