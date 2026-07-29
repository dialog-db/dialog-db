//! Error types for S3 operations.

use dialog_effects::archive::ArchiveError;
use dialog_effects::memory::MemoryError;
use dialog_effects::service::ServiceResponseError;
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

    /// A remote service returned a non-success HTTP response.
    #[error("{0}")]
    ServiceResponse(#[source] ServiceResponseError),

    /// Invalid configuration.
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Error during serialization or deserialization of data.
    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<ServiceResponseError> for S3Error {
    fn from(error: ServiceResponseError) -> Self {
        Self::ServiceResponse(error)
    }
}

impl From<reqwest::Error> for S3Error {
    fn from(error: reqwest::Error) -> Self {
        S3Error::Transport(error.to_string())
    }
}

impl From<S3Error> for ArchiveError {
    fn from(error: S3Error) -> Self {
        match error {
            S3Error::ServiceResponse(error) => ArchiveError::ServiceResponse(error),
            error => ArchiveError::Io(error.to_string()),
        }
    }
}

impl From<S3Error> for MemoryError {
    fn from(error: S3Error) -> Self {
        match error {
            S3Error::ServiceResponse(error) => MemoryError::ServiceResponse(error),
            error => MemoryError::Storage(error.to_string()),
        }
    }
}

impl From<S3Error> for dialog_effects::blob::BlobError {
    fn from(error: S3Error) -> Self {
        match error {
            S3Error::ServiceResponse(error) => {
                dialog_effects::blob::BlobError::ServiceResponse(error)
            }
            error => dialog_effects::blob::BlobError::Storage(error.to_string()),
        }
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
        Self::Configuration(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use dialog_effects::blob::BlobError;

    use super::*;

    fn service_error() -> ServiceResponseError {
        ServiceResponseError::new(
            403,
            Some("CREDENTIAL_REVOKED".to_string()),
            "Credential revoked",
        )
    }

    #[dialog_common::test]
    async fn it_preserves_service_responses_across_effect_errors() {
        let archive = ArchiveError::from(S3Error::ServiceResponse(service_error()));
        let memory = MemoryError::from(S3Error::ServiceResponse(service_error()));
        let blob = BlobError::from(S3Error::ServiceResponse(service_error()));

        assert!(matches!(
            archive,
            ArchiveError::ServiceResponse(ServiceResponseError { status: 403, .. })
        ));
        assert!(matches!(
            memory,
            MemoryError::ServiceResponse(ServiceResponseError { status: 403, .. })
        ));
        assert!(matches!(
            blob,
            BlobError::ServiceResponse(ServiceResponseError { status: 403, .. })
        ));
    }
}
