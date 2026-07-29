use dialog_effects::archive::ArchiveError;
use dialog_effects::memory::MemoryError;
use dialog_effects::service::ServiceResponseError;
use thiserror::Error;

/// The common error type used by this crate
#[derive(Error, Debug)]
pub enum DialogStorageError {
    /// An error that occurs during block encoding
    #[error("Failed to encode a block: {0}")]
    EncodeFailed(String),

    /// An error that occurs during block decoding
    #[error("Failed to decode a block: {0}")]
    DecodeFailed(String),

    /// An error that occurs when working with a storage backend
    #[error("Storage backend error: {0}")]
    StorageBackend(String),

    /// A remote service returned a non-success HTTP response.
    #[error("{0}")]
    ServiceResponse(#[source] ServiceResponseError),

    /// An error that occurs when byte hash verification fails
    #[error("Byte hash verification failed: {0}")]
    Verification(String),
}

impl From<ServiceResponseError> for DialogStorageError {
    fn from(error: ServiceResponseError) -> Self {
        Self::ServiceResponse(error)
    }
}

impl From<ArchiveError> for DialogStorageError {
    fn from(error: ArchiveError) -> Self {
        match error {
            ArchiveError::ServiceResponse(error) => Self::ServiceResponse(error),
            error => Self::StorageBackend(error.to_string()),
        }
    }
}

impl From<MemoryError> for DialogStorageError {
    fn from(error: MemoryError) -> Self {
        match error {
            MemoryError::ServiceResponse(error) => Self::ServiceResponse(error),
            error => Self::StorageBackend(error.to_string()),
        }
    }
}
