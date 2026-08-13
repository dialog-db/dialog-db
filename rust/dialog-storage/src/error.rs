use dialog_capability::access::AuthorizeError;
use dialog_effects::Rejection;
use dialog_effects::archive::ArchiveError;
use dialog_effects::memory::MemoryError;
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
    Storage(String),

    /// The request was not authorized.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),

    /// The request was not carried out, for a reason that is not an
    /// access decision.
    #[error(transparent)]
    Rejected(#[from] Rejection),

    /// An error that occurs when byte hash verification fails
    #[error("Byte hash verification failed: {0}")]
    Verification(String),
}

impl From<ArchiveError> for DialogStorageError {
    fn from(error: ArchiveError) -> Self {
        match error {
            ArchiveError::Authorization(error) => Self::Authorization(error),
            ArchiveError::Rejected(error) => Self::Rejected(error),
            error => Self::Storage(error.to_string()),
        }
    }
}

impl From<MemoryError> for DialogStorageError {
    fn from(error: MemoryError) -> Self {
        match error {
            MemoryError::Authorization(error) => Self::Authorization(error),
            MemoryError::Rejected(error) => Self::Rejected(error),
            error => Self::Storage(error.to_string()),
        }
    }
}
