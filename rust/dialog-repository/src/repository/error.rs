use dialog_credentials::Ed25519SignerError;
use dialog_effects::archive::ArchiveError;
use dialog_effects::memory::MemoryError;
use dialog_effects::storage::StorageError;
use dialog_storage::DialogStorageError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The common error type used by repository operations.
#[derive(Error, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepositoryError {
    /// A storage operation failed
    #[error("Storage error {0}")]
    StorageError(String),

    /// Repository not found
    #[error("Repository '{0}' not found")]
    NotFound(String),

    /// Repository already exists
    #[error("Repository '{0}' already exists")]
    AlreadyExists(String),

    /// Invalid internal state (should never happen in normal operation)
    #[error("Invalid state: {message}")]
    InvalidState {
        /// Description of the invalid state
        message: String,
    },
}

impl From<StorageError> for RepositoryError {
    fn from(e: StorageError) -> Self {
        Self::StorageError(e.to_string())
    }
}

impl From<ArchiveError> for RepositoryError {
    fn from(e: ArchiveError) -> Self {
        Self::StorageError(e.to_string())
    }
}

impl From<MemoryError> for RepositoryError {
    fn from(e: MemoryError) -> Self {
        Self::StorageError(e.to_string())
    }
}

impl From<DialogStorageError> for RepositoryError {
    fn from(e: DialogStorageError) -> Self {
        Self::StorageError(e.to_string())
    }
}

impl From<Ed25519SignerError> for RepositoryError {
    fn from(e: Ed25519SignerError) -> Self {
        Self::StorageError(e.to_string())
    }
}
