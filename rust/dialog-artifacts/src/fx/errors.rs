//! Error types for storage and network operations.

use dialog_storage::DialogStorageError;
use thiserror::Error;

/// Error type for local storage operations.
#[derive(Debug, Clone, Error)]
pub enum StorageError {
    /// Generic storage error.
    #[error("Storage error: {0}")]
    Storage(String),
    /// Key not found.
    #[error("Not found: {0}")]
    NotFound(String),
    /// Compare-and-swap conflict.
    #[error("Conflict: {0}")]
    Conflict(String),
}

impl From<DialogStorageError> for StorageError {
    fn from(e: DialogStorageError) -> Self {
        StorageError::Storage(format!("{:?}", e))
    }
}

impl From<std::convert::Infallible> for StorageError {
    fn from(e: std::convert::Infallible) -> Self {
        match e {}
    }
}

/// Error type for network/remote operations.
#[derive(Debug, Clone, Error)]
pub enum NetworkError {
    /// Generic network error.
    #[error("Network error: {0}")]
    Network(String),
    /// Unknown site.
    #[error("Unknown site: {0}")]
    UnknownSite(String),
    /// Connection failed.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    /// Timeout.
    #[error("Timeout: {0}")]
    Timeout(String),
}

impl From<DialogStorageError> for NetworkError {
    fn from(e: DialogStorageError) -> Self {
        NetworkError::Network(format!("{:?}", e))
    }
}

impl From<std::convert::Infallible> for NetworkError {
    fn from(e: std::convert::Infallible) -> Self {
        match e {}
    }
}
