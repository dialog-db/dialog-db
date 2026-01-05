//! Error types for storage operations.

use dialog_storage::DialogStorageError;
use thiserror::Error;

/// Error type for Memory and Store effects.
///
/// This unified error type covers both local storage and remote/network errors.
#[derive(Debug, Clone, Error)]
pub enum MemoryError {
    /// Generic storage error.
    #[error("Storage error: {0}")]
    Storage(String),
    /// Key not found.
    #[error("Not found: {0}")]
    NotFound(String),
    /// Compare-and-swap conflict.
    #[error("Conflict: {0}")]
    Conflict(String),
    /// Network error (for remote operations).
    #[error("Network error: {0}")]
    Network(String),
    /// Unknown site (for remote operations).
    #[error("Unknown site: {0}")]
    UnknownSite(String),
    /// Connection failed (for remote operations).
    #[error("Connection failed: {0}")]
    Connection(String),
    /// Timeout (for remote operations).
    #[error("Timeout: {0}")]
    Timeout(String),
}

impl From<DialogStorageError> for MemoryError {
    fn from(e: DialogStorageError) -> Self {
        MemoryError::Storage(format!("{:?}", e))
    }
}

impl From<std::convert::Infallible> for MemoryError {
    fn from(e: std::convert::Infallible) -> Self {
        match e {}
    }
}

// Backwards compatibility aliases
/// Alias for MemoryError (backwards compatibility with local storage code).
pub type StorageError = MemoryError;
/// Alias for MemoryError (backwards compatibility with network code).
pub type NetworkError = MemoryError;
