//! Error types for FS-remote operations.

use dialog_effects::archive::ArchiveError;
use dialog_effects::memory::MemoryError;
use thiserror::Error;

/// Error type for FS-remote operations.
#[derive(Debug, Error)]
pub enum FsError {
    /// The requested directory handle was not registered with the FS provider
    /// before the invocation fired. The caller must register the handle
    /// (via the thread-local registry) before any invocation is dispatched
    /// against this address.
    #[error("Unregistered FS handle: {0}")]
    UnregisteredHandle(String),

    /// I/O error against the underlying filesystem (or FS Access API).
    #[error("FS I/O error: {0}")]
    Io(String),

    /// The captured request's path escapes the registered directory root.
    /// Should never happen with well-formed capabilities — indicates a bug
    /// in request translation.
    #[error("Path containment error: {0}")]
    Containment(String),

    /// Failed to acquire a write lock for a CAS operation.
    #[error("Lock error: {0}")]
    Lock(String),
}

impl From<FsError> for ArchiveError {
    fn from(error: FsError) -> Self {
        ArchiveError::Io(error.to_string())
    }
}

impl From<FsError> for MemoryError {
    fn from(error: FsError) -> Self {
        MemoryError::Storage(error.to_string())
    }
}
