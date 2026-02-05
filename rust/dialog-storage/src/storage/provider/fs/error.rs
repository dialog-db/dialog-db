//! Error types for filesystem storage operations.

/// Errors that can occur during filesystem operations.
#[derive(Debug, thiserror::Error)]
pub enum FileSystemError {
    /// I/O operation failed.
    #[error("Filesystem I/O error: {0}")]
    Io(String),

    /// Lock acquisition failed.
    #[error("Lock error: {0}")]
    Lock(String),

    /// CAS condition failed.
    #[error("CAS condition failed: {0}")]
    Cas(String),

    /// Path containment violation (attempted to escape base directory).
    #[error("Containment violation: {0}")]
    Containment(String),
}
