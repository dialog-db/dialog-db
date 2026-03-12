use thiserror::Error;

/// Errors produced by blob storage operations.
#[derive(Error, Debug)]
pub enum DialogBlobsError {
    /// A write or move operation failed.
    #[error("Failed to put blob bytes: {0}")]
    Put(String),
    /// A read operation failed.
    #[error("Failed to get blob bytes: {0}")]
    Get(String),
    /// The requested file or directory does not exist.
    #[error("Not found: {0}")]
    NotFound(String),
    /// The storage backend could not be set up (e.g. missing data directory).
    #[error("Failed to initialize blob storage: {0}")]
    Initialization(String),
    /// A caller-supplied path could not be resolved within the root directory.
    #[error("Could not resolve path: {0}")]
    Path(String),
}
