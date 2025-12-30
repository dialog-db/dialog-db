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

    /// An error that occurs when byte hash verification fails
    #[error("Byte hash verification failed: {0}")]
    Verification(String),
}
