use thiserror::Error;

/// The common error type used by this crate
#[derive(Error, Debug)]
pub enum XStorageError {
    /// An error that occurs during block encoding
    #[error("Failed to encode a block: {0}")]
    EncodeFailed(String),

    /// An error that occurs during block decoding
    #[error("Failed to decode a block: {0}")]
    DecodeFailed(String),
}
