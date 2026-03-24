//! Error types for environment operations.

/// Error opening an environment.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// Storage backend error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Key generation or import error.
    #[error("Key error: {0}")]
    Key(String),
}
