/// Errors that can occur when opening a profile.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    /// Storage operation failed.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Key generation or import failed.
    #[error("Key error: {0}")]
    Key(String),
}
