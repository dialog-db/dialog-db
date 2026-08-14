/// Errors that can occur when opening a profile.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    /// A profile already exists at this location.
    ///
    /// Only `create` raises it; `open_or_create` treats the same
    /// condition as success and loads what is there.
    #[error("Profile already exists")]
    AlreadyExists,

    /// No profile exists at this location.
    ///
    /// Only `load` raises it, for the same reason inverted.
    #[error("Profile not found")]
    NotFound,

    /// Storage operation failed.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Key generation or import failed.
    #[error("Key error: {0}")]
    Key(String),
}
