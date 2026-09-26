/// Errors from opening a credential or saving a delegation.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// A credential already exists at this location.
    ///
    /// Only `create` raises it; `open` treats the same condition as
    /// success and loads what is there.
    #[error("Credential already exists")]
    AlreadyExists,

    /// No credential exists at this location.
    ///
    /// Only `load` raises it, for the same reason inverted.
    #[error("Credential not found")]
    NotFound,

    /// Storage operation failed.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Key generation or import failed.
    #[error("Key error: {0}")]
    Key(String),
}
