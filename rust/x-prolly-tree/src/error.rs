use thiserror::Error;
use x_storage::XStorageError;

/// The common error type used by this crate
#[derive(Error, Debug)]
pub enum XProllyTreeError {
    /// There was an attempt to access the tree in an incorrect way
    #[error("Incorrect tree access: {0}")]
    IncorrectTreeAccess(String),

    /// The tree as constructed is not valid
    #[error("Invalid tree construction: {0}")]
    InvalidConstruction(String),

    /// There was a problem when accessing storage
    #[error("Storage error: {0}")]
    Storage(XStorageError),

    /// A required block is missing from storage
    #[error("Block not found in storage: {0}")]
    MissingBlock(String),

    /// The tree did not match the expected shape
    #[error("Tree did not match expected shape: {0}")]
    UnexpectedTreeShape(String),
}

impl From<XStorageError> for XProllyTreeError {
    fn from(value: XStorageError) -> Self {
        XProllyTreeError::Storage(value)
    }
}

// TODO: This is probably an overly-broad conversion
impl From<XProllyTreeError> for XStorageError {
    fn from(value: XProllyTreeError) -> Self {
        XStorageError::EncodeFailed(format!("{value}"));
        todo!();
    }
}
