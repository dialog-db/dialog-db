use thiserror::Error;
use x_prolly_tree::XProllyTreeError;
use x_storage::XStorageError;

/// The common error type used by this crate
#[derive(Error, Debug, PartialEq)]
pub enum XFactsError {
    /// An error occured in storage-related code
    #[error("Storage operation failed: {0}")]
    Storage(String),

    /// An error occurred in prolly tree-related code
    #[error("Tree operation failed: {0}")]
    Tree(String),

    /// A database index was not shaped as expected
    #[error("Malformed database index: {0}")]
    MalformedIndex(String),

    /// Raw bytes could not be interpreted as a version
    #[error("Could not convert bytes into version: {0}")]
    InvalidVersion(String),

    /// Raw bytes could not be interpreted as a database index key
    #[error("Could not convert bytes into key: {0}")]
    InvalidKey(String),

    /// Raw bytes could not be interpreted as a typed value
    #[error("Could not convert bytes into value: {0}")]
    InvalidValue(String),

    /// Raw bytes could not be interpreted as a datum state (asserted or retracted)
    #[error("Could not convert bytes into state: {0}")]
    InvalidState(String),

    /// Raw bytes could not be interpreted as an attribute
    #[error("Invalid attribute: {0}")]
    InvalidAttribute(String),

    /// Raw bytes could not be interpreted as an entity
    #[error("Could not convert bytes into entity: {0}")]
    InvalidEntity(String),
}

impl From<XStorageError> for XFactsError {
    fn from(value: XStorageError) -> Self {
        XFactsError::Storage(format!("{value}"))
    }
}

impl From<XProllyTreeError> for XFactsError {
    fn from(value: XProllyTreeError) -> Self {
        XFactsError::Tree(format!("{value}"))
    }
}
