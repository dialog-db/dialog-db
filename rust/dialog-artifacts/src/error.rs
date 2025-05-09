use dialog_prolly_tree::DialogProllyTreeError;
use dialog_storage::DialogStorageError;
use thiserror::Error;

/// The common error type used by this crate
#[derive(Error, Debug, PartialEq)]
pub enum DialogArtifactsError {
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

    /// A causal reference was invalid
    #[error("Could not convert bytes into reference: {0}")]
    InvalidReference(String),

    /// Raw bytes could not be interpreted as a datum state (asserted or retracted)
    #[error("Could not convert bytes into state: {0}")]
    InvalidState(String),

    /// Raw bytes could not be interpreted as an attribute
    #[error("Invalid attribute: {0}")]
    InvalidAttribute(String),

    /// Raw bytes could not be interpreted as an entity
    #[error("Could not convert bytes into entity: {0}")]
    InvalidEntity(String),

    /// An attempt to export the database failed
    #[error("Could not export data: {0}")]
    Export(String),

    /// Attempted to query with an unconstrained [`ArtifactSelector`]
    #[error("An artifact selector must specify at least one field")]
    EmptySelector,
}

impl From<DialogStorageError> for DialogArtifactsError {
    fn from(value: DialogStorageError) -> Self {
        DialogArtifactsError::Storage(format!("{value}"))
    }
}

impl From<DialogProllyTreeError> for DialogArtifactsError {
    fn from(value: DialogProllyTreeError) -> Self {
        DialogArtifactsError::Tree(format!("{value}"))
    }
}
