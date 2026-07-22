use dialog_effects::archive::ArchiveError;
use dialog_effects::memory::MemoryError;
use dialog_search_tree::DialogSearchTreeError;
use dialog_storage::DialogStorageError;
use thiserror::Error;

use crate::ValueDataType;

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
    InvalidRevision(String),

    /// Raw bytes could not be interpreted as a database index key
    #[error("Could not convert bytes into key: {0}")]
    InvalidKey(String),

    /// Could not interpret some string as a URI
    #[error("Could not parse as URI: {0}")]
    InvalidUri(String),

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

    /// The attribute belongs to the reserved `dialog.` namespace, which
    /// only version-control machinery may write
    #[error("Reserved attribute (the dialog. namespace is reserved): {0}")]
    ReservedAttribute(String),

    /// Raw bytes could not be interpreted as an entity
    #[error("Could not convert bytes into entity: {0}")]
    InvalidEntity(String),

    /// An attempt to export the database failed
    #[error("Could not export data: {0}")]
    Export(String),

    /// Attempted to query with an unconstrained [`ArtifactSelector`]
    #[error("An artifact selector must specify at least one field")]
    EmptySelector,

    /// A revision signature or structural integrity check failed
    #[error("Invalid revision signature: {0}")]
    InvalidSignature(String),

    /// Causal ordering could not be determined because claims for the given
    /// version have not been replicated yet
    #[error("Incomplete history: missing claims for version {0}")]
    IncompleteHistory(String),
}

impl From<DialogStorageError> for DialogArtifactsError {
    fn from(value: DialogStorageError) -> Self {
        DialogArtifactsError::Storage(format!("{value}"))
    }
}

impl From<ArchiveError> for DialogArtifactsError {
    fn from(e: ArchiveError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<MemoryError> for DialogArtifactsError {
    fn from(e: MemoryError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<DialogSearchTreeError> for DialogArtifactsError {
    fn from(value: DialogSearchTreeError) -> Self {
        DialogArtifactsError::Tree(format!("{value}"))
    }
}

/// Errors created when types are used inconsistently with value.
#[derive(Error, Debug, PartialEq)]
pub enum TypeError {
    /// Expected type and actual type mismatch.
    #[error("Type mismatch: expected {0}, got {1}")]
    TypeMismatch(ValueDataType, ValueDataType),
}
