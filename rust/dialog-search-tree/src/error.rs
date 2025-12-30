use dialog_storage::DialogStorageError;
use thiserror::Error;

/// Errors that can occur when working with search trees.
#[derive(Error, Debug)]
pub enum DialogSearchTreeError {
    /// An error that occurs when working with a tree entry.
    #[error("Problem with tree entry: {0}")]
    Entry(String),

    /// An error that occurs when accessing a node.
    #[error("Problem accessing node: {0}")]
    Node(String),

    /// An error from the storage backend.
    #[error("{0}")]
    Storage(DialogStorageError),

    /// An error that occurs during a tree operation.
    #[error("Failed to operate the tree: {0}")]
    Operation(String),

    /// An error that occurs when retrieving an item from the cache.
    #[error("Failed to retrieve item in cache: {0}")]
    Cache(String),

    /// An error that occurs when accessing part of the tree.
    #[error("Failed to access part of the tree: {0}")]
    Access(String),

    /// An error that occurs when interpreting bytes.
    #[error("Failed to interpret bytes: {0}")]
    Encoding(String),
}

impl From<DialogStorageError> for DialogSearchTreeError {
    fn from(value: DialogStorageError) -> Self {
        DialogSearchTreeError::Storage(value)
    }
}
