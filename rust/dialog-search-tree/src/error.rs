use dialog_storage::DialogStorageError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DialogSearchTreeError {
    #[error("Problem with tree entry: {0}")]
    Entry(String),

    #[error("Problem accessing node: {0}")]
    Node(String),

    #[error("{0}")]
    Storage(DialogStorageError),

    #[error("Failed to operate the tree: {0}")]
    Operation(String),

    #[error("Failed to retrieve item in cache: {0}")]
    Cache(String),

    #[error("Failed to access part of the tree: {0}")]
    Access(String),

    #[error("Failed to interpret bytes: {0}")]
    Encoding(String),
}

impl From<DialogStorageError> for DialogSearchTreeError {
    fn from(value: DialogStorageError) -> Self {
        DialogSearchTreeError::Storage(value)
    }
}
