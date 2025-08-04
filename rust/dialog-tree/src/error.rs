use dialog_encoding::DialogEncodingError;
use dialog_storage::DialogStorageError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DialogTreeError {
    #[error("Problem with tree entry: {0}")]
    Entry(String),

    #[error("Problem accessing node: {0}")]
    Node(String),

    #[error("{0}")]
    Encoding(DialogEncodingError),

    #[error("{0}")]
    Storage(DialogStorageError),

    #[error("Failed to operate the tree: {0}")]
    Operation(String),

    #[error("Failed to retrieve item in cache: {0}")]
    Cache(String),
}

impl From<DialogStorageError> for DialogTreeError {
    fn from(value: DialogStorageError) -> Self {
        DialogTreeError::Storage(value)
    }
}

impl From<DialogEncodingError> for DialogTreeError {
    fn from(value: DialogEncodingError) -> Self {
        DialogTreeError::Encoding(value)
    }
}
