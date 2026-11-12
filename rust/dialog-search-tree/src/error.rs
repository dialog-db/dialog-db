use dialog_capability::access::{AuthorizationFailure, AuthorizeError};
use dialog_effects::Rejection;
use dialog_storage::DialogStorageError;
use thiserror::Error;

/// Errors that can occur when working with search trees.
#[derive(Error, Debug)]
pub enum DialogSearchTreeError {
    /// An error that occurs when accessing a node.
    #[error("Problem accessing node: {0}")]
    Node(String),

    /// An error from the storage backend.
    #[error("{0}")]
    Storage(#[source] DialogStorageError),

    /// An error that occurs during a tree operation.
    #[error("Failed to operate the tree: {0}")]
    Operation(String),

    /// An error that occurs when accessing part of the tree.
    #[error("Failed to access part of the tree: {0}")]
    Access(String),

    /// An error that occurs when interpreting bytes.
    #[error("Failed to interpret bytes: {0}")]
    Encoding(String),
}

impl DialogSearchTreeError {
    /// The rejection this error carries, when the storage beneath the tree
    /// refused the request for a reason that is not an access decision.
    pub fn rejection(&self) -> Option<&Rejection> {
        match self {
            Self::Storage(DialogStorageError::Rejected(rejection)) => Some(rejection),
            _ => None,
        }
    }
}

/// An access decision the storage beneath the tree made survives the hop
/// through the tree's error, so a caller can tell a withdrawn authority
/// from a store that failed.
impl AuthorizationFailure for DialogSearchTreeError {
    fn authorization_failure(&self) -> Option<&AuthorizeError> {
        match self {
            Self::Storage(error) => error.authorization_failure(),
            _ => None,
        }
    }
}

impl From<DialogStorageError> for DialogSearchTreeError {
    fn from(value: DialogStorageError) -> Self {
        DialogSearchTreeError::Storage(value)
    }
}
