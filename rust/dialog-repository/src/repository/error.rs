use dialog_effects::archive::ArchiveError;
use dialog_effects::memory::MemoryError;
use dialog_effects::storage::StorageError;
use dialog_storage::DialogStorageError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::branch::BranchName;
use super::remote::RemoteName;

/// The common error type used by repository operations.
#[derive(Error, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepositoryError {
    /// Branch with the given name was not found
    #[error("Branch {name} not found")]
    BranchNotFound {
        /// The name of the branch that was not found
        name: BranchName,
    },

    /// A storage operation failed
    #[error("Storage error {0}")]
    StorageError(String),

    /// Branch has no configured upstream
    #[error("Branch {name} has no upstream")]
    BranchHasNoUpstream {
        /// The name of the branch that has no upstream
        name: BranchName,
    },

    /// Pushing a revision failed
    #[error("Pushing revision failed: {cause}")]
    PushFailed {
        /// The underlying error message
        cause: String,
    },

    /// Remote repository not found
    #[error("Remote {remote} not found")]
    RemoteNotFound {
        /// Remote site name
        remote: RemoteName,
    },
    /// Remote repository already exists
    #[error("Remote {remote} already exist")]
    RemoteAlreadyExists {
        /// Remote site name
        remote: RemoteName,
    },
    /// Connection to remote repository failed
    #[error("Connection to remote {remote} failed")]
    RemoteConnectionError {
        /// Remote site name
        remote: RemoteName,
    },

    /// Repository not found
    #[error("Repository '{0}' not found")]
    NotFound(String),

    /// Repository already exists
    #[error("Repository '{0}' already exists")]
    AlreadyExists(String),

    /// Branch upstream is set to itself
    #[error("Upsteam of local {name} is set to itself")]
    BranchUpstreamIsItself {
        /// Branch name
        name: BranchName,
    },

    /// Invalid internal state (should never happen in normal operation)
    #[error("Invalid state: {message}")]
    InvalidState {
        /// Description of the invalid state
        message: String,
    },
}

impl From<StorageError> for RepositoryError {
    fn from(e: StorageError) -> Self {
        Self::StorageError(e.to_string())
    }
}

impl From<ArchiveError> for RepositoryError {
    fn from(e: ArchiveError) -> Self {
        Self::StorageError(e.to_string())
    }
}

impl From<MemoryError> for RepositoryError {
    fn from(e: MemoryError) -> Self {
        Self::StorageError(e.to_string())
    }
}

impl From<DialogStorageError> for RepositoryError {
    fn from(e: DialogStorageError) -> Self {
        Self::StorageError(e.to_string())
    }
}
