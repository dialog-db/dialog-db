use dialog_storage::DialogStorageError;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FmtResult};
use thiserror::Error;

use super::branch::BranchName;
use super::remote::SiteName;

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
        remote: SiteName,
    },
    /// Remote repository already exists
    #[error("Remote {remote} already exist")]
    RemoteAlreadyExists {
        /// Remote site name
        remote: SiteName,
    },
    /// Connection to remote repository failed
    #[error("Connection to remote {remote} failed")]
    RemoteConnectionError {
        /// Remote site name
        remote: SiteName,
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

impl RepositoryError {
    /// Create a new storage error
    pub fn storage_error(capability: OperationKind, cause: DialogStorageError) -> Self {
        RepositoryError::StorageError(format!("{}: {:?}", capability, cause))
    }
}

/// Identifies which operation failed when a storage error occurs.
/// Used in [`RepositoryError::StorageError`] to provide context about where the failure happened.
#[derive(Error, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationKind {
    /// Failed while resolving a branch by name
    ResolveBranch,
    /// Failed while resolving a revision
    ResolveRevision,
    /// Failed while updating a revision
    UpdateRevision,

    /// Failed during archive operation
    ArchiveError,

    /// Failed during encoding operation
    EncodeError,
}

impl Display for OperationKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            OperationKind::ResolveBranch => write!(f, "ResolveBranch"),
            OperationKind::ResolveRevision => write!(f, "ResolveRevision"),
            OperationKind::UpdateRevision => write!(f, "UpdateRevision"),
            OperationKind::ArchiveError => write!(f, "ArchiveError"),
            OperationKind::EncodeError => write!(f, "EncodeError"),
        }
    }
}
