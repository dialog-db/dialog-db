//! Error types for the replica system.

use dialog_storage::DialogStorageError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::types::{BranchId, Site};

/// The common error type used by the replica system.
#[derive(Error, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaError {
    /// Branch with the given ID was not found
    #[error("Branch {id} not found")]
    BranchNotFound {
        /// The ID of the branch that was not found
        id: BranchId,
    },

    /// A storage operation failed
    #[error("Storage error {0}")]
    StorageError(String),

    /// Branch has no configured upstream
    #[error("Branch {id} has no upstream")]
    BranchHasNoUpstream {
        /// The ID of the branch that has no upstream
        id: BranchId,
    },

    /// Pushing a revision failed
    #[error("Pushing revision failed cause {cause}")]
    PushFailed {
        /// The underlying error
        cause: DialogStorageError,
    },

    /// Remote repository not found
    #[error("Remote {remote} not found")]
    RemoteNotFound {
        /// Remote site identifier
        remote: Site,
    },

    /// Remote repository already exists
    #[error("Remote {remote} already exist")]
    RemoteAlreadyExists {
        /// Remote site identifier
        remote: Site,
    },

    /// Connection to remote repository failed
    #[error("Connection to remote {remote} failed")]
    RemoteConnectionError {
        /// Remote site identifier
        remote: Site,
    },

    /// Branch upstream is set to itself
    #[error("Upstream of local {id} is set to itself")]
    BranchUpstreamIsItself {
        /// Branch identifier
        id: BranchId,
    },
}

impl ReplicaError {
    /// Create a new storage error with operation context.
    pub fn storage_error(context: OperationContext, cause: DialogStorageError) -> Self {
        ReplicaError::StorageError(format!("{}: {:?}", context, cause))
    }
}

/// Identifies which operation failed when a storage error occurs.
/// Used in [`ReplicaError::StorageError`] to provide context about where the failure happened.
#[derive(Error, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationContext {
    /// Failed while resolving a branch by ID
    #[error("ResolveBranch")]
    ResolveBranch,

    /// Failed while resolving a revision
    #[error("ResolveRevision")]
    ResolveRevision,

    /// Failed while updating a revision
    #[error("UpdateRevision")]
    UpdateRevision,

    /// Failed during archive operation
    #[error("ArchiveError")]
    ArchiveError,

    /// Failed during encoding operation
    #[error("EncodeError")]
    EncodeError,
}

