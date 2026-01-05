//! Error types for the effectful replica system.

pub use crate::replica::{OperationContext, ReplicaError};

use crate::fx::archive::ArchiveError;
use crate::fx::errors::MemoryError;

impl From<MemoryError> for ReplicaError {
    fn from(err: MemoryError) -> Self {
        ReplicaError::StorageError(err.to_string())
    }
}

impl From<ArchiveError> for ReplicaError {
    fn from(err: ArchiveError) -> Self {
        ReplicaError::StorageError(err.to_string())
    }
}
