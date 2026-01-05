//! Remote repository types and state.
//!
//! This module contains types for representing remote repository connections
//! and their branch state.

use dialog_storage::{CborEncoder, RestStorageBackend, RestStorageConfig};
use serde::{Deserialize, Serialize};

use crate::platform::{ErrorMappingBackend, Storage as PlatformStorage};

use super::error::ReplicaError;
use super::types::{BranchId, Revision, Site};

/// Type alias for the backend used to connect to remote storage.
pub type RemoteBackend = ErrorMappingBackend<RestStorageBackend<Vec<u8>, Vec<u8>>>;

/// State of a remote branch.
///
/// Tracks the last known revision of a branch on a remote repository.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteBranchState {
    /// Site of the branch.
    pub site: Site,
    /// Branch id.
    pub id: BranchId,
    /// Revision that was fetched last.
    pub revision: Revision,
}

/// State information for a remote repository connection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteState {
    /// Name for this remote.
    pub site: Site,
    /// Address used to configure this remote.
    pub address: RestStorageConfig,
}

impl RemoteState {
    /// Creates a storage connection using this remote's configuration.
    pub fn connect(&self) -> Result<PlatformStorage<RemoteBackend>, ReplicaError> {
        let backend = RestStorageBackend::new(self.address.clone()).map_err(|_| {
            ReplicaError::RemoteConnectionError {
                remote: self.site.clone(),
            }
        })?;

        Ok(PlatformStorage::new(
            ErrorMappingBackend::new(backend),
            CborEncoder,
        ))
    }
}
