//! Mount, Load, and Save providers for FileStore and FileSystem.

use super::{Address, FileStore};
use async_trait::async_trait;
use dialog_capability::storage::{Load, Location, Save, StorageError};
use dialog_capability::{Capability, Policy, Provider};

#[async_trait]
impl Provider<Load<Vec<u8>, Address>> for FileStore {
    async fn execute(
        &self,
        input: Capability<Load<Vec<u8>, Address>>,
    ) -> Result<Vec<u8>, StorageError> {
        let address = Location::of(&input).address();
        let location = self
            .resolve(address.path())
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        location
            .read()
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))
    }
}

#[async_trait]
impl Provider<Save<Vec<u8>, Address>> for FileStore {
    async fn execute(&self, input: Capability<Save<Vec<u8>, Address>>) -> Result<(), StorageError> {
        let address = Location::of(&input).address();
        let bytes = &Save::<Vec<u8>, Address>::of(&input).content;
        let location = self
            .resolve(address.path())
            .map_err(|e| StorageError::Storage(e.to_string()))?;
        location
            .write(bytes)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))
    }
}
