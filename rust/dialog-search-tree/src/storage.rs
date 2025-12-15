use std::sync::Arc;

use dialog_common::Blake3Hash;

use dialog_storage::{DialogStorageError, StorageBackend};

pub struct ContentAddressedStorage<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    backend: Backend,
}

impl<Backend> ContentAddressedStorage<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    pub fn new(backend: Backend) -> Self {
        Self { backend }
    }

    pub async fn store(
        &mut self,
        bytes: Vec<u8>,
        expected_identity: &Blake3Hash,
    ) -> Result<(), DialogStorageError> {
        if !expected_identity.matches(&bytes) {
            return Err(DialogStorageError::Verification(format!(
                "Cannot store the provided bytes"
            )));
        }

        self.backend.set(expected_identity.clone(), bytes).await?;

        Ok(())
    }

    pub async fn retrieve(
        &self,
        identity: &Blake3Hash,
    ) -> Result<Option<Vec<u8>>, DialogStorageError> {
        if let Some(bytes) = self.backend.get(identity).await? {
            if !identity.matches(&bytes) {
                return Err(DialogStorageError::Verification(format!(
                    "Retrieved bytes did not match the provided hash"
                )));
            }

            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }
}
