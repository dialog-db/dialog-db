use dialog_common::{Blake3Hash, ConditionalSend};

use dialog_storage::{DialogStorageError, StorageBackend};

/// Content-addressed storage wrapper for tree nodes.
///
/// Provides hash-verified storage and retrieval operations.
#[derive(Clone, Debug)]
pub struct ContentAddressedStorage<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    backend: Backend,
}

impl<Backend> ContentAddressedStorage<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSend
        + 'static,
{
    /// Creates a new content-addressed storage wrapper.
    pub fn new(backend: Backend) -> Self {
        Self { backend }
    }

    /// Get a reference to the interior `StorageBackend`
    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    /// Get a mutable reference to the interior `StorageBackend`
    pub fn backend_mut(&mut self) -> &mut Backend {
        &mut self.backend
    }

    /// Stores bytes under their content hash, verifying the hash matches.
    pub async fn store(
        &mut self,
        bytes: Vec<u8>,
        expected_identity: &Blake3Hash,
    ) -> Result<(), DialogStorageError> {
        if !expected_identity.matches(&bytes) {
            return Err(DialogStorageError::Verification(
                "Cannot store the provided bytes".to_string(),
            ));
        }

        self.backend.set(expected_identity.clone(), bytes).await?;

        Ok(())
    }

    /// Retrieves bytes by their content hash, verifying the hash matches.
    pub async fn retrieve(
        &self,
        identity: &Blake3Hash,
    ) -> Result<Option<Vec<u8>>, DialogStorageError> {
        if let Some(bytes) = self.backend.get(identity).await? {
            if !identity.matches(&bytes) {
                return Err(DialogStorageError::Verification(
                    "Retrieved bytes did not match the provided hash".to_string(),
                ));
            }

            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }
}
