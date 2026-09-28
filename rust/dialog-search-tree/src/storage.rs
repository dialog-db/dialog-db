use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer, ConditionalSend, ConditionalSync};

use dialog_storage::{DialogStorageError, StorageBackend};

use crate::{DialogSearchTreeError, Load};

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
        + ConditionalSend,
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

/// Serves the tree's [`Load`] from a storage backend.
///
/// A bridge for callers that still hold a [`StorageBackend`] while the
/// capability model replaces them. The tree checks what it loads, so this
/// reads the backend without checking the bytes a second time.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> Provider<Load> for ContentAddressedStorage<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    async fn execute(&self, hash: Blake3Hash) -> Result<Option<Buffer>, DialogSearchTreeError> {
        Ok(self.backend.get(&hash).await?.map(Buffer::from))
    }
}
