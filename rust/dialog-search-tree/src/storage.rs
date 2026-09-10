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
                // TEMPORARY (#492): say WHAT came back, not just that it was
                // wrong. A bare "did not match" cannot distinguish a
                // truncated body, an error document stored as block bytes,
                // or a write-back keyed by the wrong digest — and the app's
                // keepalive retries the same block forever on this error, so
                // the difference is the whole diagnosis.
                let actual = Blake3Hash::hash(&bytes);
                let head: Vec<u8> = bytes.iter().take(32).copied().collect();
                dialog_common::probe(&format!(
                    "VERIFY FAIL wanted={identity} got={actual} len={} head={head:02x?} \
                     as_text={:?}",
                    bytes.len(),
                    String::from_utf8_lossy(&head),
                ));
                return Err(DialogStorageError::Verification(format!(
                    "Retrieved bytes did not match the provided hash \
                     (wanted {identity}, got {actual}, {} bytes)",
                    bytes.len()
                )));
            }

            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }
}
