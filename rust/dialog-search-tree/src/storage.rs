use std::sync::Arc;

use dialog_common::{Blake3Hash, ConditionalSend};

use dialog_storage::{DialogStorageError, StorageBackend};

use crate::NodeCipher;

/// Content-addressed storage wrapper for tree nodes.
///
/// Provides hash-verified storage and retrieval operations.
///
/// With a [`NodeCipher`] attached (see [`with_cipher`](Self::with_cipher)) the
/// same operations seal what they write and open what they read, and file it
/// under an address the backend cannot tie back to any content. Callers above
/// this layer are unaffected: they keep passing the content identities the
/// tree computes.
#[derive(Clone, Debug)]
pub struct ContentAddressedStorage<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    backend: Backend,
    cipher: Option<Arc<dyn NodeCipher>>,
}

impl<Backend> ContentAddressedStorage<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSend,
{
    /// Creates a new content-addressed storage wrapper.
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            cipher: None,
        }
    }

    /// Creates a wrapper that seals every node it writes.
    ///
    /// The tree above is unchanged — it addresses nodes by the same content
    /// identities either way. What differs is what the backend holds and what
    /// it is filed under.
    pub fn with_cipher(backend: Backend, cipher: Arc<dyn NodeCipher>) -> Self {
        Self {
            backend,
            cipher: Some(cipher),
        }
    }

    /// Whether nodes written through this wrapper are sealed.
    pub fn is_sealed(&self) -> bool {
        self.cipher.is_some()
    }

    /// The address `identity` is filed under in the backend.
    fn address(&self, identity: &Blake3Hash) -> Blake3Hash {
        self.cipher
            .as_ref()
            .map_or_else(|| identity.clone(), |cipher| cipher.address(identity))
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

        // The identity is checked against the plaintext before sealing, so a
        // sealed store rejects mismatched bytes for the same reason a plain
        // one does rather than writing a blob nothing can address.
        let bytes = match &self.cipher {
            Some(cipher) => cipher.seal(&bytes)?,
            None => bytes,
        };

        self.backend
            .set(self.address(expected_identity), bytes)
            .await?;

        Ok(())
    }

    /// Retrieves bytes by their content hash, verifying the hash matches.
    pub async fn retrieve(
        &self,
        identity: &Blake3Hash,
    ) -> Result<Option<Vec<u8>>, DialogStorageError> {
        if let Some(bytes) = self.backend.get(&self.address(identity)).await? {
            let bytes = match &self.cipher {
                Some(cipher) => cipher.open(&bytes)?,
                None => bytes,
            };

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
