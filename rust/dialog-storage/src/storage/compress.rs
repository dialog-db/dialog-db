use std::io::{Cursor, Read, Write};

use async_trait::async_trait;
use brotli::{CompressorWriter, Decompressor};
use dialog_common::ConditionalSync;

use crate::DialogStorageError;

use super::StorageBackend;

/// A layer over a [`StorageBackend`] that brotli-compresses incoming writes,
/// and decompresses outgoing reads.
// TODO: Should we tag compressed blobs to distinguish them from uncompressed blobs?
#[derive(Clone)]
pub struct CompressedStorage<Backend> {
    backend: Backend,
}

impl<Backend> CompressedStorage<Backend> {
    /// Wrap the provided `backend` in a compression layer
    pub fn new(backend: Backend) -> Self {
        Self { backend }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> StorageBackend for CompressedStorage<Backend>
where
    Backend: StorageBackend + ConditionalSync,
    Backend::Value: From<Vec<u8>> + AsRef<[u8]>,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let mut compressed = Cursor::new(Vec::new());

        {
            let mut writer = CompressorWriter::new(&mut compressed, 4096, 9, 20);
            writer.write(value.as_ref()).map_err(|error| {
                DialogStorageError::StorageBackend(format!("Could not compress blob: {error}"))
            })?;
        }

        self.backend
            .set(key, compressed.into_inner().into())
            .await
            .map_err(|error| error.into())
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        if let Some(value) = self.backend.get(key).await.map_err(|error| error.into())? {
            let mut decompressed = Vec::new();
            {
                let mut reader = Decompressor::new(value.as_ref(), 4096);
                reader.read_to_end(&mut decompressed).map_err(|error| {
                    DialogStorageError::StorageBackend(format!(
                        "Could not decompress blob: {error}"
                    ))
                })?;
            }
            Ok(Some(decompressed.into()))
        } else {
            Ok(None)
        }
    }
}
