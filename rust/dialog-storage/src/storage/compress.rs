use std::io::{Cursor, Read, Write};

use async_trait::async_trait;
use brotli::{CompressorWriter, Decompressor};
use dialog_common::ConditionalSync;

use crate::DialogStorageError;

use super::{Resource, StorageBackend};

const BUFFER_SIZE: usize = 4096;

/// A layer over a [`StorageBackend`] that brotli-compresses incoming writes,
/// and decompresses outgoing reads.
// TODO: Should we tag compressed blobs to distinguish them from uncompressed blobs?
#[derive(Clone)]
pub struct CompressedStorage<const COMPRESSION_LEVEL: u32, Backend> {
    backend: Backend,
}

impl<const COMPRESSION_LEVEL: u32, Backend> CompressedStorage<COMPRESSION_LEVEL, Backend> {
    /// Wrap the provided `backend` in a compression layer
    pub fn new(backend: Backend) -> Self {
        Self { backend }
    }
}

/// A wrapper resource that converts backend errors to DialogStorageError
#[derive(Debug, Clone)]
pub struct CompressedResource<R>
where
    R: Resource,
    R::Error: Into<DialogStorageError>,
{
    inner: R,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<R> Resource for CompressedResource<R>
where
    R: Resource + ConditionalSync,
    R::Error: Into<DialogStorageError>,
{
    type Value = R::Value;
    type Error = DialogStorageError;

    fn content(&self) -> &Option<Self::Value> {
        self.inner.content()
    }

    fn into_content(self) -> Option<Self::Value> {
        self.inner.into_content()
    }

    async fn reload(&mut self) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.reload().await.map_err(|e| e.into())
    }

    async fn replace(
        &mut self,
        value: Option<Self::Value>,
    ) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.replace(value).await.map_err(|e| e.into())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<const COMPRESSION_LEVEL: u32, Backend> StorageBackend
    for CompressedStorage<COMPRESSION_LEVEL, Backend>
where
    Backend: StorageBackend + ConditionalSync,
    Backend::Value: From<Vec<u8>> + AsRef<[u8]>,
    Backend::Error: Into<DialogStorageError>,
    Backend::Resource: ConditionalSync,
{
    type Key = Backend::Key;
    type Value = Backend::Value;
    type Resource = CompressedResource<Backend::Resource>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        const WINDOW_SIZE: u32 = 20;

        let mut compressed = Cursor::new(Vec::new());

        {
            let mut writer =
                CompressorWriter::new(&mut compressed, BUFFER_SIZE, COMPRESSION_LEVEL, WINDOW_SIZE);
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
                let mut reader = Decompressor::new(value.as_ref(), BUFFER_SIZE);
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

    async fn open(&self, key: &Self::Key) -> Result<Self::Resource, Self::Error> {
        let inner = self.backend.open(key).await.map_err(|error| error.into())?;
        Ok(CompressedResource { inner })
    }
}
