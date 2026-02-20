//! S3-compatible storage connection.
//!
//! This module provides [`S3Connection`] for connecting to S3-compatible
//! storage backends (AWS S3, Cloudflare R2, MinIO, etc.).

use dialog_storage::CborEncoder;
use dialog_storage::s3::Bucket;

use crate::ErrorMappingBackend;
use crate::repository::remote::{Credentials, PlatformStorage, RemoteBackend};

/// An active connection to an S3-compatible storage backend.
#[derive(Clone)]
pub struct S3Connection {
    /// Memory storage for branch revision state.
    memory: PlatformStorage<RemoteBackend>,
    /// Index storage bucket for tree blocks.
    index: Bucket<Credentials>,
}

impl std::fmt::Debug for S3Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Connection").finish_non_exhaustive()
    }
}

impl S3Connection {
    /// Create a new S3 connection with the given memory and index storage.
    pub fn new(memory: PlatformStorage<RemoteBackend>, index: Bucket<Credentials>) -> Self {
        Self { memory, index }
    }

    /// Get the memory storage connection for branch state.
    pub fn memory(&self) -> PlatformStorage<RemoteBackend> {
        self.memory.clone()
    }

    /// Get the archive/index storage connection for tree blocks.
    pub fn archive(&self) -> PlatformStorage<RemoteBackend> {
        let backend = RemoteBackend::S3(ErrorMappingBackend::new(self.index.clone()));
        PlatformStorage::new(backend, CborEncoder)
    }
}
