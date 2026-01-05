//! FileSystem connector for opening filesystem storage backends from local addresses.
//!
//! This module is only available on non-WASM targets.

use super::super::connection::Connection;
use super::super::errors::StorageError;
use super::super::local::Address as LocalAddress;
use dialog_storage::FileSystemStorageBackend;
use std::path::PathBuf;

/// FileSystem storage backend with Vec<u8> keys and values.
pub type FileSystemBackend = FileSystemStorageBackend<Vec<u8>, Vec<u8>>;

/// Connector that opens filesystem storage backends from local addresses.
///
/// This connector creates `FileSystemStorageBackend` instances from `LocalAddress`
/// by mapping DIDs or paths to directories under a base path.
#[derive(Clone)]
pub struct FileSystemConnector {
    /// Base directory for all storage.
    base_path: PathBuf,
}

impl FileSystemConnector {
    /// Create a new filesystem connector with the given base path.
    ///
    /// All storage will be placed under subdirectories of this base path.
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    /// Get the storage path for a given address.
    fn storage_path(&self, address: &LocalAddress) -> PathBuf {
        match address {
            LocalAddress::Did(did) => {
                // Sanitize DID for use as directory name
                let safe_name = did.replace([':', '/'], "_");
                self.base_path.join(safe_name)
            }
            LocalAddress::Path(path) => path.clone(),
        }
    }
}

impl Connection<FileSystemBackend> for FileSystemConnector {
    type Address = LocalAddress;
    type Error = StorageError;

    async fn open(&self, address: &Self::Address) -> Result<FileSystemBackend, Self::Error> {
        let path = self.storage_path(address);
        FileSystemStorageBackend::new(path)
            .await
            .map_err(|e| StorageError::Storage(format!("Failed to open filesystem storage: {}", e)))
    }
}
