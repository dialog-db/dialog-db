//! IndexedDB connector for opening IndexedDB storage backends from local addresses.
//!
//! This module is only available on WASM targets.

use super::super::connection::Connection;
use super::super::errors::StorageError;
use super::super::local::Address as LocalAddress;
use dialog_storage::IndexedDbStorageBackend;

/// IndexedDB storage backend with Vec<u8> keys and values.
pub type IndexedDbBackend = IndexedDbStorageBackend<Vec<u8>, Vec<u8>>;

/// Connector that opens IndexedDB storage backends from local addresses.
///
/// This connector creates `IndexedDbStorageBackend` instances from `LocalAddress`
/// by mapping DIDs or paths to database names.
#[derive(Clone)]
pub struct IndexedDbConnector {
    /// Prefix for database names.
    db_prefix: String,
    /// Store name within each database.
    store_name: String,
}

impl IndexedDbConnector {
    /// Create a new IndexedDB connector with the given database prefix.
    ///
    /// Each local address will get its own database named `{prefix}_{address}`.
    pub fn new(db_prefix: impl Into<String>) -> Self {
        Self {
            db_prefix: db_prefix.into(),
            store_name: "store".to_string(),
        }
    }

    /// Create a new IndexedDB connector with custom database prefix and store name.
    pub fn with_store_name(db_prefix: impl Into<String>, store_name: impl Into<String>) -> Self {
        Self {
            db_prefix: db_prefix.into(),
            store_name: store_name.into(),
        }
    }

    /// Get the database name for a given address.
    fn db_name(&self, address: &LocalAddress) -> String {
        let address_part = match address {
            LocalAddress::Did(did) => {
                // Sanitize DID for use as database name
                did.replace(':', "_").replace('/', "_")
            }
            LocalAddress::Path(path) => {
                // Convert path to string, sanitizing for DB name
                path.display().to_string().replace('/', "_").replace('\\', "_")
            }
        };
        format!("{}_{}", self.db_prefix, address_part)
    }
}

impl Default for IndexedDbConnector {
    fn default() -> Self {
        Self::new("dialog")
    }
}

impl Connection<IndexedDbBackend> for IndexedDbConnector {
    type Address = LocalAddress;
    type Error = StorageError;

    async fn open(&self, address: &Self::Address) -> Result<IndexedDbBackend, Self::Error> {
        let db_name = self.db_name(address);
        IndexedDbStorageBackend::new(&db_name, &self.store_name)
            .await
            .map_err(|e| StorageError::Storage(format!("Failed to open IndexedDB: {}", e)))
    }
}
