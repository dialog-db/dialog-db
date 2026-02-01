//! IndexedDB-based storage provider for WASM environments.
//!
//! This provider implements the capability-based storage API using IndexedDB
//! as the underlying storage mechanism. Each subject DID maps to a separate
//! IndexedDB database, with object stores organized by capability domain.
//!
//! # Database Structure
//!
//! For each subject DID, a database is created with the following object stores:
//!
//! - `archive/{catalog}` - Content-addressed blob storage
//! - `storage/{store}` - Key-value storage
//! - `memory/{space}` - Transactional cell storage
//!
//! # Example
//!
//! ```ignore
//! use dialog_storage::provider::IndexedDb;
//! use dialog_capability::{Subject, Provider};
//! use dialog_effects::archive::{Archive, Catalog, Get};
//!
//! let mut provider = IndexedDb::new();
//!
//! let effect = Subject::from("did:key:z6Mk...")
//!     .attenuate(Archive)
//!     .attenuate(Catalog::new("index"))
//!     .invoke(Get::new(digest));
//!
//! let result = effect.perform(&mut provider).await?;
//! ```

mod archive;
mod memory;
mod storage;

use dialog_capability::Did;
use rexie::{ObjectStore, Rexie, RexieBuilder};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

const INDEXEDDB_VERSION: u32 = 1;

/// IndexedDB-based storage provider.
///
/// Manages IndexedDB databases keyed by subject DID. Each subject gets its own
/// database with object stores for archive, storage, and memory operations.
///
/// Databases are opened lazily on first access and cached for subsequent operations.
pub struct IndexedDb {
    /// Object stores to create in each database.
    /// Format: "archive/{catalog}", "storage/{store}", "memory/{space}"
    stores: HashSet<String>,
    /// Cached database connections keyed by subject DID.
    sessions: HashMap<Did, Rc<Rexie>>,
}

impl IndexedDb {
    /// Creates a new IndexedDB provider with default stores.
    ///
    /// Default stores include:
    /// - `archive/index` - Default archive catalog
    /// - `storage/index` - Default key-value store
    /// - `memory/local` - Default memory space
    pub fn new() -> Self {
        let mut stores = HashSet::new();
        stores.insert("archive/index".to_string());
        stores.insert("storage/index".to_string());
        stores.insert("memory/local".to_string());

        Self {
            stores,
            sessions: HashMap::new(),
        }
    }

    /// Creates a new IndexedDB provider with custom stores.
    ///
    /// Store names should follow the format:
    /// - `archive/{catalog}` for archive stores
    /// - `storage/{store}` for key-value stores
    /// - `memory/{space}` for memory stores
    pub fn with_stores(stores: impl IntoIterator<Item = String>) -> Self {
        Self {
            stores: stores.into_iter().collect(),
            sessions: HashMap::new(),
        }
    }

    /// Adds a store to be created in new databases.
    ///
    /// Note: This only affects databases opened after this call.
    /// Already-opened databases will not be modified.
    pub fn add_store(&mut self, store: String) {
        self.stores.insert(store);
    }

    /// Gets or creates a database session for the given subject.
    async fn session(&mut self, subject: &Did) -> Result<Rc<Rexie>, IndexedDbError> {
        if let Some(db) = self.sessions.get(subject) {
            return Ok(Rc::clone(db));
        }

        // Use the DID as the database name
        let db_name = subject.as_ref();

        let mut builder = RexieBuilder::new(db_name).version(INDEXEDDB_VERSION);

        for store_path in &self.stores {
            builder = builder.add_object_store(ObjectStore::new(store_path).auto_increment(false));
        }

        let db = builder
            .build()
            .await
            .map_err(|e| IndexedDbError::Database(e.to_string()))?;

        let db = Rc::new(db);
        self.sessions.insert(subject.clone(), Rc::clone(&db));

        Ok(db)
    }

    /// Ensures a store exists for the given path.
    ///
    /// If the store doesn't exist in our configured stores, it will be added
    /// and databases will be reopened as needed.
    fn ensure_store(&mut self, store_path: &str) {
        if !self.stores.contains(store_path) {
            self.stores.insert(store_path.to_string());
            // Clear cached sessions so they get reopened with the new store
            self.sessions.clear();
        }
    }
}

impl Default for IndexedDb {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur during IndexedDB operations.
#[derive(Debug, thiserror::Error)]
pub enum IndexedDbError {
    /// Database operation failed.
    #[error("IndexedDB error: {0}")]
    Database(String),

    /// Transaction failed.
    #[error("Transaction error: {0}")]
    Transaction(String),

    /// Store operation failed.
    #[error("Store error: {0}")]
    Store(String),

    /// Value conversion failed.
    #[error("Value conversion error: {0}")]
    Conversion(String),
}
