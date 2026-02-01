//! IndexedDB-based storage provider for WASM environments.
//!
//! This provider implements the capability-based storage API using IndexedDB
//! as the underlying storage mechanism. Each subject DID maps to a separate
//! IndexedDB database, with object stores that are created dynamically as needed.
//!
//! # Database Structure
//!
//! For each subject DID, a database is created with object stores:
//!
//! - `archive/{catalog}` - Content-addressed blob storage (one store per catalog)
//! - `memory` - Transactional cell storage (space is encoded in the key)
//!
//! # Dynamic Store Creation
//!
//! Stores are created on-demand. When an operation requires a store that doesn't
//! exist, the database is closed, reopened with an incremented version, and the
//! new store is created during the upgrade.
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
use rexie::{ObjectStore, Rexie, RexieBuilder, TransactionMode};
use std::collections::{HashMap, HashSet};

/// Initial database version. Increment this when changing DEFAULT_STORES.
const INITIAL_VERSION: u32 = 1;

/// Default stores created when opening a new database.
/// When adding stores here, increment INITIAL_VERSION.
const DEFAULT_STORES: &[&str] = &["archive/index", "memory"];

/// Database address containing name, version, and stores.
///
/// Used to open or upgrade an IndexedDB database with a specific configuration.
#[derive(Clone)]
struct Address {
    /// Database name (subject DID).
    name: String,
    /// Database version.
    version: u32,
    /// Set of object stores in this database.
    stores: HashSet<String>,
}

impl Address {
    /// Creates a new address with default stores.
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            version: INITIAL_VERSION,
            stores: DEFAULT_STORES.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Creates an address at a specific version (stores will be populated on open).
    fn at_version(name: &str, version: u32) -> Self {
        Self {
            name: name.to_string(),
            version,
            stores: HashSet::new(),
        }
    }

    /// Opens the database at this address.
    async fn open(mut self) -> Result<Session, IndexedDbError> {
        loop {
            let mut builder = RexieBuilder::new(&self.name).version(self.version);
            for store in &self.stores {
                builder = builder.add_object_store(ObjectStore::new(store).auto_increment(false));
            }

            match builder.build().await {
                Ok(db) => {
                    self.stores = db.store_names().into_iter().collect();
                    return Ok(Session {
                        address: self,
                        db: Some(db),
                    });
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    // Check if this is a version error (database exists at higher version)
                    // Error format: "The requested version (X) is less than the existing version (Y)."
                    if let Some(existing_version) = Self::parse_existing_version(&error_msg) {
                        // Retry with the existing version
                        self.version = existing_version;
                        self.stores.clear();
                    } else {
                        return Err(IndexedDbError::Database(error_msg));
                    }
                }
            }
        }
    }

    /// Parses the existing version from a VersionError message.
    ///
    /// When opening a database at a version lower than what exists, IndexedDB returns
    /// a VersionError with the message format:
    /// "The requested version (X) is less than the existing version (Y)."
    ///
    /// We parse the version from this message because:
    /// - The `rexie` and `idb` crates wrap errors as opaque `JsValue` without structured data
    /// - The `web-sys` `IdbFactory` doesn't expose the `databases()` method that would let
    ///   us query database versions without opening (it's available in browsers but not bound)
    /// - The error message format is stable across browsers
    fn parse_existing_version(error_msg: &str) -> Option<u32> {
        let marker = "existing version (";
        let start = error_msg.find(marker)? + marker.len();
        let end = error_msg[start..].find(')')? + start;
        error_msg[start..end].parse().ok()
    }
}

/// A session represents an open connection to a subject's IndexedDB database.
///
/// Tracks the current version and available stores, allowing dynamic store
/// creation when needed.
struct Session {
    address: Address,
    db: Option<Rexie>,
}

impl Session {
    /// Creates a new session for the given address.
    fn new(address: Address) -> Self {
        Self { address, db: None }
    }

    /// Opens the database at the current address.
    async fn open(&mut self) -> Result<(), IndexedDbError> {
        let session = self.address.clone().open().await?;
        self.db = session.db;
        Ok(())
    }

    /// Closes the database connection.
    fn close(&mut self) {
        if let Some(db) = self.db.take() {
            db.close();
        }
    }

    /// Gets a store handle, opening or upgrading the database as needed.
    async fn store(&mut self, store_path: &str) -> Result<Store<'_>, IndexedDbError> {
        if self.db.is_none() {
            self.open().await?;
        }

        if !self.address.stores.contains(store_path) {
            self.close();
            self.address.stores.insert(store_path.to_string());
            self.address.version += 1;
            self.open().await?;
        }

        Ok(Store {
            db: self.db.as_ref().unwrap(),
            name: self.address.stores.get(store_path).unwrap(),
        })
    }
}

/// A temporary handle to a specific object store for performing operations.
struct Store<'a> {
    db: &'a Rexie,
    name: &'a String,
}

impl<'a> Store<'a> {
    /// Executes a read-only query on this store.
    async fn query<F, Fut, Output, E>(&self, select: F) -> Result<Output, E>
    where
        F: FnOnce(rexie::Store) -> Fut,
        Fut: std::future::Future<Output = Result<Output, E>>,
        E: From<IndexedDbError>,
    {
        let tx = self
            .db
            .transaction(&[self.name], TransactionMode::ReadOnly)
            .map_err(|e| IndexedDbError::Transaction(e.to_string()))?;

        let object_store = tx
            .store(self.name)
            .map_err(|e| IndexedDbError::Store(e.to_string()))?;

        let result = select(object_store).await?;

        tx.done()
            .await
            .map_err(|e| IndexedDbError::Transaction(e.to_string()))?;

        Ok(result)
    }

    /// Executes a read-write transaction on this store.
    async fn transact<F, Fut, Output, E>(&self, mutate: F) -> Result<Output, E>
    where
        F: FnOnce(rexie::Store) -> Fut,
        Fut: std::future::Future<Output = Result<Output, E>>,
        E: From<IndexedDbError>,
    {
        let tx = self
            .db
            .transaction(&[self.name], TransactionMode::ReadWrite)
            .map_err(|e| IndexedDbError::Transaction(e.to_string()))?;

        let object_store = tx
            .store(self.name)
            .map_err(|e| IndexedDbError::Store(e.to_string()))?;

        let result = mutate(object_store).await?;

        tx.done()
            .await
            .map_err(|e| IndexedDbError::Transaction(e.to_string()))?;

        Ok(result)
    }
}

/// IndexedDB-based storage provider.
///
/// Manages IndexedDB databases keyed by subject DID. Each subject gets its own
/// database with object stores for archive and memory operations.
///
/// Databases are opened lazily on first access and stores are created dynamically
/// as needed.
pub struct IndexedDb {
    /// Cached database sessions keyed by subject DID.
    sessions: HashMap<Did, Session>,
}

impl IndexedDb {
    /// Creates a new IndexedDB provider.
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    /// Gets or creates a session for the given subject, then gets the specified store.
    async fn store(
        &mut self,
        subject: &Did,
        store_path: &str,
    ) -> Result<Store<'_>, IndexedDbError> {
        if !self.sessions.contains_key(subject) {
            let address = Address::new(subject.as_ref());
            self.sessions.insert(subject.clone(), Session::new(address));
        }

        let session = self.sessions.get_mut(subject).unwrap();
        session.store(store_path).await
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
