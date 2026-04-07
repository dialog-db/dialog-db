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
//! - `memory` - Transactional memory storage (memory space is encoded in the key)
//!
//! # Dynamic Store Creation
//!
//! Stores are created on-demand. When an operation requires a store that doesn't
//! exist, the database is closed, reopened with an incremented version, and the
//! new store is created during the upgrade.
//!
//! # Example
//!
//! ```no_run
//! use dialog_storage::provider::IndexedDb;
//! use dialog_capability::{did, Subject};
//! use dialog_effects::archive::{Archive, Catalog, Get};
//! use dialog_common::Blake3Hash;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let provider = IndexedDb::new();
//! let digest = Blake3Hash::hash(b"hello");
//!
//! let effect = Subject::from(did!("key:z6Mk..."))
//!     .attenuate(Archive)
//!     .attenuate(Catalog::new("index"))
//!     .invoke(Get::new(digest));
//!
//! let result = effect.perform(&provider).await?;
//! # Ok(())
//! # }
//! ```

mod access;
mod archive;
mod credential;
mod memory;
mod mount;

use dialog_capability::Did;
use js_sys::Uint8Array;
use rexie::{ObjectStore, Rexie, RexieBuilder, TransactionMode};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

/// Address for IndexedDB-based storage.
///
/// A string prefix that scopes database names.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct Address(String);

impl Address {
    /// Create an address with the given prefix.
    pub fn new(prefix: impl Into<String>) -> Self {
        Self(prefix.into())
    }

    /// Profile storage address with the given name.
    pub fn profile(name: &str) -> Self {
        Self(format!("profile/{name}"))
    }

    /// Current/working storage address with the given name.
    pub fn current(name: &str) -> Self {
        Self(format!("storage/{name}"))
    }

    /// Unique temporary storage address.
    pub fn temp() -> Self {
        use dialog_common::time;
        let id = format!(
            "dialog-{}",
            time::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        Self(format!("temp/{id}"))
    }

    /// The prefix string.
    pub fn prefix(&self) -> &str {
        &self.0
    }

    /// Resolve a sub-path under this address.
    pub fn resolve(&self, segment: &str) -> Self {
        if self.0.is_empty() {
            Self(segment.to_string())
        } else {
            Self(format!("{}/{}", self.0, segment))
        }
    }
}

/// Convert bytes to a JS Uint8Array.
fn to_uint8array(bytes: &[u8]) -> Uint8Array {
    let array = Uint8Array::new_with_length(bytes.len() as u32);
    array.copy_from(bytes);
    array
}

/// A session represents an open connection to a subject's IndexedDB database.
///
/// Stores are populated from whatever exists in the database on open,
/// and new stores are created via upgrade when needed.
pub struct Session {
    /// Database name (subject DID).
    name: String,
    /// Current database version.
    version: u32,
    /// Set of object stores in this database.
    stores: HashSet<String>,
    /// Open database connection. Option used to allow taking ownership for close().
    db: Option<Rexie>,
}

impl Session {
    /// Opens the database without specifying a version.
    ///
    /// This opens whatever version currently exists, or creates a new
    /// database at version 1 if it doesn't exist.
    async fn open(name: &str) -> Result<Self, IndexedDbError> {
        let db = RexieBuilder::new(name)
            .build()
            .await
            .map_err(|e| IndexedDbError::Database(format!("{:?}", e)))?;

        let version = db
            .version()
            .map_err(|e| IndexedDbError::Database(e.to_string()))?;
        let stores = db.store_names().into_iter().collect();

        Ok(Self {
            name: name.to_string(),
            version,
            stores,
            db: Some(db),
        })
    }

    /// Closes the database connection.
    fn close(&mut self) {
        if let Some(db) = self.db.take() {
            db.close();
        }
    }

    /// Upgrades the database to add new stores.
    ///
    /// Closes the current connection, increments the version, and reopens
    /// with the new stores added.
    async fn upgrade(&mut self, stores: HashSet<String>) -> Result<(), IndexedDbError> {
        self.close();

        let new_version = self.version + 1;
        let mut builder = RexieBuilder::new(&self.name).version(new_version);
        for store in &stores {
            builder = builder.add_object_store(ObjectStore::new(store).auto_increment(false));
        }

        let db = builder
            .build()
            .await
            .map_err(|e| IndexedDbError::Database(format!("{:?}", e)))?;

        self.version = db
            .version()
            .map_err(|e| IndexedDbError::Database(e.to_string()))?;
        self.stores = db.store_names().into_iter().collect();
        self.db = Some(db);
        Ok(())
    }

    /// Gets a store handle, upgrading the database if needed.
    pub async fn store(&mut self, store_path: &str) -> Result<Store<'_>, IndexedDbError> {
        if !self.stores.contains(store_path) {
            let mut stores = self.stores.clone();
            stores.insert(store_path.to_string());
            self.upgrade(stores).await?;
        }

        Ok(Store {
            db: self.db.as_ref().unwrap(),
            name: self.stores.get(store_path).unwrap(),
        })
    }
}

/// A temporary handle to a specific object store for performing operations.
pub struct Store<'a> {
    db: &'a Rexie,
    name: &'a String,
}

impl<'a> Store<'a> {
    /// Executes a read-only query on this store.
    pub async fn query<F, Fut, Output, E>(&self, select: F) -> Result<Output, E>
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
    pub async fn transact<F, Fut, Output, E>(&self, mutate: F) -> Result<Output, E>
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
///
/// Uses `RefCell` for interior mutability since this provider only targets
/// wasm32 (single-threaded). This avoids the overhead and poisoning concerns
/// of `RwLock`.
pub struct IndexedDb {
    /// Mount prefix prepended to database names.
    mount: String,
    /// Cached database sessions keyed by name (shared across mounts).
    sessions: std::rc::Rc<RefCell<HashMap<String, Session>>>,
}

impl Clone for IndexedDb {
    fn clone(&self) -> Self {
        Self {
            mount: self.mount.clone(),
            sessions: self.sessions.clone(),
        }
    }
}

impl std::fmt::Debug for IndexedDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexedDb")
            .field("mount", &self.mount)
            .finish_non_exhaustive()
    }
}

impl IndexedDb {
    /// Creates a new IndexedDB provider with no prefix.
    pub fn new() -> Self {
        Self {
            mount: String::new(),
            sessions: std::rc::Rc::new(RefCell::new(HashMap::new())),
        }
    }

    /// Create an IndexedDB provider mounted at the given address.
    pub fn mount(address: &Address) -> Self {
        Self {
            mount: address.prefix().to_string(),
            sessions: std::rc::Rc::new(RefCell::new(HashMap::new())),
        }
    }

    /// Returns the prefixed database name for a given name.
    pub(crate) fn prefixed(&self, name: &str) -> String {
        if self.mount.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", self.mount, name)
        }
    }

    /// Opens or returns an existing session for the given name.
    ///
    /// Accepts any string-like type (`Did`, `&str`, `String`).
    /// Checks for an existing session via a short borrow, drops it before
    /// any async work, then borrows mutably to insert a new session if needed.
    pub async fn open(&self, name: &str) -> Result<(), IndexedDbError> {
        let db_name = self.prefixed(name);
        let exists = self.sessions.borrow().contains_key(&db_name);
        if !exists {
            let session = Session::open(&db_name).await?;
            self.sessions.borrow_mut().insert(db_name, session);
        }
        Ok(())
    }

    /// Temporarily removes a session from the cache for async operations.
    ///
    /// IndexedDB operations require `&mut Session` and are async, so we
    /// cannot hold a `RefCell` borrow across `.await` points. Instead we
    /// remove the session, perform the work, then re-insert it via
    /// [`IndexedDb::return_session`].
    pub fn take_session(&self, name: &str) -> Result<Session, IndexedDbError> {
        let db_name = self.prefixed(name);
        self.sessions
            .borrow_mut()
            .remove(&db_name)
            .ok_or_else(|| IndexedDbError::Database(format!("No session for {db_name}")))
    }

    /// Returns a session to the cache after async operations complete.
    pub fn return_session(&self, name: &str, session: Session) {
        let db_name = self.prefixed(name);
        self.sessions.borrow_mut().insert(db_name, session);
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

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen::JsValue;

    fn unique_db_name(prefix: &str) -> String {
        format!("did:test:{}-{}", prefix, js_sys::Date::now() as u64)
    }

    #[dialog_common::test]
    async fn it_opens_new_database_with_no_stores() -> anyhow::Result<()> {
        let db_name = unique_db_name("new-db");
        let session = Session::open(&db_name).await?;

        assert!(session.stores.is_empty());
        // New databases start at version 1
        assert_eq!(session.version, 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_reopens_existing_database() -> anyhow::Result<()> {
        let db_name = unique_db_name("reopen");

        // First session creates database and adds a store
        {
            let mut session = Session::open(&db_name).await?;
            let _store = session.store("my-store").await?;
            assert!(session.stores.contains("my-store"));
            session.close();
        }

        // Second session should see the store
        let session2 = Session::open(&db_name).await?;
        assert!(session2.stores.contains("my-store"));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_adds_store_via_upgrade() -> anyhow::Result<()> {
        let db_name = unique_db_name("add-store");

        let mut session = Session::open(&db_name).await?;
        let initial_version = session.version;
        assert!(!session.stores.contains("new-store"));

        // Request a store that doesn't exist - should trigger upgrade
        let _store = session.store("new-store").await?;

        assert!(session.stores.contains("new-store"));
        assert_eq!(session.version, initial_version + 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_multiple_store_additions() -> anyhow::Result<()> {
        let db_name = unique_db_name("multi-store");

        let mut session = Session::open(&db_name).await?;

        // Add first store
        let _store1 = session.store("store-a").await?;
        assert!(session.stores.contains("store-a"));
        let version_after_a = session.version;

        // Add second store
        let _store2 = session.store("store-b").await?;
        assert!(session.stores.contains("store-b"));
        assert_eq!(session.version, version_after_a + 1);

        // Request existing store - should not upgrade
        let _store3 = session.store("store-a").await?;
        assert_eq!(session.version, version_after_a + 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_persists_stores_across_sessions() -> anyhow::Result<()> {
        let db_name = unique_db_name("persist-stores");

        // Create database and add a custom store
        let final_version = {
            let mut session = Session::open(&db_name).await?;
            let _store = session.store("persistent-store").await?;
            let version = session.version;
            session.close();
            version
        };

        // Reopen - should see the same version and stores
        let session = Session::open(&db_name).await?;
        assert_eq!(session.version, final_version);
        assert!(session.stores.contains("persistent-store"));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_query_on_store() -> anyhow::Result<()> {
        let db_name = unique_db_name("query-store");

        let mut session = Session::open(&db_name).await?;
        let store = session.store("memory").await?;

        let result: Result<Option<JsValue>, IndexedDbError> = store
            .query(|object_store| async move {
                object_store
                    .get(JsValue::from_str("nonexistent"))
                    .await
                    .map_err(|e| IndexedDbError::Store(e.to_string()))
            })
            .await;

        assert!(result?.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_transaction_on_store() -> anyhow::Result<()> {
        let db_name = unique_db_name("transact-store");

        let mut session = Session::open(&db_name).await?;
        let store = session.store("memory").await?;

        // Write a value
        let key = JsValue::from_str("test-key");
        let value = JsValue::from_str("test-value");
        store
            .transact(|object_store| {
                let key = key.clone();
                let value = value.clone();
                async move {
                    object_store
                        .put(&value, Some(&key))
                        .await
                        .map_err(|e| IndexedDbError::Store(e.to_string()))
                }
            })
            .await?;

        // Read it back
        let result: Option<JsValue> = store
            .query(|object_store| async move {
                object_store
                    .get(JsValue::from_str("test-key"))
                    .await
                    .map_err(|e| IndexedDbError::Store(e.to_string()))
            })
            .await?;

        assert!(result.is_some());
        assert_eq!(result.unwrap().as_string(), Some("test-value".to_string()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_upgrades_for_new_store() -> anyhow::Result<()> {
        let db_name = unique_db_name("upgrade");

        let mut session = Session::open(&db_name).await?;
        let initial_version = session.version;

        // Request new store - should upgrade
        let _store = session.store("upgraded-store").await?;

        assert_eq!(session.version, initial_version + 1);
        assert!(session.stores.contains("upgraded-store"));

        Ok(())
    }
}
