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
//! let mut provider = IndexedDb::new();
//! let digest = Blake3Hash::hash(b"hello");
//!
//! let effect = Subject::from(did!("key:z6Mk..."))
//!     .attenuate(Archive)
//!     .attenuate(Catalog::new("index"))
//!     .invoke(Get::new(digest));
//!
//! let result = effect.perform(&mut provider).await?;
//! # Ok(())
//! # }
//! ```

mod archive;
mod memory;

use dialog_capability::Did;
use js_sys::Uint8Array;
use rexie::{ObjectStore, Rexie, RexieBuilder, TransactionMode};
use std::collections::{HashMap, HashSet};

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
struct Session {
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
    async fn store(&mut self, store_path: &str) -> Result<Store<'_>, IndexedDbError> {
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

    /// Opens or returns an existing session for the given subject.
    async fn open(&mut self, subject: &Did) -> Result<&mut Session, IndexedDbError> {
        if !self.sessions.contains_key(subject) {
            let session = Session::open(subject.as_ref()).await?;
            self.sessions.insert(subject.clone(), session);
        }

        Ok(self.sessions.get_mut(subject).unwrap())
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
