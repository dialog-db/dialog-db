//! IndexedDB-based storage provider for WASM environments.
//!
//! Each space maps to a single IndexedDB database whose name is derived from
//! the [`Location`](dialog_effects::storage::Location):
//!
//! - `Directory::Profile` with name "alice" -> database `"alice.profile"`
//! - `Directory::Current` with name "contacts" -> database `"contacts"`
//! - `Directory::Temp` with name "scratch" -> database `"temp.scratch"`
//!
//! Within each database, object stores are created dynamically:
//!
//! - `archive/{catalog}` - Content-addressed blob storage
//! - `memory` - Transactional memory (space/cell encoded in key)
//! - `credential` - Credential storage (address as key)
//!
//! # Connection Sharing
//!
//! Multiple `IndexedDb` instances for the same database share a single
//! connection via a thread-local pool. This is necessary because
//! [`Space`](crate::provider::space::Space) may hold the same backend
//! type in multiple fields (archive, memory, credential, certificate),
//! and having independent connections to the same database causes
//! upgrade conflicts: when one connection upgrades the schema to add a
//! new object store, the browser blocks until all other connections to
//! that database are closed.
//!
//! The pool entry is removed automatically when the last clone drops.
//!
//! # Example
//!
//! ```no_run
//! use dialog_storage::provider::IndexedDb;
//! use dialog_storage::resource::Resource;
//! use dialog_effects::storage::{Directory, Location};
//! use dialog_effects::prelude::*;
//! use dialog_common::Blake3Hash;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let location = Location::new(Directory::Profile, "alice");
//! let provider = IndexedDb::open(&location).await?;
//! let digest = Blake3Hash::hash(b"hello");
//!
//! let result = dialog_capability::did!("key:z6Mk...")
//!     .archive()
//!     .catalog("index")
//!     .get(digest)
//!     .perform(&provider)
//!     .await?;
//! # Ok(())
//! # }
//! ```

mod archive;
mod credential;
mod memory;

use js_sys::Uint8Array;
use rexie::{ObjectStore, Rexie, RexieBuilder, TransactionMode};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Convert bytes to a JS Uint8Array.
fn to_uint8array(bytes: &[u8]) -> Uint8Array {
    let array = Uint8Array::new_with_length(bytes.len() as u32);
    array.copy_from(bytes);
    array
}

/// Shared database state.
struct Connection {
    name: String,
    version: u32,
    stores: HashSet<String>,
    /// Shared via Rc so StoreSession can hold a clone across .await
    /// points without borrowing the Connection.
    db: Rc<Rexie>,
}

impl Connection {
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
            db: Rc::new(db),
        })
    }

    /// Returns a clone of the database handle.
    fn db(&self) -> Rc<Rexie> {
        self.db.clone()
    }
}

/// A handle to a named object store. Holds a shared `Rc<Rexie>` so it
/// can be used freely across .await points.
struct StoreSession {
    db: Rc<Rexie>,
    store_name: String,
}

impl StoreSession {
    async fn query<F, Fut, Output, E>(&self, select: F) -> Result<Output, E>
    where
        F: FnOnce(rexie::Store) -> Fut,
        Fut: std::future::Future<Output = Result<Output, E>>,
        E: From<IndexedDbError>,
    {
        let tx = self
            .db
            .transaction(&[&self.store_name], TransactionMode::ReadOnly)
            .map_err(|e| IndexedDbError::Transaction(e.to_string()))?;

        let object_store = tx
            .store(&self.store_name)
            .map_err(|e| IndexedDbError::Store(e.to_string()))?;

        let result = select(object_store).await?;

        tx.done()
            .await
            .map_err(|e| IndexedDbError::Transaction(e.to_string()))?;

        Ok(result)
    }

    async fn transact<F, Fut, Output, E>(&self, mutate: F) -> Result<Output, E>
    where
        F: FnOnce(rexie::Store) -> Fut,
        Fut: std::future::Future<Output = Result<Output, E>>,
        E: From<IndexedDbError>,
    {
        let tx = self
            .db
            .transaction(&[&self.store_name], TransactionMode::ReadWrite)
            .map_err(|e| IndexedDbError::Transaction(e.to_string()))?;

        let object_store = tx
            .store(&self.store_name)
            .map_err(|e| IndexedDbError::Store(e.to_string()))?;

        let result = mutate(object_store).await?;

        tx.done()
            .await
            .map_err(|e| IndexedDbError::Transaction(e.to_string()))?;

        Ok(result)
    }
}

// Thread-local connection pool. When Space holds multiple IndexedDb fields
// for the same database, they all share one Connection via this pool.
// The entry is removed when the last IndexedDb clone for that database drops.
thread_local! {
    static CONNECTIONS: RefCell<HashMap<String, Rc<RefCell<Connection>>>> =
        RefCell::new(HashMap::new());
}

/// IndexedDB-based storage provider.
///
/// Each instance is bound to a database name derived from a
/// [`Location`](dialog_effects::storage::Location). Clones share
/// the same database connection via an internal `Rc`.
#[derive(Clone)]
pub struct IndexedDb {
    name: String,
    connection: Rc<RefCell<Connection>>,
}

impl IndexedDb {
    /// Opens or retrieves a shared connection for the given database name.
    ///
    /// If a connection already exists in the thread-local pool, returns a
    /// clone sharing that connection. Otherwise opens a new database and
    /// registers it in the pool.
    async fn connect(name: String) -> Result<Self, IndexedDbError> {
        let existing = CONNECTIONS.with(|pool| pool.borrow().get(&name).cloned());

        let connection = match existing {
            Some(rc) => rc,
            None => {
                let conn = Connection::open(&name).await?;
                let rc = Rc::new(RefCell::new(conn));
                CONNECTIONS.with(|pool| {
                    pool.borrow_mut().insert(name.clone(), rc.clone());
                });
                rc
            }
        };

        Ok(Self { name, connection })
    }

    /// Gets a handle to the named object store. Upgrades the database
    /// schema if the store doesn't exist yet.
    ///
    /// The returned StoreSession holds a shared `Rc<Rexie>` so it can
    /// be used across .await points without holding a borrow on Connection.
    ///
    /// During an upgrade, the old database connection remains valid for
    /// any active StoreSession. The new connection is opened alongside it
    /// and swapped in once ready. IndexedDB's `versionchange` mechanism
    /// coordinates the transition.
    async fn store(&self, name: &str) -> Result<StoreSession, IndexedDbError> {
        // Check if upgrade is needed (brief borrow, dropped before .await).
        let needs_upgrade = !self.connection.borrow().stores.contains(name);

        if needs_upgrade {
            // Gather what we need from the connection (brief borrow).
            let (version, mut new_stores) = {
                let conn = self.connection.borrow();
                (conn.version, conn.stores.clone())
            };
            new_stores.insert(name.to_string());

            // Build the upgraded database. No RefCell borrow held here,
            // so the old Rc<Rexie> remains valid for concurrent readers.
            let new_version = version + 1;
            let mut builder = RexieBuilder::new(&self.name).version(new_version);
            for store in &new_stores {
                builder = builder.add_object_store(ObjectStore::new(store).auto_increment(false));
            }
            let db = builder
                .build()
                .await
                .map_err(|e| IndexedDbError::Database(format!("{:?}", e)))?;

            // Swap in the new connection (brief borrow). Any StoreSession
            // holding the old Rc<Rexie> keeps it alive until they drop.
            let mut conn = self.connection.borrow_mut();
            conn.version = db
                .version()
                .map_err(|e| IndexedDbError::Database(e.to_string()))?;
            conn.stores = db.store_names().into_iter().collect();
            conn.db = Rc::new(db);
        }

        let db = self.connection.borrow().db();
        Ok(StoreSession {
            db,
            store_name: name.to_string(),
        })
    }
}

impl Drop for IndexedDb {
    fn drop(&mut self) {
        // When we're the last clone (strong_count == 2: us + pool entry),
        // remove the pool entry so the connection is closed.
        if Rc::strong_count(&self.connection) == 2 {
            let _ = CONNECTIONS.try_with(|pool| {
                pool.borrow_mut().remove(&self.name);
            });
        }
    }
}

use crate::resource::Resource;
use dialog_effects::storage::{Directory, Location};

#[async_trait::async_trait(?Send)]
impl Resource<Location> for IndexedDb {
    type Error = IndexedDbError;

    async fn open(location: &Location) -> Result<Self, Self::Error> {
        let name = match &location.directory {
            Directory::Profile => format!("{}.profile", location.name),
            Directory::Current => location.name.clone(),
            Directory::Temp => format!("temp.{}", location.name),
            Directory::At(path) => format!("{}/{}", path, location.name),
        };

        Self::connect(name).await
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

use dialog_capability::access::AuthorizeError;
use dialog_effects::credential::CredentialError;

impl From<IndexedDbError> for CredentialError {
    fn from(e: IndexedDbError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<IndexedDbError> for AuthorizeError {
    fn from(e: IndexedDbError) -> Self {
        Self::Configuration(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::unique_name;
    use wasm_bindgen::JsValue;

    #[dialog_common::test]
    async fn it_opens_new_database_with_no_stores() -> anyhow::Result<()> {
        let db = IndexedDb::connect(unique_name("new-db")).await?;
        assert!(db.connection.borrow().stores.is_empty());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_derives_database_name_from_location() -> anyhow::Result<()> {
        let profile = IndexedDb::open(&Location::new(Directory::Profile, "alice")).await?;
        assert_eq!(profile.name, "alice.profile");

        let current = IndexedDb::open(&Location::new(Directory::Current, "contacts")).await?;
        assert_eq!(current.name, "contacts");

        let temp = IndexedDb::open(&Location::new(Directory::Temp, "scratch")).await?;
        assert_eq!(temp.name, "temp.scratch");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_shares_connection_across_clones() -> anyhow::Result<()> {
        let name = unique_name("shared");
        let a = IndexedDb::connect(name.clone()).await?;
        let b = IndexedDb::connect(name).await?;

        assert!(Rc::ptr_eq(&a.connection, &b.connection));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_different_databases() -> anyhow::Result<()> {
        let a = IndexedDb::connect(unique_name("iso-a")).await?;
        let b = IndexedDb::connect(unique_name("iso-b")).await?;

        assert!(!Rc::ptr_eq(&a.connection, &b.connection));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_creates_store_via_upgrade() -> anyhow::Result<()> {
        let db = IndexedDb::connect(unique_name("upgrade")).await?;
        let initial_version = db.connection.borrow().version;

        let _store = db.store("new-store").await?;

        let conn = db.connection.borrow();
        assert!(conn.stores.contains("new-store"));
        assert_eq!(conn.version, initial_version + 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_cleans_up_pool_on_drop() -> anyhow::Result<()> {
        let name = unique_name("cleanup");

        {
            let _db = IndexedDb::connect(name.clone()).await?;
            let has_entry = CONNECTIONS.with(|pool| pool.borrow().contains_key(&name));
            assert!(has_entry, "pool should have entry while alive");
        }

        let has_entry = CONNECTIONS.with(|pool| pool.borrow().contains_key(&name));
        assert!(!has_entry, "pool should be cleaned up after drop");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_query_on_store() -> anyhow::Result<()> {
        let db = IndexedDb::connect(unique_name("query")).await?;
        let store = db.store("memory").await?;

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
        let db = IndexedDb::connect(unique_name("transact")).await?;
        let store = db.store("memory").await?;

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
    async fn it_creates_expected_object_stores() -> anyhow::Result<()> {
        use dialog_capability::{Did, Subject};
        use dialog_common::Blake3Hash;
        use dialog_credentials::{Ed25519Signer, SignerCredential};
        use dialog_effects::archive::{Archive, Catalog, Put};
        use dialog_effects::memory::{Cell, Memory, Publish, Space};
        use dialog_effects::prelude::*;
        use dialog_varsig::Principal;

        let db = IndexedDb::connect(unique_name("stores-layout")).await?;

        // Archive creates "archive/{catalog}" stores
        let subject = {
            let signer = Ed25519Signer::generate().await.unwrap();
            Subject::from(Principal::did(&signer))
        };
        let content = b"test".to_vec();
        let digest = Blake3Hash::hash(&content);

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(digest, content))
            .perform(&db)
            .await?;

        // Memory creates "memory" store
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("head"))
            .invoke(Publish::new(b"value", None))
            .perform(&db)
            .await?;

        // Credential creates "credential" store
        let signer = Ed25519Signer::generate().await.unwrap();
        let cred = dialog_credentials::Credential::Signer(SignerCredential::from(signer.clone()));
        let did = Principal::did(&signer);
        did.credential().key("self").save(cred).perform(&db).await?;

        // Verify the store names match our expectations
        let conn = db.connection.borrow();
        assert!(
            conn.stores.contains("archive/index"),
            "expected archive/index store, got: {:?}",
            conn.stores
        );
        assert!(
            conn.stores.contains("memory"),
            "expected memory store, got: {:?}",
            conn.stores
        );
        assert!(
            conn.stores.contains("credential"),
            "expected credential store, got: {:?}",
            conn.stores
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_data_across_locations() -> anyhow::Result<()> {
        use dialog_credentials::{Ed25519Signer, SignerCredential};
        use dialog_effects::prelude::*;
        use dialog_varsig::Principal;

        let db1 = IndexedDb::connect(unique_name("space1")).await?;
        let db2 = IndexedDb::connect(unique_name("space2")).await?;

        // Save credential in db1
        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);
        let cred = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        did.clone()
            .credential()
            .key("self")
            .save(cred)
            .perform(&db1)
            .await?;

        // db1 should have it
        let loaded = did
            .clone()
            .credential()
            .key("self")
            .load()
            .perform(&db1)
            .await;
        assert!(loaded.is_ok(), "db1 should have the credential");

        // db2 should NOT have it
        let missing = did.credential().key("self").load().perform(&db2).await;
        assert!(missing.is_err(), "db2 should not see db1's credential");

        Ok(())
    }
}
