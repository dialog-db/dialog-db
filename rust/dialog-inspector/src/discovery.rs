//! Discovery of dialog-db instances in browser IndexedDB.
//!
//! This module provides the ability to enumerate all IndexedDB databases
//! visible to the current origin and identify which ones are dialog-db
//! instances (by checking for the expected object store schema).
//!
//! Database probing uses [`rexie`] (the same IndexedDB wrapper that
//! `dialog-storage` uses internally), keeping a single abstraction layer
//! over IDB. The only raw `js_sys` FFI is for `indexedDB.databases()`,
//! which rexie does not wrap.

use js_sys::{Array, Promise, Reflect};
use rexie::RexieBuilder;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

/// Information about a discovered IndexedDB database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseInfo {
    /// The database name (corresponds to the dialog-db identifier)
    pub name: String,
    /// The IndexedDB schema version
    pub version: u64,
}

/// Enumerate all IndexedDB databases accessible from the current origin.
///
/// This calls the `indexedDB.databases()` browser API which returns metadata
/// about every database without opening them. The returned list includes *all*
/// databases, not just dialog-db ones — use [`probe_database`] to filter.
///
/// Note: `indexedDB.databases()` is not wrapped by `rexie`, so we use a
/// minimal `js_sys` FFI binding here.
pub async fn list_databases() -> Result<Vec<DatabaseInfo>, JsValue> {
    let global: web_sys::Window = js_sys::global().unchecked_into();
    let idb = global
        .indexed_db()?
        .ok_or_else(|| JsValue::from_str("indexedDB not available"))?;

    let promise: Promise = idb_databases(&idb)?;
    let result = JsFuture::from(promise).await?;
    let array: Array = result.unchecked_into();

    let mut databases = Vec::with_capacity(array.length() as usize);

    for i in 0..array.length() {
        let entry = array.get(i);
        let name = Reflect::get(&entry, &"name".into())?
            .as_string()
            .unwrap_or_default();
        let version = Reflect::get(&entry, &"version".into())?
            .as_f64()
            .unwrap_or(0.0) as u64;

        databases.push(DatabaseInfo { name, version });
    }

    Ok(databases)
}

/// Check whether a database with the given name has the dialog-db object store
/// schema (contains `"index"` and `"memory"` stores).
///
/// Opens the database via [`rexie`] (the same crate `dialog-storage` uses),
/// inspects its object store names, then closes it.
pub async fn probe_database(name: &str) -> Result<bool, JsValue> {
    let db = RexieBuilder::new(name)
        .build()
        .await
        .map_err(|e| JsValue::from_str(&format!("{e:?}")))?;

    let store_names = db.store_names();
    let has_index = store_names.iter().any(|s| s == "index");
    let has_memory = store_names.iter().any(|s| s == "memory");

    db.close();

    Ok(has_index && has_memory)
}

/// Discover all dialog-db instances in the current origin.
///
/// This combines [`list_databases`] and [`probe_database`] to return only
/// databases that have the expected dialog-db schema.
pub async fn discover_instances() -> Result<Vec<DatabaseInfo>, JsValue> {
    let all = list_databases().await?;
    let mut instances = Vec::new();

    for db_info in all {
        match probe_database(&db_info.name).await {
            Ok(true) => instances.push(db_info),
            _ => continue,
        }
    }

    Ok(instances)
}

// -- Minimal FFI for indexedDB.databases() --

/// Call `indexedDB.databases()` which rexie does not wrap.
/// Returns a Promise that resolves to an array of `{ name, version }` objects.
fn idb_databases(factory: &web_sys::IdbFactory) -> Result<Promise, JsValue> {
    let databases_fn = Reflect::get(factory.as_ref(), &"databases".into())?;
    let databases_fn: js_sys::Function = databases_fn.unchecked_into();
    databases_fn
        .call0(factory.as_ref())
        .map(|v| v.unchecked_into())
}
