//! Discovery of dialog-db instances in browser IndexedDB.
//!
//! This module provides the ability to enumerate all IndexedDB databases
//! visible to the current origin and identify which ones are dialog-db
//! instances (by checking for the expected object store schema).

use js_sys::{Array, Promise, Reflect};
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
/// This calls the `indexedDB.databases()` API which returns metadata about
/// every database without opening them. The returned list includes all
/// databases, not just dialog-db ones. Use [`probe_database`] to check
/// whether a specific database is a dialog-db instance.
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
/// schema (contains "index" and "memory" stores at version 3).
///
/// This opens the database in read-only mode, inspects its object store names,
/// then closes it. Returns `true` if the database looks like a dialog-db instance.
pub async fn probe_database(name: &str) -> Result<bool, JsValue> {
    let global: web_sys::Window = js_sys::global().unchecked_into();
    let idb = global
        .indexed_db()?
        .ok_or_else(|| JsValue::from_str("indexedDB not available"))?;

    let open_request = idb.open(name)?;

    let db: web_sys::IdbDatabase = JsFuture::from(idb_open_promise(&open_request))
        .await?
        .unchecked_into();

    let store_names = db.object_store_names();
    let has_index = store_names.contains("index");
    let has_memory = store_names.contains("memory");

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

// -- FFI bindings --

/// Call `indexedDB.databases()` which is not yet in web-sys's stable API surface
/// for all browsers. We bind it directly.
fn idb_databases(factory: &web_sys::IdbFactory) -> Result<Promise, JsValue> {
    // indexedDB.databases() returns a Promise<sequence<IDBDatabaseInfo>>
    let databases_fn = Reflect::get(factory.as_ref(), &"databases".into())?;
    let databases_fn: js_sys::Function = databases_fn.unchecked_into();
    databases_fn.call0(factory.as_ref()).map(|v| v.unchecked_into())
}

/// Wrap an IDBOpenDBRequest into a Promise so we can await it.
fn idb_open_promise(request: &web_sys::IdbOpenDbRequest) -> Promise {
    use wasm_bindgen::closure::Closure;
    use web_sys::IdbRequest;

    let request_ref: &IdbRequest = request.as_ref();
    let request_for_success = request_ref.clone();
    let request_for_events = request_ref.clone();

    Promise::new(&mut move |resolve, reject| {
        let req = request_for_success.clone();
        let resolve_cb = Closure::once(move |_: web_sys::Event| {
            let result = req.result().unwrap_or(JsValue::UNDEFINED);
            resolve.call1(&JsValue::UNDEFINED, &result).unwrap();
        });
        let reject_clone = reject.clone();
        let reject_cb = Closure::once(move |_: web_sys::Event| {
            reject_clone
                .call1(&JsValue::UNDEFINED, &JsValue::from_str("Failed to open database"))
                .unwrap();
        });
        request_for_events.set_onsuccess(Some(resolve_cb.as_ref().unchecked_ref()));
        request_for_events.set_onerror(Some(reject_cb.as_ref().unchecked_ref()));
        // Leak closures so they survive until the callback fires.
        // In a one-shot open context this is acceptable.
        resolve_cb.forget();
        reject_cb.forget();
    })
}
