//! Unified storage types — platform-agnostic `Store` and `Address` enums.

use serde::{Deserialize, Serialize};

use crate::provider::{Volatile, volatile};
use dialog_effects::{archive, credential, memory, storage};

#[cfg(not(target_arch = "wasm32"))]
use crate::provider::{FileStore, fs};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::provider::{IndexedDb, indexeddb};

/// Address for a storage location.
///
/// Tags provider-specific addresses under a single enum so that
/// capabilities can carry routing info without knowing the concrete provider.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Address {
    /// Filesystem address (native only).
    #[cfg(not(target_arch = "wasm32"))]
    FileSystem(fs::Address),

    /// IndexedDB address (web only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    IndexedDb(indexeddb::Address),

    /// Volatile (in-memory) address.
    Volatile(volatile::Address),
}

/// A concrete storage backend.
///
/// Platform-gated enum that dispatches `Provider` impls to the active variant.
/// Use `#[derive(Provider)]` with `#[provide(...)]` to generate the dispatch.
#[derive(Clone, Debug, dialog_capability::Provider)]
#[provide(
    archive::Get,
    archive::Put,
    memory::Resolve,
    memory::Publish,
    memory::Retract,
    storage::Get,
    storage::Set,
    storage::Delete,
    storage::List,
    credential::Load,
    credential::Save
)]
pub enum Store {
    /// Filesystem-backed store (native only).
    #[cfg(not(target_arch = "wasm32"))]
    FileSystem(FileStore),

    /// IndexedDB-backed store (web only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    IndexedDb(IndexedDb),

    /// In-memory volatile store.
    Volatile(Volatile),
}
