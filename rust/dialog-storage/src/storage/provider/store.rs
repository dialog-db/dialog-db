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

#[cfg(not(target_arch = "wasm32"))]
impl From<fs::Address> for Address {
    fn from(addr: fs::Address) -> Self {
        Self::FileSystem(addr)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<indexeddb::Address> for Address {
    fn from(addr: indexeddb::Address) -> Self {
        Self::IndexedDb(addr)
    }
}

impl From<volatile::Address> for Address {
    fn from(addr: volatile::Address) -> Self {
        Self::Volatile(addr)
    }
}

impl Address {
    /// Resolve a sub-path under this address.
    ///
    /// Returns an error if the segment would escape the base address.
    pub fn resolve(&self, segment: &str) -> Result<Self, StorageError> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::FileSystem(addr) => addr
                .resolve(segment)
                .map(Self::FileSystem)
                .map_err(|e| StorageError::Storage(e.to_string())),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::IndexedDb(addr) => Ok(Self::IndexedDb(addr.resolve(segment))),
            Self::Volatile(addr) => Ok(Self::Volatile(addr.resolve(segment))),
        }
    }

    /// Profile storage address with the given name.
    ///
    /// Uses the platform default: FileSystem on native, IndexedDb on web.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn profile(name: &str) -> Self {
        Self::FileSystem(fs::Address::profile().resolve(name).expect("valid name"))
    }

    /// Profile storage address with the given name (web).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub fn profile(name: &str) -> Self {
        Self::IndexedDb(indexeddb::Address::new(format!("profile/{name}")))
    }

    /// Current/working directory storage address with the given name.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn current(name: &str) -> Self {
        Self::FileSystem(fs::Address::current().resolve(name).expect("valid name"))
    }

    /// Current storage address with the given name (web).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub fn current(name: &str) -> Self {
        Self::IndexedDb(indexeddb::Address::new(format!("storage/{name}")))
    }

    /// Temporary storage address with the given name.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn temp(name: &str) -> Self {
        Self::FileSystem(fs::Address::temp().resolve(name).expect("valid name"))
    }

    /// Temporary storage address with the given name (web).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub fn temp(name: &str) -> Self {
        Self::IndexedDb(indexeddb::Address::new(format!("temp/{name}")))
    }
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

use dialog_capability::storage::StorageError;

impl Store {
    /// Create a Store from an Address.
    ///
    /// Matches the address variant and creates the corresponding store.
    pub fn mount(address: &Address) -> Result<Self, StorageError> {
        match address {
            #[cfg(not(target_arch = "wasm32"))]
            Address::FileSystem(addr) => {
                let store = crate::provider::FileSystem::mount(addr)
                    .map_err(|e| StorageError::Storage(e.to_string()))?;
                Ok(Self::FileSystem(store))
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Address::IndexedDb(addr) => {
                Ok(Self::IndexedDb(crate::provider::IndexedDb::mount(addr)))
            }
            Address::Volatile(addr) => Ok(Self::Volatile(Volatile::mount(addr))),
        }
    }
}
