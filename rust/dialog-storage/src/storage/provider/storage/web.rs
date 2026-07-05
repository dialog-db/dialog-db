//! WASM/web type aliases and defaults.

use crate::provider::{IndexedDb, Space, Storage};

/// Space backed by IndexedDB providers.
pub type WebSpace = Space<IndexedDb, IndexedDb, IndexedDb, IndexedDb>;

impl Default for Storage<WebSpace> {
    fn default() -> Self {
        Self::new()
    }
}
