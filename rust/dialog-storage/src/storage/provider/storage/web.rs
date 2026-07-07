//! WASM/web type aliases and defaults.

use crate::provider::{FileSystem, IndexedDb, Space, Storage};

/// Space backed by IndexedDB providers.
pub type WebSpace = Space<IndexedDb, IndexedDb, IndexedDb, IndexedDb>;

impl Default for Storage<WebSpace> {
    fn default() -> Self {
        Self::new()
    }
}

/// Space that keeps archive and memory in OPFS (via the
/// [`FileSystem`](crate::provider::FileSystem) provider) while leaving
/// credential and certificate storage on IndexedDB.
///
/// On the web, OPFS gives archive blobs and memory cells direct File System
/// Access throughput, but a signer credential is a non-extractable WebCrypto
/// key with no byte form, so credentials (and the certificates derived from
/// them) stay on IndexedDB. The same [`Location`](dialog_effects::storage::Location)
/// opens every slot: the OPFS subdirectory for archive/memory, an IndexedDB
/// database for credential/certificate.
pub type WebOpfsSpace = Space<FileSystem, FileSystem, IndexedDb, IndexedDb>;

impl Storage<WebOpfsSpace> {
    /// Create an environment that stores archive and memory in OPFS, with
    /// credential and certificate storage on IndexedDB. See [`WebOpfsSpace`].
    pub fn opfs() -> Self {
        Self::new()
    }
}
