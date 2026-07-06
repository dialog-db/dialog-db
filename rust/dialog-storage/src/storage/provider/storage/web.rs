//! WASM/web type aliases and defaults.

use crate::provider::{FileSystem, IndexedDb, Space, Storage};

/// Space backed by IndexedDB providers for blocks, memory, credentials, and
/// certificates, with blobs on an OPFS-backed
/// [`FileSystem`](crate::provider::FileSystem).
///
/// IndexedDB has no blob provider (and buffering whole binary objects through
/// it would forfeit streaming), so blob effects route to OPFS, which gives
/// whole, hash-addressable objects direct File System Access throughput and
/// range reads. Blocks and cells stay on IndexedDB. The same
/// [`Location`](dialog_effects::storage::Location) opens both: the IndexedDB
/// database for archive/memory/credential/certificate, an OPFS subtree for
/// blobs.
pub type WebSpace = Space<IndexedDb, IndexedDb, IndexedDb, IndexedDb, FileSystem>;

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
pub type WebOpfsSpace = Space<FileSystem, FileSystem, IndexedDb, IndexedDb, FileSystem>;

impl Storage<WebOpfsSpace> {
    /// Create an environment that stores archive and memory in OPFS, with
    /// credential and certificate storage on IndexedDB. See [`WebOpfsSpace`].
    pub fn opfs() -> Self {
        Self::new()
    }
}
