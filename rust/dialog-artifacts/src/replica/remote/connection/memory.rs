//! In-memory storage connection.
//!
//! This module provides [`MemoryConnection`] for testing and local development
//! without requiring actual remote infrastructure.

use super::super::{Connect, Connection, PlatformStorage, RemoteBackend, SigningAuthority};
use dialog_capability::Did;
use dialog_storage::{CborEncoder, MemoryStorageBackend};

/// An active connection to an in-memory storage backend.
///
/// This is useful for testing and local development scenarios where
/// you want remote-like behavior without actual network calls.
#[derive(Clone)]
pub struct MemoryConnection {
    /// Memory storage for branch revision state.
    memory: PlatformStorage<RemoteBackend>,
    /// Index storage for tree blocks.
    index: MemoryStorageBackend<Vec<u8>, Vec<u8>>,
}

impl std::fmt::Debug for MemoryConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryConnection").finish_non_exhaustive()
    }
}

impl Default for MemoryConnection {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryConnection {
    /// Create a new in-memory connection.
    pub fn new() -> Self {
        let backend = RemoteBackend::Memory(MemoryStorageBackend::default());
        let memory = PlatformStorage::new(backend, CborEncoder);
        let index = MemoryStorageBackend::default();
        Self { memory, index }
    }

    /// Get the memory storage connection for branch state.
    pub fn memory(&self) -> PlatformStorage<RemoteBackend> {
        self.memory.clone()
    }

    /// Get the archive/index storage connection for tree blocks.
    pub fn archive(&self) -> PlatformStorage<RemoteBackend> {
        let backend = RemoteBackend::Memory(self.index.clone());
        PlatformStorage::new(backend, CborEncoder)
    }

    /// Get the index storage for direct operations.
    pub fn index(&self) -> &MemoryStorageBackend<Vec<u8>, Vec<u8>> {
        &self.index
    }
}

impl Connect for MemoryConnection {
    fn connect(self, _: SigningAuthority, _: &Did) -> Connection {
        self.into()
    }
}
