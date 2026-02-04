//! Remote connection types for different storage backends.
//!
//! This module provides the [`Connection`] enum which represents
//! an active connection to a remote storage backend. Different backends
//! are enabled via feature flags.

mod memory;
#[cfg(feature = "s3")]
mod s3;

use super::{PlatformStorage, RemoteBackend};

// Re-export connection types
pub use memory::MemoryConnection;
#[cfg(feature = "s3")]
pub use s3::S3Connection;

/// An active connection to a remote storage backend.
///
/// This enum represents the different types of remote connections supported.
/// Each variant is gated behind its respective feature flag.
#[derive(Clone, Debug)]
pub enum Connection {
    /// S3-compatible storage connection (includes both direct S3 and UCAN-based access).
    #[cfg(feature = "s3")]
    S3(Box<S3Connection>),
    /// In-memory storage connection (useful for testing).
    Memory(Box<MemoryConnection>),
}

impl Connection {
    /// Get the memory storage connection for branch state.
    pub fn memory(&self) -> PlatformStorage<RemoteBackend> {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(s3) => s3.memory(),
            Self::Memory(mem) => mem.memory(),
        }
    }

    /// Get the archive/index storage connection for tree blocks.
    pub fn archive(&self) -> PlatformStorage<RemoteBackend> {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(s3) => s3.archive(),
            Self::Memory(mem) => mem.archive(),
        }
    }
}

impl From<MemoryConnection> for Connection {
    fn from(connection: MemoryConnection) -> Self {
        Self::Memory(Box::new(connection))
    }
}

#[cfg(feature = "s3")]
impl From<S3Connection> for Connection {
    fn from(connection: S3Connection) -> Self {
        Self::S3(Box::new(connection))
    }
}
