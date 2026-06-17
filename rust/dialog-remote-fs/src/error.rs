//! Error type for FS-remote credential resolution.

use dialog_effects::archive::ArchiveError;
use dialog_effects::memory::MemoryError;
use thiserror::Error;

/// Error raised while resolving an [`FsAddress`](crate::FsAddress) to a
/// registered directory.
///
/// The actual filesystem I/O is performed by `dialog_storage`'s isomorphic
/// [`FileSystem`](dialog_storage::provider::FileSystem) provider and surfaces
/// its own errors; this crate only adds the registry-lookup failure.
#[derive(Debug, Error)]
pub enum FsError {
    /// The requested directory handle was not registered with the FS provider
    /// before the invocation fired. The caller must register the handle (via
    /// the thread-local registry) before any invocation is dispatched against
    /// this address.
    #[error("Unregistered FS handle: {0}")]
    UnregisteredHandle(String),
}

impl From<FsError> for ArchiveError {
    fn from(error: FsError) -> Self {
        ArchiveError::Storage(error.to_string())
    }
}

impl From<FsError> for MemoryError {
    fn from(error: FsError) -> Self {
        MemoryError::Storage(error.to_string())
    }
}
