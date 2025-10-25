//! Remote synchronization protocol for dialog-db

pub mod backend;

pub use backend::{RevisionStorageBackend, RevisionStorageBackendError, Subject};

// Re-export the artifacts revision type (not the replica revision type)
pub use dialog_artifacts::artifacts::Revision;
pub use dialog_storage::StorageBackend;
