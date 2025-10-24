//! Remote synchronization protocol for dialog-db

pub mod backend;

pub use backend::{RevisionStorageBackend, RevisionStorageBackendError, Subject};

pub use dialog_artifacts::Revision;
pub use dialog_storage::StorageBackend;
