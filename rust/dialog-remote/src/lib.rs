//! Remote synchronization protocol for dialog-db
//!
//! This crate implements revision storage backends for managing remote tree state.
//! Backends implement `RevisionStorageBackend` (which extends `StorageBackend` with
//! specialized types for revisions) with compare-and-swap semantics, enabling
//! git-like synchronization between replicas.
//!
//! # Architecture
//!
//! The core abstraction is `RevisionStorageBackend`, a trait alias for:
//! ```ignore
//! StorageBackend<Key = Subject, Value = RevisionUpgrade, Error = RevisionBackendError>
//! ```
//!
//! This provides two methods:
//! - `get(subject)` - Query the current revision for a subject (DID)
//! - `set(subject, upgrade)` - Update revision with compare-and-swap (checks upgrade.origin)
//!
//! # Backends
//!
//! - **MemoryBackend** - In-memory implementation for testing with provider/consumer pattern
//! - **RestBackend** - HTTP-based implementation supporting None and Bearer authentication
//!
//! # Example
//!
//! ```
//! use dialog_remote::backend::{MemoryBackendProvider, Subject, RevisionUpgrade, RevisionUpgradeRecord, RevisionStorageBackend};
//! use dialog_remote::StorageBackend;
//! use dialog_artifacts::Revision;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = MemoryBackendProvider::new();
//! let subject = Subject::new("did:key:z6Mkk...");
//! let initial = Revision::new(&[1; 32]);
//!
//! provider.initialize(&subject, initial.clone()).await?;
//!
//! let mut backend = provider.connect();
//!
//! // Get current revision
//! let current = backend.get(&subject).await?.unwrap();
//!
//! // Update with compare-and-swap
//! let new_rev = Revision::new(&[2; 32]);
//! let upgrade = RevisionUpgradeRecord::new(current.revision().clone(), new_rev);
//! backend.set(subject, upgrade).await?;
//! # Ok(())
//! # }
//! ```

pub mod backend;

pub use backend::{
    AuthMethod, MemoryBackend, MemoryBackendProvider, RestBackend, RestBackendConfig,
    RevisionBackendError, RevisionPayload, RevisionStorageBackend, RevisionUpgrade,
    RevisionUpgradeRecord, Subject,
};
pub use dialog_artifacts::Revision;
pub use dialog_storage::StorageBackend;
