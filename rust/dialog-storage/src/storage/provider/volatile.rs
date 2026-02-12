//! Volatile in-memory storage provider.
//!
//! This provider implements the capability-based storage API using in-memory
//! hash maps. Data is not persisted and will be lost when the provider is dropped.
//! Primary use case for this provider is testing.
//!
//!
//! # Structure
//!
//! Each subject DID maps to a `Session` containing:
//! - `archive` - HashMap keyed by (catalog, digest) for content-addressed storage
//! - `memory` - HashMap keyed by (space, cell) for transactional memory
//!
//! # Example
//!
//! ```no_run
//! use dialog_storage::provider::Volatile;
//! use dialog_capability::{did, Did, Subject};
//! use dialog_effects::archive::{Archive, Catalog, Get};
//! use dialog_common::Blake3Hash;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let mut provider = Volatile::new();
//! let digest = Blake3Hash::hash(b"hello");
//!
//! let effect = Subject::from(did!("key:z6Mk..."))
//!     .attenuate(Archive)
//!     .attenuate(Catalog::new("index"))
//!     .invoke(Get::new(digest));
//!
//! let result = effect.perform(&mut provider).await?;
//! # Ok(())
//! # }
//! ```

mod archive;
mod memory;

use dialog_capability::Did;
use std::collections::HashMap;

/// Archive key: (catalog, digest_base58)
type ArchiveKey = (String, String);

/// Memory key: (space, cell)
type MemoryKey = (String, String);

/// A session holds the in-memory storage for a single subject.
#[derive(Default)]
struct Session {
    /// Content-addressed blob storage keyed by (catalog, digest).
    archive: HashMap<ArchiveKey, Vec<u8>>,
    /// Transactional memory storage keyed by (space, cell).
    memory: HashMap<MemoryKey, Vec<u8>>,
}

/// Volatile in-memory storage provider.
///
/// A simple provider that stores all data in memory. Each subject DID gets its
/// own session with separate archive and memory storage. Data is not persisted.
#[derive(Default)]
pub struct Volatile {
    sessions: HashMap<Did, Session>,
}

impl Volatile {
    /// Creates a new volatile provider.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets or creates a session for the given subject.
    fn session(&mut self, subject: &Did) -> &mut Session {
        self.sessions.entry(subject.clone()).or_default()
    }
}

/// Errors that can occur during volatile storage operations.
#[derive(Debug, thiserror::Error)]
pub enum VolatileError {
    /// CAS condition failed.
    #[error("CAS condition failed: {0}")]
    Cas(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use base58::ToBase58;
    use dialog_capability::did;
    use dialog_common::Blake3Hash;

    #[dialog_common::test]
    fn it_creates_new_provider() {
        let provider = Volatile::new();
        assert!(provider.sessions.is_empty());
    }

    #[dialog_common::test]
    fn it_creates_session_on_demand() {
        let mut provider = Volatile::new();
        let subject = did!("test:subject1");

        let _session = provider.session(&subject);
        assert!(provider.sessions.contains_key(&subject));
    }

    #[dialog_common::test]
    fn it_reuses_existing_session() {
        let mut provider = Volatile::new();
        let subject = did!("test:subject2");

        // First access creates session
        let digest = Blake3Hash::hash(b"test").as_bytes().to_base58();
        provider
            .session(&subject)
            .archive
            .insert(("catalog".to_string(), digest), b"value".to_vec());

        // Second access should see the same data
        let session = provider.session(&subject);
        assert_eq!(session.archive.len(), 1);
    }

    #[dialog_common::test]
    fn it_isolates_sessions_by_subject() {
        let mut provider = Volatile::new();
        let subject1 = did!("test:subject-a");
        let subject2 = did!("test:subject-b");

        let digest = Blake3Hash::hash(b"test").as_bytes().to_base58();
        provider
            .session(&subject1)
            .archive
            .insert(("catalog".to_string(), digest.clone()), b"value1".to_vec());

        provider
            .session(&subject2)
            .archive
            .insert(("catalog".to_string(), digest), b"value2".to_vec());

        assert_eq!(provider.session(&subject1).archive.len(), 1);
        assert_eq!(provider.session(&subject2).archive.len(), 1);
    }
}
