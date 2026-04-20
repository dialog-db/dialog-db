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
//! let provider = Volatile::new();
//! let digest = Blake3Hash::hash(b"hello");
//!
//! let effect = Subject::from(did!("key:z6Mk..."))
//!     .attenuate(Archive)
//!     .attenuate(Catalog::new("index"))
//!     .invoke(Get::new(digest));
//!
//! let result = effect.perform(&provider).await?;
//! # Ok(())
//! # }
//! ```

mod archive;
mod credential;
mod memory;

use dialog_capability::Did;
use dialog_credentials::credential::CredentialExport;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Archive key: (catalog, digest_base58)
type ArchiveKey = (String, String);

/// Memory key: (space, cell)
type MemoryKey = (String, String);

/// A session holds the in-memory storage for a single subject.
#[derive(Default, Debug)]
struct Session {
    /// Content-addressed blob storage keyed by (catalog, digest).
    archive: HashMap<ArchiveKey, Vec<u8>>,
    /// Transactional memory storage keyed by (space, cell).
    memory: HashMap<MemoryKey, Vec<u8>>,
    /// Credential storage keyed by address.
    credentials: HashMap<String, CredentialExport>,
    /// Secret storage keyed by site address.
    secrets: HashMap<String, Vec<u8>>,
}

/// Volatile in-memory storage provider.
///
/// A simple provider that stores all data in memory. Each subject DID gets its
/// own session with separate archive and memory storage. Data is not persisted.
///
/// Uses `parking_lot::RwLock` for interior mutability so that
/// `Provider::execute` can take `&self`. All lock guards are dropped before
/// any `.await` points. Unlike `std::sync::RwLock`, `parking_lot` locks are
/// infallible (no poisoning).
#[derive(Debug, Clone)]
pub struct Volatile {
    /// Prefix for scoping this provider to a location.
    mount: String,
    sessions: Arc<RwLock<HashMap<Did, Session>>>,
}

impl Default for Volatile {
    fn default() -> Self {
        Self {
            mount: String::new(),
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Volatile {
    /// Creates a new volatile provider with no prefix.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a scoped key by prepending the mount prefix.
    fn scoped_key(&self, key: &str) -> String {
        if self.mount.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.mount, key)
        }
    }
}

use crate::resource::Resource;
use dialog_effects::storage::{Directory, Location};

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Resource<Location> for Volatile {
    type Error = std::convert::Infallible;

    /// Open a volatile provider scoped to the given location.
    ///
    /// The prefix is derived from the directory and name to match
    /// the naming convention used by persistent backends:
    /// - `Directory::Profile` with name "alice" -> prefix "alice.profile"
    /// - `Directory::Current` with name "contacts" -> prefix "contacts"
    /// - `Directory::Temp` with name "scratch" -> prefix "temp.scratch"
    async fn open(location: &Location) -> Result<Self, Self::Error> {
        let prefix = match &location.directory {
            Directory::Profile => format!("{}.profile", location.name),
            Directory::Current => location.name.clone(),
            Directory::Temp => format!("temp.{}", location.name),
            Directory::At(path) => format!("{}/{}", path, location.name),
        };

        Ok(Self {
            mount: prefix,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        })
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
        assert!(provider.sessions.read().is_empty());
    }

    #[dialog_common::test]
    fn it_creates_session_on_demand() {
        let provider = Volatile::new();
        let subject = did!("test:subject1");

        provider
            .sessions
            .write()
            .entry(subject.clone())
            .or_default();
        assert!(provider.sessions.read().contains_key(&subject));
    }

    #[dialog_common::test]
    fn it_reuses_existing_session() {
        let provider = Volatile::new();
        let subject = did!("test:subject2");

        // First access creates session
        let digest = Blake3Hash::hash(b"test").as_bytes().to_base58();
        provider
            .sessions
            .write()
            .entry(subject.clone())
            .or_default()
            .archive
            .insert(("catalog".to_string(), digest), b"value".to_vec());

        // Second access should see the same data
        let sessions = provider.sessions.read();
        let session = sessions.get(&subject).unwrap();
        assert_eq!(session.archive.len(), 1);
    }

    #[dialog_common::test]
    fn it_isolates_sessions_by_subject() {
        let provider = Volatile::new();
        let subject1 = did!("test:subject-a");
        let subject2 = did!("test:subject-b");

        let digest = Blake3Hash::hash(b"test").as_bytes().to_base58();
        provider
            .sessions
            .write()
            .entry(subject1.clone())
            .or_default()
            .archive
            .insert(("catalog".to_string(), digest.clone()), b"value1".to_vec());

        provider
            .sessions
            .write()
            .entry(subject2.clone())
            .or_default()
            .archive
            .insert(("catalog".to_string(), digest), b"value2".to_vec());

        let sessions = provider.sessions.read();
        assert_eq!(sessions.get(&subject1).unwrap().archive.len(), 1);
        assert_eq!(sessions.get(&subject2).unwrap().archive.len(), 1);
    }

    /// Demonstrates that a provider can be shared across concurrent tasks,
    /// which is the key motivation for `Provider::execute` taking `&self`
    /// instead of `&mut self`.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_supports_concurrent_access() -> anyhow::Result<()> {
        use dialog_capability::Subject;
        use dialog_effects::archive::{Archive, Catalog, Get, Put};
        use std::sync::Arc;

        let provider = Arc::new(Volatile::new());

        // Spawn multiple tasks that write to the same provider concurrently.
        let mut handles = Vec::new();
        for i in 0..10u8 {
            let provider = provider.clone();
            let handle = tokio::spawn(async move {
                let subject = Subject::from(did!("test:concurrent"));
                let content = vec![i; 64];
                let digest = Blake3Hash::hash(&content);

                subject
                    .clone()
                    .attenuate(Archive)
                    .attenuate(Catalog::new("index"))
                    .invoke(Put::new(digest.clone(), content))
                    .perform(provider.as_ref())
                    .await
                    .unwrap();

                let result = subject
                    .attenuate(Archive)
                    .attenuate(Catalog::new("index"))
                    .invoke(Get::new(digest))
                    .perform(provider.as_ref())
                    .await
                    .unwrap();

                assert!(result.is_some());
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }
}
