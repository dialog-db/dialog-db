//! Capability-based S3 authorization.
//!
//! This module defines S3 storage and memory effects that integrate with
//! `dialog_common::capability` system. Effects can be authorized via the
//! `Access` trait and performed via the `Provider` trait.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//!   └── Storage
//!         └── Store { name }
//!               └── Get { key } | Set { key, checksum } | Delete { key } | List
//!
//! Subject (repository DID)
//!   └── Memory
//!         └── Space { name }
//!               └── Cell { name }
//!                     └── Resolve | Update { when, checksum } | Delete { when }
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use dialog_common::capability::{Subject, Capability};
//! use dialog_s3_credentials::capability::storage;
//!
//! // Build capability chain
//! let capability: Capability<storage::Get> = Subject::from("did:key:z...")
//!     .attenuate(storage::Storage)
//!     .attenuate(storage::Store::new("index"))
//!     .invoke(storage::Get::new("my-key"));
//!
//! // Acquire authorization
//! let authorized = capability.acquire(&credentials).await?;
//!
//! // Perform to get RequestDescriptor
//! let descriptor = authorized.perform(&mut credentials).await;
//! ```

use crate::{Checksum, RequestDescriptor};
use dialog_common::capability::{Attenuation, Effect, Policy, Subject};
use serde::{Deserialize, Serialize};

/// Storage capability effects for key-value operations.
pub mod storage {
    use super::*;

    /// Root attenuation for storage operations.
    ///
    /// Attaches to Subject and provides the `/storage` command path segment.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Storage;

    impl Attenuation for Storage {
        type Of = Subject;
    }

    /// Store policy that scopes operations to a named store.
    ///
    /// This is a policy (not attenuation) so it doesn't contribute to the command path.
    /// It restricts operations to a specific store (e.g., "index", "blob").
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Store {
        /// The store name (e.g., "index", "blob").
        pub name: String,
    }

    impl Store {
        /// Create a new Store policy.
        pub fn new(name: impl Into<String>) -> Self {
            Self { name: name.into() }
        }
    }

    impl Policy for Store {
        type Of = Storage;
    }

    /// Get value by key.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Get {
        /// The storage key (already encoded).
        pub key: String,
    }

    impl Get {
        /// Create a new Get effect.
        pub fn new(key: impl Into<String>) -> Self {
            Self { key: key.into() }
        }
    }

    impl Effect for Get {
        type Of = Store;
        type Output = RequestDescriptor;
    }

    /// Set value with key and checksum.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Set {
        /// The storage key.
        pub key: String,
        /// Checksum for integrity verification.
        pub checksum: Checksum,
    }

    impl Set {
        /// Create a new Set effect.
        pub fn new(key: impl Into<String>, checksum: Checksum) -> Self {
            Self {
                key: key.into(),
                checksum,
            }
        }
    }

    impl Effect for Set {
        type Of = Store;
        type Output = RequestDescriptor;
    }

    /// Delete value by key.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Delete {
        /// The storage key.
        pub key: String,
    }

    impl Delete {
        /// Create a new Delete effect.
        pub fn new(key: impl Into<String>) -> Self {
            Self { key: key.into() }
        }
    }

    impl Effect for Delete {
        type Of = Store;
        type Output = RequestDescriptor;
    }

    /// List keys in store.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct List {
        /// Continuation token for pagination.
        pub continuation_token: Option<String>,
    }

    impl List {
        /// Create a new List effect.
        pub fn new(continuation_token: Option<String>) -> Self {
            Self { continuation_token }
        }
    }

    impl Effect for List {
        type Of = Store;
        type Output = RequestDescriptor;
    }

    /// Build the S3 path for a storage effect.
    pub fn path(store: &Store, key: &str) -> String {
        if store.name.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", store.name, key)
        }
    }
}

/// Memory capability effects for transactional operations.
pub mod memory {
    use super::*;

    /// Edition identifier for CAS operations.
    pub type Edition = String;

    /// Precondition for memory operations.
    #[derive(Debug, Clone, PartialEq)]
    pub enum Precondition {
        /// No precondition.
        None,
        /// Only succeed if edition matches.
        IfMatch(Edition),
        /// Only succeed if no current value exists.
        IfNoneMatch,
    }

    /// Root attenuation for memory operations.
    ///
    /// Attaches to Subject and provides the `/memory` command path segment.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Memory;

    impl Attenuation for Memory {
        type Of = Subject;
    }

    /// Space policy that scopes operations to a memory space.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Space {
        /// The space name (typically a DID).
        pub name: String,
    }

    impl Space {
        /// Create a new Space policy.
        pub fn new(name: impl Into<String>) -> Self {
            Self { name: name.into() }
        }
    }

    impl Policy for Space {
        type Of = Memory;
    }

    /// Cell policy that scopes operations to a specific cell within a space.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Cell {
        /// The cell name.
        pub name: String,
    }

    impl Cell {
        /// Create a new Cell policy.
        pub fn new(name: impl Into<String>) -> Self {
            Self { name: name.into() }
        }
    }

    impl Policy for Cell {
        type Of = Space;
    }

    /// Resolve current cell content and edition.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Resolve;

    impl Effect for Resolve {
        type Of = Cell;
        type Output = RequestDescriptor;
    }

    /// Publish cell content with CAS semantics.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Publish {
        /// Expected current edition for CAS. None means "if-none-match".
        pub when: Option<Edition>,
        /// Checksum for integrity verification.
        pub checksum: Checksum,
    }

    impl Publish {
        /// Create a new Publish effect.
        pub fn new(when: Option<Edition>, checksum: Checksum) -> Self {
            Self { when, checksum }
        }

        /// Get the precondition for this publish.
        pub fn precondition(&self) -> Precondition {
            match &self.when {
                Some(edition) => Precondition::IfMatch(edition.clone()),
                None => Precondition::IfNoneMatch,
            }
        }
    }

    impl Effect for Publish {
        type Of = Cell;
        type Output = RequestDescriptor;
    }

    /// Retract cell with CAS semantics.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Retract {
        /// Required current edition.
        pub when: Edition,
    }

    impl Retract {
        /// Create a new Retract effect.
        pub fn new(when: Edition) -> Self {
            Self { when }
        }

        /// Get the precondition for this retract.
        pub fn precondition(&self) -> Precondition {
            Precondition::IfMatch(self.when.clone())
        }
    }

    impl Effect for Retract {
        type Of = Cell;
        type Output = RequestDescriptor;
    }

    /// Build the S3 path for a memory effect.
    pub fn path(space: &Space, cell: &Cell) -> String {
        format!("{}/{}", space.name, cell.name)
    }
}

/// Archive capability effects for content-addressed storage operations.
pub mod archive {
    use super::*;

    /// Root attenuation for archive operations.
    ///
    /// Attaches to Subject and provides the `/archive` command path segment.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Archive;

    impl Attenuation for Archive {
        type Of = Subject;
    }

    /// Catalog policy that scopes operations to a named catalog.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Catalog {
        /// The catalog name (e.g., "index", "blobs").
        pub name: String,
    }

    impl Catalog {
        /// Create a new Catalog policy.
        pub fn new(name: impl Into<String>) -> Self {
            Self { name: name.into() }
        }
    }

    impl Policy for Catalog {
        type Of = Archive;
    }

    /// Get content by digest.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Get {
        /// The digest of the content (hex-encoded or base58).
        pub digest: String,
    }

    impl Get {
        /// Create a new Get effect.
        pub fn new(digest: impl Into<String>) -> Self {
            Self {
                digest: digest.into(),
            }
        }
    }

    impl Effect for Get {
        type Of = Catalog;
        type Output = RequestDescriptor;
    }

    /// Put content by digest with checksum.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Put {
        /// The digest of the content (hex-encoded or base58).
        pub digest: String,
        /// Checksum for integrity verification.
        pub checksum: Checksum,
    }

    impl Put {
        /// Create a new Put effect.
        pub fn new(digest: impl Into<String>, checksum: Checksum) -> Self {
            Self {
                digest: digest.into(),
                checksum,
            }
        }
    }

    impl Effect for Put {
        type Of = Catalog;
        type Output = RequestDescriptor;
    }

    /// Build the S3 path for an archive effect.
    pub fn path(catalog: &Catalog, digest: &str) -> String {
        if catalog.name.is_empty() {
            digest.to_string()
        } else {
            format!("{}/{}", catalog.name, digest)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_common::capability::Capability;

    #[test]
    fn storage_get_command_path() {
        let cap: Capability<storage::Get> = Subject::from("did:key:zSpace")
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::Get::new("my-key"));

        assert_eq!(cap.subject(), "did:key:zSpace");
        // Store is a Policy (not Attenuation), so only Storage and Get contribute
        assert_eq!(cap.ability(), "/storage/get");
    }

    #[test]
    fn storage_set_command_path() {
        let checksum = Checksum::Sha256([0u8; 32]);
        let cap: Capability<storage::Set> = Subject::from("did:key:zSpace")
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("blob"))
            .invoke(storage::Set::new("my-key", checksum));

        assert_eq!(cap.ability(), "/storage/set");
    }

    #[test]
    fn memory_resolve_command_path() {
        let cap: Capability<memory::Resolve> = Subject::from("did:key:zSpace")
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zUser"))
            .attenuate(memory::Cell::new("main"))
            .invoke(memory::Resolve);

        assert_eq!(cap.subject(), "did:key:zSpace");
        // Space and Cell are Policies, only Memory and Resolve contribute
        assert_eq!(cap.ability(), "/memory/resolve");
    }

    #[test]
    fn memory_publish_command_path() {
        let checksum = Checksum::Sha256([0u8; 32]);
        let cap: Capability<memory::Publish> = Subject::from("did:key:zSpace")
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zUser"))
            .attenuate(memory::Cell::new("main"))
            .invoke(memory::Publish::new(Some("etag-123".into()), checksum));

        assert_eq!(cap.ability(), "/memory/publish");
    }

    #[test]
    fn storage_path_helper() {
        let store = storage::Store::new("index");
        assert_eq!(storage::path(&store, "my-key"), "index/my-key");

        let empty_store = storage::Store::new("");
        assert_eq!(storage::path(&empty_store, "key"), "key");
    }

    #[test]
    fn memory_path_helper() {
        let space = memory::Space::new("did:key:zUser");
        let cell = memory::Cell::new("main");
        assert_eq!(memory::path(&space, &cell), "did:key:zUser/main");
    }
}
