//! Storage capability hierarchy.
//!
//! Storage provides key-value store operations.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//!   └── Storage (ability: /storage)
//!         └── Store { store: String }
//!               ├── Get { key } → Effect → Result<Option<Bytes>, StorageError>
//!               ├── Set { key, value } → Effect → Result<(), StorageError>
//!               ├── Delete { key } → Effect → Result<(), StorageError>
//!               └── List { continuation_token } → Effect → Result<ListResult, StorageError>
//! ```

pub use dialog_capability::{Attenuation, Capability, Effect, Policy, Subject};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Root attenuation for storage operations.
///
/// Attaches to Subject and provides the `/storage` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Storage;

impl Attenuation for Storage {
    type Of = Subject;
}

/// Store policy that scopes operations to a named store.
///
/// This is a policy (not attenuation) so it doesn't contribute to the ability path.
/// It restricts operations to a specific store (e.g., "index", "blob").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Store {
    /// The store name (e.g., "index", "blob").
    pub store: String,
}

impl Store {
    /// Create a new Store policy.
    pub fn new(name: impl Into<String>) -> Self {
        Self { store: name.into() }
    }
}

impl Policy for Store {
    type Of = Storage;
}

/// Get operation - retrieves a value by key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Get {
    /// The key to look up.
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
    type Output = Result<Option<Vec<u8>>, StorageError>;
}

/// Extension trait for `Capability<Get>` to access its fields.
pub trait GetCapability {
    /// Get the store name from the capability chain.
    fn store(&self) -> &str;
    /// Get the key from the capability chain.
    fn key(&self) -> &str;
}

impl GetCapability for Capability<Get> {
    fn store(&self) -> &str {
        &Store::of(self).store
    }

    fn key(&self) -> &str {
        &Get::of(self).key
    }
}

/// Set operation - sets a value for a key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Set {
    /// The key to update.
    pub key: String,
    /// The value to set.
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
}

impl Set {
    /// Create a new Set effect.
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl Effect for Set {
    type Of = Store;
    type Output = Result<(), StorageError>;
}

/// Extension trait for `Capability<Set>` to access its fields.
pub trait SetCapability {
    /// Get the store name from the capability chain.
    fn store(&self) -> &str;
    /// Get the key from the capability chain.
    fn key(&self) -> &str;
    /// Get the value from the capability chain.
    fn value(&self) -> &[u8];
}

impl SetCapability for Capability<Set> {
    fn store(&self) -> &str {
        &Store::of(self).store
    }

    fn key(&self) -> &str {
        &Set::of(self).key
    }

    fn value(&self) -> &[u8] {
        &Set::of(self).value
    }
}

/// Delete operation - removes a key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delete {
    /// The key to delete.
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
    type Output = Result<(), StorageError>;
}

/// Extension trait for `Capability<Delete>` to access its fields.
pub trait DeleteCapability {
    /// Get the store name from the capability chain.
    fn store(&self) -> &str;
    /// Get the key from the capability chain.
    fn key(&self) -> &str;
}

impl DeleteCapability for Capability<Delete> {
    fn store(&self) -> &str {
        &Store::of(self).store
    }

    fn key(&self) -> &str {
        &Delete::of(self).key
    }
}

/// List operation - lists keys in a store.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    type Output = Result<ListResult, StorageError>;
}

/// Result of a list operation.
#[derive(Debug, Clone)]
pub struct ListResult {
    /// Object keys returned in this response.
    pub keys: Vec<String>,
    /// If true, there are more results to fetch.
    pub is_truncated: bool,
    /// Token to use for fetching the next page of results.
    pub next_continuation_token: Option<String>,
}

/// Extension trait for `Capability<List>` to access its fields.
pub trait ListCapability {
    /// Get the store name from the capability chain.
    fn store(&self) -> &str;
    /// Get the continuation token from the capability chain.
    fn continuation_token(&self) -> Option<&str>;
}

impl ListCapability for Capability<List> {
    fn store(&self) -> &str {
        &Store::of(self).store
    }

    fn continuation_token(&self) -> Option<&str> {
        List::of(self).continuation_token.as_deref()
    }
}

/// Errors that can occur during storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Storage backend error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_builds_storage_claim_path() {
        let claim = Subject::from("did:key:zSpace").attenuate(Storage);

        assert_eq!(claim.subject(), "did:key:zSpace");
        assert_eq!(claim.ability(), "/storage");
    }

    #[test]
    fn it_builds_store_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Storage)
            .attenuate(Store::new("index"));

        assert_eq!(claim.subject(), "did:key:zSpace");
        // Store is Policy, not Ability, so it doesn't add to path
        assert_eq!(claim.ability(), "/storage");
    }

    #[test]
    fn it_builds_get_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Get::new("my-key"));

        assert_eq!(claim.ability(), "/storage/get");
    }

    #[test]
    fn it_builds_set_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Set::new("my-key", vec![4, 5, 6]));

        assert_eq!(claim.ability(), "/storage/set");

        // Use policy() method to extract nested constraints
        assert_eq!(claim.policy::<Store, _>().store, "index");
        assert_eq!(claim.policy::<Set, _>().key, "my-key");
    }

    #[cfg(feature = "ucan")]
    mod parameters_tests {
        use super::*;
        use dialog_capability::ucan::parameters;
        use ipld_core::ipld::Ipld;

        #[test]
        fn it_collects_storage_parameters() {
            let cap = Subject::from("did:key:zSpace").attenuate(Storage);
            let params = parameters(&cap);

            // Storage is a unit struct, should produce empty map
            assert!(params.is_empty());
        }

        #[test]
        fn it_collects_store_parameters() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Storage)
                .attenuate(Store::new("index"));
            let params = parameters(&cap);

            assert_eq!(params.get("store"), Some(&Ipld::String("index".into())));
        }

        #[test]
        fn it_collects_get_parameters() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Storage)
                .attenuate(Store::new("index"))
                .invoke(Get::new("my-key"));
            let params = parameters(&cap);

            assert_eq!(params.get("store"), Some(&Ipld::String("index".into())));
            assert_eq!(params.get("key"), Some(&Ipld::String("my-key".into())));
        }

        #[test]
        fn it_collects_set_parameters() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Storage)
                .attenuate(Store::new("mystore"))
                .invoke(Set::new("my-key", vec![30, 40, 50]));
            let params = parameters(&cap);

            assert_eq!(params.get("store"), Some(&Ipld::String("mystore".into())));
            assert_eq!(params.get("key"), Some(&Ipld::String("my-key".into())));
            assert_eq!(params.get("value"), Some(&Ipld::Bytes(vec![30, 40, 50])));
        }

        #[test]
        fn it_collects_delete_parameters() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Storage)
                .attenuate(Store::new("trash"))
                .invoke(Delete::new("to-delete"));
            let params = parameters(&cap);

            assert_eq!(params.get("store"), Some(&Ipld::String("trash".into())));
            assert_eq!(params.get("key"), Some(&Ipld::String("to-delete".into())));
        }
    }
}
