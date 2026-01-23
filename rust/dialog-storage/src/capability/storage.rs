//! Storage capability hierarchy.
//!
//! Storage provides key-value store operations.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//!   └── Storage (cmd: /storage)
//!         └── Store { store: String }
//!               ├── Get { key } → Effect → Result<Option<Bytes>, StorageError>
//!               ├── Set { key, value } → Effect → Result<(), StorageError>
//!               └── Delete { key } → Effect → Result<(), StorageError>
//! ```

use dialog_common::Bytes;
use dialog_common::capability::{Attenuation, Capability, Effect, Policy, Subject};
use thiserror::Error;

// Storage Ability

/// Storage ability - restricts to storage operations.
///
/// Adds `/storage` to the command path.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Storage;

impl Attenuation for Storage {
    type Of = Subject;
}

// Store Policy

/// Store policy - restricts storage access to a specific store.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Store {
    /// The store name.
    pub store: String,
}

impl Policy for Store {
    type Of = Storage;
}

// Lookup Effect

/// Lookup operation - retrieves a value by key.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Get {
    /// The key to look up.
    pub key: Bytes,
}

impl Effect for Get {
    type Of = Store;
    type Output = Result<Option<Bytes>, StorageError>;
}

/// Extension trait for `Capability<Get>` to access its fields.
pub trait GetCapability {
    /// Get the store name from the capability chain.
    fn store(&self) -> &str;
    /// Get the key from the capability chain.
    fn key(&self) -> &Bytes;
}

impl GetCapability for Capability<Get> {
    fn store(&self) -> &str {
        &Store::of(self).store
    }

    fn key(&self) -> &Bytes {
        &Get::of(self).key
    }
}

// Update Effect

/// Update operation - sets a value for a key.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Set {
    /// The key to update.
    pub key: Bytes,
    /// The value to set.
    pub value: Bytes,
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
    fn key(&self) -> &Bytes;
    /// Get the value from the capability chain.
    fn value(&self) -> &Bytes;
}

impl SetCapability for Capability<Set> {
    fn store(&self) -> &str {
        &Store::of(self).store
    }

    fn key(&self) -> &Bytes {
        &Set::of(self).key
    }

    fn value(&self) -> &Bytes {
        &Set::of(self).value
    }
}

/// Delete operation - removes a key.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Delete {
    /// The key to delete.
    pub key: Bytes,
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
    fn key(&self) -> &Bytes;
}

impl DeleteCapability for Capability<Delete> {
    fn store(&self) -> &str {
        &Store::of(self).store
    }

    fn key(&self) -> &Bytes {
        &Delete::of(self).key
    }
}

// Storage Error

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
    fn test_storage_claim_path() {
        let claim = Subject::from("did:key:zSpace").attenuate(Storage);

        assert_eq!(claim.subject(), "did:key:zSpace");
        assert_eq!(claim.ability(), "/storage");
    }

    #[test]
    fn test_store_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Storage)
            .attenuate(Store {
                store: "index".into(),
            });

        assert_eq!(claim.subject(), "did:key:zSpace");
        // Store is Policy, not Ability, so it doesn't add to path
        assert_eq!(claim.ability(), "/storage");
    }

    #[test]
    fn test_get_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Storage)
            .attenuate(Store {
                store: "index".into(),
            })
            .invoke(Get {
                key: vec![1, 2, 3].into(),
            });

        assert_eq!(claim.ability(), "/storage/get");
    }

    #[test]
    fn test_set_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Storage)
            .attenuate(Store {
                store: "index".into(),
            })
            .attenuate(Set {
                key: vec![1, 2, 3].into(),
                value: vec![4, 5, 6].into(),
            });

        assert_eq!(claim.ability(), "/storage/set");

        // Use policy() method to extract nested constraints
        assert_eq!(claim.policy::<Store, _>().store, "index");
        assert_eq!(claim.policy::<Set, _>().key.as_slice(), &[1, 2, 3]);
    }

    #[cfg(feature = "ucan")]
    mod parameters_tests {
        use super::*;
        use crate::capability::Settings;
        use ipld_core::ipld::Ipld;

        #[test]
        fn test_storage_parameters() {
            let cap = Subject::from("did:key:zSpace").attenuate(Storage);
            let params = cap.parameters();

            // Storage is a unit struct, should produce empty map
            assert!(params.is_empty());
        }

        #[test]
        fn test_store_parameters() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Storage)
                .attenuate(Store {
                    store: "index".into(),
                });
            let params = cap.parameters();

            assert_eq!(params.get("store"), Some(&Ipld::String("index".into())));
        }

        #[test]
        fn test_get_parameters() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Storage)
                .attenuate(Store {
                    store: "index".into(),
                })
                .attenuate(Get {
                    key: vec![1, 2, 3].into(),
                });
            let params = cap.parameters();

            assert_eq!(params.get("store"), Some(&Ipld::String("index".into())));
            assert_eq!(params.get("key"), Some(&Ipld::Bytes(vec![1, 2, 3])));
        }

        #[test]
        fn test_set_parameters() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Storage)
                .attenuate(Store {
                    store: "mystore".into(),
                })
                .attenuate(Set {
                    key: vec![10, 20].into(),
                    value: vec![30, 40, 50].into(),
                });
            let params = cap.parameters();

            assert_eq!(params.get("store"), Some(&Ipld::String("mystore".into())));
            assert_eq!(params.get("key"), Some(&Ipld::Bytes(vec![10, 20])));
            assert_eq!(params.get("value"), Some(&Ipld::Bytes(vec![30, 40, 50])));
        }

        #[test]
        fn test_delete_parameters() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Storage)
                .attenuate(Store {
                    store: "trash".into(),
                })
                .attenuate(Delete {
                    key: vec![99].into(),
                });
            let params = cap.parameters();

            assert_eq!(params.get("store"), Some(&Ipld::String("trash".into())));
            assert_eq!(params.get("key"), Some(&Ipld::Bytes(vec![99])));
        }
    }
}
