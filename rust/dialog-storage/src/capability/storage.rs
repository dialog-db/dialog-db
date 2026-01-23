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
pub use dialog_common::capability::{Attenuation, Capability, Effect, Policy, Subject};

// S3 authorization types (only available with s3 feature)
#[cfg(feature = "s3")]
pub use dialog_s3_credentials::storage::{
    Delete as AuthorizeDelete, Get as AuthorizeGet, Set as AuthorizeSet, Storage, Store,
};

use thiserror::Error;

/// Get operation - retrieves a value by key.
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

#[cfg(all(test, feature = "s3"))]
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
            .attenuate(Store {
                store: "index".into(),
            });

        assert_eq!(claim.subject(), "did:key:zSpace");
        // Store is Policy, not Ability, so it doesn't add to path
        assert_eq!(claim.ability(), "/storage");
    }

    #[test]
    fn it_builds_get_claim_path() {
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
    fn it_builds_set_claim_path() {
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
        use ipld_core::ipld::Ipld;

        #[test]
        fn it_collects_storage_parameters() {
            let cap = Subject::from("did:key:zSpace").attenuate(Storage);
            let params = cap.parameters();

            // Storage is a unit struct, should produce empty map
            assert!(params.is_empty());
        }

        #[test]
        fn it_collects_store_parameters() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Storage)
                .attenuate(Store {
                    store: "index".into(),
                });
            let params = cap.parameters();

            assert_eq!(params.get("store"), Some(&Ipld::String("index".into())));
        }

        #[test]
        fn it_collects_get_parameters() {
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
        fn it_collects_set_parameters() {
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
        fn it_collects_delete_parameters() {
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
