//! Storage access commands.
//!
//! Request types for key-value storage operations.
//! Each type implements `Claim` to provide HTTP method, path, and other request details.
//!
//! # Two APIs
//!
//! 1. **Direct API**: Use `StorageClaim::get(subject, store, key)` for direct S3 access
//! 2. **Capability API**: Use `Capability<Get>` with the capability hierarchy for UCAN flows

use super::{AuthorizationError, Claim, RequestDescriptor};
use crate::Checksum;
use base58::ToBase58;
use dialog_common::Bytes;
use dialog_common::capability::{Capability, Effect, Policy};
use serde::Deserialize;

use crate::capability::storage::Store;

/// A storage claim that can be directly signed.
///
/// This wraps a storage operation with subject and store context,
/// allowing it to be used with `Signer::sign()` directly.
#[derive(Debug)]
pub struct StorageClaim<T> {
    /// Subject DID (path prefix)
    pub subject: String,
    /// Store name
    pub store: String,
    /// The operation
    pub operation: T,
}

impl<T> StorageClaim<T> {
    /// Create a new storage claim.
    pub fn new(subject: impl Into<String>, store: impl Into<String>, operation: T) -> Self {
        Self {
            subject: subject.into(),
            store: store.into(),
            operation,
        }
    }
}

impl StorageClaim<Get> {
    /// Create a GET claim.
    pub fn get(
        subject: impl Into<String>,
        store: impl Into<String>,
        key: impl Into<Bytes>,
    ) -> Self {
        Self::new(subject, store, Get::new(key))
    }
}

impl StorageClaim<Set> {
    /// Create a SET claim.
    pub fn set(
        subject: impl Into<String>,
        store: impl Into<String>,
        key: impl Into<Bytes>,
        checksum: Checksum,
    ) -> Self {
        Self::new(subject, store, Set::new(key, checksum))
    }
}

impl StorageClaim<Delete> {
    /// Create a DELETE claim.
    pub fn delete(
        subject: impl Into<String>,
        store: impl Into<String>,
        key: impl Into<Bytes>,
    ) -> Self {
        Self::new(subject, store, Delete::new(key))
    }
}

impl StorageClaim<List> {
    /// Create a LIST claim.
    pub fn list(
        subject: impl Into<String>,
        store: impl Into<String>,
        continuation_token: Option<String>,
    ) -> Self {
        Self::new(subject, store, List::new(continuation_token))
    }
}

impl Claim for StorageClaim<Get> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject,
            self.store,
            self.operation.key.as_slice().to_base58()
        )
    }
    fn store(&self) -> &str {
        &self.store
    }
}

impl Claim for StorageClaim<Set> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject,
            self.store,
            self.operation.key.as_slice().to_base58()
        )
    }
    fn store(&self) -> &str {
        &self.store
    }
    fn checksum(&self) -> Option<&Checksum> {
        Some(&self.operation.checksum)
    }
}

impl Claim for StorageClaim<Delete> {
    fn method(&self) -> &'static str {
        "DELETE"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject,
            self.store,
            self.operation.key.as_slice().to_base58()
        )
    }
    fn store(&self) -> &str {
        &self.store
    }
}

impl Claim for StorageClaim<List> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        String::new()
    }
    fn store(&self) -> &str {
        &self.store
    }
    fn params(&self) -> Option<Vec<(String, String)>> {
        let mut params = vec![
            ("list-type".to_owned(), "2".to_owned()),
            (
                "prefix".to_owned(),
                format!("{}/{}", self.subject, self.store),
            ),
        ];

        if let Some(token) = &self.operation.continuation_token {
            params.push(("continuation-token".to_owned(), token.clone()));
        }

        Some(params)
    }
}

// UCAN support: implement Ability and ToIpldArgs for StorageClaim types
#[cfg(feature = "ucan")]
mod ucan_impls {
    use super::*;
    use dialog_common::capability::{Ability, Did, ToIpldArgs};
    use ipld_core::ipld::Ipld;
    use std::collections::BTreeMap;

    impl Ability for StorageClaim<Get> {
        fn subject(&self) -> &Did {
            &self.subject
        }
        fn command(&self) -> String {
            "/storage/get".to_string()
        }
    }

    impl ToIpldArgs for StorageClaim<Get> {
        fn to_ipld_args(&self) -> Ipld {
            let mut map = BTreeMap::new();
            map.insert("store".to_string(), Ipld::String(self.store.clone()));
            map.insert("key".to_string(), Ipld::Bytes(self.operation.key.to_vec()));
            Ipld::Map(map)
        }
    }

    impl Ability for StorageClaim<Set> {
        fn subject(&self) -> &Did {
            &self.subject
        }
        fn command(&self) -> String {
            "/storage/set".to_string()
        }
    }

    impl ToIpldArgs for StorageClaim<Set> {
        fn to_ipld_args(&self) -> Ipld {
            let mut map = BTreeMap::new();
            map.insert("store".to_string(), Ipld::String(self.store.clone()));
            map.insert("key".to_string(), Ipld::Bytes(self.operation.key.to_vec()));
            map.insert(
                "checksum".to_string(),
                Ipld::Bytes(self.operation.checksum.as_bytes().to_vec()),
            );
            Ipld::Map(map)
        }
    }

    impl Ability for StorageClaim<Delete> {
        fn subject(&self) -> &Did {
            &self.subject
        }
        fn command(&self) -> String {
            "/storage/delete".to_string()
        }
    }

    impl ToIpldArgs for StorageClaim<Delete> {
        fn to_ipld_args(&self) -> Ipld {
            let mut map = BTreeMap::new();
            map.insert("store".to_string(), Ipld::String(self.store.clone()));
            map.insert("key".to_string(), Ipld::Bytes(self.operation.key.to_vec()));
            Ipld::Map(map)
        }
    }

    impl Ability for StorageClaim<List> {
        fn subject(&self) -> &Did {
            &self.subject
        }
        fn command(&self) -> String {
            "/storage/list".to_string()
        }
    }

    impl ToIpldArgs for StorageClaim<List> {
        fn to_ipld_args(&self) -> Ipld {
            let mut map = BTreeMap::new();
            map.insert("store".to_string(), Ipld::String(self.store.clone()));
            if let Some(token) = &self.operation.continuation_token {
                map.insert(
                    "continuation_token".to_string(),
                    Ipld::String(token.clone()),
                );
            }
            Ipld::Map(map)
        }
    }
}

/// Get value by key.
///
/// The key should be already encoded for S3 compatibility
/// (e.g., using base58 for binary keys).
#[derive(Debug, Deserialize)]
pub struct Get {
    /// The key to look up.
    pub key: Bytes,
}

impl Get {
    /// Create a new Get command.
    pub fn new(key: impl Into<Bytes>) -> Self {
        Self { key: key.into() }
    }
}

/// Get is an effect that produces `RequestDescriptor` that can
/// be used to perform actual get from the s3 bucket.
impl Effect for Get {
    type Of = Store;
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

impl Claim for Capability<Get> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            self.store(),
            Get::of(&self).key.as_slice().to_base58()
        )
    }
    fn store(&self) -> &str {
        &Store::of(self).name
    }
}

/// Set value with key and checksum.
///
/// The key should be already encoded for S3 compatibility.
#[derive(Debug, Deserialize)]
pub struct Set {
    /// The storage key (already encoded).
    pub key: Bytes,
    /// Checksum for integrity verification (32 bytes SHA-256).
    pub checksum: Checksum,
}

/// Set is an effect that produces `RequestDescriptor` that can
/// be used to perform actual set is in the s3 bucket.
impl Effect for Set {
    type Of = Store;
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

impl Set {
    /// Create a new Set command.
    pub fn new(key: impl Into<Bytes>, checksum: Checksum) -> Self {
        Self {
            key: key.into(),
            checksum,
        }
    }
}

impl Claim for Capability<Set> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            self.store(),
            Set::of(&self).key.as_slice().to_base58()
        )
    }
    fn store(&self) -> &str {
        &Store::of(self).name
    }
    fn checksum(&self) -> Option<&Checksum> {
        Some(&Set::of(&self).checksum)
    }
}

/// Delete value by key.
///
/// The key should be already encoded for S3 compatibility.
#[derive(Debug, Deserialize)]
pub struct Delete {
    /// The storage key
    pub key: Bytes,
}

/// Get is an effect that produces `RequestDescriptor` that can
/// be used to perform actual get from the s3 bucket.
impl Effect for Delete {
    type Of = Store;
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

impl Delete {
    /// Create a new Delete command.
    pub fn new(key: impl Into<Bytes>) -> Self {
        Self { key: key.into() }
    }
}

impl Claim for Capability<Delete> {
    fn method(&self) -> &'static str {
        "DELETE"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            self.store(),
            Delete::of(&self).key.as_slice().to_base58()
        )
    }
    fn store(&self) -> &str {
        &Store::of(self).name
    }
}

/// List keys in store.
#[derive(Debug, Deserialize)]
pub struct List {
    /// Continuation token for pagination.
    pub continuation_token: Option<String>,
}

impl List {
    /// Create a new List command.
    pub fn new(continuation_token: Option<String>) -> Self {
        Self { continuation_token }
    }
}

impl Effect for List {
    type Of = Store;
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

impl Claim for Capability<List> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        String::new()
    }
    fn store(&self) -> &str {
        &Store::of(self).name
    }
    fn params(&self) -> Option<Vec<(String, String)>> {
        let mut params = vec![
            ("list-type".to_owned(), "2".to_owned()),
            (
                "prefix".to_owned(),
                format!("{}/{}", self.subject(), self.store()),
            ),
        ];

        if let Some(token) = &List::of(&self).continuation_token {
            params.push(("continuation-token".to_owned(), token.clone()));
        }

        Some(params)
    }
}
