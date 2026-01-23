//! Storage access commands.
//!
//! Request types for key-value storage operations.
//! Each type implements `Claim` to provide HTTP method, path, and other request details.
//!
//! # Two APIs
//!
//! 1. **Direct API**: Use `StorageClaim::get(subject, store, key)` for direct S3 access
//! 2. **Capability API**: Use `Capability<Get>` with the capability hierarchy for UCAN flows

use super::{AccessError, AuthorizedRequest, S3Request};
use crate::Checksum;
use base58::ToBase58;
use dialog_common::Bytes;
use dialog_common::capability::{Attenuation, Capability, Effect, Policy, Subject};
use serde::{Deserialize, Serialize};

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

/// Get value by key.
///
/// The key should be already encoded for S3 compatibility
/// (e.g., using base58 for binary keys).
#[derive(Debug, Serialize, Deserialize)]
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
    type Output = Result<AuthorizedRequest, AccessError>;
}

impl S3Request for Capability<Get> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            Store::of(self).store,
            Get::of(self).key.as_slice().to_base58()
        )
    }
}

/// Set value with key and checksum.
///
/// The key should be already encoded for S3 compatibility.
#[derive(Debug, Serialize, Deserialize)]
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
    type Output = Result<AuthorizedRequest, AccessError>;
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

impl S3Request for Capability<Set> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            &Store::of(self).store,
            Set::of(self).key.as_slice().to_base58()
        )
    }
    fn checksum(&self) -> Option<&Checksum> {
        Some(&Set::of(self).checksum)
    }
}

/// Delete value by key.
///
/// The key should be already encoded for S3 compatibility.
#[derive(Debug, Serialize, Deserialize)]
pub struct Delete {
    /// The storage key
    pub key: Bytes,
}

/// Get is an effect that produces `RequestDescriptor` that can
/// be used to perform actual get from the s3 bucket.
impl Effect for Delete {
    type Of = Store;
    type Output = Result<AuthorizedRequest, AccessError>;
}

impl Delete {
    /// Create a new Delete command.
    pub fn new(key: impl Into<Bytes>) -> Self {
        Self { key: key.into() }
    }
}

impl S3Request for Capability<Delete> {
    fn method(&self) -> &'static str {
        "DELETE"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            &Store::of(self).store,
            Delete::of(self).key.as_slice().to_base58()
        )
    }
}

/// List keys in store.
#[derive(Debug, Serialize, Deserialize)]
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
    type Output = Result<AuthorizedRequest, AccessError>;
}

impl S3Request for Capability<List> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        String::new()
    }
    fn params(&self) -> Option<Vec<(String, String)>> {
        let mut params = vec![
            ("list-type".to_owned(), "2".to_owned()),
            (
                "prefix".to_owned(),
                format!("{}/{}", self.subject(), &Store::of(self).store),
            ),
        ];

        if let Some(token) = &List::of(self).continuation_token {
            params.push(("continuation-token".to_owned(), token.clone()));
        }

        Some(params)
    }
}
