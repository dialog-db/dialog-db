//! Storage S3 request implementations.
//!
//! This module provides S3-specific effect types and `S3Request` implementations
//! for storage operations, enabling them to be translated into presigned S3 URLs.

use super::{AccessError, AuthorizedRequest, S3Request};
use crate::Checksum;
use base58::ToBase58;
use dialog_capability::{Capability, Effect, Policy};
use dialog_common::Bytes;
use serde::{Deserialize, Serialize};

// Re-export hierarchy types from dialog-effects
pub use dialog_effects::storage::{Storage, Store};

/// Get value by key (S3 authorization).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Get {
    /// The key to look up.
    pub key: Bytes,
}

impl Get {
    /// Create a new Get effect.
    pub fn new(key: impl Into<Bytes>) -> Self {
        Self { key: key.into() }
    }
}

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

/// Set value with key and checksum (S3 authorization).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Set {
    /// The storage key.
    pub key: Bytes,
    /// Checksum for integrity verification (SHA-256).
    pub checksum: Checksum,
}

impl Set {
    /// Create a new Set effect.
    pub fn new(key: impl Into<Bytes>, checksum: Checksum) -> Self {
        Self {
            key: key.into(),
            checksum,
        }
    }
}

impl Effect for Set {
    type Of = Store;
    type Output = Result<AuthorizedRequest, AccessError>;
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

/// Delete value by key (S3 authorization).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delete {
    /// The storage key to delete.
    pub key: Bytes,
}

impl Delete {
    /// Create a new Delete effect.
    pub fn new(key: impl Into<Bytes>) -> Self {
        Self { key: key.into() }
    }
}

impl Effect for Delete {
    type Of = Store;
    type Output = Result<AuthorizedRequest, AccessError>;
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

/// List keys in store (S3 authorization).
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
