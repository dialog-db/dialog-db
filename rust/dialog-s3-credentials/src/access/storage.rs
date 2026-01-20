//! Storage access commands.
//!
//! Dos for authorizing key-value storage operations.
//! Each effect returns a `Result<RequestDescriptor, AuthorizationError>` that can
//! be used to make the actual HTTP request.

use super::Claim;
use super::{AuthorizationError, RequestDescriptor};
use crate::Checksum;
use dialog_common::Effect;
use serde::Deserialize;

#[cfg(feature = "ucan")]
use super::Args;
#[cfg(feature = "ucan")]
use dialog_common::Provider;

/// Storage command enum for UCAN parsing.
#[cfg(feature = "ucan")]
#[derive(Debug)]
pub enum Do {
    Get(Get),
    Set(Set),
    Delete(Delete),
    List(List),
}

#[cfg(feature = "ucan")]
impl Effect for Do {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

#[cfg(feature = "ucan")]
impl<'a> TryFrom<(&'a [&'a str], Args<'a>)> for Do {
    type Error = AuthorizationError;

    fn try_from((segments, args): (&'a [&'a str], Args<'a>)) -> Result<Self, Self::Error> {
        match segments {
            ["get"] => Ok(Do::Get(args.deserialize()?)),
            ["set"] => Ok(Do::Set(args.deserialize()?)),
            ["delete"] => Ok(Do::Delete(args.deserialize()?)),
            ["list"] => Ok(Do::List(args.deserialize()?)),
            _ => Err(AuthorizationError::Invocation(format!(
                "Unknown storage command: {:?}",
                segments
            ))),
        }
    }
}

/// Trait for providers that can execute all storage commands.
#[cfg(feature = "ucan")]
pub trait StorageProvider:
    Provider<Get> + Provider<Set> + Provider<Delete> + Provider<List>
{
}

#[cfg(feature = "ucan")]
impl<T> StorageProvider for T where
    T: Provider<Get> + Provider<Set> + Provider<Delete> + Provider<List>
{
}

#[cfg(feature = "ucan")]
impl Do {
    /// Perform this command using the given provider.
    pub async fn perform<P: StorageProvider>(
        self,
        provider: &P,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Do::Get(cmd) => cmd.perform(provider).await,
            Do::Set(cmd) => cmd.perform(provider).await,
            Do::Delete(cmd) => cmd.perform(provider).await,
            Do::List(cmd) => cmd.perform(provider).await,
        }
    }
}

/// Get value by key.
///
/// The key should be already encoded for S3 compatibility
/// (e.g., using base58 for binary keys).
#[derive(Debug, Deserialize)]
pub struct Get {
    /// The store name (e.g., "index", "blob"). Empty string for root.
    pub store: String,
    /// The storage key (already encoded).
    pub key: String,
}

impl Get {
    /// Create a new Get command.
    pub fn new(store: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            store: store.into(),
            key: key.into(),
        }
    }
}

impl Effect for Get {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

impl Claim for Get {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        if self.store.is_empty() {
            self.key.clone()
        } else {
            format!("{}/{}", self.store, self.key)
        }
    }
}

/// Set value with key and checksum.
///
/// The key should be already encoded for S3 compatibility.
#[derive(Debug, Deserialize)]
pub struct Set {
    /// The store name (e.g., "index", "blob"). Empty string for root.
    pub store: String,
    /// The storage key (already encoded).
    pub key: String,
    /// Checksum for integrity verification (32 bytes SHA-256).
    pub checksum: Checksum,
}

impl Set {
    /// Create a new Set command.
    pub fn new(store: impl Into<String>, key: impl Into<String>, checksum: Checksum) -> Self {
        Self {
            store: store.into(),
            key: key.into(),
            checksum,
        }
    }
}

impl Effect for Set {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

impl Claim for Set {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        if self.store.is_empty() {
            self.key.clone()
        } else {
            format!("{}/{}", self.store, self.key)
        }
    }
    fn checksum(&self) -> Option<&Checksum> {
        Some(&self.checksum)
    }
}

/// Delete value by key.
///
/// The key should be already encoded for S3 compatibility.
#[derive(Debug, Deserialize)]
pub struct Delete {
    /// The store name (e.g., "index", "blob"). Empty string for root.
    pub store: String,
    /// The storage key (already encoded).
    pub key: String,
}

impl Delete {
    /// Create a new Delete command.
    pub fn new(store: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            store: store.into(),
            key: key.into(),
        }
    }
}

impl Effect for Delete {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

impl Claim for Delete {
    fn method(&self) -> &'static str {
        "DELETE"
    }
    fn path(&self) -> String {
        if self.store.is_empty() {
            self.key.clone()
        } else {
            format!("{}/{}", self.store, self.key)
        }
    }
}

/// List keys in store.
#[derive(Debug, Deserialize)]
pub struct List {
    /// The store name to list under.
    pub store: String,
    /// Continuation token for pagination.
    pub continuation_token: Option<String>,
}

impl List {
    /// Create a new List command.
    pub fn new(store: impl Into<String>, continuation_token: Option<String>) -> Self {
        Self {
            store: store.into(),
            continuation_token,
        }
    }
}

impl Claim for List {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        String::new()
    }
    fn params(&self) -> Option<Vec<(&str, &str)>> {
        let mut params = vec![("list-type", "2"), ("prefix", &self.store)];

        if let Some(token) = &self.continuation_token {
            params.push(("continuation-token", token));
        }

        Some(params)
    }
}

impl Effect for List {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}
