//! Archive access commands.
//!
//! Dos for authorizing content-addressed storage operations.
//! Each effect returns a `Result<RequestDescriptor, AuthorizationError>` that can
//! be used to make the actual HTTP request.

use super::{AuthorizationError, Claim, RequestDescriptor};
use crate::Checksum;
use dialog_common::Effect;
use serde::Deserialize;

#[cfg(feature = "ucan")]
use super::Args;
#[cfg(feature = "ucan")]
use dialog_common::Provider;

/// Archive command enum for UCAN parsing.
#[cfg(feature = "ucan")]
#[derive(Debug)]
pub enum Do {
    Get(Get),
    Put(Put),
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
            ["put"] => Ok(Do::Put(args.deserialize()?)),
            ["delete"] => Ok(Do::Delete(args.deserialize()?)),
            ["list"] => Ok(Do::List(args.deserialize()?)),
            _ => Err(AuthorizationError::Invocation(format!(
                "Unknown archive command: {:?}",
                segments
            ))),
        }
    }
}

/// Trait for providers that can execute all archive commands.
#[cfg(feature = "ucan")]
pub trait ArchiveProvider:
    Provider<Get> + Provider<Put> + Provider<Delete> + Provider<List>
{
}

#[cfg(feature = "ucan")]
impl<T> ArchiveProvider for T where
    T: Provider<Get> + Provider<Put> + Provider<Delete> + Provider<List>
{
}

#[cfg(feature = "ucan")]
impl Do {
    /// Perform this command using the given provider.
    pub async fn perform<P: ArchiveProvider>(
        self,
        provider: &P,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Do::Get(cmd) => cmd.perform(provider).await,
            Do::Put(cmd) => cmd.perform(provider).await,
            Do::Delete(cmd) => cmd.perform(provider).await,
            Do::List(cmd) => cmd.perform(provider).await,
        }
    }
}

/// Get content by digest.
#[derive(Debug, Deserialize)]
pub struct Get {
    /// The catalog (e.g., "index", "blob").
    pub catalog: String,
    /// Content digest (Blake3 hash, used as S3 key).
    pub digest: String,
}

impl Get {
    /// Create a new Get command.
    pub fn new(catalog: impl Into<String>, digest: impl Into<String>) -> Self {
        Self {
            catalog: catalog.into(),
            digest: digest.into(),
        }
    }
}

impl Claim for Get {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        format!("{}/{}", self.catalog, self.digest)
    }
}

impl Effect for Get {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

/// Put content with digest and checksum.
#[derive(Debug, Deserialize)]
pub struct Put {
    /// The catalog (e.g., "index", "blob").
    pub catalog: String,
    /// Content digest (Blake3 hash, used as S3 key).
    pub digest: String,
    /// Checksum for integrity verification.
    pub checksum: Checksum,
}

impl Put {
    /// Create a new Put command.
    pub fn new(catalog: impl Into<String>, digest: impl Into<String>, checksum: Checksum) -> Self {
        Self {
            catalog: catalog.into(),
            digest: digest.into(),
            checksum,
        }
    }
}

impl Claim for Put {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!("{}/{}", self.catalog, self.digest)
    }
    fn checksum(&self) -> Option<&Checksum> {
        Some(&self.checksum)
    }
}

impl Effect for Put {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

/// Delete content by digest.
#[derive(Debug, Deserialize)]
pub struct Delete {
    /// The catalog.
    pub catalog: String,
    /// Content digest.
    pub digest: String,
}

impl Delete {
    /// Create a new Delete command.
    pub fn new(catalog: impl Into<String>, digest: impl Into<String>) -> Self {
        Self {
            catalog: catalog.into(),
            digest: digest.into(),
        }
    }
}

impl Claim for Delete {
    fn method(&self) -> &'static str {
        "DELETE"
    }
    fn path(&self) -> String {
        format!("{}/{}", self.catalog, self.digest)
    }
}

impl Effect for Delete {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

/// List objects in catalog.
#[derive(Debug, Deserialize)]
pub struct List {
    /// The catalog.
    pub catalog: String,
    /// Continuation token for pagination.
    pub continuation_token: Option<String>,
}

impl List {
    /// Create a new List command.
    pub fn new(catalog: impl Into<String>, continuation_token: Option<String>) -> Self {
        Self {
            catalog: catalog.into(),
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
        let mut params = vec![("list-type", "2"), ("prefix", &self.catalog)];

        if let Some(token) = &self.continuation_token {
            params.push(("continuation-token", token));
        }

        Some(params)
    }
}

impl Effect for List {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}
