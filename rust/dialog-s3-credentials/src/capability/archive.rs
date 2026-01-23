//! Archive access commands.
//!
//! Request types for content-addressed storage operations.
//! Each type implements `Claim` to provide HTTP method, path, and other request details.

use super::{AccessError, AuthorizedRequest, S3Request};
use crate::Checksum;
use base58::ToBase58;
use dialog_common::Blake3Hash;
use dialog_common::capability::{Attenuation, Capability, Effect, Policy, Subject};
use serde::{Deserialize, Serialize};

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
    pub catalog: String,
}

impl Catalog {
    /// Create a new Catalog policy.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            catalog: name.into(),
        }
    }
}

impl Policy for Catalog {
    type Of = Archive;
}

/// Get content by digest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Get {
    /// The blake3 digest of the content to retrieve.
    pub digest: Blake3Hash,
}

impl Get {
    /// Create a new Get command.
    pub fn new(digest: impl Into<Blake3Hash>) -> Self {
        Self {
            digest: digest.into(),
        }
    }
}

/// Get is an effect that produces `RequestDescriptor` that can
/// be used to perform actual get from the s3 bucket.
impl Effect for Get {
    type Of = Catalog;
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
            Catalog::of(self).catalog,
            Get::of(self).digest.as_bytes().to_base58()
        )
    }
}

/// Put content with digest and checksum.
#[derive(Debug, Serialize, Deserialize)]
pub struct Put {
    /// Content digest (Blake3 hash, used as S3 key).
    pub digest: Blake3Hash,
    /// Checksum for integrity verification.
    pub checksum: Checksum,
}

impl Put {
    /// Create a new Put command.
    pub fn new(digest: impl Into<Blake3Hash>, checksum: Checksum) -> Self {
        Self {
            digest: digest.into(),
            checksum,
        }
    }
}

/// Put is an effect that produces `RequestDescriptor` that can
/// be used to perform actual put into the s3 bucket.
impl Effect for Put {
    type Of = Catalog;
    type Output = Result<AuthorizedRequest, AccessError>;
}

impl S3Request for Capability<Put> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            Catalog::of(self).catalog,
            Put::of(self).digest.as_bytes().to_base58()
        )
    }
    fn checksum(&self) -> Option<&Checksum> {
        Some(&Put::of(self).checksum)
    }
}
