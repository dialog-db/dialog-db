//! Archive access commands.
//!
//! Request types for content-addressed storage operations.
//! Each type implements `Claim` to provide HTTP method, path, and other request details.

use super::{AccessError, AuthorizedRequest, S3Request};
use crate::Checksum;
pub use crate::capability::archive::{Archive, Catalog};
use base58::ToBase58;
use dialog_common::Blake3Hash;
use dialog_common::capability::{Capability, Effect, Policy};
use serde::{Deserialize, Serialize};

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
            Catalog::of(&self).catalog,
            Get::of(&self).digest.as_bytes().to_base58()
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
            Catalog::of(&self).catalog,
            Put::of(&self).digest.as_bytes().to_base58()
        )
    }
    fn checksum(&self) -> Option<&Checksum> {
        Some(&Put::of(&self).checksum)
    }
}
