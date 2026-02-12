//! Archive S3 request implementations.
//!
//! This module provides S3-specific effect types and `S3Request` implementations
//! for archive (content-addressed) operations, enabling them to be translated into presigned S3 URLs.

use super::{AccessError, AuthorizedRequest, S3Request};
use crate::Checksum;
use base58::ToBase58;
use dialog_capability::{Capability, Effect, Policy};
use dialog_common::Blake3Hash;
use serde::{Deserialize, Serialize};

// Re-export hierarchy types from dialog-effects
pub use dialog_effects::archive::{Archive, Catalog};

/// Get content by digest (S3 authorization).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Get {
    /// The blake3 digest of the content to retrieve.
    #[serde(with = "dialog_common::as_bytes")]
    pub digest: Blake3Hash,
}

impl Get {
    /// Create a new Get effect.
    pub fn new<T>(digest: T) -> Self
    where
        Blake3Hash: From<T>,
    {
        Self {
            digest: digest.into(),
        }
    }
}

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

/// Put content with digest and checksum (S3 authorization).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Put {
    /// Content digest (Blake3 hash, used as S3 key).
    #[serde(with = "dialog_common::as_bytes")]
    pub digest: Blake3Hash,
    /// Checksum for integrity verification.
    pub checksum: Checksum,
}

impl Put {
    /// Create a new Put effect.
    pub fn new<T>(digest: T, checksum: Checksum) -> Self
    where
        Blake3Hash: From<T>,
    {
        Self {
            digest: digest.into(),
            checksum,
        }
    }
}

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
