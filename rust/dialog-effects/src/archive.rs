//! Archive capability hierarchy.
//!
//! Archive provides content-addressed blob storage organized into catalogs.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//!   └── Archive (ability: /archive)
//!         └── Catalog { catalog: String }
//!               ├── Get { digest } → Effect → Result<Option<Bytes>, ArchiveError>
//!               └── Put { digest, content } → Effect → Result<(), ArchiveError>
//! ```

use std::error::Error;

pub use dialog_capability::{
    Attenuate, Attenuation, Capability, DialogCapabilityAuthorizationError,
    DialogCapabilityPerformError, Effect, Policy, StorageError, Subject,
    access::AuthorizeError,
};
pub use dialog_common::Blake3Hash;
use dialog_common::Checksum;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Archive ability - restricts to archive operations.
///
/// Attaches to Subject and provides the `/archive` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Archive;

impl Attenuation for Archive {
    type Of = Subject;
}

/// Catalog policy that scopes operations to a named catalog.
///
/// Does not add to ability path but constrains invocation arguments.
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

/// Get operation - retrieves content by digest.
///
/// Requires `Capability<Catalog>` access level.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Get {
    /// The blake3 digest of the content to retrieve.
    #[serde(with = "dialog_common::as_bytes")]
    pub digest: Blake3Hash,
}

impl Get {
    /// Create a new Get effect.
    pub fn new(digest: impl Into<Blake3Hash>) -> Self {
        Self {
            digest: digest.into(),
        }
    }
}

impl Effect for Get {
    type Of = Catalog;
    type Output = Result<Option<Vec<u8>>, ArchiveError>;
}

/// Put operation - stores content by digest.
///
/// Requires `Capability<Catalog>` access level.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Put {
    /// The blake3 digest of the content (must match hash of content).
    #[serde(with = "dialog_common::as_bytes")]
    pub digest: Blake3Hash,
    /// The content to store.
    #[serde(with = "serde_bytes")]
    #[attenuate(into = Checksum, with = Checksum::sha256, rename = checksum)]
    pub content: Vec<u8>,
}

impl Put {
    /// Create a new Put effect.
    pub fn new(digest: impl Into<Blake3Hash>, content: impl Into<Vec<u8>>) -> Self {
        Self {
            digest: digest.into(),
            content: content.into(),
        }
    }
}

impl Effect for Put {
    type Of = Catalog;
    type Output = Result<(), ArchiveError>;
}

pub mod prelude;

/// Errors that can occur during archive operations.
#[derive(Debug, Error)]
pub enum ArchiveError {
    /// Content digest mismatch.
    #[error("Content digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch {
        /// Expected digest.
        expected: String,
        /// Actual digest.
        actual: String,
    },

    /// Authorization error occurred.
    #[error("Unauthorized error: {0}")]
    AuthorizationError(String),

    /// Execution error occurred during operation.
    #[error("Executions error: {0}")]
    ExecutionError(String),

    /// Storage backend error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(String),
}

impl From<StorageError> for ArchiveError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<DialogCapabilityAuthorizationError> for ArchiveError {
    fn from(value: DialogCapabilityAuthorizationError) -> Self {
        ArchiveError::AuthorizationError(value.to_string())
    }
}

impl From<AuthorizeError> for ArchiveError {
    fn from(value: AuthorizeError) -> Self {
        ArchiveError::AuthorizationError(value.to_string())
    }
}

impl<E: Error> From<DialogCapabilityPerformError<E>> for ArchiveError {
    fn from(value: DialogCapabilityPerformError<E>) -> Self {
        match value {
            DialogCapabilityPerformError::Authorization(error) => {
                ArchiveError::AuthorizationError(error.to_string())
            }
            DialogCapabilityPerformError::Execution(error) => {
                ArchiveError::ExecutionError(error.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::did;

    #[test]
    fn it_builds_archive_claim_path() {
        let claim = Subject::from(did!("key:zSpace")).attenuate(Archive);

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/archive");
    }

    #[test]
    fn it_builds_catalog_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Archive)
            .attenuate(Catalog::new("index"));

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        // Catalog is Policy, not Ability, so it doesn't add to path
        assert_eq!(claim.ability(), "/archive");
    }

    #[test]
    fn it_builds_get_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new([0u8; 32]));

        assert_eq!(claim.ability(), "/archive/get");
    }

    #[test]
    fn it_builds_put_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new([0u8; 32], Vec::new()));

        assert_eq!(claim.ability(), "/archive/put");
    }
}
