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

pub use dialog_capability::{Attenuation, Capability, Effect, PerformError, Policy, Subject};
pub use dialog_common::{Blake3Hash, Bytes};
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Get {
    /// The blake3 digest of the content to retrieve.
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
    type Output = Result<Option<Bytes>, ArchiveError>;
}

/// Extension trait for `Capability<Get>` to access its fields.
pub trait GetCapability {
    /// Get the catalog name from the capability chain.
    fn catalog(&self) -> &str;
    /// Get the digest from the capability chain.
    fn digest(&self) -> &Blake3Hash;
}

impl GetCapability for Capability<Get> {
    fn catalog(&self) -> &str {
        &Catalog::of(self).catalog
    }

    fn digest(&self) -> &Blake3Hash {
        &Get::of(self).digest
    }
}

/// Put operation - stores content by digest.
///
/// Requires `Capability<Catalog>` access level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Put {
    /// The blake3 digest of the content (must match hash of content).
    pub digest: Blake3Hash,
    /// The content to store.
    pub content: Bytes,
}

impl Put {
    /// Create a new Put effect.
    pub fn new(digest: impl Into<Blake3Hash>, content: impl Into<Bytes>) -> Self {
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

/// Extension trait for `Capability<Put>` to access its fields.
pub trait PutCapability {
    /// Get the catalog name from the capability chain.
    fn catalog(&self) -> &str;
    /// Get the digest from the capability chain.
    fn digest(&self) -> &Blake3Hash;
    /// Get the content from the capability chain.
    fn content(&self) -> &Bytes;
}

impl PutCapability for Capability<Put> {
    fn catalog(&self) -> &str {
        &Catalog::of(self).catalog
    }

    fn digest(&self) -> &Blake3Hash {
        &Put::of(self).digest
    }

    fn content(&self) -> &Bytes {
        &Put::of(self).content
    }
}

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

impl From<dialog_capability::AuthorizationError> for ArchiveError {
    fn from(value: dialog_capability::AuthorizationError) -> Self {
        ArchiveError::AuthorizationError(value.to_string())
    }
}

impl<E: Error> From<PerformError<E>> for ArchiveError {
    fn from(value: PerformError<E>) -> Self {
        match value {
            PerformError::Authorization(error) => {
                ArchiveError::AuthorizationError(error.to_string())
            }
            PerformError::Excution(error) => ArchiveError::ExecutionError(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_builds_archive_claim_path() {
        let claim = Subject::from("did:key:zSpace").attenuate(Archive);

        assert_eq!(claim.subject(), "did:key:zSpace");
        assert_eq!(claim.ability(), "/archive");
    }

    #[test]
    fn it_builds_catalog_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Archive)
            .attenuate(Catalog::new("index"));

        assert_eq!(claim.subject(), "did:key:zSpace");
        // Catalog is Policy, not Ability, so it doesn't add to path
        assert_eq!(claim.ability(), "/archive");
    }

    #[test]
    fn it_builds_get_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new([0u8; 32]));

        assert_eq!(claim.ability(), "/archive/get");
    }

    #[test]
    fn it_builds_put_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new([0u8; 32], Bytes::new()));

        assert_eq!(claim.ability(), "/archive/put");
    }

    #[cfg(feature = "ucan")]
    mod parameters_tests {
        use super::*;
        use dialog_capability::ucan::parameters;
        use ipld_core::ipld::Ipld;

        #[test]
        fn it_collects_archive_parameters() {
            let cap = Subject::from("did:key:zSpace").attenuate(Archive);
            let params = parameters(&cap);

            // Archive is a unit struct, should produce empty map
            assert!(params.is_empty());
        }

        #[test]
        fn it_collects_catalog_parameters() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Archive)
                .attenuate(Catalog::new("blobs"));
            let params = parameters(&cap);

            assert_eq!(params.get("catalog"), Some(&Ipld::String("blobs".into())));
        }

        #[test]
        fn it_collects_get_parameters() {
            let digest = Blake3Hash::from([1u8; 32]);
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest));
            let params = parameters(&cap);

            assert_eq!(params.get("catalog"), Some(&Ipld::String("index".into())));
            assert_eq!(params.get("digest"), Some(&Ipld::Bytes([1u8; 32].to_vec())));
        }

        #[test]
        fn it_collects_put_parameters() {
            let digest = Blake3Hash::from([2u8; 32]);
            let content = b"hello world".to_vec();
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Archive)
                .attenuate(Catalog::new("data"))
                .invoke(Put::new(digest, content.clone()));
            let params = parameters(&cap);

            assert_eq!(params.get("catalog"), Some(&Ipld::String("data".into())));
            assert_eq!(params.get("digest"), Some(&Ipld::Bytes([2u8; 32].to_vec())));
            assert_eq!(params.get("content"), Some(&Ipld::Bytes(content)));
        }
    }
}
