//! Archive capability hierarchy.
//!
//! Archive provides content-addressed blob storage organized into catalogs.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//!   └── Archive (cmd: /archive)
//!         └── Catalog { catalog: String }
//!               ├── Get { digest } → Effect → Result<Option<Bytes>, ArchiveError>
//!               └── Put { digest, content } → Effect → Result<(), ArchiveError>
//! ```

use std::error::Error;

use dialog_common::capability::{Attenuation, Capability, Effect, PerformError, Policy, Subject};
use dialog_common::{Blake3Hash, Bytes};
use dialog_s3_credentials::AccessError;
use thiserror::Error;

// Archive Ability

/// Archive ability - restricts to archive operations.
///
/// Adds `/archive` to the command path.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Archive;

impl Attenuation for Archive {
    type Of = Subject;
}

// Catalog Policy

/// Catalog policy - restricts archive access to a specific catalog.
///
/// Does not add to command path but constrains invocation arguments.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Catalog {
    /// The catalog name (e.g., "index", "blobs").
    pub catalog: String,
}

impl Policy for Catalog {
    type Of = Archive;
}

// Get Effect

/// Get operation - retrieves content by digest.
///
/// Requires `Capability<Catalog>` access level.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Get {
    /// The blake3 digest of the content to retrieve.
    pub digest: Blake3Hash,
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

// Put Effect

/// Put operation - stores content by digest.
///
/// Requires `Capability<Catalog>` access level.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Put {
    /// The blake3 digest of the content (must match hash of content).
    pub digest: Blake3Hash,
    /// The content to store.
    pub content: Bytes,
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

// Archive Error

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

    #[error("Unauthorized error: {0}")]
    AuthorizationError(String),

    #[error("Executions error: {0}")]
    ExecutionError(String),

    /// Storage backend error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(String),
}

impl From<AccessError> for ArchiveError {
    fn from(value: AccessError) -> Self {
        ArchiveError::AuthorizationError(value.to_string())
    }
}

impl From<dialog_common::capability::AuthorizationError> for ArchiveError {
    fn from(value: dialog_common::capability::AuthorizationError) -> Self {
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
    fn test_archive_claim_path() {
        let claim = Subject::from("did:key:zSpace").attenuate(Archive);

        assert_eq!(claim.subject(), "did:key:zSpace");
        assert_eq!(claim.ability(), "/archive");
    }

    #[test]
    fn test_catalog_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Archive)
            .attenuate(Catalog {
                catalog: "index".into(),
            });

        assert_eq!(claim.subject(), "did:key:zSpace");
        // Catalog is Policy, not Ability, so it doesn't add to path
        assert_eq!(claim.ability(), "/archive");
    }

    #[test]
    fn test_get_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Archive)
            .attenuate(Catalog {
                catalog: "index".into(),
            })
            .attenuate(Get {
                digest: Blake3Hash::from([0u8; 32]),
            });

        assert_eq!(claim.ability(), "/archive/get");
    }

    #[test]
    fn test_put_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Archive)
            .attenuate(Catalog {
                catalog: "index".into(),
            })
            .attenuate(Put {
                digest: Blake3Hash::from([0u8; 32]),
                content: Bytes::new(),
            });

        assert_eq!(claim.ability(), "/archive/put");
    }

    #[cfg(feature = "ucan")]
    mod parameters_tests {
        use super::*;
        use crate::capability::Settings;
        use ipld_core::ipld::Ipld;

        #[test]
        fn test_archive_parameters() {
            let cap = Subject::from("did:key:zSpace").attenuate(Archive);
            let params = cap.parameters();

            // Archive is a unit struct, should produce empty map
            assert!(params.is_empty());
        }

        #[test]
        fn test_catalog_parameters() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Archive)
                .attenuate(Catalog {
                    catalog: "blobs".into(),
                });
            let params = cap.parameters();

            assert_eq!(params.get("catalog"), Some(&Ipld::String("blobs".into())));
        }

        #[test]
        fn test_get_parameters() {
            let digest = Blake3Hash::from([1u8; 32]);
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Archive)
                .attenuate(Catalog {
                    catalog: "index".into(),
                })
                .attenuate(Get { digest });
            let params = cap.parameters();

            assert_eq!(params.get("catalog"), Some(&Ipld::String("index".into())));
            assert_eq!(params.get("digest"), Some(&Ipld::Bytes([1u8; 32].to_vec())));
        }

        #[test]
        fn test_put_parameters() {
            let digest = Blake3Hash::from([2u8; 32]);
            let content = b"hello world".to_vec();
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Archive)
                .attenuate(Catalog {
                    catalog: "data".into(),
                })
                .attenuate(Put {
                    digest,
                    content: content.clone().into(),
                });
            let params = cap.parameters();

            assert_eq!(params.get("catalog"), Some(&Ipld::String("data".into())));
            assert_eq!(params.get("digest"), Some(&Ipld::Bytes([2u8; 32].to_vec())));
            assert_eq!(params.get("content"), Some(&Ipld::Bytes(content)));
        }
    }
}
