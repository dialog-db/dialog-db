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
//!               ├── Put { digest, content } → Effect → Result<(), ArchiveError>
//!               └── Import { blocks } → Effect → Result<(), ArchiveError>
//! ```

use std::error::Error;

pub use dialog_capability::{
    Attenuate, Attenuation, Capability, DialogCapabilityAuthorizationError,
    DialogCapabilityPerformError, Effect, Policy, StorageError, Subject, access::AuthorizeError,
};
pub use dialog_common::Blake3Hash;
pub use dialog_common::Buffer;
use dialog_common::Checksum;
use serde::de::Error as DeserializationError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
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

/// Put operation - stores a single content-addressed block.
///
/// Requires `Capability<Catalog>` access level.
///
/// The block is a plain [`Buffer`]: its digest is derived from the bytes
/// (and memoized), so no digest travels in the payload and providers don't
/// re-verify. The attenuation projects the block into the same `digest` /
/// `checksum` parameters the previous shape carried, so signed chains and
/// the S3 authorizer wire format are unchanged.
#[derive(Debug, Clone, Attenuate)]
pub struct Put {
    /// The block to store.
    #[attenuate(into = Blake3Hash, with = digest_of, rename = digest, serde_with = "dialog_common::as_bytes")]
    #[attenuate(into = Checksum, with = checksum_of, rename = checksum)]
    pub block: Buffer,
}

/// Serializes in the legacy `{ digest, content }` wire shape (the
/// invocation arguments deployed authorizers parse), deriving the digest
/// from the block.
impl Serialize for Put {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            #[serde(with = "dialog_common::as_bytes")]
            digest: Blake3Hash,
            #[serde(with = "serde_bytes")]
            content: &'a [u8],
        }

        Wire {
            digest: self.block.blake3_hash().clone(),
            content: self.block.as_ref(),
        }
        .serialize(serializer)
    }
}

/// Deserializes the legacy `{ digest, content }` wire shape, re-deriving
/// the digest from the content and rejecting a mismatch, so a held `Put`
/// is always digest-consistent.
impl<'de> Deserialize<'de> for Put {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            #[serde(with = "dialog_common::as_bytes")]
            digest: Blake3Hash,
            #[serde(with = "serde_bytes")]
            content: Vec<u8>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let put = Put::new(Buffer::from(wire.content));
        if put.block.blake3_hash() != &wire.digest {
            return Err(DeserializationError::custom(format!(
                "put digest mismatch: declared {}, computed {}",
                wire.digest,
                put.block.blake3_hash()
            )));
        }
        Ok(put)
    }
}

/// Projects a block to its blake3 digest for the attenuation shape.
fn digest_of(block: Buffer) -> Blake3Hash {
    block.blake3_hash().clone()
}

/// Projects a block to its content checksum for the attenuation shape.
fn checksum_of(block: Buffer) -> Checksum {
    Checksum::sha256(block.as_ref())
}

impl Put {
    /// Create a new Put effect.
    pub fn new(block: impl Into<Buffer>) -> Self {
        Self {
            block: block.into(),
        }
    }
}

impl Effect for Put {
    type Of = Catalog;
    type Output = Result<(), ArchiveError>;
}

/// Import operation - stores a batch of content-addressed blocks.
///
/// Requires `Capability<Catalog>` access level.
///
/// Blocks are plain [`Buffer`]s: content addressing means their identity
/// is derived from the bytes (and the buffer memoizes its hash), so no
/// digest travels in the payload. Integrity across a wire boundary is a
/// capability concern: the attenuation embedded in the signed invocation
/// carries the per-block checksums.
///
/// # Semantics
///
/// On success every block is durable. On failure the whole import can be
/// retried: puts of content-addressed blocks are idempotent (same digest,
/// same content), and blocks are unobservable until something references
/// them, so a partial failure leaves only harmless unreferenced blocks.
/// No atomicity is promised — in this architecture the atomic boundary is
/// the revision publish (a compare-and-swap on a memory cell), not block
/// persistence. Providers are free to persist the batch in a single
/// round trip where the platform allows (e.g. one IndexedDB read-write
/// transaction) purely as an optimization.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Import {
    /// The blocks to store.
    #[attenuate(into = Vec<Checksum>, with = block_checksums, rename = checksums)]
    pub blocks: Vec<Buffer>,
}

/// Projects an [`Import`]'s blocks to their content checksums for the
/// attenuation shape embedded in delegations and invocations.
fn block_checksums(blocks: Vec<Buffer>) -> Vec<Checksum> {
    blocks
        .iter()
        .map(|buffer| Checksum::sha256(buffer.as_ref()))
        .collect()
}

impl Import {
    /// Create a new Import effect.
    pub fn new(blocks: impl IntoIterator<Item = impl Into<Buffer>>) -> Self {
        Self {
            blocks: blocks.into_iter().map(Into::into).collect(),
        }
    }
}

impl Effect for Import {
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
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

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
            .invoke(Put::new(Buffer::from(Vec::new())));

        assert_eq!(claim.ability(), "/archive/put");
    }

    #[dialog_common::test]
    fn it_roundtrips_import_payloads() {
        // Buffers serialize as bare bytes and deserialize realigned; the
        // memoized hash is rederived on the receiving side.
        let import = Import::new([Buffer::from(vec![1u8, 2, 3])]);
        let bytes = serde_ipld_dagcbor::to_vec(&import).unwrap();
        let restored: Import = serde_ipld_dagcbor::from_slice(&bytes).unwrap();
        assert_eq!(restored.blocks, import.blocks);
        assert_eq!(
            restored.blocks[0].blake3_hash(),
            &Blake3Hash::hash(&[1u8, 2, 3][..])
        );
    }

    #[dialog_common::test]
    fn it_attenuates_imports_to_block_checksums() {
        let import = Import::new([Buffer::from(vec![1u8]), Buffer::from(vec![2u8, 2])]);
        let attenuation = import.clone().into_attenuation();
        assert_eq!(
            attenuation.checksums,
            vec![
                Checksum::sha256(&[1u8][..]),
                Checksum::sha256(&[2u8, 2][..]),
            ]
        );
        assert_eq!(import.blocks.len(), 2);
    }
}
