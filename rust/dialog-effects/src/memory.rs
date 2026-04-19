//! Memory capability hierarchy.
//!
//! Memory provides transactional cell storage with CAS (Compare-And-Swap) semantics.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//!   └── Memory (ability: /memory)
//!         └── Space { space: String }
//!               └── Cell { cell: String }
//!                     ├── Resolve → Effect → Result<Option<Edition<Vec<u8>>>, MemoryError>
//!                     ├── Publish { content, when } → Effect → Result<Bytes, MemoryError>
//!                     └── Retract { when } → Effect → Result<(), MemoryError>
//! ```

use std::fmt;
use std::io;
use std::str;

use base58::ToBase58;
use dialog_capability::access::AuthorizeError;
pub use dialog_capability::{
    Attenuate, Attenuation, Capability, Effect, Policy, StorageError, Subject,
};
use dialog_common::Checksum;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Root attenuation for memory operations.
///
/// Attaches to Subject and provides the `/memory` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Memory;

impl Attenuation for Memory {
    type Of = Subject;
}

/// Space policy that scopes operations to a memory space.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Space {
    /// The space name (typically a DID).
    pub space: String,
}

impl Space {
    /// Create a new Space policy.
    pub fn new(name: impl Into<String>) -> Self {
        Self { space: name.into() }
    }
}

impl Policy for Space {
    type Of = Memory;
}

/// Cell policy that scopes operations to a specific cell within a space.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Cell {
    /// The cell name.
    pub cell: String,
}

impl Cell {
    /// Create a new Cell policy.
    pub fn new(name: impl Into<String>) -> Self {
        Self { cell: name.into() }
    }
}

impl Policy for Cell {
    type Of = Space;
}

/// Opaque version identifier for CAS operations.
///
/// Backends produce version tokens in whatever form suits them -- S3 hands
/// back ASCII ETags, content-addressed stores hand back raw hashes. This
/// newtype keeps the underlying bytes intact (no lossy UTF-8 conversion)
/// while providing readable [`Debug`] / [`Display`] output.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Version(#[serde(with = "serde_bytes")] Vec<u8>);

impl Version {
    /// View the raw version bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume the wrapper and return the raw bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Whether this version is empty (zero-length). Empty versions are
    /// sometimes used as sentinels for "no prior version".
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for Version {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<&[u8]> for Version {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for Version {
    fn from(bytes: &[u8; N]) -> Self {
        Self(bytes.to_vec())
    }
}

impl From<String> for Version {
    fn from(s: String) -> Self {
        Self(s.into_bytes())
    }
}

impl From<&str> for Version {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
}

impl From<dialog_common::Blake3Hash> for Version {
    fn from(hash: dialog_common::Blake3Hash) -> Self {
        Self(hash.as_bytes().to_vec())
    }
}

impl From<&dialog_common::Blake3Hash> for Version {
    fn from(hash: &dialog_common::Blake3Hash) -> Self {
        Self(hash.as_bytes().to_vec())
    }
}

impl AsRef<[u8]> for Version {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Render bytes as a UTF-8 string when all bytes are printable ASCII,
/// otherwise fall back to base58.
impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.iter().all(|b| b.is_ascii_graphic() || *b == b' ') {
            // Safety: all bytes are ASCII graphic/space, hence valid UTF-8.
            f.write_str(str::from_utf8(&self.0).expect("ascii is valid utf8"))
        } else {
            f.write_str(&self.0.to_base58())
        }
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Version({})", self)
    }
}

/// A cell's current state: content and its version.
///
/// Returned by [`Resolve`] when the cell has content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edition<T> {
    /// The cell's current content.
    pub content: T,
    /// The version identifier for this content.
    pub version: Version,
}

/// Resolve operation - reads current cell content and version.
///
/// Returns `None` if the cell has no content (empty/uninitialized).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Attenuate)]
pub struct Resolve;

impl Effect for Resolve {
    type Of = Cell;
    type Output = Result<Option<Edition<Vec<u8>>>, MemoryError>;
}

/// Extension trait for `Capability<Resolve>` to access its fields.
pub trait ResolveCapability {
    /// Get the space name from the capability chain.
    fn space(&self) -> &str;
    /// Get the cell name from the capability chain.
    fn cell(&self) -> &str;
}

impl ResolveCapability for Capability<Resolve> {
    fn space(&self) -> &str {
        &Space::of(self).space
    }

    fn cell(&self) -> &str {
        &Cell::of(self).cell
    }
}

/// Publish operation - sets cell content with CAS semantics.
///
/// - If `when` is `None`, expects cell to be empty (first publish)
/// - If `when` is `Some(edition)`, expects current edition to match
/// - Returns new edition on success
/// - Returns `MemoryError::VersionMismatch` if expectation doesn't match
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Publish {
    /// The content to publish.
    #[serde(with = "serde_bytes")]
    #[attenuate(into = Checksum, with = Checksum::sha256, rename = checksum)]
    pub content: Vec<u8>,
    /// The expected current version, or None if expecting empty cell.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<Version>,
}

impl Publish {
    /// Create a new Publish effect.
    pub fn new(content: impl Into<Vec<u8>>, when: Option<Version>) -> Self {
        Self {
            content: content.into(),
            when,
        }
    }
}

impl Effect for Publish {
    type Of = Cell;
    type Output = Result<Version, MemoryError>;
}

/// Extension trait for `Capability<Publish>` to access its fields.
pub trait PublishCapability {
    /// Get the space name from the capability chain.
    fn space(&self) -> &str;
    /// Get the cell name from the capability chain.
    fn cell(&self) -> &str;
    /// Get the content to publish.
    fn content(&self) -> &[u8];
    /// Get the expected version (when condition).
    fn when(&self) -> Option<&Version>;
}

impl PublishCapability for Capability<Publish> {
    fn space(&self) -> &str {
        &Space::of(self).space
    }

    fn cell(&self) -> &str {
        &Cell::of(self).cell
    }

    fn content(&self) -> &[u8] {
        &Publish::of(self).content
    }

    fn when(&self) -> Option<&Version> {
        Publish::of(self).when.as_ref()
    }
}

/// Retract operation - removes cell content with CAS semantics.
///
/// - Requires `when` to match current edition
/// - Returns `MemoryError::VersionMismatch` if edition doesn't match
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Retract {
    /// The expected current version.
    pub when: Version,
}

impl Retract {
    /// Create a new Retract effect.
    pub fn new(when: impl Into<Version>) -> Self {
        Self { when: when.into() }
    }
}

impl Effect for Retract {
    type Of = Cell;
    type Output = Result<(), MemoryError>;
}

/// Extension trait for `Capability<Retract>` to access its fields.
pub trait RetractCapability {
    /// Get the space name from the capability chain.
    fn space(&self) -> &str;
    /// Get the cell name from the capability chain.
    fn cell(&self) -> &str;
    /// Get the expected version (when condition).
    fn when(&self) -> &Version;
}

impl RetractCapability for Capability<Retract> {
    fn space(&self) -> &str {
        &Space::of(self).space
    }

    fn cell(&self) -> &str {
        &Cell::of(self).cell
    }

    fn when(&self) -> &Version {
        &Retract::of(self).when
    }
}

pub mod prelude;

/// Errors that can occur during memory operations.
#[derive(Debug, Error)]
pub enum MemoryError {
    /// CAS edition mismatch.
    #[error("Version mismatch: expected {expected:?}, got {actual:?}")]
    VersionMismatch {
        /// The expected version.
        expected: Option<Version>,
        /// The actual version found.
        actual: Option<Version>,
    },

    /// Storage backend error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Authorization error.
    #[error("Authorization error: {0}")]
    Authorization(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

impl From<StorageError> for MemoryError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<AuthorizeError> for MemoryError {
    fn from(e: AuthorizeError) -> Self {
        Self::Authorization(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::did;

    #[test]
    fn it_builds_memory_claim_path() {
        let claim = Subject::from(did!("key:zSpace")).attenuate(Memory);

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/memory");
    }

    #[test]
    fn it_builds_space_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Memory)
            .attenuate(Space::new("local"));

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        // Space is Policy, not Ability, so it doesn't add to path
        assert_eq!(claim.ability(), "/memory");
    }

    #[test]
    fn it_builds_cell_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("main"));

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        // Cell is Policy, not Ability, so it doesn't add to path
        assert_eq!(claim.ability(), "/memory");
    }

    #[test]
    fn it_builds_resolve_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("main"))
            .invoke(Resolve);

        assert_eq!(claim.ability(), "/memory/resolve");
    }

    #[test]
    fn it_builds_publish_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("main"))
            .invoke(Publish::new(b"test", None));

        assert_eq!(claim.ability(), "/memory/publish");
    }

    #[test]
    fn it_builds_retract_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("main"))
            .invoke(Retract::new(b"v1"));

        assert_eq!(claim.ability(), "/memory/retract");
    }
}
