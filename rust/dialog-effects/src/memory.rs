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
//!                     ├── Resolve → Effect → Result<Option<Publication>, MemoryError>
//!                     ├── Publish { content, when } → Effect → Result<Bytes, MemoryError>
//!                     └── Retract { when } → Effect → Result<(), MemoryError>
//! ```

pub use dialog_capability::{Attenuation, Capability, Effect, Policy, Subject};
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

/// Edition identifier for CAS operations.
pub type Edition = String;

/// A cell's current state: content and its edition.
///
/// Returned by [`Resolve`] when the cell has content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Publication {
    /// The cell's current content.
    #[serde(with = "serde_bytes")]
    pub content: Vec<u8>,
    /// The edition identifier for this content.
    #[serde(with = "serde_bytes")]
    pub edition: Vec<u8>,
}

/// Resolve operation - reads current cell content and edition.
///
/// Returns `None` if the cell has no content (empty/uninitialized).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Resolve;

impl Effect for Resolve {
    type Of = Cell;
    type Output = Result<Option<Publication>, MemoryError>;
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
/// - Returns `MemoryError::EditionMismatch` if expectation doesn't match
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Publish {
    /// The content to publish.
    #[serde(with = "serde_bytes")]
    pub content: Vec<u8>,
    /// The expected current edition, or None if expecting empty cell.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<serde_bytes::ByteBuf>,
}

impl Publish {
    /// Create a new Publish effect.
    pub fn new(content: impl Into<Vec<u8>>, when: Option<Vec<u8>>) -> Self {
        Self {
            content: content.into(),
            when: when.map(serde_bytes::ByteBuf::from),
        }
    }
}

impl Effect for Publish {
    type Of = Cell;
    type Output = Result<Vec<u8>, MemoryError>;
}

/// Extension trait for `Capability<Publish>` to access its fields.
pub trait PublishCapability {
    /// Get the space name from the capability chain.
    fn space(&self) -> &str;
    /// Get the cell name from the capability chain.
    fn cell(&self) -> &str;
    /// Get the content to publish.
    fn content(&self) -> &[u8];
    /// Get the expected edition (when condition).
    fn when(&self) -> Option<&[u8]>;
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

    fn when(&self) -> Option<&[u8]> {
        Publish::of(self).when.as_ref().map(|b| b.as_ref())
    }
}

/// Retract operation - removes cell content with CAS semantics.
///
/// - Requires `when` to match current edition
/// - Returns `MemoryError::EditionMismatch` if edition doesn't match
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Retract {
    /// The expected current edition.
    #[serde(with = "serde_bytes")]
    pub when: Vec<u8>,
}

impl Retract {
    /// Create a new Retract effect.
    pub fn new(when: impl Into<Vec<u8>>) -> Self {
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
    /// Get the expected edition (when condition).
    fn when(&self) -> &[u8];
}

impl RetractCapability for Capability<Retract> {
    fn space(&self) -> &str {
        &Space::of(self).space
    }

    fn cell(&self) -> &str {
        &Cell::of(self).cell
    }

    fn when(&self) -> &[u8] {
        &Retract::of(self).when
    }
}

/// Errors that can occur during memory operations.
#[derive(Debug, Error)]
pub enum MemoryError {
    /// CAS edition mismatch.
    #[error("Edition mismatch: expected {expected:?}, got {actual:?}")]
    EditionMismatch {
        /// The expected edition.
        expected: Option<String>,
        /// The actual edition found.
        actual: Option<String>,
    },

    /// Storage backend error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
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

    #[cfg(feature = "ucan")]
    mod parameters_tests {
        use super::*;
        use dialog_capability::ucan::parameters;
        use ipld_core::ipld::Ipld;

        #[test]
        fn it_collects_resolve_capability_parameters() {
            let cap = Subject::from(did!("key:zSpace"))
                .attenuate(Memory)
                .attenuate(Space::new("remote"))
                .attenuate(Cell::new("config"))
                .invoke(Resolve);
            let params = parameters(&cap);

            assert_eq!(params.get("space"), Some(&Ipld::String("remote".into())));
            assert_eq!(params.get("cell"), Some(&Ipld::String("config".into())));
        }

        #[test]
        fn it_collects_publish_capability_parameters() {
            let cap = Subject::from(did!("key:zSpace"))
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("main"))
                .invoke(Publish {
                    content: b"hello".to_vec(),
                    when: Some(b"v1".to_vec().into()),
                });
            let params = parameters(&cap);

            assert_eq!(params.get("space"), Some(&Ipld::String("local".into())));
            assert_eq!(params.get("cell"), Some(&Ipld::String("main".into())));
            assert_eq!(params.get("content"), Some(&Ipld::Bytes(b"hello".to_vec())));
            assert_eq!(params.get("when"), Some(&Ipld::Bytes(b"v1".to_vec())));
        }

        #[test]
        fn it_collects_retract_capability_parameters() {
            let cap = Subject::from(did!("key:zSpace"))
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("main"))
                .invoke(Retract::new(b"v1"));
            let params = parameters(&cap);

            assert_eq!(params.get("space"), Some(&Ipld::String("local".into())));
            assert_eq!(params.get("cell"), Some(&Ipld::String("main".into())));
            assert_eq!(params.get("when"), Some(&Ipld::Bytes(b"v1".to_vec())));
        }
    }
}
