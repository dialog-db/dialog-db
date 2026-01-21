//! Memory capability hierarchy.
//!
//! Memory provides transactional cell storage with CAS (Compare-And-Swap) semantics.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//!   └── Memory (cmd: /memory)
//!         └── Space { space: String }
//!               └── Cell { cell: String }
//!                     ├── Resolve → Effect → Result<Option<Publication>, MemoryError>
//!                     ├── Publish { content, when } → Effect → Result<Bytes, MemoryError>
//!                     └── Retract { when } → Effect → Result<(), MemoryError>
//! ```

use dialog_common::Bytes;
use dialog_common::capability::{Attenuation, Capability, Effect, Policy, Subject};
use thiserror::Error;

// Memory Ability

/// Memory ability - restricts to memory operations.
///
/// Adds `/memory` to the command path.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Memory;

impl Attenuation for Memory {
    type Of = Subject;
}

// Space Policy

/// Space policy - restricts memory access to a specific space.
///
/// Spaces are named subdivisions of memory (e.g., "local", "remote", "site").
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Space {
    /// The space name.
    pub space: String,
}

impl Policy for Space {
    type Of = Memory;
}

// Cell Policy

/// Cell policy - restricts space access to a specific cell.
///
/// Cells are individual memory locations within a space.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Cell {
    /// The cell name.
    pub cell: String,
}

impl Policy for Cell {
    type Of = Space;
}

// Publication

/// A cell's current state: content and its edition.
///
/// Returned by [`Resolve`] when the cell has content.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Publication {
    /// The cell's current content.
    pub content: Bytes,
    /// The edition identifier for this content.
    pub edition: Bytes,
}

// Resolve Effect

/// Resolve operation - reads current cell content and edition.
///
/// Returns `None` if the cell has no content (empty/uninitialized).
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
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

// Publish Effect

/// Publish operation - sets cell content with CAS semantics.
///
/// - If `when` is `None`, expects cell to be empty (first publish)
/// - If `when` is `Some(edition)`, expects current edition to match
/// - Returns new edition on success
/// - Returns `MemoryError::EditionMismatch` if expectation doesn't match
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Publish {
    /// The content to publish.
    pub content: Bytes,
    /// The expected current edition, or None if expecting empty cell.
    pub when: Option<Bytes>,
}

impl Effect for Publish {
    type Of = Cell;
    type Output = Result<Bytes, MemoryError>;
}

/// Extension trait for `Capability<Publish>` to access its fields.
pub trait PublishCapability {
    /// Get the space name from the capability chain.
    fn space(&self) -> &str;
    /// Get the cell name from the capability chain.
    fn cell(&self) -> &str;
    /// Get the content to publish.
    fn content(&self) -> &Bytes;
    /// Get the expected edition (when condition).
    fn when(&self) -> Option<&Bytes>;
}

impl PublishCapability for Capability<Publish> {
    fn space(&self) -> &str {
        &Space::of(self).space
    }

    fn cell(&self) -> &str {
        &Cell::of(self).cell
    }

    fn content(&self) -> &Bytes {
        &Publish::of(self).content
    }

    fn when(&self) -> Option<&Bytes> {
        Publish::of(self).when.as_ref()
    }
}

// Retract Effect

/// Retract operation - removes cell content with CAS semantics.
///
/// - Requires `when` to match current edition
/// - Returns `MemoryError::EditionMismatch` if edition doesn't match
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Retract {
    /// The expected current edition.
    pub when: Bytes,
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
    fn when(&self) -> &Bytes;
}

impl RetractCapability for Capability<Retract> {
    fn space(&self) -> &str {
        &Space::of(self).space
    }

    fn cell(&self) -> &str {
        &Cell::of(self).cell
    }

    fn when(&self) -> &Bytes {
        &Retract::of(self).when
    }
}

// Memory Error

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

    #[test]
    fn test_memory_claim_path() {
        let claim = Subject::from("did:key:zSpace").attenuate(Memory);

        assert_eq!(claim.subject(), "did:key:zSpace");
        assert_eq!(claim.ability(), "/memory");
    }

    #[test]
    fn test_space_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Memory)
            .attenuate(Space {
                space: "local".into(),
            });

        assert_eq!(claim.subject(), "did:key:zSpace");
        // Space is Policy, not Ability, so it doesn't add to path
        assert_eq!(claim.ability(), "/memory");
    }

    #[test]
    fn test_cell_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Memory)
            .attenuate(Space {
                space: "local".into(),
            })
            .attenuate(Cell {
                cell: "main".into(),
            });

        assert_eq!(claim.subject(), "did:key:zSpace");
        // Cell is Policy, not Ability, so it doesn't add to path
        assert_eq!(claim.ability(), "/memory");
    }

    #[test]
    fn test_resolve_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Memory)
            .attenuate(Space {
                space: "local".into(),
            })
            .attenuate(Cell {
                cell: "main".into(),
            })
            .attenuate(Resolve);

        assert_eq!(claim.ability(), "/memory/resolve");
    }

    #[test]
    fn test_publish_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Memory)
            .attenuate(Space {
                space: "local".into(),
            })
            .attenuate(Cell {
                cell: "main".into(),
            })
            .attenuate(Publish {
                content: b"test".to_vec().into(),
                when: None,
            });

        assert_eq!(claim.ability(), "/memory/publish");
    }

    #[test]
    fn test_retract_claim_path() {
        let claim = Subject::from("did:key:zSpace")
            .attenuate(Memory)
            .attenuate(Space {
                space: "local".into(),
            })
            .attenuate(Cell {
                cell: "main".into(),
            })
            .attenuate(Retract {
                when: b"v1".to_vec().into(),
            });

        assert_eq!(claim.ability(), "/memory/retract");
    }

    #[cfg(feature = "ucan")]
    mod args_tests {
        use super::*;
        use crate::capability::ToIpldArgs;
        use ipld_core::ipld::Ipld;

        #[test]
        fn test_memory_args() {
            let cap = Subject::from("did:key:zSpace").attenuate(Memory);
            let args = cap.to_ipld_args();

            // Memory is a unit struct, should produce empty map
            assert_eq!(args, Ipld::Map(Default::default()));
        }

        #[test]
        fn test_space_args() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Memory)
                .attenuate(Space {
                    space: "local".into(),
                });
            let args = cap.to_ipld_args();

            let Ipld::Map(map) = args else {
                panic!("Expected Map, got {:?}", args);
            };
            assert_eq!(map.get("space"), Some(&Ipld::String("local".into())));
        }

        #[test]
        fn test_cell_args() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Memory)
                .attenuate(Space {
                    space: "local".into(),
                })
                .attenuate(Cell {
                    cell: "main".into(),
                });
            let args = cap.to_ipld_args();

            let Ipld::Map(map) = args else {
                panic!("Expected Map, got {:?}", args);
            };
            assert_eq!(map.get("space"), Some(&Ipld::String("local".into())));
            assert_eq!(map.get("cell"), Some(&Ipld::String("main".into())));
        }

        #[test]
        fn test_resolve_args() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Memory)
                .attenuate(Space {
                    space: "remote".into(),
                })
                .attenuate(Cell {
                    cell: "config".into(),
                })
                .attenuate(Resolve);
            let args = cap.to_ipld_args();

            let Ipld::Map(map) = args else {
                panic!("Expected Map, got {:?}", args);
            };
            // Resolve is a unit struct, doesn't add fields but preserves parent args
            assert_eq!(map.get("space"), Some(&Ipld::String("remote".into())));
            assert_eq!(map.get("cell"), Some(&Ipld::String("config".into())));
        }

        #[test]
        fn test_publish_args_with_when() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Memory)
                .attenuate(Space {
                    space: "local".into(),
                })
                .attenuate(Cell {
                    cell: "main".into(),
                })
                .attenuate(Publish {
                    content: b"hello".to_vec().into(),
                    when: Some(b"v1".to_vec().into()),
                });
            let args = cap.to_ipld_args();

            let Ipld::Map(map) = args else {
                panic!("Expected Map, got {:?}", args);
            };
            assert_eq!(map.get("space"), Some(&Ipld::String("local".into())));
            assert_eq!(map.get("cell"), Some(&Ipld::String("main".into())));
            assert_eq!(map.get("content"), Some(&Ipld::Bytes(b"hello".to_vec())));
            assert_eq!(map.get("when"), Some(&Ipld::Bytes(b"v1".to_vec())));
        }

        #[test]
        fn test_publish_args_first_publish() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Memory)
                .attenuate(Space {
                    space: "local".into(),
                })
                .attenuate(Cell {
                    cell: "main".into(),
                })
                .attenuate(Publish {
                    content: b"hello".to_vec().into(),
                    when: None,
                });
            let args = cap.to_ipld_args();

            let Ipld::Map(map) = args else {
                panic!("Expected Map, got {:?}", args);
            };
            assert_eq!(map.get("space"), Some(&Ipld::String("local".into())));
            assert_eq!(map.get("cell"), Some(&Ipld::String("main".into())));
            assert_eq!(map.get("content"), Some(&Ipld::Bytes(b"hello".to_vec())));
            // None serializes as Ipld::Null
            assert_eq!(map.get("when"), Some(&Ipld::Null));
        }

        #[test]
        fn test_retract_args() {
            let cap = Subject::from("did:key:zSpace")
                .attenuate(Memory)
                .attenuate(Space {
                    space: "local".into(),
                })
                .attenuate(Cell {
                    cell: "main".into(),
                })
                .attenuate(Retract {
                    when: b"v1".to_vec().into(),
                });
            let args = cap.to_ipld_args();

            let Ipld::Map(map) = args else {
                panic!("Expected Map, got {:?}", args);
            };
            assert_eq!(map.get("space"), Some(&Ipld::String("local".into())));
            assert_eq!(map.get("cell"), Some(&Ipld::String("main".into())));
            assert_eq!(map.get("when"), Some(&Ipld::Bytes(b"v1".to_vec())));
        }
    }
}
