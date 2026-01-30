//! Memory S3 request implementations.
//!
//! This module provides S3-specific effect types and `S3Request` implementations
//! for memory (CAS cell) operations, enabling them to be translated into presigned S3 URLs.

use super::{AccessError, AuthorizedRequest, Precondition, S3Request};
use crate::Checksum;
use dialog_capability::{Capability, Effect, Policy};
use serde::{Deserialize, Serialize};

// Re-export hierarchy types from dialog-effects
pub use dialog_effects::memory::{Cell, Edition, Memory, Space};

/// Resolve current cell content and edition (S3 authorization).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Resolve;

impl Effect for Resolve {
    type Of = Cell;
    type Output = Result<AuthorizedRequest, AccessError>;
}

impl S3Request for Capability<Resolve> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            &Space::of(self).space,
            &Cell::of(self).cell
        )
    }
}

/// Publish content to a cell with CAS semantics (S3 authorization).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Publish {
    /// The checksum of the content to publish.
    pub checksum: Checksum,
    /// The expected current edition, or None if expecting empty cell.
    pub when: Option<Edition>,
}

impl Publish {
    /// Create a new Publish effect.
    pub fn new(checksum: Checksum, when: Option<Edition>) -> Self {
        Self { checksum, when }
    }
}

impl Effect for Publish {
    type Of = Cell;
    type Output = Result<AuthorizedRequest, AccessError>;
}

impl S3Request for Capability<Publish> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            &Space::of(self).space,
            &Cell::of(self).cell
        )
    }
    fn checksum(&self) -> Option<&Checksum> {
        Some(&Publish::of(self).checksum)
    }
    fn precondition(&self) -> Precondition {
        match &Publish::of(self).when {
            Some(edition) => Precondition::IfMatch(edition.clone()),
            None => Precondition::IfNoneMatch,
        }
    }
}

/// Retract (delete) cell content with CAS semantics (S3 authorization).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Retract {
    /// Required current edition. Delete is no-op if edition doesn't match.
    pub when: Edition,
}

impl Retract {
    /// Create a new Retract effect.
    pub fn new(when: impl Into<Edition>) -> Self {
        Self { when: when.into() }
    }
}

impl Effect for Retract {
    type Of = Cell;
    type Output = Result<AuthorizedRequest, AccessError>;
}

impl S3Request for Capability<Retract> {
    fn method(&self) -> &'static str {
        "DELETE"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            &Space::of(self).space,
            &Cell::of(self).cell
        )
    }
    fn precondition(&self) -> Precondition {
        Precondition::IfMatch(Retract::of(self).when.clone())
    }
}
