//! Memory access commands.
//!
//! Request types for transactional memory (CAS) operations.
//! Each type implements `Claim` to provide HTTP method, path, and other request details.
//!
//! # Two APIs
//!
//! 1. **Direct API**: Use `MemoryClaim::resolve(subject, space, cell)` for direct S3 access
//! 2. **Capability API**: Use `Capability<Resolve>` with the capability hierarchy for UCAN flows

use super::{AccessError, AuthorizedRequest, Precondition, S3Request};
use crate::Checksum;
use dialog_common::capability::{Attenuation, Capability, Effect, Policy, Subject};
use serde::{Deserialize, Serialize};

/// Root attenuation for memory operations.
///
/// Attaches to Subject and provides the `/memory` command path segment.
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

/// Resolve current cell content and edition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resolve;

/// Resolve is an effect that produces `RequestDescriptor` that can
/// be used to perform get from the s3 bucket.
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Publish {
    /// The content to publish.
    pub checksum: Checksum,
    /// The expected current edition, or None if expecting empty cell.
    pub when: Option<Edition>,
}

/// Publish is an effect that produces `RequestDescriptor` that can
/// be used to perform preconditioned put in the s3 bucket.
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

/// Delete cell with CAS semantics.
///
/// Delete only succeeds if current edition matches `when`.
/// If `when` doesn't match, the delete is a no-op.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Retract {
    /// Required current edition. Delete is no-op if edition doesn't match.
    pub when: Edition,
}

/// Retract is an effect that produces `RequestDescriptor` that can
/// be used to perform delete in the s3 bucket.
impl Effect for Retract {
    type Of = Cell;
    type Output = Result<AuthorizedRequest, AccessError>;
}

impl Retract {
    /// Create a new Retract command.
    pub fn new(when: impl Into<Edition>) -> Self {
        Self { when: when.into() }
    }
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
