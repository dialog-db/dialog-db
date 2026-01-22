//! Memory access commands.
//!
//! Request types for transactional memory (CAS) operations.
//! Each type implements `Claim` to provide HTTP method, path, and other request details.
//!
//! # Two APIs
//!
//! 1. **Direct API**: Use `MemoryClaim::resolve(subject, space, cell)` for direct S3 access
//! 2. **Capability API**: Use `Capability<Resolve>` with the capability hierarchy for UCAN flows

use super::{AuthorizationError, AuthorizedRequest, Precondition, S3Request};
use crate::Checksum;
use crate::capability::memory::{Cell, Space};
use dialog_common::capability::{Capability, Effect, Policy};
use serde::{Deserialize, Serialize};

/// Edition identifier for CAS operations.
pub type Edition = String;

/// Resolve current cell content and edition.
#[derive(Debug, Serialize, Deserialize)]
pub struct Resolve;

/// Resolve is an effect that produces `RequestDescriptor` that can
/// be used to perform get from the s3 bucket.
impl Effect for Resolve {
    type Of = Cell;
    type Output = Result<AuthorizedRequest, AuthorizationError>;
}

impl S3Request for Capability<Resolve> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            &Space::of(self).name,
            &Cell::of(self).name
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
    type Output = Result<AuthorizedRequest, AuthorizationError>;
}

impl S3Request for Capability<Publish> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.subject(),
            &Space::of(self).name,
            &Cell::of(self).name
        )
    }
    fn checksum(&self) -> Option<&Checksum> {
        Some(&Publish::of(&self).checksum)
    }
    fn precondition(&self) -> Precondition {
        match &Publish::of(&self).when {
            Some(edition) => Precondition::IfMatch(edition.clone()),
            None => Precondition::IfNoneMatch,
        }
    }
}

/// Delete cell with CAS semantics.
///
/// Delete only succeeds if current edition matches `when`.
/// If `when` doesn't match, the delete is a no-op.
#[derive(Debug, Serialize, Deserialize)]
pub struct Retract {
    /// Required current edition. Delete is no-op if edition doesn't match.
    pub when: Edition,
}

/// Retract is an effect that produces `RequestDescriptor` that can
/// be used to perform delete in the s3 bucket.
impl Effect for Retract {
    type Of = Cell;
    type Output = Result<AuthorizedRequest, AuthorizationError>;
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
            &Space::of(self).name,
            &Cell::of(self).name
        )
    }
    fn precondition(&self) -> Precondition {
        Precondition::IfMatch(Retract::of(&self).when.clone())
    }
}
