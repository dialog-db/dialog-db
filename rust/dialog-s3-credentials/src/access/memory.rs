//! Memory access commands.
//!
//! Request types for transactional memory (CAS) operations.
//! Each type implements `Claim` to provide HTTP method, path, and other request details.
//!
//! # Two APIs
//!
//! 1. **Direct API**: Use `MemoryClaim::resolve(subject, space, cell)` for direct S3 access
//! 2. **Capability API**: Use `Capability<Resolve>` with the capability hierarchy for UCAN flows

use super::{AuthorizationError, Claim, Precondition, RequestDescriptor};
use crate::Checksum;
use crate::capability::memory::{Cell, Space};
use dialog_common::capability::{Capability, Effect, Policy};
use serde::Deserialize;

/// Edition identifier for CAS operations.
pub type Edition = String;

/// A memory claim that can be directly signed.
///
/// This wraps a memory operation with subject, space, and cell context,
/// allowing it to be used with `Signer::sign()` directly.
#[derive(Debug)]
pub struct MemoryClaim<T> {
    /// Subject DID (path prefix)
    pub subject: String,
    /// Space name
    pub space: String,
    /// Cell name
    pub cell: String,
    /// The operation
    pub operation: T,
}

impl<T> MemoryClaim<T> {
    /// Create a new memory claim.
    pub fn new(
        subject: impl Into<String>,
        space: impl Into<String>,
        cell: impl Into<String>,
        operation: T,
    ) -> Self {
        Self {
            subject: subject.into(),
            space: space.into(),
            cell: cell.into(),
            operation,
        }
    }
}

impl MemoryClaim<Resolve> {
    /// Create a RESOLVE claim.
    pub fn resolve(
        subject: impl Into<String>,
        space: impl Into<String>,
        cell: impl Into<String>,
    ) -> Self {
        Self::new(subject, space, cell, Resolve)
    }
}

impl MemoryClaim<Publish> {
    /// Create a PUBLISH claim.
    pub fn publish(
        subject: impl Into<String>,
        space: impl Into<String>,
        cell: impl Into<String>,
        checksum: Checksum,
        when: Option<Edition>,
    ) -> Self {
        Self::new(subject, space, cell, Publish { checksum, when })
    }
}

impl MemoryClaim<Retract> {
    /// Create a RETRACT claim.
    pub fn retract(
        subject: impl Into<String>,
        space: impl Into<String>,
        cell: impl Into<String>,
        when: impl Into<Edition>,
    ) -> Self {
        Self::new(subject, space, cell, Retract::new(when))
    }
}

impl Claim for MemoryClaim<Resolve> {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        format!("{}/{}/{}", self.subject, self.space, self.cell)
    }
    fn store(&self) -> &str {
        &self.space
    }
}

impl Claim for MemoryClaim<Publish> {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!("{}/{}/{}", self.subject, self.space, self.cell)
    }
    fn store(&self) -> &str {
        &self.space
    }
    fn checksum(&self) -> Option<&Checksum> {
        Some(&self.operation.checksum)
    }
    fn precondition(&self) -> Precondition {
        match &self.operation.when {
            Some(edition) => Precondition::IfMatch(edition.clone()),
            None => Precondition::IfNoneMatch,
        }
    }
}

impl Claim for MemoryClaim<Retract> {
    fn method(&self) -> &'static str {
        "DELETE"
    }
    fn path(&self) -> String {
        format!("{}/{}/{}", self.subject, self.space, self.cell)
    }
    fn store(&self) -> &str {
        &self.space
    }
    fn precondition(&self) -> Precondition {
        Precondition::IfMatch(self.operation.when.clone())
    }
}

/// Resolve current cell content and edition.
#[derive(Debug, Deserialize)]
pub struct Resolve;

/// Resolve is an effect that produces `RequestDescriptor` that can
/// be used to perform get from the s3 bucket.
impl Effect for Resolve {
    type Of = Cell;
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

impl Claim for Capability<Resolve> {
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
    fn store(&self) -> &str {
        &Space::of(self).name
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
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

impl Claim for Capability<Publish> {
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
    fn store(&self) -> &str {
        &Space::of(self).name
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
#[derive(Debug, Deserialize)]
pub struct Retract {
    /// Required current edition. Delete is no-op if edition doesn't match.
    pub when: Edition,
}

/// Retract is an effect that produces `RequestDescriptor` that can
/// be used to perform delete in the s3 bucket.
impl Effect for Retract {
    type Of = Cell;
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

impl Retract {
    /// Create a new Retract command.
    pub fn new(when: impl Into<Edition>) -> Self {
        Self { when: when.into() }
    }
}

impl Claim for Capability<Retract> {
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
    fn store(&self) -> &str {
        &Space::of(self).name
    }
    fn precondition(&self) -> Precondition {
        Precondition::IfMatch(Retract::of(&self).when.clone())
    }
}
