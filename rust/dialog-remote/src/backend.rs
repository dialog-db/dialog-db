//! Backend implementations for revision storage
//!
//! This module provides implementations of `StorageBackend` for revision management.
//! Backends store and retrieve types implementing `RevisionUpgrade` with compare-and-swap semantics.

use dialog_artifacts::Revision;
use dialog_storage::{DialogStorageError, StorageBackend};
use std::fmt::Display;
use thiserror::Error;

mod memory;
pub use memory::*;

mod rest;
pub use rest::*;

/// Represents a subject (DID) that owns a revision
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Subject(String);

impl Subject {
    /// Create a new Subject from a DID string
    pub fn new(did: impl Into<String>) -> Self {
        Self(did.into())
    }

    /// Get the DID string
    pub fn did(&self) -> &str {
        &self.0
    }
}

impl Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.did())
    }
}

impl From<String> for Subject {
    fn from(did: String) -> Self {
        Self(did)
    }
}

impl From<&str> for Subject {
    fn from(did: &str) -> Self {
        Self(did.to_string())
    }
}

/// Trait for types that represent a revision upgrade with compare-and-swap semantics.
///
/// Types implementing this trait contain both:
/// - `revision()` - The new revision being published
/// - `origin()` - The revision this upgrade is based on (expected current value)
///
/// When storing, backends must check that the current revision matches `origin()`
/// before updating to `revision()`. This provides atomic compare-and-swap semantics.
pub trait RevisionUpgrade {
    /// Get the new revision being published
    fn revision(&self) -> &Revision;

    /// Get the expected current revision (for CAS check)
    fn origin(&self) -> &Revision;
}

/// Simple struct implementing RevisionUpgrade
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionUpgradeRecord {
    /// The new revision
    pub revision: Revision,
    /// The prior revision being upgraded from (for CAS check)
    pub origin: Revision,
}

impl RevisionUpgradeRecord {
    /// Create a new revision upgrade
    pub fn new(origin: Revision, revision: Revision) -> Self {
        Self { revision, origin }
    }
}

impl RevisionUpgrade for RevisionUpgradeRecord {
    fn revision(&self) -> &Revision {
        &self.revision
    }

    fn origin(&self) -> &Revision {
        &self.origin
    }
}

/// Error type for revision backend operations
#[derive(Debug, Error)]
pub enum RevisionBackendError {
    /// Access to the subject is not authorized
    #[error("Access to {subject} is not authorized: {reason}")]
    Unauthorized { subject: Subject, reason: String },

    /// The revision does not match the expected value (precondition failed)
    #[error("Upgrading {subject} failed: expected {expected:?}, got {actual:?}")]
    RevisionMismatch {
        subject: Subject,
        expected: Revision,
        actual: Revision,
    },

    /// Failed to fetch the current revision
    #[error("Failed to fetch revision for {subject}: {reason}")]
    FetchFailed { subject: Subject, reason: String },

    /// Failed to publish a new revision
    #[error("Failed to publish revision for {subject}: {reason}")]
    PublishFailed { subject: Subject, reason: String },

    /// Internal error occurred while performing the operation
    #[error("Unable to access {subject} due to provider error: {reason}")]
    ProviderError { subject: Subject, reason: String },

    /// The subject (DID) was not found
    #[error("Subject {subject} not found")]
    NotFound { subject: Subject },
}

/// Revision storage backends implement StorageBackend with:
/// - Key = Subject
/// - Value: impl RevisionUpgrade
/// - Error = RevisionBackendError
pub trait RevisionStorageBackend: StorageBackend<Key = Subject, Error = RevisionBackendError>
where
    Self::Value: RevisionUpgrade,
{
}

impl<T> RevisionStorageBackend for T
where
    T: StorageBackend<Key = Subject, Error = RevisionBackendError>,
    T::Value: RevisionUpgrade,
{
}

impl From<RevisionBackendError> for DialogStorageError {
    fn from(error: RevisionBackendError) -> Self {
        DialogStorageError::StorageBackend(error.to_string())
    }
}
