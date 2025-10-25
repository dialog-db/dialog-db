//! Backend implementations for revision storage
//!
//! This module provides implementations of `StorageBackend` for revision management.
//! Backends store and retrieve types implementing `RevisionUpgrade` with compare-and-swap semantics.

// Use the artifacts revision type (not the replica revision type)
use dialog_artifacts::artifacts::Revision;
use dialog_storage::{AtomicStorageBackend, DialogStorageError};
use std::fmt::Display;
use thiserror::Error;

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

/// Error type for revision backend operations
#[derive(Debug, Error)]
pub enum RevisionStorageBackendError {
    /// Access to the subject is not authorized
    #[error("Access to {subject} is not authorized: {reason}")]
    Unauthorized { subject: Subject, reason: String },

    /// The revision does not match the expected value (precondition failed)
    #[error("Upgrading {subject} failed: expected {expected:?}, got {actual:?}")]
    RevisionMismatch {
        subject: Subject,
        expected: Option<Revision>,
        actual: Option<Revision>,
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
pub trait RevisionStorageBackend:
    AtomicStorageBackend<Key = Subject, Value = Revision, Error = RevisionStorageBackendError>
{
}

impl<T: AtomicStorageBackend<Key = Subject, Value = Revision, Error = RevisionStorageBackendError>>
    RevisionStorageBackend for T
{
}

impl From<RevisionStorageBackendError> for DialogStorageError {
    fn from(error: RevisionStorageBackendError) -> Self {
        DialogStorageError::StorageBackend(error.to_string())
    }
}
