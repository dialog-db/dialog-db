use crate::access::AuthorizeError;
use crate::subject::Did;
use std::error::Error as StdError;
use std::fmt::{self, Debug, Display, Formatter};
use thiserror::Error;

/// Errors from capability-routed storage operations.
///
/// These are the "we failed" side of an effect's error: the backend broke,
/// or the request named a subject this environment cannot serve. A caller
/// supplying bad input is reported by the effect's own error type, not here.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Storage backend error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// This environment has no provider for the requested subject.
    ///
    /// Distinct from a backend failure: nothing is broken. Not-found is
    /// relative to this environment rather than absolute -- the subject may
    /// exist elsewhere, and loading the space it belongs to makes the same
    /// request succeed -- so callers may retry after mounting rather than
    /// treat it as terminal.
    #[error(
        "No provider found for subject {subject}; it must be mounted before it can be accessed"
    )]
    SubjectNotFound {
        /// The subject this environment cannot serve.
        subject: Did,
    },
}

/// Error during fork execution.
#[derive(Debug, Error)]
pub enum ForkError {
    /// Authorization was denied.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),
}

/// Error type for capability execution failures.
pub enum DialogCapabilityPerformError<E: StdError> {
    /// Error during effect execution.
    Execution(E),
    /// Error during authorization verification.
    Authorization(AuthorizeError),
}

impl<E: StdError> Debug for DialogCapabilityPerformError<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Execution(e) => f.debug_tuple("Execution").field(e).finish(),
            Self::Authorization(e) => f.debug_tuple("Authorization").field(e).finish(),
        }
    }
}

impl<E: StdError> Display for DialogCapabilityPerformError<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Execution(e) => write!(f, "Execution error: {e}"),
            Self::Authorization(e) => write!(f, "Authorization error: {e}"),
        }
    }
}

impl<E: StdError + 'static> StdError for DialogCapabilityPerformError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Execution(e) => Some(e),
            Self::Authorization(e) => Some(e),
        }
    }
}
