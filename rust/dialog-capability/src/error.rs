use crate::access::AuthorizeError;
use crate::subject::Did;
use std::error::Error as StdError;
use std::fmt::{self, Debug, Display, Formatter};
use std::io;
use thiserror::Error;

/// Error that can occur during signing operations.
#[derive(Debug, Error)]
pub enum SignError {
    /// The signing key is not available or cannot be used.
    #[error("Signing key unavailable: {0}")]
    KeyUnavailable(String),

    /// An error occurred during the signing operation.
    #[error("Signing failed: {0}")]
    SigningFailed(String),
}

/// Errors that can occur during authorization.
#[derive(Debug, Error)]
pub enum DialogCapabilityAuthorizationError {
    /// Subject does not match the issuer's DID for self-authorization.
    #[error("Not authorized: subject '{subject}' does not match issuer '{issuer}'")]
    NotOwner {
        /// The subject DID from the capability.
        subject: Did,
        /// The issuer's DID.
        issuer: Did,
    },

    /// Audience does not match the issuer's DID for delegation/invocation.
    #[error("Cannot delegate/invoke: audience '{audience}' does not match issuer '{issuer}'")]
    NotAudience {
        /// The audience DID from the authorization.
        audience: Did,
        /// The issuer's DID.
        issuer: Did,
    },

    /// No valid delegation chain found.
    #[error("No valid delegation chain found from '{subject}' to '{audience}'")]
    NoDelegationChain {
        /// The subject DID.
        subject: Did,
        /// The audience DID.
        audience: Did,
    },

    /// Policy constraint violation.
    #[error("Policy constraint violation: {message}")]
    PolicyViolation {
        /// Description of the violation.
        message: String,
    },

    /// Serialization error during signing.
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Errors from capability-routed storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Storage backend error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
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
    Authorization(DialogCapabilityAuthorizationError),
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
