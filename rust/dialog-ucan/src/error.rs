//! Error types for UCAN access services.
//!
//! This module provides error types that can be used by HTTP handlers
//! to return consistent error responses. The error codes map to specific
//! HTTP status codes and provide structured error information.

use crate::VerificationError;
use serde::Serialize;

/// Error codes returned by UCAN access services.
///
/// These codes provide fine-grained error classification for API responses.
/// Each code maps to an HTTP status code via [`ErrorCode::status_code`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    // 400 Bad Request - Input validation errors
    /// Base64 decoding failed
    InvalidBase64,
    /// DAG-CBOR parsing failed
    InvalidCbor,
    /// Invalid argument (generic)
    InvalidArgument,

    // 401 Unauthorized - Authentication errors
    /// Signature verification failed
    SignatureInvalid,
    /// Audience does not match subject
    AudienceMismatch,
    /// Invocation has expired
    InvocationExpired,
    /// Required proof not found in store
    ProofNotFound,
    /// Proof delegation has expired
    ProofExpired,
    /// Proof delegation is not yet valid (nbf in future)
    ProofNotYetValid,

    // 403 Forbidden - Authorization errors
    /// Delegation chain is invalid
    ChainInvalid,
    /// Command in invocation does not match expected command
    CommandMismatch,
    /// Subject not authorized by proof chain
    SubjectNotAllowed,

    // 500 Internal Server Error
    /// Internal server error
    InternalError,
}

impl ErrorCode {
    /// Get the HTTP status code for this error.
    pub fn status_code(&self) -> u16 {
        match self {
            // 400 Bad Request
            ErrorCode::InvalidBase64 | ErrorCode::InvalidCbor | ErrorCode::InvalidArgument => 400,

            // 401 Unauthorized
            ErrorCode::SignatureInvalid
            | ErrorCode::AudienceMismatch
            | ErrorCode::InvocationExpired
            | ErrorCode::ProofNotFound
            | ErrorCode::ProofExpired
            | ErrorCode::ProofNotYetValid => 401,

            // 403 Forbidden
            ErrorCode::ChainInvalid | ErrorCode::CommandMismatch | ErrorCode::SubjectNotAllowed => {
                403
            }

            // 500 Internal Server Error
            ErrorCode::InternalError => 500,
        }
    }
}

/// Service error with code and message.
///
/// This is a generic error type that can be converted to HTTP responses
/// by framework-specific code. It contains an [`ErrorCode`] for classification
/// and a human-readable message.
#[derive(Debug)]
pub struct ServiceError {
    /// The error code
    pub code: ErrorCode,
    /// Human-readable error message
    pub message: String,
}

impl ServiceError {
    /// Create a new service error.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// Get the HTTP status code for this error.
    pub fn status_code(&self) -> u16 {
        self.code.status_code()
    }

    // Convenience constructors for common errors

    /// Invalid base64 encoding.
    pub fn invalid_base64(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidBase64, message)
    }

    /// Invalid DAG-CBOR encoding.
    pub fn invalid_cbor(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidCbor, message)
    }

    /// Invalid argument.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidArgument, message)
    }

    /// Signature verification failed.
    pub fn signature_invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::SignatureInvalid, message)
    }

    /// Audience does not match subject.
    pub fn audience_mismatch(expected: &str, got: &str) -> Self {
        Self::new(
            ErrorCode::AudienceMismatch,
            format!(
                "Audience mismatch: audience ({}) must equal subject ({})",
                got, expected
            ),
        )
    }

    /// Invocation has expired.
    pub fn invocation_expired() -> Self {
        Self::new(ErrorCode::InvocationExpired, "Invocation has expired")
    }

    /// Proof not found in delegation store.
    pub fn proof_not_found(cid: &str) -> Self {
        Self::new(
            ErrorCode::ProofNotFound,
            format!("Proof not found: {}", cid),
        )
    }

    /// Delegation chain is invalid.
    pub fn chain_invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::ChainInvalid, message)
    }

    /// Subject not authorized by proof.
    pub fn subject_not_allowed() -> Self {
        Self::new(ErrorCode::SubjectNotAllowed, "Subject not allowed by proof")
    }

    /// Internal server error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InternalError, message)
    }
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for ServiceError {}

/// Convert a [`VerificationError`] into a [`ServiceError`].
///
/// This mapping provides appropriate error codes for each verification failure type.
impl From<VerificationError> for ServiceError {
    fn from(err: VerificationError) -> Self {
        match err {
            VerificationError::ParseError(msg) => ServiceError::invalid_cbor(msg),
            VerificationError::InvalidSignature(msg) => ServiceError::signature_invalid(msg),
            VerificationError::AudienceMismatch { expected, got } => {
                ServiceError::audience_mismatch(&expected, &got)
            }
            VerificationError::Expired => ServiceError::invocation_expired(),
            VerificationError::ProofExpired { index } => {
                ServiceError::new(ErrorCode::ProofExpired, format!("Proof[{}] expired", index))
            }
            VerificationError::ProofNotYetValid { index } => ServiceError::new(
                ErrorCode::ProofNotYetValid,
                format!("Proof[{}] not yet valid", index),
            ),
            VerificationError::SubjectNotAllowed => ServiceError::subject_not_allowed(),
            VerificationError::InvalidIssuerChain => {
                ServiceError::chain_invalid("Invalid proof issuer chain")
            }
            VerificationError::RootIssuerNotSubject => {
                ServiceError::chain_invalid("Root proof issuer is not the subject")
            }
            VerificationError::CommandMismatch { expected, found } => ServiceError::new(
                ErrorCode::CommandMismatch,
                format!(
                    "Command mismatch: expected {:?}, found {:?}",
                    expected, found
                ),
            ),
            VerificationError::ProofNotFound(cid) => ServiceError::proof_not_found(&cid),
            VerificationError::PredicateFailed(msg) => {
                ServiceError::chain_invalid(format!("Predicate failed: {}", msg))
            }
            VerificationError::PredicateRunError(msg) => {
                ServiceError::chain_invalid(format!("Predicate run error: {}", msg))
            }
            VerificationError::WaitingOnPromise(msg) => {
                ServiceError::chain_invalid(format!("Waiting on promise: {}", msg))
            }
            VerificationError::InternalError(msg) => ServiceError::internal(msg),
        }
    }
}
