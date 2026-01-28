//! Authorization trait for capability-based access control.
//!
//! The `Authorization` trait represents proof of authority over a capability.

use super::{Authority, subject::Did};

/// Errors that can occur during authorization.
#[derive(Debug, thiserror::Error)]
pub enum AuthorizationError {
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

/// Trait for proof of authority over a capability.
///
/// `Authorization` represents an abstract proof that `audience` has authority
/// to exercise a capability on `subject`. It can be:
///
/// - Self-issued (when subject == audience, i.e., owner acting directly)
/// - Derived from a delegation chain
pub trait Authorization: Sized {
    /// The subject (resource owner) this authorization covers.
    fn subject(&self) -> &Did;

    /// The audience who has been granted authority.
    fn audience(&self) -> &Did;

    /// The ability path this authorization permits.
    fn ability(&self) -> &str;

    /// Creates authorized invocation
    fn invoke<A: Authority>(&self, authority: &A) -> Result<Self, AuthorizationError>;
}
