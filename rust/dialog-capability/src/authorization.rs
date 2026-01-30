//! Authorization trait for capability-based access control.
//!
//! The `Authorization` trait represents proof of authority over a capability.

use crate::{Authority, subject::Did};
use async_trait::async_trait;
use dialog_common::{ConditionalSend, ConditionalSync};

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
#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
pub trait Authorization: Sized + ConditionalSend {
    /// The subject (resource owner) this authorization covers.
    fn subject(&self) -> &Did;

    /// The audience who has been granted authority.
    fn audience(&self) -> &Did;

    /// The ability path this authorization permits.
    fn ability(&self) -> &str;

    /// Creates authorized invocation by signing with the provided authority.
    async fn invoke<A: Authority + ConditionalSend + ConditionalSync>(
        &self,
        authority: &A,
    ) -> Result<Self, AuthorizationError>;
}
