//! Authorization trait for capability-based access control.
//!
//! The `Authorization` trait represents proof of authority over a capability.

use crate::{Authority, DialogCapabilityAuthorizationError, subject::Did};

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
    fn invoke<A: Authority>(
        &self,
        authority: &A,
    ) -> Result<Self, DialogCapabilityAuthorizationError>;
}
