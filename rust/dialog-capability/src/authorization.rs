//! Authorization trait for capability-based access control.
//!
//! The `Authorization` trait represents proof of authority over a capability.

use crate::{Authority, DialogCapabilityAuthorizationError, Did, Signature};
use async_trait::async_trait;
use dialog_common::{ConditionalSend, ConditionalSync};

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
    /// The signature type required by this authorization.
    type Signature: Signature;

    /// The subject (resource owner) this authorization covers.
    fn subject(&self) -> &Did;

    /// The audience who has been granted authority.
    fn audience(&self) -> &Did;

    /// The ability path this authorization permits.
    fn ability(&self) -> &str;

    /// Creates authorized invocation by signing with the provided authority.
    async fn invoke<
        A: Authority<Signature = Self::Signature> + Clone + ConditionalSend + ConditionalSync,
    >(
        &self,
        authority: &A,
    ) -> Result<Self, DialogCapabilityAuthorizationError>;
}
