use crate::{DialogCapabilitySignError, Did};
use async_trait::async_trait;
use dialog_common::ConditionalSend;

/// A principal with a DID identity.
pub trait Principal {
    /// Get this principal's DID.
    fn did(&self) -> &Did;
}

/// An authority that can sign data.
///
/// Extends `Principal` with the ability to sign payloads.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Authority: Principal + ConditionalSend {
    /// Sign the given payload.
    async fn sign(&mut self, payload: &[u8]) -> Result<Vec<u8>, DialogCapabilitySignError>;

    /// Try to export the raw Ed25519 secret key bytes for delegation purposes.
    ///
    /// Returns `Some([u8; 32])` if this authority uses Ed25519 and supports key export,
    /// `None` otherwise.
    ///
    /// This is used by UCAN delegation to construct signing keys without requiring
    /// a direct dependency on `ed25519_dalek` in the trait definition.
    fn secret_key_bytes(&self) -> Option<[u8; 32]> {
        None
    }
}
