//! Principal and Authority traits for identity and signing.
//!
//! These traits define the identity model for capability-based access control.

use super::subject::Did;

/// A principal with a DID identity.
pub trait Principal {
    /// Get this principal's DID.
    fn did(&self) -> &Did;
}

/// An authority that can sign data.
///
/// Extends `Principal` with the ability to sign payloads.
pub trait Authority: Principal {
    /// Sign the given payload.
    fn sign(&mut self, payload: &[u8]) -> Vec<u8>;

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
