//! UCAN authority and operator identity management.
//!
//! This module provides types for representing UCAN authorities that can
//! sign invocations and delegations.

use ed25519_dalek::SigningKey;
use ucan::did::{Ed25519Did, Ed25519Signer};

/// UCAN-specific authority that wraps an Ed25519 signer.
///
/// This type implements `dialog_common::capability::Authority` and can be used
/// to create UCAN delegations. It bridges the generic `Authority` trait with
/// UCAN's specific signing requirements.
#[derive(Debug, Clone)]
pub struct UcanAuthority {
    signing_key: SigningKey,
    signer: Ed25519Signer,
    /// Cached DID string for the Principal trait.
    did_string: String,
}

impl UcanAuthority {
    /// Create a new UCAN authority from an Ed25519 signing key.
    pub fn from_signing_key(signing_key: SigningKey) -> Self {
        let signer = Ed25519Signer::new(signing_key.clone());
        let did_string = signer.did().to_string();
        Self {
            signing_key,
            signer,
            did_string,
        }
    }

    /// Create from a 32-byte secret key.
    pub fn from_secret(secret: &[u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(secret);
        Self::from_signing_key(signing_key)
    }

    /// Generate a new random authority.
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut rand_core::OsRng);
        Self::from_signing_key(signing_key)
    }

    /// Get the DID of this authority.
    pub fn did(&self) -> Ed25519Did {
        self.signer.did().clone()
    }

    /// Get the underlying signer (for building UCAN delegations).
    pub fn signer(&self) -> &Ed25519Signer {
        &self.signer
    }
}

impl dialog_common::capability::Principal for UcanAuthority {
    fn did(&self) -> &dialog_common::capability::Did {
        &self.did_string
    }
}

impl dialog_common::capability::Authority for UcanAuthority {
    fn sign(&self, payload: &[u8]) -> Vec<u8> {
        use ed25519_dalek::Signer;
        self.signing_key.sign(payload).to_bytes().to_vec()
    }

    fn secret_key_bytes(&self) -> Option<[u8; 32]> {
        Some(self.signing_key.to_bytes())
    }
}

/// Identity of an operator making UCAN invocations.
///
/// The operator is the entity that signs UCAN invocations. They must have
/// been granted authority by the subject(s) they wish to access.
#[derive(Debug, Clone)]
pub struct OperatorIdentity {
    signer: Ed25519Signer,
}

impl OperatorIdentity {
    /// Generate a new random operator identity.
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut rand_core::OsRng);
        let signer = Ed25519Signer::new(signing_key);
        Self { signer }
    }

    /// Create an operator identity from a 32-byte secret key.
    pub fn from_secret(secret: &[u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(secret);
        let signer = Ed25519Signer::new(signing_key);
        Self { signer }
    }

    /// Returns the DID of this operator.
    pub fn did(&self) -> Ed25519Did {
        self.signer.did().clone()
    }

    /// Returns a reference to the underlying signer.
    pub fn signer(&self) -> &Ed25519Signer {
        &self.signer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_common::capability::Authority;

    #[test]
    fn test_operator_identity_from_secret() {
        let secret = [0u8; 32];
        let identity = OperatorIdentity::from_secret(&secret);
        let did_str = identity.did().to_string();
        assert!(did_str.starts_with("did:key:z"));
    }

    #[test]
    fn test_operator_identity_deterministic() {
        let secret = [42u8; 32];
        let identity1 = OperatorIdentity::from_secret(&secret);
        let identity2 = OperatorIdentity::from_secret(&secret);
        assert_eq!(identity1.did().to_string(), identity2.did().to_string());
    }

    #[test]
    fn test_ucan_authority_sign() {
        let authority = UcanAuthority::from_secret(&[42u8; 32]);

        // Signing should produce a 64-byte Ed25519 signature
        let payload = b"test payload";
        let signature = authority.sign(payload);
        assert_eq!(signature.len(), 64);

        // Same payload should produce same signature (deterministic)
        let signature2 = authority.sign(payload);
        assert_eq!(signature, signature2);
    }

    #[test]
    fn test_ucan_authority_did() {
        let authority1 = UcanAuthority::from_secret(&[1u8; 32]);
        let authority2 = UcanAuthority::from_secret(&[2u8; 32]);

        // Different secrets should produce different DIDs
        assert_ne!(authority1.did().to_string(), authority2.did().to_string());

        // Same secret should produce same DID
        let authority1_copy = UcanAuthority::from_secret(&[1u8; 32]);
        assert_eq!(
            authority1.did().to_string(),
            authority1_copy.did().to_string()
        );
    }
}
