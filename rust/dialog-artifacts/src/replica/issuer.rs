//! Issuer represents a principal operating a replica.
//!
//! An issuer holds a signing key and can sign data, creating
//! cryptographic proof of authorship for revisions.

use ed25519_dalek::ed25519::signature::SignerMut;
use ed25519_dalek::{Signature, SigningKey, SECRET_KEY_LENGTH};

use super::error::ReplicaError;
use super::types::Principal;

/// Represents a principal operating a replica.
///
/// An issuer holds a signing key derived from a passphrase or secret,
/// and can sign data to prove authorship.
#[derive(Clone, PartialEq, Eq)]
pub struct Issuer {
    id: String,
    key: SigningKey,
    principal: Principal,
}

impl std::fmt::Debug for Issuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.did())
    }
}

impl Issuer {
    /// Creates a new issuer from a passphrase by hashing it to derive a signing key.
    pub fn from_passphrase(passphrase: &str) -> Self {
        let bytes = passphrase.as_bytes();
        Self::from_secret(blake3::hash(bytes).as_bytes())
    }

    /// Creates a new issuer from a secret key.
    pub fn from_secret(secret: &[u8; SECRET_KEY_LENGTH]) -> Self {
        Issuer::new(SigningKey::from_bytes(secret))
    }

    /// Creates a new issuer from a signing key.
    pub fn new(key: SigningKey) -> Self {
        let principal = Principal::from_bytes(key.verifying_key().to_bytes());

        Self {
            id: principal.did(),
            key,
            principal,
        }
    }

    /// Generates a new issuer with a random signing key.
    pub fn generate() -> Result<Self, ReplicaError> {
        Ok(Self::new(SigningKey::generate(&mut rand::thread_rng())))
    }

    /// Signs a payload with this issuer's signing key.
    pub fn sign(&mut self, payload: &[u8]) -> Signature {
        self.key.sign(payload)
    }

    /// Returns the DID (Decentralized Identifier) for this issuer.
    pub fn did(&self) -> &str {
        &self.id
    }

    /// Returns the principal (public key bytes) for this issuer.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }
}
