//! Symmetric sealing under a key the caller already holds.
//!
//! [`secret`](crate::secret) seals *to an identity*: it performs an ECDH with
//! the recipient's agreement key and binds their DID into the ciphertext. That
//! is the right shape for handing a secret to someone, and the wrong shape for
//! encrypting bulk content under a key a group has already agreed on.
//!
//! This module exposes the layer underneath — HKDF-SHA256 and AES-256-GCM,
//! running in Rust natively and through `WebCrypto` in the browser, producing
//! identical bytes either way.
//!
//! ```no_run
//! # use dialog_credentials::symmetric;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let key = symmetric::derive_key(&[7u8; 32], b"dialog/example/v1").await?;
//! let sealed = symmetric::encrypt(&key, &[0u8; 12], b"content", b"context").await?;
//! let opened = symmetric::decrypt(&key, &[0u8; 12], &sealed, b"context").await?;
//! assert_eq!(opened, b"content");
//! # Ok(())
//! # }
//! ```
//!
//! # Nonces are the caller's problem
//!
//! AES-GCM fails catastrophically if a key and nonce ever encrypt two
//! different messages. Nothing here prevents that: a caller either derives a
//! fresh key per message (as [`secret`](crate::secret) does, via a single-use
//! ephemeral pair) or derives the nonce from the plaintext so that a repeat is
//! a repeat of the same message.

use crate::secret::{SecretError, platform};

/// Derive a 256-bit key from input keying material, bound to `info`.
///
/// HKDF-SHA256 with an empty salt. `info` is the domain separator: material
/// derived under one label is unrelated to the same material under another.
///
/// # Errors
///
/// Returns [`SecretError::Crypto`] if the platform's KDF fails.
pub async fn derive_key(material: &[u8; 32], info: &[u8]) -> Result<[u8; 32], SecretError> {
    platform::derive_key(material, info).await
}

/// Encrypt `plain` under `key`, authenticating `aad` alongside it.
///
/// The returned bytes are the ciphertext with its 16-byte tag appended.
///
/// # Errors
///
/// Returns [`SecretError::Crypto`] if the platform's cipher fails.
pub async fn encrypt(
    key: &[u8; 32],
    nonce: &[u8; 12],
    plain: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, SecretError> {
    platform::encrypt(key, nonce, plain, aad).await
}

/// Decrypt what [`encrypt`] produced.
///
/// # Errors
///
/// Returns [`SecretError::Failed`] if the key, nonce or `aad` differ from
/// those used to encrypt, or if the ciphertext has been tampered with. The
/// cases are deliberately indistinguishable.
pub async fn decrypt(
    key: &[u8; 32],
    nonce: &[u8; 12],
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, SecretError> {
    platform::decrypt(key, nonce, ciphertext, aad).await
}
