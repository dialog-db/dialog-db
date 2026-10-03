//! Native AES-256-GCM and HKDF-SHA256 via RustCrypto.

use super::super::SecretError;
use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use hkdf::Hkdf;
use sha2::Sha256;

/// Derive an AES-256 key from a shared secret.
pub(crate) async fn derive_key(shared: &[u8; 32], info: &[u8]) -> Result<[u8; 32], SecretError> {
    let hkdf = Hkdf::<Sha256>::new(None, shared);
    let mut key = [0u8; 32];
    hkdf.expand(info, &mut key)
        .map_err(|e| SecretError::Crypto(e.to_string()))?;
    Ok(key)
}

/// Encrypt with AES-256-GCM.
pub(crate) async fn encrypt(
    key: &[u8; 32],
    nonce: &[u8; 12],
    plain: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, SecretError> {
    let cipher = Aes256Gcm::new(key.into());
    cipher
        .encrypt(Nonce::from_slice(nonce), Payload { msg: plain, aad })
        .map_err(|e| SecretError::Crypto(e.to_string()))
}

/// Decrypt with AES-256-GCM.
pub(crate) async fn decrypt(
    key: &[u8; 32],
    nonce: &[u8; 12],
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, SecretError> {
    let cipher = Aes256Gcm::new(key.into());
    // Any authentication failure -- wrong key, wrong context, tampering --
    // collapses to the same error so nothing can be probed.
    cipher
        .decrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| SecretError::Failed)
}
