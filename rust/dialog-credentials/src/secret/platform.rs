//! Platform crypto for sealing and revealing.
//!
//! Native runs AES-256-GCM and HKDF-SHA256 in Rust; the browser routes both
//! through `WebCrypto` so they use the platform's accelerated, constant-time
//! implementations rather than a software AES compiled to wasm. Both produce
//! the same bytes -- AES-GCM and HKDF are fully specified -- so a secret sealed
//! on one platform opens on the other.

use super::{Context, SealedSecret, SecretError};
use crate::ed25519::{Ed25519Verifier, X25519PublicKey, X25519SecretKey};

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod native;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod web;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use native as backend;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use web as backend;

/// Build the HKDF `info` string binding a key to its purpose and participants.
///
/// Including both public keys means a derived key is usable only for this exact
/// pair, and the context label keeps a secret sealed for one purpose from being
/// opened as another. This binding is what provides domain separation between
/// the Ed25519 signing use of an identity and its X25519 agreement use.
fn info(context: Context, ephemeral: &[u8; 32], recipient: &[u8; 32]) -> Vec<u8> {
    let label = context.as_str().as_bytes();
    let mut info = Vec::with_capacity(label.len() + 65);
    info.extend_from_slice(label);
    info.push(0x00);
    info.extend_from_slice(ephemeral);
    info.extend_from_slice(recipient);
    info
}

/// The additional authenticated data: the recipient's DID.
///
/// Binding the recipient means a sealed secret cannot be lifted from one
/// entry and replayed against another.
fn aad(recipient: &Ed25519Verifier) -> Vec<u8> {
    recipient.to_string().into_bytes()
}

/// Conceal `plain` to `recipient`.
pub(super) async fn conceal(
    recipient_key: &X25519PublicKey,
    recipient: &Ed25519Verifier,
    context: Context,
    plain: &[u8],
) -> Result<SealedSecret, SecretError> {
    // A fresh ephemeral pair per message: the sender's long-term key never
    // performs the agreement, so compromising it later does not open past
    // messages.
    let ephemeral = X25519SecretKey::ephemeral().await?;
    let ephemeral_public_key = ephemeral.public_key().to_bytes();
    let recipient_bytes = recipient_key.to_bytes();

    let shared = ephemeral.diffie_hellman(recipient_key).await?;
    let key = backend::derive_key(
        &shared,
        &info(context, &ephemeral_public_key, &recipient_bytes),
    )
    .await?;

    let nonce = random_nonce()?;
    let ciphertext = backend::encrypt(&key, &nonce, plain, &aad(recipient)).await?;

    Ok(SealedSecret {
        ephemeral_public_key,
        nonce,
        ciphertext,
    })
}

/// Reveal a secret sealed to `recipient`.
pub(super) async fn reveal(
    key: &X25519SecretKey,
    recipient: &Ed25519Verifier,
    context: Context,
    sealed: &SealedSecret,
) -> Result<Vec<u8>, SecretError> {
    let ephemeral = X25519PublicKey::from_bytes(&sealed.ephemeral_public_key).await?;
    let recipient_bytes = key.public_key().to_bytes();

    let shared = key.diffie_hellman(&ephemeral).await?;
    let derived = backend::derive_key(
        &shared,
        &info(context, &sealed.ephemeral_public_key, &recipient_bytes),
    )
    .await?;

    backend::decrypt(&derived, &sealed.nonce, &sealed.ciphertext, &aad(recipient)).await
}

/// Generate a random AES-GCM nonce.
///
/// Each message derives a fresh key (the ephemeral pair is single-use), so a
/// random nonce carries no reuse risk across messages.
fn random_nonce() -> Result<[u8; 12], SecretError> {
    let mut nonce = [0u8; 12];
    getrandom::getrandom(&mut nonce).map_err(|e| SecretError::Crypto(e.to_string()))?;
    Ok(nonce)
}
