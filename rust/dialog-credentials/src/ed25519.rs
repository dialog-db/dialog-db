//! Ed25519 key types, DID, and signer implementations.

use dialog_varsig::eddsa::Ed25519Signature;

// Platform-specific implementations
pub mod native;

// WebCrypto is only available in web browsers (wasm32 + unknown OS)
// Not available in WASI or other WASM environments
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod web;

// Submodules
mod error;
mod resolver;
mod signer;
mod verifier;

// Re-export all public types for backwards compatibility
pub use crate::key::KeyExport;
pub use error::{Ed25519DidFromStrError, Ed25519KeyError, Ed25519ResolveError, Ed25519SignerError};
pub use resolver::Ed25519KeyResolver;
pub use signer::Ed25519Signer;
pub use verifier::Ed25519Verifier;

// Re-export WebCrypto types on WASM
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use crate::key::{ExtractableKey, WebCryptoError};

// Key material types

/// Ed25519 verifying key.
///
/// This enum abstracts over different Ed25519 verification implementations:
/// - `Native`: Uses `ed25519_dalek::VerifyingKey` for native platforms
/// - `WebCrypto`: Uses the browser's `WebCrypto` API (web WASM only)
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)] // CryptoKey is not Copy on WASM
pub enum Ed25519VerifyingKey {
    /// Native verifying key using `ed25519_dalek`.
    Native(native::VerifyingKey),

    /// WebCrypto verifying key (web WASM only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    WebCrypto(web::VerifyingKey),
}

impl From<native::VerifyingKey> for Ed25519VerifyingKey {
    fn from(key: native::VerifyingKey) -> Self {
        Self::Native(key)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<web::VerifyingKey> for Ed25519VerifyingKey {
    fn from(key: web::VerifyingKey) -> Self {
        Self::WebCrypto(key)
    }
}

impl Ed25519VerifyingKey {
    /// Get the raw public key bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 32] {
        match self {
            Self::Native(key) => key.to_bytes(),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => key.to_bytes(),
        }
    }
}

impl Ed25519VerifyingKey {
    /// Verify a signature for the given message asynchronously.
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if verification fails.
    #[allow(clippy::unused_async)]
    pub async fn verify_signature(
        &self,
        msg: &[u8],
        signature: &Ed25519Signature,
    ) -> Result<(), signature::Error> {
        match self {
            Self::Native(key) => {
                use signature::Verifier;
                let dalek_sig = ed25519_dalek::Signature::from(*signature);
                key.verify(msg, &dalek_sig)
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => web::verify(key.crypto_key(), msg, signature).await,
        }
    }
}

impl PartialEq for Ed25519VerifyingKey {
    fn eq(&self, other: &Self) -> bool {
        self.to_bytes() == other.to_bytes()
    }
}

impl Eq for Ed25519VerifyingKey {}

/// Ed25519 signing key.
///
/// This enum abstracts over different Ed25519 signing implementations:
/// - `Native`: Uses `ed25519_dalek::SigningKey` for native platforms
/// - `WebCrypto`: Uses the browser's `WebCrypto` API (web WASM only)
#[derive(Debug, Clone)]
pub enum Ed25519SigningKey {
    /// Native signing key using `ed25519_dalek`.
    Native(native::SigningKey),

    /// WebCrypto signing key (web WASM only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    WebCrypto(web::SigningKey),
}

impl Ed25519SigningKey {
    /// Get the verifying (public) key.
    #[must_use]
    pub fn verifying_key(&self) -> Ed25519VerifyingKey {
        match self {
            Self::Native(key) => Ed25519VerifyingKey::Native(key.verifying_key()),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => Ed25519VerifyingKey::WebCrypto(key.verifying_key()),
        }
    }

    /// Generate a new Ed25519 signing key.
    ///
    /// On WASM, uses the `WebCrypto` API (non-extractable key by default).
    /// On native, uses `ed25519_dalek` with random bytes from `getrandom`.
    ///
    /// # Errors
    ///
    /// On WASM, returns an error if key generation fails or the browser
    /// doesn't support Ed25519. On native, returns an error if the RNG fails.
    #[allow(clippy::unused_async)]
    pub async fn generate() -> Result<Self, Ed25519KeyError> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Ok(Self::WebCrypto(web::SigningKey::generate().await?))
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            let mut seed = [0u8; 32];
            getrandom::getrandom(&mut seed).map_err(Ed25519KeyError::Rng)?;
            Ok(Self::Native(ed25519_dalek::SigningKey::from_bytes(&seed)))
        }
    }

    /// Export the key material.
    ///
    /// For `Native` keys, returns `KeyExport::Extractable` with the raw seed bytes.
    /// For `WebCrypto` keys, delegates to [`web::SigningKey::export`].
    ///
    /// # Errors
    ///
    /// On WASM with a non-extractable `WebCrypto` key, returns
    /// `KeyExport::NonExtractable` (not an error). Errors only if the
    /// `WebCrypto` export operation itself fails.
    #[allow(clippy::unused_async)]
    pub async fn export(&self) -> Result<KeyExport, Ed25519KeyError> {
        match self {
            Self::Native(key) => Ok(KeyExport::Extractable(key.to_bytes().to_vec())),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => Ok(key.export().await?),
        }
    }

    /// Import from a [`KeyExport`].
    ///
    /// On native, `Extractable(bytes)` constructs a native `ed25519_dalek::SigningKey`.
    ///
    /// On WASM, both variants are routed through [`web::SigningKey::import`] so
    /// that `Extractable` seeds produce a **non-extractable** `WebCrypto` key
    /// (matching the security default of [`web::SigningKey::import`]).
    ///
    /// # Errors
    ///
    /// Returns an error if the seed has the wrong length or the `WebCrypto` import fails.
    #[allow(clippy::unused_async)] // async is needed on WASM
    pub async fn import(key: impl Into<KeyExport>) -> Result<Self, Ed25519KeyError> {
        let key = key.into();

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Ok(Self::WebCrypto(web::SigningKey::import(key).await?))
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            match key {
                KeyExport::Extractable(ref bytes) => {
                    let seed: [u8; 32] = bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| Ed25519KeyError::InvalidSeedLength(bytes.len()))?;
                    Ok(Self::Native(ed25519_dalek::SigningKey::from_bytes(&seed)))
                }
            }
        }
    }
}

impl From<ed25519_dalek::SigningKey> for Ed25519SigningKey {
    fn from(key: ed25519_dalek::SigningKey) -> Self {
        Self::Native(key)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<web::SigningKey> for Ed25519SigningKey {
    fn from(key: web::SigningKey) -> Self {
        Self::WebCrypto(key)
    }
}

impl Ed25519SigningKey {
    /// Sign a message asynchronously.
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if signing fails.
    #[allow(clippy::unused_async)]
    pub async fn sign_bytes(&self, msg: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        match self {
            Self::Native(key) => {
                use signature::Signer;
                let sig = key.try_sign(msg)?;
                Ok(Ed25519Signature::from(sig))
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => key.sign_bytes(msg).await,
        }
    }
}
