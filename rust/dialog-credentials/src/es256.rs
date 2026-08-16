//! ES256 (ECDSA P-256) key types, DID, and signer implementations.

use dialog_varsig::ecdsa::Es256Signature;

// WebCrypto is only available in web browsers (wasm32 + unknown OS). Kept
// private (a `mod`, not `pub mod`) so the crate-root glob re-export does not
// collide with the `ed25519::web` module of the same name; siblings reach it
// via `super::web`.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod web;

// Submodules
mod error;
mod resolver;
mod signer;
mod verifier;

// Re-export public types
pub use crate::key::KeyExport;
pub use error::{Es256DidFromStrError, Es256KeyError, Es256ResolveError, Es256SignerError};
pub use resolver::Es256KeyResolver;
pub use signer::Es256Signer;
pub use verifier::Es256Verifier;

// Re-export WebCrypto helpers on WASM.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use crate::key::{ExtractableKey, WebCryptoError};

/// ES256 verifying (public) key.
///
/// Mirrors the `Ed25519VerifyingKey` shape:
/// - `Native`: uses the `p256` crate for non-WASM platforms.
/// - `WebCrypto`: uses the browser's `WebCrypto` API (web WASM only).
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)] // CryptoKey is not Copy on WASM
pub enum Es256VerifyingKey {
    /// Native verifying key using the `p256` crate.
    Native(p256::ecdsa::VerifyingKey),

    /// WebCrypto verifying key (web WASM only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    WebCrypto(web::VerifyingKey),
}

impl From<p256::ecdsa::VerifyingKey> for Es256VerifyingKey {
    fn from(key: p256::ecdsa::VerifyingKey) -> Self {
        Self::Native(key)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<web::VerifyingKey> for Es256VerifyingKey {
    fn from(key: web::VerifyingKey) -> Self {
        Self::WebCrypto(key)
    }
}

impl Es256VerifyingKey {
    /// Get the SEC1 compressed public key bytes (33 bytes).
    ///
    /// This is the encoding used inside a P-256 `did:key`. The `WebCrypto` arm
    /// caches the same compressed representation, so `did:key` output is
    /// identical across arms for the same key.
    #[must_use]
    pub fn to_compressed_bytes(&self) -> [u8; 33] {
        match self {
            Self::Native(key) => {
                let point = key.to_encoded_point(true);
                let mut bytes = [0u8; 33];
                bytes.copy_from_slice(point.as_bytes());
                bytes
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => key.to_compressed_bytes(),
        }
    }

    /// Verify a signature for the given message.
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if verification fails.
    #[allow(clippy::unused_async)]
    pub async fn verify_signature(
        &self,
        msg: &[u8],
        signature: &Es256Signature,
    ) -> Result<(), signature::Error> {
        match self {
            Self::Native(key) => {
                use signature::Verifier;
                let sig = p256::ecdsa::Signature::try_from(*signature)?;
                key.verify(msg, &sig)
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => key.verify_signature(msg, signature).await,
        }
    }
}

impl PartialEq for Es256VerifyingKey {
    fn eq(&self, other: &Self) -> bool {
        self.to_compressed_bytes() == other.to_compressed_bytes()
    }
}

impl Eq for Es256VerifyingKey {}

/// ES256 signing key.
///
/// Enum-shaped like [`Es256VerifyingKey`]:
/// - `Native`: uses the `p256` crate for non-WASM platforms.
/// - `WebCrypto`: uses the browser's `WebCrypto` API (web WASM only), with
///   non-extractable keys by default.
#[derive(Debug, Clone)]
pub enum Es256SigningKey {
    /// Native signing key using the `p256` crate.
    Native(p256::ecdsa::SigningKey),

    /// WebCrypto signing key (web WASM only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    WebCrypto(web::SigningKey),
}

impl Es256SigningKey {
    /// Get the verifying (public) key.
    #[must_use]
    pub fn verifying_key(&self) -> Es256VerifyingKey {
        match self {
            Self::Native(key) => Es256VerifyingKey::Native(*key.verifying_key()),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => Es256VerifyingKey::WebCrypto(key.verifying_key()),
        }
    }

    /// Generate a new ES256 signing key.
    ///
    /// On WASM, uses the `WebCrypto` API (non-extractable key by default).
    /// On native, uses the `p256` crate with random bytes from `getrandom`.
    ///
    /// # Errors
    ///
    /// On WASM, returns an error if key generation fails or the browser does
    /// not support ECDSA P-256. On native, returns an error if the RNG fails.
    #[allow(clippy::unused_async)]
    pub async fn generate() -> Result<Self, Es256KeyError> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Ok(Self::WebCrypto(web::SigningKey::generate().await?))
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            let mut seed = [0u8; 32];
            getrandom::getrandom(&mut seed).map_err(Es256KeyError::Rng)?;
            let key = p256::ecdsa::SigningKey::from_slice(&seed)
                .map_err(|_| Es256KeyError::InvalidSeedLength(seed.len()))?;
            Ok(Self::Native(key))
        }
    }

    /// Export the key material.
    ///
    /// For `Native` keys, returns `KeyExport::Extractable` with the raw 32-byte
    /// scalar. For `WebCrypto` keys, delegates to [`web::SigningKey::export`],
    /// which yields `NonExtractable` for a non-extractable key.
    ///
    /// # Errors
    ///
    /// On WASM, returns an error if the `WebCrypto` export fails.
    #[allow(clippy::unused_async)]
    pub async fn export(&self) -> Result<KeyExport, Es256KeyError> {
        match self {
            Self::Native(key) => Ok(KeyExport::Extractable(key.to_bytes().to_vec())),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => Ok(key.export().await?),
        }
    }

    /// Import from a [`KeyExport`].
    ///
    /// On native, `Extractable(bytes)` constructs a `p256::ecdsa::SigningKey`
    /// from the 32-byte scalar.
    ///
    /// On WASM, both variants route through [`web::SigningKey::import`] so an
    /// `Extractable` scalar produces a non-extractable `WebCrypto` key.
    ///
    /// # Errors
    ///
    /// Returns an error if the scalar is invalid or the `WebCrypto` import
    /// fails.
    #[allow(clippy::unused_async)]
    pub async fn import(key: impl Into<KeyExport>) -> Result<Self, Es256KeyError> {
        let key = key.into();

        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            Ok(Self::WebCrypto(web::SigningKey::import(key).await?))
        }

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            match key {
                KeyExport::Extractable(ref bytes) => {
                    let signing_key = p256::ecdsa::SigningKey::from_slice(bytes)
                        .map_err(|_| Es256KeyError::InvalidSeedLength(bytes.len()))?;
                    Ok(Self::Native(signing_key))
                }
            }
        }
    }

    /// Sign a message.
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if signing fails.
    #[allow(clippy::unused_async)]
    pub async fn sign_bytes(&self, msg: &[u8]) -> Result<Es256Signature, signature::Error> {
        match self {
            Self::Native(key) => {
                use signature::Signer;
                let sig: p256::ecdsa::Signature = key.try_sign(msg)?;
                Ok(Es256Signature::from(sig))
            }
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(key) => key.sign_bytes(msg).await,
        }
    }
}

impl From<p256::ecdsa::SigningKey> for Es256SigningKey {
    fn from(key: p256::ecdsa::SigningKey) -> Self {
        Self::Native(key)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<web::SigningKey> for Es256SigningKey {
    fn from(key: web::SigningKey) -> Self {
        Self::WebCrypto(key)
    }
}
