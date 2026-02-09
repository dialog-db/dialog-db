//! Error types for Ed25519 key operations.

use thiserror::Error;

/// Errors from [`super::Ed25519SigningKey::import`] or [`super::Ed25519SigningKey::export`].
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)]
pub enum Ed25519KeyError {
    /// The seed bytes have the wrong length (expected 32).
    InvalidSeedLength(usize),

    /// Random number generation failed (native only).
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    Rng(getrandom::Error),

    /// WebCrypto operation failed (WASM only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    WebCrypto(crate::key::WebCryptoError),
}

impl std::fmt::Display for Ed25519KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSeedLength(n) => write!(f, "expected 32 seed bytes, got {n}"),
            #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
            Self::Rng(e) => write!(f, "RNG error: {e}"),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Ed25519KeyError {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<crate::key::WebCryptoError> for Ed25519KeyError {
    fn from(e: crate::key::WebCryptoError) -> Self {
        Self::WebCrypto(e)
    }
}

/// Error type for [`super::signer::Ed25519Signer`] operations.
///
/// On WASM this wraps [`crate::key::WebCryptoError`]; on native this wraps
/// [`getrandom::Error`] (the only thing that can fail is RNG for `generate`).
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)] // Not Copy on WASM (WebCryptoError contains String)
pub enum Ed25519SignerError {
    /// Random number generation failed (native only, from `generate`).
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    Rng(getrandom::Error),

    /// `WebCrypto` operation failed (WASM only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    WebCrypto(crate::key::WebCryptoError),

    /// Key import/export error.
    Key(Ed25519KeyError),
}

impl std::fmt::Display for Ed25519SignerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
            Self::Rng(e) => write!(f, "RNG error: {e}"),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(e) => write!(f, "{e}"),
            Self::Key(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Ed25519SignerError {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl From<getrandom::Error> for Ed25519SignerError {
    fn from(e: getrandom::Error) -> Self {
        Self::Rng(e)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<crate::key::WebCryptoError> for Ed25519SignerError {
    fn from(e: crate::key::WebCryptoError) -> Self {
        Self::WebCrypto(e)
    }
}

impl From<Ed25519KeyError> for Ed25519SignerError {
    fn from(e: Ed25519KeyError) -> Self {
        Self::Key(e)
    }
}

/// Errors that can occur when parsing an `Ed25519Principal` from a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
pub enum Ed25519DidFromStrError {
    /// The DID header is invalid.
    #[error("invalid did header")]
    InvalidDidHeader,

    /// The base58 prefix 'z' is missing.
    #[error("missing base58 prefix 'z'")]
    MissingBase58Prefix,

    /// The base58 encoding is invalid.
    #[error("invalid base58 encoding")]
    InvalidBase58,

    /// The key bytes are invalid.
    #[error("invalid key bytes")]
    InvalidKey,
}

/// Error type for Ed25519 DID resolution.
#[derive(Debug, Clone, Copy, Error)]
pub enum Ed25519ResolveError {
    /// The DID could not be parsed as an Ed25519 did:key.
    #[error("invalid ed25519 did:key: {0}")]
    InvalidDid(#[from] Ed25519DidFromStrError),
}
