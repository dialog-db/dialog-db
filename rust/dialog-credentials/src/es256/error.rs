//! Error types for ES256 (P-256) key operations.

use thiserror::Error;

/// Errors from [`super::Es256SigningKey::import`] or [`super::Es256SigningKey::export`].
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)]
pub enum Es256KeyError {
    /// The seed bytes have the wrong length or are not a valid P-256 scalar.
    InvalidSeedLength(usize),

    /// Random number generation failed (native only).
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    Rng(getrandom::Error),

    /// A `WebCrypto` operation failed (WASM only).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    WebCrypto(crate::key::WebCryptoError),
}

impl std::fmt::Display for Es256KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSeedLength(n) => write!(f, "invalid P-256 scalar, got {n} bytes"),
            #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
            Self::Rng(e) => write!(f, "RNG error: {e}"),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Self::WebCrypto(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Es256KeyError {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<crate::key::WebCryptoError> for Es256KeyError {
    fn from(e: crate::key::WebCryptoError) -> Self {
        Self::WebCrypto(e)
    }
}

/// Error type for [`super::signer::Es256Signer`] operations.
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)]
pub enum Es256SignerError {
    /// Random number generation failed (from `generate`).
    Rng(getrandom::Error),

    /// Key import/export error.
    Key(Es256KeyError),
}

impl std::fmt::Display for Es256SignerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rng(e) => write!(f, "RNG error: {e}"),
            Self::Key(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Es256SignerError {}

impl From<getrandom::Error> for Es256SignerError {
    fn from(e: getrandom::Error) -> Self {
        Self::Rng(e)
    }
}

impl From<Es256KeyError> for Es256SignerError {
    fn from(e: Es256KeyError) -> Self {
        Self::Key(e)
    }
}

/// Errors that can occur when parsing an `Es256Verifier` from a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
pub enum Es256DidFromStrError {
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

/// Error type for ES256 DID resolution.
#[derive(Debug, Clone, Copy, Error)]
pub enum Es256ResolveError {
    /// The DID could not be parsed as an ES256 did:key.
    #[error("invalid es256 did:key: {0}")]
    InvalidDid(#[from] Es256DidFromStrError),
}
