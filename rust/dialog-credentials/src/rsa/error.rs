//! Error types for RSA key operations.

use thiserror::Error;

/// Errors from [`super::RsaSigningKey`] import/export/generation.
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)]
pub enum RsaKeyError {
    /// The key bytes are not a valid PKCS#1 RSA private key.
    InvalidPrivateKey,

    /// The key is neither RSA-2048 nor RSA-4096 (the only supported sizes).
    UnsupportedKeySize(usize),

    /// Random number generation failed (native key generation only).
    Rng,
}

impl std::fmt::Display for RsaKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPrivateKey => write!(f, "invalid PKCS#1 RSA private key"),
            Self::UnsupportedKeySize(bits) => {
                write!(
                    f,
                    "unsupported RSA key size: {bits} bits (want 2048 or 4096)"
                )
            }
            Self::Rng => write!(f, "RSA key generation RNG error"),
        }
    }
}

impl std::error::Error for RsaKeyError {}

/// Error type for [`super::RsaSigner`] operations.
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)]
pub enum RsaSignerError {
    /// Key import/export/generation error.
    Key(RsaKeyError),
}

impl std::fmt::Display for RsaSignerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Key(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for RsaSignerError {}

impl From<RsaKeyError> for RsaSignerError {
    fn from(e: RsaKeyError) -> Self {
        Self::Key(e)
    }
}

/// Errors that can occur when parsing an [`super::RsaVerifier`] from a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
pub enum RsaDidFromStrError {
    /// The DID header is invalid.
    #[error("invalid did header")]
    InvalidDidHeader,

    /// The base58 prefix 'z' is missing.
    #[error("missing base58 prefix 'z'")]
    MissingBase58Prefix,

    /// The base58 encoding is invalid.
    #[error("invalid base58 encoding")]
    InvalidBase58,

    /// The multicodec prefix is not `rsa-pub` (`0x1205`).
    #[error("not an rsa-pub did:key")]
    WrongMulticodec,

    /// The key bytes are not a valid PKCS#1 RSA public key.
    #[error("invalid rsa public key bytes")]
    InvalidKey,

    /// The key is neither RSA-2048 nor RSA-4096.
    #[error("unsupported rsa key size")]
    UnsupportedKeySize,
}

/// Error type for RSA DID resolution.
#[derive(Debug, Clone, Copy, Error)]
pub enum RsaResolveError {
    /// The DID could not be parsed as an RSA did:key.
    #[error("invalid rsa did:key: {0}")]
    InvalidDid(#[from] RsaDidFromStrError),
}
