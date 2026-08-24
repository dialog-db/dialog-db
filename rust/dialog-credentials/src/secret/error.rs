//! Errors from sealing and revealing secrets.

use crate::ed25519::{Ed25519KeyError, Ed25519SignerError};

/// Errors from [`Seal::conceal`] and [`Secret::reveal`].
///
/// [`Seal::conceal`]: super::Seal::conceal
/// [`Secret::reveal`]: super::Secret::reveal
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)] // Crypto carries a String
pub enum SecretError {
    /// The secret could not be revealed.
    ///
    /// The message was sealed to a different identity or context, or it has
    /// been tampered with. These are deliberately not distinguished: telling
    /// them apart would let an attacker probe which part of a forged message
    /// was wrong.
    Failed,

    /// The encoded message is malformed or truncated.
    Malformed,

    /// No X25519 agreement key is available for this identity.
    ///
    /// Only reachable on the browser, for a key restored from an archive
    /// written before agreement keys were stored.
    AgreementKeyUnavailable,

    /// The recipient's DID does not yield a usable agreement key.
    InvalidRecipient,

    /// A platform crypto operation failed.
    Crypto(String),
}

impl std::fmt::Display for SecretError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed => write!(f, "could not reveal secret"),
            Self::Malformed => write!(f, "malformed sealed secret"),
            Self::AgreementKeyUnavailable => {
                write!(f, "no agreement key available for this identity")
            }
            Self::InvalidRecipient => write!(f, "recipient has no usable agreement key"),
            Self::Crypto(e) => write!(f, "crypto operation failed: {e}"),
        }
    }
}

impl std::error::Error for SecretError {}

impl From<Ed25519KeyError> for SecretError {
    fn from(e: Ed25519KeyError) -> Self {
        match e {
            Ed25519KeyError::AgreementKeyUnavailable => Self::AgreementKeyUnavailable,
            other => Self::Crypto(other.to_string()),
        }
    }
}

impl From<Ed25519SignerError> for SecretError {
    fn from(e: Ed25519SignerError) -> Self {
        match e {
            Ed25519SignerError::Key(key) => key.into(),
            other => Self::Crypto(other.to_string()),
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<crate::key::WebCryptoError> for SecretError {
    fn from(e: crate::key::WebCryptoError) -> Self {
        Self::Crypto(e.to_string())
    }
}
