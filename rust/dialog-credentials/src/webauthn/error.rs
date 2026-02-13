//! Error types for WebAuthn operations.

use thiserror::Error;

/// Errors from WebAuthn signature verification.
#[derive(Debug, Clone, Error)]
pub enum WebAuthnVerifyError {
    /// The inner ECDSA signature is invalid (DER-decode or verification failed).
    #[error("invalid ECDSA signature: {0}")]
    InvalidSignature(String),

    /// The `clientDataJSON` could not be parsed as JSON.
    #[error("invalid clientDataJSON: {0}")]
    InvalidClientData(String),

    /// The challenge in `clientDataJSON` does not match the expected payload hash.
    #[error("challenge mismatch")]
    ChallengeMismatch,

    /// The authenticator data is too short or malformed.
    #[error("invalid authenticator data")]
    InvalidAuthenticatorData,
}

/// Errors when parsing a WebAuthn verifier from a DID string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
pub enum WebAuthnDidFromStrError {
    /// The DID header is invalid (expected `did:key:z...`).
    #[error("invalid did header")]
    InvalidDidHeader,

    /// The base58 prefix 'z' is missing.
    #[error("missing base58 prefix 'z'")]
    MissingBase58Prefix,

    /// The key bytes are invalid.
    #[error("invalid key bytes")]
    InvalidKey,
}

/// Errors from WebAuthn DID resolution.
#[derive(Debug, Clone, Copy, Error)]
pub enum WebAuthnResolveError {
    /// The DID could not be parsed as a P-256 did:key.
    #[error("invalid P-256 did:key: {0}")]
    InvalidDid(#[from] WebAuthnDidFromStrError),
}
