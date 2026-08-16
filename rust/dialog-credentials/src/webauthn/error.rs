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

/// Errors when parsing a WebAuthn verifier from a `did:key` string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
pub enum WebAuthnDidFromStrError {
    /// The DID header is invalid (expected `did:key:z...`).
    #[error("invalid did header")]
    InvalidDidHeader,

    /// The base58 prefix 'z' is missing.
    #[error("missing base58 prefix 'z'")]
    MissingBase58Prefix,

    /// The base58 body could not be decoded.
    #[error("invalid base58")]
    InvalidBase58,

    /// The multicodec prefix is not the WebAuthn P-256 tag.
    #[error("not a WebAuthn did:key")]
    WrongMulticodec,

    /// The key bytes are invalid.
    #[error("invalid key bytes")]
    InvalidKey,
}

/// Errors from WebAuthn DID resolution.
#[derive(Debug, Clone, Copy, Error)]
pub enum WebAuthnResolveError {
    /// The DID could not be parsed as a WebAuthn `did:key`.
    #[error("invalid WebAuthn did:key: {0}")]
    InvalidDid(#[from] WebAuthnDidFromStrError),
}
