//! Errors produced while parsing and verifying DKIM material.

use thiserror::Error;

/// A failure parsing an email, a `DKIM-Signature` header, a DKIM DNS record, or
/// verifying the header signature.
///
/// Every variant is a hard refusal: there is no lenient fallback that would let
/// an unverified email pass as verified.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DkimError {
    /// The raw bytes were not a parseable email (no header/body boundary, a
    /// header line with no colon, and so on).
    #[error("malformed email: {0}")]
    MalformedEmail(String),

    /// The email carried no `DKIM-Signature` header.
    #[error("email has no DKIM-Signature header")]
    NoSignatureHeader,

    /// The `DKIM-Signature` header could not be parsed into tags, or a required
    /// tag (`v`, `a`, `d`, `s`, `h`, `bh`, `b`) was missing.
    #[error("malformed DKIM-Signature header: {0}")]
    MalformedSignature(String),

    /// The `a=` tag named an algorithm this crate does not implement.
    #[error("unsupported DKIM algorithm: {0}")]
    UnsupportedAlgorithm(String),

    /// The `c=` tag named a canonicalization this crate does not implement.
    #[error("unsupported canonicalization: {0}")]
    UnsupportedCanonicalization(String),

    /// A header listed in `h=` was not present in the email, so the signed
    /// string cannot be reconstructed. (Per RFC 6376 a signer may list a header
    /// name more than it appears to signal non-existence; this crate rejects a
    /// header count mismatch rather than guessing.)
    #[error("signed header {0:?} listed in h= is missing from the email")]
    MissingSignedHeader(String),

    /// The DKIM DNS TXT record could not be parsed, or named an unsupported key
    /// type, or carried no `p=` public key.
    #[error("malformed DKIM DNS record: {0}")]
    MalformedDnsRecord(String),

    /// The public key bytes were not a valid key of the named type.
    #[error("invalid DKIM public key: {0}")]
    InvalidPublicKey(String),

    /// A base64 value (`b=`, `bh=`, or `p=`) was not valid base64.
    #[error("invalid base64 in {0}")]
    InvalidBase64(String),

    /// The public key type does not match the `a=` algorithm (for example an
    /// RSA key with an `ed25519-sha256` signature).
    #[error("DKIM key type does not match signature algorithm")]
    KeyAlgorithmMismatch,

    /// The header signature `b=` did not verify against the reconstructed signed
    /// string with the domain public key.
    #[error("DKIM signature verification failed")]
    VerificationFailed,

    /// The signer's `x=` expiration has passed. The signer's own statement that
    /// the signature is no longer valid, which no policy above may relax.
    #[error("DKIM signature has expired")]
    SignatureExpired,

    /// The RSA key is below the RFC 8301 floor of 1024 bits. A weak DKIM key
    /// can be factored offline, which would let its holder mint a binding for
    /// any mailbox at the domain.
    #[error("DKIM RSA key is too weak: {0} bits, minimum 1024")]
    WeakPublicKey(usize),
}
