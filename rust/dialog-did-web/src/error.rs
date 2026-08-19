//! Error type for DID resolution.

use thiserror::Error;

/// Failure resolving a DID to a verifier.
///
/// Every variant is a hard refusal. There is no silent fallback: a DID that
/// cannot be resolved to a supported verification method is an error, not a
/// default key.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ResolveError {
    /// The DID method is not handled by this resolver.
    #[error("unsupported DID method: {0}")]
    UnsupportedMethod(String),

    /// The DID string is malformed for its method.
    #[error("malformed DID: {0}")]
    MalformedDid(String),

    /// The `did:web` document could not be fetched (network failure, non-2xx
    /// status, and so on).
    #[error("could not fetch DID document: {0}")]
    Fetch(String),

    /// The fetched DID document was not valid JSON in the expected shape.
    #[error("could not parse DID document: {0}")]
    MalformedDocument(String),

    /// The DID document had no verification method this build can use.
    #[error("no supported verification method in DID document")]
    NoSupportedVerificationMethod,

    /// A verification method used a key type or encoding this build does not
    /// support.
    #[error("unsupported verification method key: {0}")]
    UnsupportedKey(String),
}
