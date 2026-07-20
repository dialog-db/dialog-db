use std::fmt::{self, Display};

use base58::ToBase58;
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

use super::HistoryError;

/// Ed25519 principal committing (and signing) a revision, represented by its
/// verifying key bytes
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Issuer(pub [u8; 32]);

/// Ed25519 authority on whose behalf a revision is committed, represented by
/// its verifying key bytes.
///
/// Authorization of the issuer to act for the authority is established out of
/// band (e.g. via UCAN delegation) and is not modeled here.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Authority(pub [u8; 32]);

/// Ed25519 signature by the issuer over a revision payload
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Signature(#[serde(with = "BigArray")] pub [u8; 64]);

impl From<ed25519_dalek::VerifyingKey> for Issuer {
    fn from(key: ed25519_dalek::VerifyingKey) -> Self {
        Self(key.to_bytes())
    }
}

impl From<ed25519_dalek::VerifyingKey> for Authority {
    fn from(key: ed25519_dalek::VerifyingKey) -> Self {
        Self(key.to_bytes())
    }
}

impl From<ed25519_dalek::Signature> for Signature {
    fn from(signature: ed25519_dalek::Signature) -> Self {
        Self(signature.to_bytes())
    }
}

/// Decode the Ed25519 verifying key named by a `did:key` identifier: the
/// `z`-multibase base58btc encoding of the two-byte ed25519 multicodec
/// prefix (`0xed 0x01`) followed by the 32 key bytes.
pub fn ed25519_key_of(did: &str) -> Result<ed25519_dalek::VerifyingKey, HistoryError> {
    use base58::FromBase58 as _;
    let encoded = did.strip_prefix("did:key:z").ok_or_else(|| {
        HistoryError::InvalidSignature(format!("Issuer {did} is not a did:key"))
    })?;
    let bytes = encoded.from_base58().map_err(|_| {
        HistoryError::InvalidSignature(format!("Issuer {did} is not valid base58"))
    })?;
    let key: [u8; 32] = bytes
        .strip_prefix(&[0xed, 0x01])
        .and_then(|key| key.try_into().ok())
        .ok_or_else(|| {
            HistoryError::InvalidSignature(format!(
                "Issuer {did} is not an ed25519 did:key"
            ))
        })?;
    ed25519_dalek::VerifyingKey::from_bytes(&key).map_err(|error| {
        HistoryError::InvalidSignature(format!("Invalid issuer key: {error}"))
    })
}

/// Verify `signature` as an Ed25519 signature over `payload` by the key the
/// issuer's `did:key` identifier names
pub fn verify_issuer_signature(
    issuer: &str,
    payload: &[u8],
    signature: &[u8],
) -> Result<(), HistoryError> {
    let key = ed25519_key_of(issuer)?;
    let signature: [u8; 64] = signature.try_into().map_err(|_| {
        HistoryError::InvalidSignature(format!(
            "Signature must be 64 bytes, got {}",
            signature.len()
        ))
    })?;
    key.verify_strict(payload, &ed25519_dalek::Signature::from_bytes(&signature))
        .map_err(|error| {
            HistoryError::InvalidSignature(format!("Signature mismatch: {error}"))
        })
}

impl Display for Issuer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.to_base58())
    }
}

impl Display for Authority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.to_base58())
    }
}

impl fmt::Debug for Issuer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Issuer({self})")
    }
}

impl fmt::Debug for Authority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Authority({self})")
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Signature({})", self.0.to_base58())
    }
}
