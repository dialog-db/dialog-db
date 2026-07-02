use std::fmt::{self, Display};

use base58::ToBase58;
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

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
