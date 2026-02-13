//! `EdDSA` signature algorithms.

use super::{
    SignatureAlgorithm,
    curve::Edwards25519,
    hash::{Multihasher, Sha2_512},
};

use crate::signature::Signature;
use signature::SignatureEncoding;
use std::marker::PhantomData;

/// The `EdDSA` signature algorithm.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdDsa<C: EdDsaCurve, H: Multihasher>(PhantomData<(C, H)>);

impl<C: EdDsaCurve, H: Multihasher> EdDsa<C, H> {
    /// Create a new `EdDsa` instance.
    #[must_use]
    pub const fn new() -> Self {
        EdDsa(PhantomData)
    }
}

/// The EdDSA-compatible curves
pub trait EdDsaCurve: Sized {}
impl EdDsaCurve for Edwards25519 {}

// TODO waiting on ed448_goldilocks to cut a stable release with signing
// impl EdDsaCurve for Edwards448 {}

/// The Ed25519 signature algorithm.
///
/// The `EdDSA` signing algorithm with the Edwards25519 curve with SHA2-512 hashing.
#[cfg(all(feature = "edwards25519", feature = "sha2_512"))]
pub type Ed25519 = EdDsa<Edwards25519, Sha2_512>;

/// Ed25519 signature bytes (64 bytes).
///
/// This is a platform-agnostic representation of an Ed25519 signature.
/// It can be produced by either native (`ed25519_dalek`) or `WebCrypto` signers,
/// and can be converted to/from `ed25519_dalek::Signature` for verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Ed25519Signature(#[serde(with = "serde_bytes")] pub [u8; 64]);

impl Ed25519Signature {
    /// Create a new signature from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 64]) -> Self {
        Self(bytes)
    }

    /// Get the raw signature bytes.
    #[must_use]
    pub const fn to_bytes(&self) -> [u8; 64] {
        self.0
    }
}

impl From<[u8; 64]> for Ed25519Signature {
    fn from(bytes: [u8; 64]) -> Self {
        Self(bytes)
    }
}

impl From<Ed25519Signature> for [u8; 64] {
    fn from(sig: Ed25519Signature) -> Self {
        sig.0
    }
}

#[cfg(feature = "edwards25519")]
impl From<ed25519_dalek::Signature> for Ed25519Signature {
    fn from(sig: ed25519_dalek::Signature) -> Self {
        Self(sig.to_bytes())
    }
}

#[cfg(feature = "edwards25519")]
impl From<Ed25519Signature> for ed25519_dalek::Signature {
    fn from(sig: Ed25519Signature) -> Self {
        ed25519_dalek::Signature::from_bytes(&sig.0)
    }
}

impl SignatureEncoding for Ed25519Signature {
    type Repr = [u8; 64];
}

impl TryFrom<&[u8]> for Ed25519Signature {
    type Error = signature::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; 64] = bytes.try_into().map_err(|_| signature::Error::new())?;
        Ok(Self(bytes))
    }
}

impl Signature for Ed25519Signature {
    type Algorithm = Ed25519;
}

#[cfg(all(feature = "edwards25519", feature = "sha2_512"))]
impl SignatureAlgorithm for Ed25519 {
    fn prefix(&self) -> u64 {
        0xed
    }

    fn config_tags(&self) -> Vec<u64> {
        vec![0xed, 0x13]
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if *bytes.get(0..=2)? == [0xed, 0xed, 0x13] {
            Some((EdDsa(PhantomData), bytes.get(3..)?))
        } else {
            None
        }
    }
}
