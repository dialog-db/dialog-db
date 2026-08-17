//! ECDSA signature algorithms.

#[cfg(feature = "secp384r1")]
use super::curve::Secp384r1;
#[cfg(feature = "secp521r1")]
use super::curve::Secp521r1;
use super::{
    SignatureAlgorithm,
    curve::{Secp256k1, Secp256r1},
    hash::Multihasher,
};
#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
use crate::signature::Signature;
#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
use signature::SignatureEncoding;
use std::marker::PhantomData;

/// The ECDSA signature algorithm.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EcDsa<C: EcDsaCurve, H: Multihasher>(PhantomData<(C, H)>);

/// ECDSA-compatible curves
pub trait EcDsaCurve {}

#[cfg(feature = "secp256k1")]
impl EcDsaCurve for Secp256k1 {}

#[cfg(feature = "secp256r1")]
impl EcDsaCurve for Secp256r1 {}

#[cfg(feature = "secp384r1")]
impl EcDsaCurve for Secp384r1 {}

#[cfg(feature = "secp521r1")]
impl EcDsaCurve for Secp521r1 {}

/// The ES256 signature algorithm.
#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
pub type Es256 = EcDsa<Secp256r1, super::hash::Sha2_256>;

#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
impl SignatureAlgorithm for Es256 {
    fn prefix(&self) -> u64 {
        0xec
    }

    fn config_tags(&self) -> Vec<u64> {
        vec![0x1201, 0x15]
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if bytes.get(0..=2)? == [0xec, 0x1201, 0x15] {
            Some((Self::default(), bytes.get(3..)?))
        } else {
            None
        }
    }
}

/// ES256 (ECDSA over P-256) signature bytes (64 bytes: r followed by s).
///
/// This is a platform-agnostic fixed-width representation of a P-256 ECDSA
/// signature. It converts to and from `p256::ecdsa::Signature`, whose
/// canonical fixed-size encoding is exactly `r || s`, 32 bytes each.
#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Es256Signature(#[serde(with = "serde_bytes")] pub [u8; 64]);

#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
impl Es256Signature {
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

#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
impl From<[u8; 64]> for Es256Signature {
    fn from(bytes: [u8; 64]) -> Self {
        Self(bytes)
    }
}

#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
impl From<Es256Signature> for [u8; 64] {
    fn from(sig: Es256Signature) -> Self {
        sig.0
    }
}

#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
impl From<p256::ecdsa::Signature> for Es256Signature {
    fn from(sig: p256::ecdsa::Signature) -> Self {
        Self(sig.to_bytes().into())
    }
}

#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
impl TryFrom<Es256Signature> for p256::ecdsa::Signature {
    type Error = signature::Error;

    fn try_from(sig: Es256Signature) -> Result<Self, Self::Error> {
        p256::ecdsa::Signature::from_slice(&sig.0)
    }
}

#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
impl SignatureEncoding for Es256Signature {
    type Repr = [u8; 64];
}

#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
impl TryFrom<&[u8]> for Es256Signature {
    type Error = signature::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; 64] = bytes.try_into().map_err(|_| signature::Error::new())?;
        Ok(Self(bytes))
    }
}

#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
impl Signature for Es256Signature {
    type Algorithm = Es256;
}

/// The ES384 signature algorithm.
#[cfg(all(feature = "secp384r1", feature = "sha2_384"))]
pub type Es384 = EcDsa<Secp384r1, super::hash::Sha2_384>;

#[cfg(all(feature = "secp384r1", feature = "sha2_384"))]
impl SignatureAlgorithm for Es384 {
    fn prefix(&self) -> u64 {
        0xec
    }

    fn config_tags(&self) -> Vec<u64> {
        vec![0x1201, 0x20]
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if bytes.get(0..=2)? == [0xec, 0x1202, 0x20] {
            Some((Self::default(), bytes.get(3..)?))
        } else {
            None
        }
    }
}

/// The ES512 signature algorithm.
#[cfg(all(feature = "secp521r1", feature = "sha2_512"))]
pub type Es512 = EcDsa<Secp521r1, super::hash::Sha2_512>;

#[cfg(all(feature = "secp521r1", feature = "sha2_512"))]
impl SignatureAlgorithm for Es512 {
    fn prefix(&self) -> u64 {
        0xec
    }

    fn config_tags(&self) -> Vec<u64> {
        vec![0x1202, 0x13]
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if bytes.get(0..=2)? == [0xec, 0x1202, 0x13] {
            Some((Self::default(), bytes.get(3..)?))
        } else {
            None
        }
    }
}

/// The ES256K signature algorithm.
#[cfg(all(feature = "secp256k1", feature = "sha2_256"))]
pub type Es256k = EcDsa<Secp256k1, super::hash::Sha2_256>;

#[cfg(all(feature = "secp256k1", feature = "sha2_256"))]
impl SignatureAlgorithm for Es256k {
    fn prefix(&self) -> u64 {
        0xec
    }

    fn config_tags(&self) -> Vec<u64> {
        vec![0xe7, 0x12]
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if *bytes.get(0..=2)? == [0xec, 0xe7, 0x12] {
            Some((Self::default(), bytes.get(3..)?))
        } else {
            None
        }
    }
}
