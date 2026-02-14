//! RSA signature algorithm configuration.

#[cfg(feature = "rsa")]
use {
    super::hash::Multihasher,
    crate::signature::Signature,
    signature::SignatureEncoding,
    std::marker::PhantomData,
};

#[cfg(all(feature = "rsa", feature = "sha2_256"))]
use super::{SignatureAlgorithm, hash::Sha2_256};

/// The RSA signature algorithm.
///
/// The `const L` type parameter represents the key length in bytes.
#[cfg(feature = "rsa")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Rsa<const L: usize, H: Multihasher>(PhantomData<H>);

/// The RS256 signature algorithm.
///
/// The `const L` type parameter represents the key length in bytes.
#[cfg(all(feature = "rsa", feature = "sha2_256"))]
pub type Rs256<const L: usize> = Rsa<L, Sha2_256>;

/// RSA-2048 with SHA-256 signature type alias.
#[cfg(all(feature = "rsa", feature = "sha2_256"))]
pub type Rs256_2048Signature = RsaSignature<256>;

/// RSA-4096 with SHA-256 signature type alias.
#[cfg(all(feature = "rsa", feature = "sha2_256"))]
pub type Rs256_4096Signature = RsaSignature<512>;

/// RSA PKCS#1 v1.5 signature bytes.
///
/// This is a platform-agnostic representation of an RSA signature.
/// It can be produced by either native (`rsa` crate) or `WebCrypto` signers,
/// and can be converted to/from `rsa::pkcs1v15::Signature` for verification.
///
/// The `const L` type parameter represents the signature length in bytes
/// (256 for RSA-2048, 512 for RSA-4096).
#[cfg(feature = "rsa")]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct RsaSignature<const L: usize>(#[serde(with = "serde_bytes")] pub Vec<u8>);

#[cfg(feature = "rsa")]
impl<const L: usize> RsaSignature<L> {
    /// Create a new signature from raw bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if `bytes` length does not match `L`.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, signature::Error> {
        if bytes.len() != L {
            return Err(signature::Error::new());
        }
        Ok(Self(bytes))
    }

    /// Get the raw signature bytes.
    #[must_use]
    pub fn to_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(feature = "rsa")]
impl<const L: usize> TryFrom<&[u8]> for RsaSignature<L> {
    type Error = signature::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != L {
            return Err(signature::Error::new());
        }
        Ok(Self(bytes.to_vec()))
    }
}

#[cfg(feature = "rsa")]
impl<const L: usize> SignatureEncoding for RsaSignature<L> {
    type Repr = Box<[u8]>;
}

#[cfg(feature = "rsa")]
impl<const L: usize> From<RsaSignature<L>> for Box<[u8]> {
    fn from(sig: RsaSignature<L>) -> Self {
        sig.0.into_boxed_slice()
    }
}

#[cfg(feature = "rsa")]
impl<const L: usize> From<rsa::pkcs1v15::Signature> for RsaSignature<L> {
    fn from(sig: rsa::pkcs1v15::Signature) -> Self {
        Self(sig.to_vec())
    }
}

#[cfg(feature = "rsa")]
impl<const L: usize> TryFrom<RsaSignature<L>> for rsa::pkcs1v15::Signature {
    type Error = signature::Error;

    fn try_from(sig: RsaSignature<L>) -> Result<Self, Self::Error> {
        rsa::pkcs1v15::Signature::try_from(sig.0.as_slice())
    }
}

#[cfg(all(feature = "rsa", feature = "sha2_256"))]
impl Signature for RsaSignature<256> {
    type Algorithm = Rs256<256>;
}

#[cfg(all(feature = "rsa", feature = "sha2_256"))]
impl Signature for RsaSignature<512> {
    type Algorithm = Rs256<512>;
}

#[cfg(all(feature = "rsa", feature = "sha2_256"))]
impl SignatureAlgorithm for Rs256<256> {
    fn prefix(&self) -> u64 {
        0x1205
    }

    fn config_tags(&self) -> Vec<u64> {
        vec![0x12, 0x0100]
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if bytes.get(0..=2)? == [0x1205, 0x12, 0x0100] {
            Some((Rsa(PhantomData), bytes.get(3..)?))
        } else {
            None
        }
    }
}

#[cfg(all(feature = "rsa", feature = "sha2_256"))]
impl SignatureAlgorithm for Rs256<512> {
    fn prefix(&self) -> u64 {
        0x1205
    }

    fn config_tags(&self) -> Vec<u64> {
        vec![0x12, 0x0200]
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if bytes.get(0..=2)? == [0x1205, 0x12, 0x0200] {
            Some((Rsa(PhantomData), bytes.get(3..)?))
        } else {
            None
        }
    }
}
