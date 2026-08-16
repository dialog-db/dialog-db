//! RSA signature algorithm configuration.

#[cfg(feature = "rsa")]
use {
    super::hash::Multihasher, crate::signature::Signature, signature::SignatureEncoding,
    std::marker::PhantomData,
};

#[cfg(all(feature = "rsa", feature = "sha2_256"))]
use super::{SignatureAlgorithm, hash::Sha2_256};

/// Multicodec code for the RSA signature algorithm.
#[cfg(all(feature = "rsa", feature = "sha2_256"))]
const RSA_SIG_PREFIX: u64 = 0x1205;

/// Multicodec code for the SHA2-256 hash algorithm.
#[cfg(all(feature = "rsa", feature = "sha2_256"))]
const SHA2_256_TAG: u64 = Sha2_256::MULTIHASH_TAG;

/// Key size tag for RSA-2048 (256-byte signatures).
#[cfg(all(feature = "rsa", feature = "sha2_256"))]
const RSA_2048_KEY_SIZE_TAG: u64 = 0x0100;

/// Key size tag for RSA-4096 (512-byte signatures).
#[cfg(all(feature = "rsa", feature = "sha2_256"))]
const RSA_4096_KEY_SIZE_TAG: u64 = 0x0200;

/// The RSA signature algorithm.
///
/// The `const L` type parameter represents the signature length in bytes.
#[cfg(feature = "rsa")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Rsa<const L: usize, H: Multihasher>(PhantomData<H>);

/// The RS256 signature algorithm (RSA PKCS#1 v1.5 with SHA-256).
///
/// The `const L` type parameter represents the signature length in bytes.
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
/// This is a platform-agnostic representation of an RSA signature. It can be
/// produced by a native (`rsa` crate) signer and converted to/from
/// `rsa::pkcs1v15::Signature` for verification.
///
/// The `const L` type parameter represents the signature length in bytes
/// (256 for RSA-2048, 512 for RSA-4096). Unlike the fixed-width elliptic-curve
/// signatures, an RSA signature body equals the modulus size, so the width is
/// carried in the type rather than a fixed array.
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
        RSA_SIG_PREFIX
    }

    fn config_tags(&self) -> Vec<u64> {
        vec![SHA2_256_TAG, RSA_2048_KEY_SIZE_TAG]
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if bytes.get(0..=2)? == [RSA_SIG_PREFIX, SHA2_256_TAG, RSA_2048_KEY_SIZE_TAG] {
            Some((Rsa(PhantomData), bytes.get(3..)?))
        } else {
            None
        }
    }
}

#[cfg(all(feature = "rsa", feature = "sha2_256"))]
impl SignatureAlgorithm for Rs256<512> {
    fn prefix(&self) -> u64 {
        RSA_SIG_PREFIX
    }

    fn config_tags(&self) -> Vec<u64> {
        vec![SHA2_256_TAG, RSA_4096_KEY_SIZE_TAG]
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if bytes.get(0..=2)? == [RSA_SIG_PREFIX, SHA2_256_TAG, RSA_4096_KEY_SIZE_TAG] {
            Some((Rsa(PhantomData), bytes.get(3..)?))
        } else {
            None
        }
    }
}

#[cfg(all(test, feature = "rsa", feature = "sha2_256"))]
mod tests {
    use super::*;

    #[test]
    fn rsa2048_header_construction() {
        let algorithm = Rs256::<256>::default();
        assert_eq!(algorithm.prefix(), RSA_SIG_PREFIX);
        assert_eq!(
            algorithm.config_tags(),
            vec![SHA2_256_TAG, RSA_2048_KEY_SIZE_TAG]
        );
    }

    #[test]
    fn rsa4096_header_construction() {
        let algorithm = Rs256::<512>::default();
        assert_eq!(algorithm.prefix(), RSA_SIG_PREFIX);
        assert_eq!(
            algorithm.config_tags(),
            vec![SHA2_256_TAG, RSA_4096_KEY_SIZE_TAG]
        );
    }

    #[test]
    fn rsa_algorithm_reader_distinguishes_key_sizes() {
        // The full header is prefix followed by config tags. Each key size must
        // parse only its own header and reject the other's key-size tag.
        let header_2048 = [RSA_SIG_PREFIX, SHA2_256_TAG, RSA_2048_KEY_SIZE_TAG];
        let header_4096 = [RSA_SIG_PREFIX, SHA2_256_TAG, RSA_4096_KEY_SIZE_TAG];

        assert!(Rs256::<256>::try_from_tags(&header_2048).is_some());
        assert!(Rs256::<256>::try_from_tags(&header_4096).is_none());

        assert!(Rs256::<512>::try_from_tags(&header_4096).is_some());
        assert!(Rs256::<512>::try_from_tags(&header_2048).is_none());
    }

    #[test]
    fn rsa_algorithm_reader_returns_trailing_tags() {
        let mut header = vec![RSA_SIG_PREFIX, SHA2_256_TAG, RSA_2048_KEY_SIZE_TAG];
        header.push(0x99);
        let (_, rest) = Rs256::<256>::try_from_tags(&header).unwrap();
        assert_eq!(rest, &[0x99]);
    }

    #[test]
    fn rsa_signature_length_is_enforced() {
        assert!(RsaSignature::<256>::from_bytes(vec![0u8; 256]).is_ok());
        assert!(RsaSignature::<256>::from_bytes(vec![0u8; 255]).is_err());
        assert!(RsaSignature::<512>::from_bytes(vec![0u8; 512]).is_ok());
        assert!(RsaSignature::<512>::from_bytes(vec![0u8; 256]).is_err());
    }

    #[test]
    fn rsa_sign_and_verify_roundtrip() {
        use rsa::pkcs1::DecodeRsaPrivateKey;
        use rsa::pkcs1v15::{Signature as RsaCrateSignature, SigningKey, VerifyingKey};
        use rsa::signature::{Signer, Verifier};
        use rsa::{RsaPrivateKey, RsaPublicKey, sha2::Sha256};

        // A cached 2048-bit key: generating one per test is far too slow.
        let der = include_bytes!("../rsa/fixtures/test_2048.pkcs1.der");
        let private_key = RsaPrivateKey::from_pkcs1_der(der).unwrap();
        let public_key = RsaPublicKey::from(&private_key);

        let signing_key = SigningKey::<Sha256>::new(private_key);
        let verifying_key = VerifyingKey::<Sha256>::new(public_key);

        let msg = b"rsa varsig roundtrip";
        let raw: RsaCrateSignature = signing_key.sign(msg);
        let sig = RsaSignature::<256>::from(raw);
        assert_eq!(sig.to_bytes().len(), 256);

        let restored = RsaCrateSignature::try_from(sig.clone()).unwrap();
        verifying_key.verify(msg, &restored).unwrap();

        // A tampered message must not verify.
        let restored = RsaCrateSignature::try_from(sig).unwrap();
        assert!(verifying_key.verify(b"other", &restored).is_err());
    }
}
