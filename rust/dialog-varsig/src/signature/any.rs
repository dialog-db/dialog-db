//! Algorithm-agnostic signature.
//!
//! The [`Signature`](super::Signature) and
//! [`Verifier`](super::Verifier) traits are each generic over a single concrete
//! signature type. To carry a signature without committing to an algorithm at
//! the type level, [`AnySignature`] stores an algorithm tag alongside the raw
//! signature body, and [`AnyAlgorithm`] is the matching
//! [`SignatureAlgorithm`](crate::SignatureAlgorithm).
//!
//! The algorithm tag is authoritative: a varsig header already names the
//! algorithm separately from the signature body, and
//! [`AnySignature::from_algorithm_bytes`](super::Signature::from_algorithm_bytes)
//! recovers the tag from that header rather than guessing from the bytes, which
//! for two algorithms that share a signature width is impossible.
//!
//! The signer and verifier that pair a private or public key with these types
//! live one layer up, in `dialog-credentials`; this module owns only the
//! signature value and its algorithm descriptor.

use super::Signature;
use crate::SignatureAlgorithm;
use crate::algorithm::eddsa::{Ed25519, Ed25519Signature};
use ::signature::SignatureEncoding;

#[cfg(feature = "es256")]
use crate::algorithm::ecdsa::{Es256, Es256Signature};

/// The algorithm tag carried by [`AnySignature`] and [`AnyAlgorithm`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlgorithmTag {
    /// Ed25519 (`EdDSA` over Edwards25519).
    Ed25519,
    /// ES256 (ECDSA over P-256).
    #[cfg(feature = "es256")]
    Es256,
}

/// Algorithm-agnostic [`SignatureAlgorithm`].
///
/// Wraps the concrete varsig algorithm descriptors. `Default` resolves to
/// Ed25519 to satisfy the trait bound; the meaningful tag always travels with an
/// [`AnySignature`] value, so the default is never used to interpret bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AnyAlgorithm(pub AlgorithmTag);

impl Default for AnyAlgorithm {
    fn default() -> Self {
        AnyAlgorithm(AlgorithmTag::Ed25519)
    }
}

impl SignatureAlgorithm for AnyAlgorithm {
    fn prefix(&self) -> u64 {
        match self.0 {
            AlgorithmTag::Ed25519 => Ed25519::default().prefix(),
            #[cfg(feature = "es256")]
            AlgorithmTag::Es256 => Es256::default().prefix(),
        }
    }

    fn config_tags(&self) -> Vec<u64> {
        match self.0 {
            AlgorithmTag::Ed25519 => Ed25519::default().config_tags(),
            #[cfg(feature = "es256")]
            AlgorithmTag::Es256 => Es256::default().config_tags(),
        }
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if let Some((_, rest)) = Ed25519::try_from_tags(bytes) {
            return Some((AnyAlgorithm(AlgorithmTag::Ed25519), rest));
        }
        #[cfg(feature = "es256")]
        if let Some((_, rest)) = Es256::try_from_tags(bytes) {
            return Some((AnyAlgorithm(AlgorithmTag::Es256), rest));
        }
        None
    }
}

/// Algorithm-agnostic signature: an algorithm tag plus the raw signature bytes.
///
/// Both supported algorithms use a 64-byte fixed-width signature, so the bytes
/// are stored inline. The tag lets a verifier reject a signature produced by a
/// different algorithm than it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnySignature {
    algorithm: AlgorithmTag,
    bytes: [u8; 64],
}

impl AnySignature {
    /// The algorithm that produced this signature.
    #[must_use]
    pub const fn algorithm(&self) -> AlgorithmTag {
        self.algorithm
    }

    /// The raw 64-byte signature.
    #[must_use]
    pub const fn to_bytes(&self) -> [u8; 64] {
        self.bytes
    }

    /// Reconstruct a signature from the algorithm named by a varsig header and
    /// the raw signature body.
    ///
    /// The header is the single source of truth for the algorithm; the 64-byte
    /// body alone cannot distinguish algorithms that share a signature width.
    /// This refuses (rather than defaulting) when the body is not 64 bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if `bytes` is not exactly 64 bytes.
    pub fn from_algorithm_and_bytes(
        algorithm: &AnyAlgorithm,
        bytes: &[u8],
    ) -> Result<Self, ::signature::Error> {
        let bytes: [u8; 64] = bytes.try_into().map_err(|_| ::signature::Error::new())?;
        Ok(Self {
            algorithm: algorithm.0,
            bytes,
        })
    }
}

impl From<Ed25519Signature> for AnySignature {
    fn from(sig: Ed25519Signature) -> Self {
        Self {
            algorithm: AlgorithmTag::Ed25519,
            bytes: sig.to_bytes(),
        }
    }
}

#[cfg(feature = "es256")]
impl From<Es256Signature> for AnySignature {
    fn from(sig: Es256Signature) -> Self {
        Self {
            algorithm: AlgorithmTag::Es256,
            bytes: sig.to_bytes(),
        }
    }
}

/// [`AnySignature`] encodes as its raw 64-byte body. The algorithm tag is
/// carried out of band by the varsig header, which already names the algorithm
/// separately from the signature body.
impl SignatureEncoding for AnySignature {
    type Repr = [u8; 64];
}

impl From<AnySignature> for [u8; 64] {
    fn from(sig: AnySignature) -> Self {
        sig.bytes
    }
}

impl TryFrom<&[u8]> for AnySignature {
    type Error = ::signature::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; 64] = bytes.try_into().map_err(|_| ::signature::Error::new())?;
        // Bytes alone cannot name the algorithm. This byte-only path exists only
        // to satisfy `SignatureEncoding`; decode goes through
        // `from_algorithm_bytes`, which takes the tag from the varsig header.
        Ok(Self {
            algorithm: AlgorithmTag::Ed25519,
            bytes,
        })
    }
}

/// Serializes as the raw 64-byte body, exactly like a concrete signature. The
/// algorithm is not written here; a varsig envelope carries it in the header,
/// and decode recovers it via [`AnySignature::from_algorithm_bytes`].
impl serde::Serialize for AnySignature {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.bytes)
    }
}

impl<'de> serde::Deserialize<'de> for AnySignature {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Only the body is on the wire. This exists to satisfy the `Signature +
        // Deserialize` bound; a varsig envelope never decodes the signature this
        // way, it goes through `from_algorithm_bytes` with the header algorithm.
        let bytes = serde_bytes::ByteBuf::deserialize(deserializer)?;
        let bytes: [u8; 64] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| serde::de::Error::custom("expected a 64-byte signature"))?;
        Ok(Self {
            algorithm: AlgorithmTag::Ed25519,
            bytes,
        })
    }
}

impl Signature for AnySignature {
    type Algorithm = AnyAlgorithm;

    fn from_algorithm_bytes(
        algorithm: &Self::Algorithm,
        bytes: &[u8],
    ) -> Result<Self, ::signature::Error> {
        Self::from_algorithm_and_bytes(algorithm, bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_algorithm_bytes_takes_tag_from_header_not_bytes() {
        // The 64-byte body cannot name its algorithm. Reconstruction must take
        // the tag from the (header-provided) algorithm, never guess from bytes.
        let bytes = [7u8; 64];

        let ed = <AnySignature as Signature>::from_algorithm_bytes(
            &AnyAlgorithm(AlgorithmTag::Ed25519),
            &bytes,
        )
        .unwrap();
        assert_eq!(ed.algorithm(), AlgorithmTag::Ed25519);

        #[cfg(feature = "es256")]
        {
            let es = <AnySignature as Signature>::from_algorithm_bytes(
                &AnyAlgorithm(AlgorithmTag::Es256),
                &bytes,
            )
            .unwrap();
            assert_eq!(es.algorithm(), AlgorithmTag::Es256);
            // Same bytes, different header algorithm, different tag: the header
            // is authoritative.
            assert_ne!(ed.algorithm(), es.algorithm());
        }
    }

    #[test]
    fn from_algorithm_bytes_refuses_wrong_length() {
        let short = [0u8; 32];
        assert!(
            <AnySignature as Signature>::from_algorithm_bytes(
                &AnyAlgorithm(AlgorithmTag::Ed25519),
                &short,
            )
            .is_err()
        );
    }
}
