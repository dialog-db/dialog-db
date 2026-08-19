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

#[cfg(feature = "webauthn")]
use crate::algorithm::webauthn::{WebAuthnP256, WebAuthnSignature};

#[cfg(feature = "rsa")]
use crate::algorithm::rsa::{Rs256, RsaSignature};

/// The algorithm tag carried by [`AnySignature`] and [`AnyAlgorithm`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlgorithmTag {
    /// Ed25519 (`EdDSA` over Edwards25519).
    Ed25519,
    /// ES256 (ECDSA over P-256).
    #[cfg(feature = "es256")]
    Es256,
    /// WebAuthn (P-256 assertion carrying authenticator context).
    #[cfg(feature = "webauthn")]
    WebAuthn,
    /// RSA-2048 PKCS#1 v1.5 with SHA-256 (256-byte signatures).
    #[cfg(feature = "rsa")]
    Rsa2048,
    /// RSA-4096 PKCS#1 v1.5 with SHA-256 (512-byte signatures).
    #[cfg(feature = "rsa")]
    Rsa4096,
}

impl AlgorithmTag {
    /// Whether a signature body of `len` bytes is valid for this algorithm.
    ///
    /// Ed25519 and ES256 are fixed-width at 64 bytes. WebAuthn is variable
    /// length: its body is a varint-length-prefixed `clientDataJSON` and
    /// `authenticatorData` followed by a DER ECDSA signature, so the only
    /// structural constraint here is non-emptiness (the full parse happens when
    /// the concrete `WebAuthnSignature` is reconstructed).
    #[must_use]
    fn accepts_len(self, len: usize) -> bool {
        match self {
            AlgorithmTag::Ed25519 => len == 64,
            #[cfg(feature = "es256")]
            AlgorithmTag::Es256 => len == 64,
            #[cfg(feature = "webauthn")]
            AlgorithmTag::WebAuthn => len > 0,
            // RSA signatures equal the modulus size: 256 bytes for RSA-2048 and
            // 512 bytes for RSA-4096. The key size tag in the varsig header,
            // not the body, is what distinguishes the two algorithms.
            #[cfg(feature = "rsa")]
            AlgorithmTag::Rsa2048 => len == 256,
            #[cfg(feature = "rsa")]
            AlgorithmTag::Rsa4096 => len == 512,
        }
    }
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
            #[cfg(feature = "webauthn")]
            AlgorithmTag::WebAuthn => WebAuthnP256::default().prefix(),
            #[cfg(feature = "rsa")]
            AlgorithmTag::Rsa2048 => Rs256::<256>::default().prefix(),
            #[cfg(feature = "rsa")]
            AlgorithmTag::Rsa4096 => Rs256::<512>::default().prefix(),
        }
    }

    fn config_tags(&self) -> Vec<u64> {
        match self.0 {
            AlgorithmTag::Ed25519 => Ed25519::default().config_tags(),
            #[cfg(feature = "es256")]
            AlgorithmTag::Es256 => Es256::default().config_tags(),
            #[cfg(feature = "webauthn")]
            AlgorithmTag::WebAuthn => WebAuthnP256::default().config_tags(),
            #[cfg(feature = "rsa")]
            AlgorithmTag::Rsa2048 => Rs256::<256>::default().config_tags(),
            #[cfg(feature = "rsa")]
            AlgorithmTag::Rsa4096 => Rs256::<512>::default().config_tags(),
        }
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        // WebAuthn MUST be tried before Es256: a WebAuthn-over-Es256 header is
        // the full Es256 header followed by the 0x300001 marker, so Es256 alone
        // would match its prefix and swallow the inner tags. Trying WebAuthn
        // first consumes the marker and only falls through to bare Es256 when the
        // marker is absent.
        #[cfg(feature = "webauthn")]
        if let Some((_, rest)) = WebAuthnP256::try_from_tags(bytes) {
            return Some((AnyAlgorithm(AlgorithmTag::WebAuthn), rest));
        }
        if let Some((_, rest)) = Ed25519::try_from_tags(bytes) {
            return Some((AnyAlgorithm(AlgorithmTag::Ed25519), rest));
        }
        #[cfg(feature = "es256")]
        if let Some((_, rest)) = Es256::try_from_tags(bytes) {
            return Some((AnyAlgorithm(AlgorithmTag::Es256), rest));
        }
        // The two RSA headers share a prefix and hash tag and differ only in the
        // trailing key-size tag, so each `try_from_tags` matches only its own
        // size and there is no ordering hazard between them.
        #[cfg(feature = "rsa")]
        if let Some((_, rest)) = Rs256::<256>::try_from_tags(bytes) {
            return Some((AnyAlgorithm(AlgorithmTag::Rsa2048), rest));
        }
        #[cfg(feature = "rsa")]
        if let Some((_, rest)) = Rs256::<512>::try_from_tags(bytes) {
            return Some((AnyAlgorithm(AlgorithmTag::Rsa4096), rest));
        }
        None
    }
}

/// Algorithm-agnostic signature: an algorithm tag plus the raw signature bytes.
///
/// The body is stored variable-length (`Box<[u8]>`): the varsig header names the
/// algorithm, and the algorithm determines the width. The two algorithms defined
/// today are 64-byte fixed-width, but the type imposes no such limit, leaving
/// room for algorithms with wider or variable signatures (RSA, `WebAuthn`). The
/// tag lets a verifier reject a signature produced by a different algorithm than
/// it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnySignature {
    algorithm: AlgorithmTag,
    bytes: Box<[u8]>,
}

impl AnySignature {
    /// The algorithm that produced this signature.
    #[must_use]
    pub const fn algorithm(&self) -> AlgorithmTag {
        self.algorithm
    }

    /// The raw signature body.
    #[must_use]
    pub fn to_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Reconstruct a signature from the algorithm named by a varsig header and
    /// the raw signature body.
    ///
    /// The header is the single source of truth for the algorithm; the body
    /// alone cannot distinguish algorithms that share a signature width. The
    /// body length is validated against the named algorithm and this refuses
    /// (rather than defaulting) when the length does not match.
    ///
    /// # Errors
    ///
    /// Returns an error if `bytes` is not a valid signature length for
    /// `algorithm`.
    pub fn from_algorithm_and_bytes(
        algorithm: &AnyAlgorithm,
        bytes: &[u8],
    ) -> Result<Self, ::signature::Error> {
        if !algorithm.0.accepts_len(bytes.len()) {
            return Err(::signature::Error::new());
        }
        Ok(Self {
            algorithm: algorithm.0,
            bytes: Box::from(bytes),
        })
    }
}

impl From<Ed25519Signature> for AnySignature {
    fn from(sig: Ed25519Signature) -> Self {
        Self {
            algorithm: AlgorithmTag::Ed25519,
            bytes: Box::from(sig.to_bytes().as_slice()),
        }
    }
}

#[cfg(feature = "es256")]
impl From<Es256Signature> for AnySignature {
    fn from(sig: Es256Signature) -> Self {
        Self {
            algorithm: AlgorithmTag::Es256,
            bytes: Box::from(sig.to_bytes().as_slice()),
        }
    }
}

#[cfg(feature = "webauthn")]
impl From<WebAuthnSignature> for AnySignature {
    fn from(sig: WebAuthnSignature) -> Self {
        Self {
            algorithm: AlgorithmTag::WebAuthn,
            bytes: sig.to_vec().into_boxed_slice(),
        }
    }
}

#[cfg(feature = "rsa")]
impl From<RsaSignature<256>> for AnySignature {
    fn from(sig: RsaSignature<256>) -> Self {
        Self {
            algorithm: AlgorithmTag::Rsa2048,
            bytes: sig.0.into_boxed_slice(),
        }
    }
}

#[cfg(feature = "rsa")]
impl From<RsaSignature<512>> for AnySignature {
    fn from(sig: RsaSignature<512>) -> Self {
        Self {
            algorithm: AlgorithmTag::Rsa4096,
            bytes: sig.0.into_boxed_slice(),
        }
    }
}

/// [`AnySignature`] encodes as its raw signature body. The algorithm tag is
/// carried out of band by the varsig header, which already names the algorithm
/// separately from the signature body.
impl SignatureEncoding for AnySignature {
    type Repr = Box<[u8]>;
}

impl From<AnySignature> for Box<[u8]> {
    fn from(sig: AnySignature) -> Self {
        sig.bytes
    }
}

impl TryFrom<&[u8]> for AnySignature {
    type Error = ::signature::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        // Bytes alone cannot name the algorithm. This byte-only path exists only
        // to satisfy `SignatureEncoding`; decode goes through
        // `from_algorithm_bytes`, which takes the tag from the varsig header.
        Ok(Self {
            algorithm: AlgorithmTag::Ed25519,
            bytes: Box::from(bytes),
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
        Ok(Self {
            algorithm: AlgorithmTag::Ed25519,
            bytes: bytes.into_vec().into_boxed_slice(),
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
        // A 32-byte body is not a valid ed25519 signature length; per-algorithm
        // validation refuses it rather than defaulting.
        let short = [0u8; 32];
        assert!(
            <AnySignature as Signature>::from_algorithm_bytes(
                &AnyAlgorithm(AlgorithmTag::Ed25519),
                &short,
            )
            .is_err()
        );

        #[cfg(feature = "es256")]
        assert!(
            <AnySignature as Signature>::from_algorithm_bytes(
                &AnyAlgorithm(AlgorithmTag::Es256),
                &short,
            )
            .is_err()
        );
    }

    #[test]
    fn ed25519_and_es256_roundtrip_as_64_byte_bodies() {
        let bytes = [3u8; 64];

        let ed = <AnySignature as Signature>::from_algorithm_bytes(
            &AnyAlgorithm(AlgorithmTag::Ed25519),
            &bytes,
        )
        .unwrap();
        assert_eq!(ed.to_bytes(), &bytes[..]);
        assert_eq!(ed.to_bytes().len(), 64);

        #[cfg(feature = "es256")]
        {
            let es = <AnySignature as Signature>::from_algorithm_bytes(
                &AnyAlgorithm(AlgorithmTag::Es256),
                &bytes,
            )
            .unwrap();
            assert_eq!(es.to_bytes(), &bytes[..]);
            assert_eq!(es.to_bytes().len(), 64);
        }
    }

    #[test]
    fn agnostic_ed25519_wire_bytes_match_concrete() {
        // The agnostic signature must serialize byte-identically to a concrete
        // Ed25519Signature carrying the same body. This is the on-wire proof
        // that widening the in-memory body to Box<[u8]> did not change the
        // encoding for the fixed-width algorithms.
        let body = [9u8; 64];

        let concrete = Ed25519Signature::from_bytes(body);
        let agnostic = AnySignature::from(concrete);

        let concrete_wire = serde_ipld_dagcbor::to_vec(&concrete).unwrap();
        let agnostic_wire = serde_ipld_dagcbor::to_vec(&agnostic).unwrap();

        assert_eq!(concrete_wire, agnostic_wire);
    }

    #[cfg(feature = "webauthn")]
    #[test]
    fn webauthn_header_distinguishes_from_es256() {
        use crate::SignatureAlgorithm;

        // The WebAuthn header ends in the 0x300001 marker; Es256 ends in 0x15.
        // Feeding each header through the agnostic try_from_tags must land on
        // the matching tag and never confuse the two.
        let webauthn = AnyAlgorithm(AlgorithmTag::WebAuthn);
        let mut wa_header = vec![webauthn.prefix()];
        wa_header.extend(webauthn.config_tags());
        let (parsed, rest) = AnyAlgorithm::try_from_tags(&wa_header).unwrap();
        assert_eq!(parsed.0, AlgorithmTag::WebAuthn);
        assert!(rest.is_empty());

        #[cfg(feature = "es256")]
        {
            let es = AnyAlgorithm(AlgorithmTag::Es256);
            let mut es_header = vec![es.prefix()];
            es_header.extend(es.config_tags());
            let (parsed_es, _) = AnyAlgorithm::try_from_tags(&es_header).unwrap();
            assert_eq!(parsed_es.0, AlgorithmTag::Es256);
            // The two headers are not equal, so neither can parse as the other.
            assert_ne!(wa_header, es_header);
        }
    }

    #[cfg(feature = "webauthn")]
    #[test]
    fn webauthn_variable_body_roundtrips_through_any() {
        use crate::algorithm::webauthn::WebAuthnSignature;

        let sig = WebAuthnSignature::new(
            br#"{"type":"webauthn.get","challenge":"abc"}"#.to_vec(),
            vec![0xAA; 37],
            vec![0x30, 0x44, 0x02, 0x20], // DER-ish stub
        );
        let expected = sig.to_vec();

        let any = AnySignature::from(sig);
        assert_eq!(any.algorithm(), AlgorithmTag::WebAuthn);
        // A WebAuthn body is variable length; the agnostic signature stores it
        // verbatim and a verifier can reconstruct the concrete signature from it.
        assert_eq!(any.to_bytes(), expected.as_slice());
        let restored = WebAuthnSignature::from_bytes(any.to_bytes()).unwrap();
        assert_eq!(restored.authenticator_data, vec![0xAA; 37]);
    }

    #[cfg(feature = "rsa")]
    #[test]
    fn rsa_header_distinguishes_2048_from_4096() {
        use crate::SignatureAlgorithm;

        let rsa2048 = AnyAlgorithm(AlgorithmTag::Rsa2048);
        let rsa4096 = AnyAlgorithm(AlgorithmTag::Rsa4096);

        let mut header_2048 = vec![rsa2048.prefix()];
        header_2048.extend(rsa2048.config_tags());
        let mut header_4096 = vec![rsa4096.prefix()];
        header_4096.extend(rsa4096.config_tags());

        // The two headers share prefix + hash tag but differ in the key-size
        // tag, so each parses back to its own tag and never the other.
        assert_ne!(header_2048, header_4096);
        let (parsed_2048, rest) = AnyAlgorithm::try_from_tags(&header_2048).unwrap();
        assert_eq!(parsed_2048.0, AlgorithmTag::Rsa2048);
        assert!(rest.is_empty());
        let (parsed_4096, rest) = AnyAlgorithm::try_from_tags(&header_4096).unwrap();
        assert_eq!(parsed_4096.0, AlgorithmTag::Rsa4096);
        assert!(rest.is_empty());
    }

    #[cfg(feature = "rsa")]
    #[test]
    fn rsa_from_algorithm_bytes_enforces_width() {
        use crate::algorithm::rsa::RsaSignature;

        // A 2048 header accepts only a 256-byte body; a 4096 header only 512.
        let ok_2048 = <AnySignature as Signature>::from_algorithm_bytes(
            &AnyAlgorithm(AlgorithmTag::Rsa2048),
            &[0u8; 256],
        )
        .unwrap();
        assert_eq!(ok_2048.algorithm(), AlgorithmTag::Rsa2048);
        assert!(
            <AnySignature as Signature>::from_algorithm_bytes(
                &AnyAlgorithm(AlgorithmTag::Rsa2048),
                &[0u8; 512],
            )
            .is_err()
        );
        let ok_4096 = <AnySignature as Signature>::from_algorithm_bytes(
            &AnyAlgorithm(AlgorithmTag::Rsa4096),
            &[0u8; 512],
        )
        .unwrap();
        assert_eq!(ok_4096.algorithm(), AlgorithmTag::Rsa4096);

        // The concrete signature body roundtrips through the agnostic wrapper.
        let sig = RsaSignature::<256>::from_bytes(vec![7u8; 256]).unwrap();
        let any = AnySignature::from(sig.clone());
        assert_eq!(any.algorithm(), AlgorithmTag::Rsa2048);
        assert_eq!(any.to_bytes(), sig.to_bytes());
    }
}
