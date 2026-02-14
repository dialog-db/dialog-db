//! Varsig header, signature trait, and signing/verification re-exports.

pub mod signer;
pub mod verifier;

use super::{Codec, Format, SignatureAlgorithm};
use ::signature::SignatureEncoding;
use dialog_common::{ConditionalSend, ConditionalSync};
use serde::{Deserialize, Serialize};
pub use signer::Signer;
use std::{fmt::Debug, marker::PhantomData};
pub use verifier::Verifier;

/// Cryptographic signature produced by `Signer` and verified by `Verifier`.
pub trait Signature: SignatureEncoding + Debug + ConditionalSend + ConditionalSync {
    /// The signature algorithm that produces this signature type.
    type Algorithm: SignatureAlgorithm + ConditionalSend + ConditionalSync;
}

/// Variable signature configuration that ties signature algorithm
/// to payload encoding, which can be used to sign / verify cryptographic
/// signatures conforming to [varsig] specification.
///
/// [varsig]:https://github.com/ChainAgnostic/varsig/blob/main/README.md
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct Varsig<A: SignatureAlgorithm, C, T> {
    algorithm: A,
    codec: C,
    _data: PhantomData<T>,
}

impl<V: SignatureAlgorithm, C, T> Varsig<V, C, T> {
    /// Create a new Varsig header.
    ///
    /// The signature algorithm is constructed via `Default`.
    ///
    /// ## Parameters
    ///
    /// - `codec`: The codec to use for encoding the payload.
    pub fn new(codec: C) -> Self {
        Varsig {
            algorithm: V::default(),
            codec,
            _data: PhantomData,
        }
    }

    /// Get the [signature algorithm] for this varsig configuration.
    ///
    /// [signature algorithm]: <https://github.com/ChainAgnostic/varsig/blob/main/README.md#signature-algorithm>
    pub const fn algorithm(&self) -> &V {
        &self.algorithm
    }

    /// Get the `Codec` used for [payload encoding] in this varsig configuration.
    ///
    /// [payload encoding]: <https://github.com/ChainAgnostic/varsig/blob/main/README.md#payload-encoding>
    pub const fn codec(&self) -> &C {
        &self.codec
    }

    /// Encode a payload using this header's codec.
    ///
    /// # Errors
    ///
    /// Returns the codec's encoding error if encoding fails.
    pub fn encode(&self, payload: &T) -> Result<Vec<u8>, C::EncodingError>
    where
        C: Codec<T>,
        T: Serialize,
    {
        let mut buffer = Vec::new();
        self.codec.encode_payload(payload, &mut buffer)?;
        Ok(buffer)
    }
}

impl<V: SignatureAlgorithm, C: Format, T> Serialize for Varsig<V, C, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut bytes = Vec::new();

        // Varsig tag
        leb128::write::unsigned(&mut bytes, 0x34).map_err(|e| {
            serde::ser::Error::custom(format!(
                "unable to varsig prefix tag write into new owned vec: {e}"
            ))
        })?;

        // Version tag
        leb128::write::unsigned(&mut bytes, 0x01).map_err(|e| {
            serde::ser::Error::custom(format!(
                "unable to write varsig version tag into owned vec with one element: {e}"
            ))
        })?;

        // Signature algorithm tag
        leb128::write::unsigned(&mut bytes, self.algorithm.prefix()).map_err(|e| {
            serde::ser::Error::custom(format!(
                "unable to write signature algorithm prefix tag: {e}"
            ))
        })?;

        for segment in &self.algorithm.config_tags() {
            leb128::write::unsigned(&mut bytes, *segment).map_err(|e| {
                serde::ser::Error::custom(format!(
                    "unable to write varsig config segment into owned vec {segment}: {e}",
                ))
            })?;
        }

        // Codec tag
        leb128::write::unsigned(&mut bytes, self.codec.multicodec_code()).map_err(|e| {
            serde::ser::Error::custom(format!(
                "unable to write varsig version tag into owned vec with one element: {e}"
            ))
        })?;

        serializer.serialize_bytes(&bytes)
    }
}

impl<'de, V: SignatureAlgorithm, C: Format, T> Deserialize<'de> for Varsig<V, C, T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: serde_bytes::ByteBuf =
            serde::Deserialize::deserialize(deserializer).map_err(|e| {
                serde::de::Error::custom(format!("unable to deserialize varsig header: {e}"))
            })?;

        let mut cursor = std::io::Cursor::new(bytes.as_slice());
        let len = bytes.len() as u64;

        let varsig_tag = leb128::read::unsigned(&mut cursor).map_err(|e| {
            serde::de::Error::custom(format!("unable to read leb128 unsigned: {e}"))
        })?;
        if varsig_tag != 0x34 {
            return Err(serde::de::Error::custom(format!(
                "expected varsig tag 0x34, found {varsig_tag:#x}"
            )));
        }

        let version_tag = leb128::read::unsigned(&mut cursor).map_err(|e| {
            serde::de::Error::custom(format!("unable to read leb128 unsigned: {e}"))
        })?;
        if version_tag != 0x01 {
            return Err(serde::de::Error::custom(format!(
                "expected varsig version tag 0x01, found {version_tag:#x}"
            )));
        }

        let mut remaining = Vec::new();
        while cursor.position() < len {
            let seg = leb128::read::unsigned(&mut cursor).map_err(|e| {
                serde::de::Error::custom(format!("unable to read leb128 unsigned segment: {e}"))
            })?;
            remaining.push(seg);
        }

        let (algorithm, more) = V::try_from_tags(remaining.as_slice()).ok_or_else(|| {
            serde::de::Error::custom("unable to create signature algorithm from tags")
        })?;
        let codec = C::try_from_tags(more)
            .ok_or_else(|| serde::de::Error::custom("unable to create codec from tags"))?;

        Ok(Varsig {
            algorithm,
            codec,
            _data: std::marker::PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm::eddsa::{Ed25519, Ed25519Signature};
    use std::io::{BufRead, Write};
    use testresult::TestResult;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use wasm_bindgen_test::wasm_bindgen_test;

    /// Minimal test codec that just uses serde_bytes-style identity encoding.
    /// Encodes `String` as raw UTF-8 bytes.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct TestCodec;

    impl Format for TestCodec {
        fn multicodec_code(&self) -> u64 {
            0x71 // same as DAG-CBOR for header serialization tests
        }

        fn try_from_tags(code: &[u64]) -> Option<Self> {
            if code.len() == 1 && code[0] == 0x71 {
                Some(TestCodec)
            } else {
                None
            }
        }
    }

    impl Codec<String> for TestCodec {
        type EncodingError = std::io::Error;
        type DecodingError = std::io::Error;

        fn encode_payload<W: Write>(
            &self,
            payload: &String,
            buffer: &mut W,
        ) -> Result<(), Self::EncodingError> {
            buffer.write_all(payload.as_bytes())
        }

        fn decode_payload<R: BufRead>(
            &self,
            reader: &mut R,
        ) -> Result<String, Self::DecodingError> {
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf)?;
            String::from_utf8(buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        }
    }

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestPayload {
        message: String,
        count: u8,
    }

    impl Codec<TestPayload> for TestCodec {
        type EncodingError = std::io::Error;
        type DecodingError = std::io::Error;

        fn encode_payload<W: Write>(
            &self,
            payload: &TestPayload,
            buffer: &mut W,
        ) -> Result<(), Self::EncodingError> {
            let json = format!("{}:{}", payload.message, payload.count);
            buffer.write_all(json.as_bytes())
        }

        fn decode_payload<R: BufRead>(
            &self,
            reader: &mut R,
        ) -> Result<TestPayload, Self::DecodingError> {
            let mut buf = String::new();
            reader.read_to_string(&mut buf)?;
            let parts: Vec<&str> = buf.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "bad format",
                ));
            }
            let count = parts[1]
                .parse()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            Ok(TestPayload {
                message: parts[0].to_string(),
                count,
            })
        }
    }

    #[test]
    fn test_ed25519_varsig_header_construction() {
        let fixture: Varsig<Ed25519, TestCodec, String> = Varsig::new(TestCodec);
        assert_eq!(fixture.algorithm(), &Ed25519::default());
        assert_eq!(fixture.codec(), &TestCodec);
    }

    #[test]
    fn test_algorithm_reader() {
        let varsig: Varsig<Ed25519, TestCodec, String> = Varsig::new(TestCodec);
        assert_eq!(varsig.algorithm(), &Ed25519::default());
    }

    #[test]
    fn test_codec_reader() {
        let varsig: Varsig<Ed25519, TestCodec, String> = Varsig::new(TestCodec);
        assert_eq!(varsig.codec(), &TestCodec);
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn test_sign_and_verify() -> TestResult {
        use super::{signer::Signer, verifier::Verifier};

        // Lightweight wrappers that impl Signer/Verifier for tests.
        struct TestSigner(ed25519_dalek::SigningKey);
        struct TestVerifier(ed25519_dalek::VerifyingKey);

        impl Verifier<Ed25519Signature> for TestVerifier {
            async fn verify(
                &self,
                msg: &[u8],
                signature: &Ed25519Signature,
            ) -> Result<(), signature::Error> {
                use signature::Verifier as _;
                let dalek_sig = ed25519_dalek::Signature::from(*signature);
                self.0.verify(msg, &dalek_sig)
            }
        }

        impl Signer<Ed25519Signature> for TestSigner {
            async fn sign(&self, msg: &[u8]) -> Result<Ed25519Signature, signature::Error> {
                use signature::Signer as _;
                let sig = self.0.try_sign(msg)?;
                Ok(Ed25519Signature::from(sig))
            }
        }

        let payload = TestPayload {
            message: "Hello, Varsig!".to_string(),
            count: 42,
        };

        let dalek_sk = ed25519_dalek::SigningKey::from_bytes(&[42u8; 32]);
        let sk = TestSigner(dalek_sk.clone());
        let vk = TestVerifier(dalek_sk.verifying_key());
        let varsig: Varsig<Ed25519, TestCodec, TestPayload> = Varsig::new(TestCodec);

        let encoded = varsig.encode(&payload)?;
        let sig = sk.sign(&encoded).await?;
        vk.verify(&encoded, &sig).await?;

        Ok(())
    }

    #[cfg(feature = "rs256_2048")]
    #[test]
    fn test_rs256_2048_varsig_header_construction() {
        use crate::algorithm::rsa::Rs256;

        let fixture: Varsig<Rs256<256>, TestCodec, String> = Varsig::new(TestCodec);
        assert_eq!(fixture.algorithm(), &Rs256::<256>::default());
        assert_eq!(fixture.codec(), &TestCodec);
    }

    #[cfg(feature = "rs256_4096")]
    #[test]
    fn test_rs256_4096_varsig_header_construction() {
        use crate::algorithm::rsa::Rs256;

        let fixture: Varsig<Rs256<512>, TestCodec, String> = Varsig::new(TestCodec);
        assert_eq!(fixture.algorithm(), &Rs256::<512>::default());
        assert_eq!(fixture.codec(), &TestCodec);
    }

    #[cfg(feature = "rs256_2048")]
    #[test]
    fn test_rs256_2048_algorithm_reader() {
        use crate::algorithm::rsa::Rs256;

        let varsig: Varsig<Rs256<256>, TestCodec, String> = Varsig::new(TestCodec);
        assert_eq!(varsig.algorithm(), &Rs256::<256>::default());
    }

    #[cfg(feature = "rs256_4096")]
    #[test]
    fn test_rs256_4096_algorithm_reader() {
        use crate::algorithm::rsa::Rs256;

        let varsig: Varsig<Rs256<512>, TestCodec, String> = Varsig::new(TestCodec);
        assert_eq!(varsig.algorithm(), &Rs256::<512>::default());
    }

    #[cfg(feature = "rs256_2048")]
    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn test_rsa_2048_sign_and_verify() -> TestResult {
        use super::{signer::Signer, verifier::Verifier};
        use crate::algorithm::rsa::{Rs256, RsaSignature};
        use rand::SeedableRng;

        struct TestRsaSigner(rsa::pkcs1v15::SigningKey<sha2::Sha256>);
        struct TestRsaVerifier(rsa::pkcs1v15::VerifyingKey<sha2::Sha256>);

        impl Signer<RsaSignature<256>> for TestRsaSigner {
            async fn sign(
                &self,
                msg: &[u8],
            ) -> Result<RsaSignature<256>, signature::Error> {
                use signature::Signer as _;
                let sig = self.0.try_sign(msg)?;
                Ok(RsaSignature::from(sig))
            }
        }

        impl Verifier<RsaSignature<256>> for TestRsaVerifier {
            async fn verify(
                &self,
                msg: &[u8],
                sig: &RsaSignature<256>,
            ) -> Result<(), signature::Error> {
                use signature::Verifier as _;
                let rsa_sig = rsa::pkcs1v15::Signature::try_from(sig.to_bytes())?;
                self.0.verify(msg, &rsa_sig)
            }
        }

        let mut rng = rand_chacha::ChaCha20Rng::seed_from_u64(42);
        let private_key = rsa::RsaPrivateKey::new(&mut rng, 2048)?;
        let public_key = rsa::RsaPublicKey::from(&private_key);

        let sk = TestRsaSigner(rsa::pkcs1v15::SigningKey::<sha2::Sha256>::new_unprefixed(private_key));
        let vk = TestRsaVerifier(rsa::pkcs1v15::VerifyingKey::<sha2::Sha256>::new_unprefixed(public_key));

        let payload = TestPayload {
            message: "Hello, RSA Varsig!".to_string(),
            count: 42,
        };

        let varsig: Varsig<Rs256<256>, TestCodec, TestPayload> = Varsig::new(TestCodec);
        let encoded = varsig.encode(&payload)?;
        let sig = sk.sign(&encoded).await?;
        vk.verify(&encoded, &sig).await?;

        Ok(())
    }

    #[cfg(feature = "rs256_4096")]
    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn test_rsa_4096_sign_and_verify() -> TestResult {
        use super::{signer::Signer, verifier::Verifier};
        use crate::algorithm::rsa::{Rs256, RsaSignature};
        use rand::SeedableRng;

        struct TestRsaSigner(rsa::pkcs1v15::SigningKey<sha2::Sha256>);
        struct TestRsaVerifier(rsa::pkcs1v15::VerifyingKey<sha2::Sha256>);

        impl Signer<RsaSignature<512>> for TestRsaSigner {
            async fn sign(
                &self,
                msg: &[u8],
            ) -> Result<RsaSignature<512>, signature::Error> {
                use signature::Signer as _;
                let sig = self.0.try_sign(msg)?;
                Ok(RsaSignature::from(sig))
            }
        }

        impl Verifier<RsaSignature<512>> for TestRsaVerifier {
            async fn verify(
                &self,
                msg: &[u8],
                sig: &RsaSignature<512>,
            ) -> Result<(), signature::Error> {
                use signature::Verifier as _;
                let rsa_sig = rsa::pkcs1v15::Signature::try_from(sig.to_bytes())?;
                self.0.verify(msg, &rsa_sig)
            }
        }

        let mut rng = rand_chacha::ChaCha20Rng::seed_from_u64(99);
        let private_key = rsa::RsaPrivateKey::new(&mut rng, 4096)?;
        let public_key = rsa::RsaPublicKey::from(&private_key);

        let sk = TestRsaSigner(rsa::pkcs1v15::SigningKey::<sha2::Sha256>::new_unprefixed(private_key));
        let vk = TestRsaVerifier(rsa::pkcs1v15::VerifyingKey::<sha2::Sha256>::new_unprefixed(public_key));

        let payload = TestPayload {
            message: "Hello, RSA-4096 Varsig!".to_string(),
            count: 99,
        };

        let varsig: Varsig<Rs256<512>, TestCodec, TestPayload> = Varsig::new(TestCodec);
        let encoded = varsig.encode(&payload)?;
        let sig = sk.sign(&encoded).await?;
        vk.verify(&encoded, &sig).await?;

        Ok(())
    }
}
