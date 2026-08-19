//! RSA signer implementation.

use super::{RsaKeySize, RsaSigningKey, error::RsaSignerError, verifier::RsaVerifier};
use dialog_capability::Issuer;
use dialog_varsig::{AnyAlgorithm, AnySignature, Did, Principal, Signer};
use serde::Serialize;

/// An RSA (PKCS#1 v1.5, SHA-256) `did:key` signer.
///
/// Wraps a native `rsa` crate signing key. The signature it produces is an
/// [`AnySignature`] tagged with the key's size (`Rsa2048` or `Rsa4096`), so the
/// key size travels with the signature through the agnostic layer rather than
/// being fixed at the type level.
#[derive(Debug, Clone)]
pub struct RsaSigner {
    did: RsaVerifier,
    signer: RsaSigningKey,
}

impl From<RsaSigningKey> for RsaSigner {
    fn from(signer: RsaSigningKey) -> Self {
        let did = RsaVerifier::from(signer.verifying_key());
        Self { did, signer }
    }
}

impl RsaSigner {
    /// Import a keypair from PKCS#1 DER private-key bytes.
    ///
    /// RSA key generation is expensive, so callers supply an existing key rather
    /// than generating one; there is deliberately no `generate` here.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a valid RSA private key of a
    /// supported size (2048 or 4096 bits).
    pub fn from_pkcs1_der(bytes: &[u8]) -> Result<Self, RsaSignerError> {
        Ok(RsaSigningKey::from_pkcs1_der(bytes)?.into())
    }

    /// The key size (2048 or 4096).
    #[must_use]
    pub const fn key_size(&self) -> RsaKeySize {
        self.signer.size()
    }

    /// Get the associated RSA DID (verifier).
    #[must_use]
    pub const fn rsa_did(&self) -> &RsaVerifier {
        &self.did
    }

    /// Get the inner signing key.
    #[must_use]
    pub const fn signing_key(&self) -> &RsaSigningKey {
        &self.signer
    }

    /// The PKCS#1 DER encoding of the private key.
    #[must_use]
    pub fn to_pkcs1_der(&self) -> Vec<u8> {
        self.signer.to_pkcs1_der()
    }
}

impl std::fmt::Display for RsaSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.did)
    }
}

impl Signer<AnySignature> for RsaSigner {
    async fn sign(&self, msg: &[u8]) -> Result<AnySignature, signature::Error> {
        let body = self.signer.sign_bytes(msg);
        let algorithm = AnyAlgorithm(self.key_size().algorithm_tag());
        AnySignature::from_algorithm_and_bytes(&algorithm, &body)
    }
}

impl Principal for RsaSigner {
    fn did(&self) -> Did {
        self.did.did()
    }
}

impl Issuer for RsaSigner {
    type Signature = AnySignature;
}

impl Serialize for RsaSigner {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.did.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_varsig::Verifier as _;

    /// A cached 2048-bit RSA key. Generating one per test is far too slow.
    fn test_signer() -> RsaSigner {
        let der = include_bytes!("fixtures/test_2048.pkcs1.der");
        RsaSigner::from_pkcs1_der(der).unwrap()
    }

    #[dialog_common::test]
    async fn rsa_sign_verify_roundtrip() {
        let signer = test_signer();
        let verifier = signer.rsa_did().clone();
        let msg = b"hello rsa";

        let sig = Signer::sign(&signer, msg).await.unwrap();
        assert_eq!(sig.algorithm(), RsaKeySize::Rsa2048.algorithm_tag());
        assert_eq!(sig.to_bytes().len(), 256);
        verifier.verify(msg, &sig).await.unwrap();

        // A tampered message must not verify.
        assert!(verifier.verify(b"other", &sig).await.is_err());
    }

    #[dialog_common::test]
    async fn rsa_did_is_stable() {
        let signer = test_signer();
        let did1 = Principal::did(&signer);
        let did2 = signer.rsa_did().did();
        assert_eq!(did1, did2);
    }

    #[dialog_common::test]
    async fn rsa_did_key_roundtrips_to_verifier() {
        let signer = test_signer();
        let did = Principal::did(&signer);
        let parsed: RsaVerifier = did.as_str().parse().unwrap();
        assert_eq!(&parsed, signer.rsa_did());

        // The parsed verifier accepts the signer's signatures.
        let msg = b"resolve then verify";
        let sig = Signer::sign(&signer, msg).await.unwrap();
        parsed.verify(msg, &sig).await.unwrap();
    }
}
