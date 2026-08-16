//! Algorithm-agnostic signer, verifier, and signature types.
//!
//! The varsig [`Signer`](dialog_varsig::Signer) and
//! [`Verifier`](dialog_varsig::Verifier) traits are each generic over a single
//! concrete signature type. To hold an identity without committing to an
//! algorithm, we wrap the per-algorithm types in enums and implement the varsig
//! traits over a single [`AnySignature`] enum that carries an algorithm tag
//! alongside the raw signature bytes.
//!
//! The concrete arms stay authoritative: `AnyVerifier::Ed25519` verifies only
//! ed25519 signatures, `AnyVerifier::Es256` only P-256, and a signature whose
//! tag does not match the verifier's arm is rejected. This keeps the
//! ed25519 path byte-identical while making room for P-256 and future
//! algorithms.

use dialog_capability::Issuer;
use dialog_varsig::{
    Did, Principal, SignatureAlgorithm, Signer, Verifier, ecdsa::Es256, ecdsa::Es256Signature,
    eddsa::Ed25519, eddsa::Ed25519Signature, signature::Signature,
};
use signature::SignatureEncoding;

use crate::{Ed25519Signer, Ed25519Verifier, Es256Signer, Es256Verifier};

/// The algorithm tag carried by [`AnySignature`] and [`AnyAlgorithm`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Algorithm {
    /// Ed25519 (`EdDSA` over Edwards25519).
    Ed25519,
    /// ES256 (ECDSA over P-256).
    Es256,
}

/// Algorithm-agnostic [`SignatureAlgorithm`].
///
/// Wraps the concrete varsig algorithm descriptors. `Default` resolves to
/// Ed25519 to satisfy the trait bound; the meaningful tag always travels with
/// an [`AnySignature`] value, so the default is never used to interpret bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AnyAlgorithm(pub Algorithm);

impl Default for AnyAlgorithm {
    fn default() -> Self {
        AnyAlgorithm(Algorithm::Ed25519)
    }
}

impl SignatureAlgorithm for AnyAlgorithm {
    fn prefix(&self) -> u64 {
        match self.0 {
            Algorithm::Ed25519 => Ed25519::default().prefix(),
            Algorithm::Es256 => Es256::default().prefix(),
        }
    }

    fn config_tags(&self) -> Vec<u64> {
        match self.0 {
            Algorithm::Ed25519 => Ed25519::default().config_tags(),
            Algorithm::Es256 => Es256::default().config_tags(),
        }
    }

    fn try_from_tags(bytes: &[u64]) -> Option<(Self, &[u64])> {
        if let Some((_, rest)) = Ed25519::try_from_tags(bytes) {
            Some((AnyAlgorithm(Algorithm::Ed25519), rest))
        } else if let Some((_, rest)) = Es256::try_from_tags(bytes) {
            Some((AnyAlgorithm(Algorithm::Es256), rest))
        } else {
            None
        }
    }
}

/// Algorithm-agnostic signature: an algorithm tag plus the raw signature bytes.
///
/// Both supported algorithms use a 64-byte fixed-width signature, so the bytes
/// are stored inline. The tag lets [`AnyVerifier`] reject a signature produced
/// by a different algorithm than it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnySignature {
    algorithm: Algorithm,
    bytes: [u8; 64],
}

impl AnySignature {
    /// The algorithm that produced this signature.
    #[must_use]
    pub const fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// The raw 64-byte signature.
    #[must_use]
    pub const fn to_bytes(&self) -> [u8; 64] {
        self.bytes
    }
}

impl From<Ed25519Signature> for AnySignature {
    fn from(sig: Ed25519Signature) -> Self {
        Self {
            algorithm: Algorithm::Ed25519,
            bytes: sig.to_bytes(),
        }
    }
}

impl From<Es256Signature> for AnySignature {
    fn from(sig: Es256Signature) -> Self {
        Self {
            algorithm: Algorithm::Es256,
            bytes: sig.to_bytes(),
        }
    }
}

/// `AnySignature` encodes as its raw 64-byte body. The algorithm tag is carried
/// out of band (by the verifier's arm), matching how a varsig header already
/// names the algorithm separately from the signature body.
impl SignatureEncoding for AnySignature {
    type Repr = [u8; 64];
}

impl From<AnySignature> for [u8; 64] {
    fn from(sig: AnySignature) -> Self {
        sig.bytes
    }
}

impl TryFrom<&[u8]> for AnySignature {
    type Error = signature::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; 64] = bytes.try_into().map_err(|_| signature::Error::new())?;
        // With no header, default to Ed25519; callers that know the algorithm
        // construct via the typed `From` impls instead.
        Ok(Self {
            algorithm: Algorithm::Ed25519,
            bytes,
        })
    }
}

impl Signature for AnySignature {
    type Algorithm = AnyAlgorithm;
}

/// Algorithm-agnostic verifier.
#[derive(Debug, Clone, PartialEq)]
pub enum AnyVerifier {
    /// An Ed25519 verifier.
    Ed25519(Ed25519Verifier),
    /// An ES256 (P-256) verifier.
    Es256(Es256Verifier),
}

impl From<Ed25519Verifier> for AnyVerifier {
    fn from(v: Ed25519Verifier) -> Self {
        Self::Ed25519(v)
    }
}

impl From<Es256Verifier> for AnyVerifier {
    fn from(v: Es256Verifier) -> Self {
        Self::Es256(v)
    }
}

impl AnyVerifier {
    /// The algorithm this verifier checks.
    #[must_use]
    pub const fn algorithm(&self) -> Algorithm {
        match self {
            Self::Ed25519(_) => Algorithm::Ed25519,
            Self::Es256(_) => Algorithm::Es256,
        }
    }

    /// Get the ed25519 verifier, if this is an ed25519 arm.
    #[must_use]
    pub const fn as_ed25519(&self) -> Option<&Ed25519Verifier> {
        match self {
            Self::Ed25519(v) => Some(v),
            Self::Es256(_) => None,
        }
    }

    /// Get the es256 verifier, if this is an es256 arm.
    #[must_use]
    pub const fn as_es256(&self) -> Option<&Es256Verifier> {
        match self {
            Self::Es256(v) => Some(v),
            Self::Ed25519(_) => None,
        }
    }

    /// Parse a `did:key` string into an algorithm-agnostic verifier, trying
    /// ed25519 first, then es256.
    ///
    /// # Errors
    ///
    /// Returns an error if the string is not a supported `did:key`.
    pub fn from_did_key(s: &str) -> Result<Self, AnyDidFromStrError> {
        if let Ok(v) = s.parse::<Ed25519Verifier>() {
            return Ok(Self::Ed25519(v));
        }
        if let Ok(v) = s.parse::<Es256Verifier>() {
            return Ok(Self::Es256(v));
        }
        Err(AnyDidFromStrError)
    }
}

impl std::str::FromStr for AnyVerifier {
    type Err = AnyDidFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_did_key(s)
    }
}

impl std::fmt::Display for AnyVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ed25519(v) => write!(f, "{v}"),
            Self::Es256(v) => write!(f, "{v}"),
        }
    }
}

impl Principal for AnyVerifier {
    fn did(&self) -> Did {
        match self {
            Self::Ed25519(v) => v.did(),
            Self::Es256(v) => v.did(),
        }
    }
}

impl Verifier<AnySignature> for AnyVerifier {
    async fn verify(&self, msg: &[u8], signature: &AnySignature) -> Result<(), signature::Error> {
        if signature.algorithm() != self.algorithm() {
            return Err(signature::Error::new());
        }
        match self {
            Self::Ed25519(v) => {
                let sig = Ed25519Signature::from_bytes(signature.to_bytes());
                v.verify(msg, &sig).await
            }
            Self::Es256(v) => {
                let sig = Es256Signature::from_bytes(signature.to_bytes());
                v.verify(msg, &sig).await
            }
        }
    }
}

/// Error returned when a `did:key` string is not a supported algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("unsupported or invalid did:key")]
pub struct AnyDidFromStrError;

/// Algorithm-agnostic signer: a wrapped local key that can sign.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum AnySigner {
    /// An Ed25519 signer.
    Ed25519(Ed25519Signer),
    /// An ES256 (P-256) signer.
    Es256(Es256Signer),
}

impl From<Ed25519Signer> for AnySigner {
    fn from(s: Ed25519Signer) -> Self {
        Self::Ed25519(s)
    }
}

impl From<Es256Signer> for AnySigner {
    fn from(s: Es256Signer) -> Self {
        Self::Es256(s)
    }
}

impl AnySigner {
    /// The algorithm this signer produces.
    #[must_use]
    pub const fn algorithm(&self) -> Algorithm {
        match self {
            Self::Ed25519(_) => Algorithm::Ed25519,
            Self::Es256(_) => Algorithm::Es256,
        }
    }

    /// Get the ed25519 signer, if this is an ed25519 arm.
    #[must_use]
    pub const fn as_ed25519(&self) -> Option<&Ed25519Signer> {
        match self {
            Self::Ed25519(s) => Some(s),
            Self::Es256(_) => None,
        }
    }

    /// Get the es256 signer, if this is an es256 arm.
    #[must_use]
    pub const fn as_es256(&self) -> Option<&Es256Signer> {
        match self {
            Self::Es256(s) => Some(s),
            Self::Ed25519(_) => None,
        }
    }

    /// The algorithm-agnostic verifier for this signer's public key.
    #[must_use]
    pub fn verifier(&self) -> AnyVerifier {
        match self {
            Self::Ed25519(s) => AnyVerifier::Ed25519(s.ed25519_did().clone()),
            Self::Es256(s) => AnyVerifier::Es256(s.es256_did().clone()),
        }
    }
}

impl Principal for AnySigner {
    fn did(&self) -> Did {
        match self {
            Self::Ed25519(s) => s.did(),
            Self::Es256(s) => s.did(),
        }
    }
}

impl Signer<AnySignature> for AnySigner {
    async fn sign(&self, msg: &[u8]) -> Result<AnySignature, signature::Error> {
        match self {
            Self::Ed25519(s) => Ok(AnySignature::from(Signer::sign(s, msg).await?)),
            Self::Es256(s) => Ok(AnySignature::from(Signer::sign(s, msg).await?)),
        }
    }
}

impl Issuer for AnySigner {
    type Signature = AnySignature;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    async fn any_ed25519_sign_verify() {
        let signer = AnySigner::from(Ed25519Signer::generate().await.unwrap());
        let verifier = signer.verifier();
        let msg = b"agnostic";
        let sig = Signer::sign(&signer, msg).await.unwrap();
        assert_eq!(sig.algorithm(), Algorithm::Ed25519);
        verifier.verify(msg, &sig).await.unwrap();
    }

    #[dialog_common::test]
    async fn any_es256_sign_verify() {
        let signer = AnySigner::from(Es256Signer::generate().await.unwrap());
        let verifier = signer.verifier();
        let msg = b"agnostic";
        let sig = Signer::sign(&signer, msg).await.unwrap();
        assert_eq!(sig.algorithm(), Algorithm::Es256);
        verifier.verify(msg, &sig).await.unwrap();
    }

    #[dialog_common::test]
    async fn any_cross_algorithm_rejected() {
        // A signature from an ed25519 signer must not verify against an es256
        // verifier, even though both signature bodies are 64 bytes.
        let ed = AnySigner::from(Ed25519Signer::generate().await.unwrap());
        let es_verifier = AnySigner::from(Es256Signer::generate().await.unwrap()).verifier();
        let msg = b"agnostic";
        let sig = Signer::sign(&ed, msg).await.unwrap();
        assert!(es_verifier.verify(msg, &sig).await.is_err());
    }

    #[dialog_common::test]
    async fn any_verifier_did_key_roundtrip() {
        for signer in [
            AnySigner::from(Ed25519Signer::generate().await.unwrap()),
            AnySigner::from(Es256Signer::generate().await.unwrap()),
        ] {
            let verifier = signer.verifier();
            let did = verifier.did();
            let parsed = AnyVerifier::from_did_key(did.as_str()).unwrap();
            assert_eq!(parsed.did(), did);
            assert_eq!(parsed.algorithm(), verifier.algorithm());
        }
    }
}
