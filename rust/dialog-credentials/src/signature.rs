//! Algorithm-agnostic signer, verifier, and signature types.
//!
//! The varsig [`Signer`](dialog_varsig::Signer) and
//! [`Verifier`](dialog_varsig::Verifier) traits are each generic over a single
//! concrete signature type. To hold an identity without committing to an
//! algorithm, this module wraps the per-algorithm types in enums and implements
//! the varsig traits over a single [`Signature`] value that carries an
//! algorithm tag alongside the raw signature bytes.
//!
//! These enums are the crate's default identity types: `dialog-credentials`
//! deals in [`Signer`] / [`Verifier`] / [`Signature`], not in any one algorithm.
//! ed25519 is always available; other algorithms (currently ES256 / P-256) are
//! feature-gated and add arms to the enums when enabled.
//!
//! The concrete arms stay authoritative: an [`Verifier::Ed25519`] verifies only
//! ed25519 signatures, and a signature whose tag does not match the verifier's
//! arm is rejected. This keeps the ed25519 path byte-identical while making room
//! for further algorithms.

// With only the `ed25519` feature the enums below are single-arm. That is
// intentional: they are always enums so that enabling another algorithm is
// purely additive. Silence the lints that fire for the single-arm shape.
#![allow(clippy::single_match_else)]

use dialog_capability::Issuer;
use dialog_common::ConditionalSync;
use dialog_varsig::{
    Did, Principal, Signer as VarsigSigner, Verifier as VarsigVerifier, eddsa::Ed25519Signature,
};

pub use dialog_varsig::{AlgorithmTag, AnyAlgorithm as Algorithm, AnySignature as Signature};

use crate::{Ed25519Signer, Ed25519Verifier};

#[cfg(feature = "es256")]
use crate::{Es256Signer, Es256Verifier};
#[cfg(feature = "es256")]
use dialog_varsig::ecdsa::Es256Signature;

/// Algorithm-agnostic verifier.
#[derive(Debug, Clone, PartialEq)]
pub enum Verifier {
    /// An Ed25519 verifier.
    Ed25519(Ed25519Verifier),
    /// An ES256 (P-256) verifier.
    #[cfg(feature = "es256")]
    Es256(Es256Verifier),
}

impl From<Ed25519Verifier> for Verifier {
    fn from(v: Ed25519Verifier) -> Self {
        Self::Ed25519(v)
    }
}

#[cfg(feature = "es256")]
impl From<Es256Verifier> for Verifier {
    fn from(v: Es256Verifier) -> Self {
        Self::Es256(v)
    }
}

impl Verifier {
    /// The algorithm this verifier checks.
    #[must_use]
    pub const fn algorithm(&self) -> AlgorithmTag {
        match self {
            Self::Ed25519(_) => AlgorithmTag::Ed25519,
            #[cfg(feature = "es256")]
            Self::Es256(_) => AlgorithmTag::Es256,
        }
    }

    /// Get the ed25519 verifier, if this is an ed25519 arm.
    #[must_use]
    pub const fn as_ed25519(&self) -> Option<&Ed25519Verifier> {
        match self {
            Self::Ed25519(v) => Some(v),
            #[cfg(feature = "es256")]
            Self::Es256(_) => None,
        }
    }

    /// Get the es256 verifier, if this is an es256 arm.
    #[cfg(feature = "es256")]
    #[must_use]
    pub const fn as_es256(&self) -> Option<&Es256Verifier> {
        match self {
            Self::Es256(v) => Some(v),
            Self::Ed25519(_) => None,
        }
    }

    /// Parse a `did:key` string into an algorithm-agnostic verifier, trying
    /// each enabled algorithm in turn.
    ///
    /// # Errors
    ///
    /// Returns an error if the string is not a supported `did:key`.
    pub fn from_did_key(s: &str) -> Result<Self, DidFromStrError> {
        if let Ok(v) = s.parse::<Ed25519Verifier>() {
            return Ok(Self::Ed25519(v));
        }
        #[cfg(feature = "es256")]
        if let Ok(v) = s.parse::<Es256Verifier>() {
            return Ok(Self::Es256(v));
        }
        Err(DidFromStrError)
    }
}

impl std::str::FromStr for Verifier {
    type Err = DidFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_did_key(s)
    }
}

impl std::fmt::Display for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ed25519(v) => write!(f, "{v}"),
            #[cfg(feature = "es256")]
            Self::Es256(v) => write!(f, "{v}"),
        }
    }
}

impl Principal for Verifier {
    fn did(&self) -> Did {
        match self {
            Self::Ed25519(v) => v.did(),
            #[cfg(feature = "es256")]
            Self::Es256(v) => v.did(),
        }
    }
}

impl VarsigVerifier<Signature> for Verifier {
    async fn verify(&self, msg: &[u8], signature: &Signature) -> Result<(), signature::Error> {
        if signature.algorithm() != self.algorithm() {
            return Err(signature::Error::new());
        }
        match self {
            Self::Ed25519(v) => {
                // The body is variable-length; reconstruct the concrete
                // fixed-width signature, refusing a body of the wrong length.
                let sig = Ed25519Signature::try_from(signature.to_bytes())
                    .map_err(|_| signature::Error::new())?;
                v.verify(msg, &sig).await
            }
            #[cfg(feature = "es256")]
            Self::Es256(v) => {
                let sig = Es256Signature::try_from(signature.to_bytes())
                    .map_err(|_| signature::Error::new())?;
                v.verify(msg, &sig).await
            }
        }
    }
}

/// Error returned when a `did:key` string is not a supported algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("unsupported or invalid did:key")]
pub struct DidFromStrError;

/// Algorithm-agnostic signer: a wrapped local key that can sign.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Signer {
    /// An Ed25519 signer.
    Ed25519(Ed25519Signer),
    /// An ES256 (P-256) signer.
    #[cfg(feature = "es256")]
    Es256(Es256Signer),
}

impl From<Ed25519Signer> for Signer {
    fn from(s: Ed25519Signer) -> Self {
        Self::Ed25519(s)
    }
}

#[cfg(feature = "es256")]
impl From<Es256Signer> for Signer {
    fn from(s: Es256Signer) -> Self {
        Self::Es256(s)
    }
}

impl Signer {
    /// The algorithm this signer produces.
    #[must_use]
    pub const fn algorithm(&self) -> AlgorithmTag {
        match self {
            Self::Ed25519(_) => AlgorithmTag::Ed25519,
            #[cfg(feature = "es256")]
            Self::Es256(_) => AlgorithmTag::Es256,
        }
    }

    /// Get the ed25519 signer, if this is an ed25519 arm.
    #[must_use]
    pub const fn as_ed25519(&self) -> Option<&Ed25519Signer> {
        match self {
            Self::Ed25519(s) => Some(s),
            #[cfg(feature = "es256")]
            Self::Es256(_) => None,
        }
    }

    /// Get the es256 signer, if this is an es256 arm.
    #[cfg(feature = "es256")]
    #[must_use]
    pub const fn as_es256(&self) -> Option<&Es256Signer> {
        match self {
            Self::Es256(s) => Some(s),
            Self::Ed25519(_) => None,
        }
    }

    /// The algorithm-agnostic verifier for this signer's public key.
    #[must_use]
    pub fn verifier(&self) -> Verifier {
        match self {
            Self::Ed25519(s) => Verifier::Ed25519(s.ed25519_did().clone()),
            #[cfg(feature = "es256")]
            Self::Es256(s) => Verifier::Es256(s.es256_did().clone()),
        }
    }
}

impl Principal for Signer {
    fn did(&self) -> Did {
        match self {
            Self::Ed25519(s) => s.did(),
            #[cfg(feature = "es256")]
            Self::Es256(s) => s.did(),
        }
    }
}

impl VarsigSigner<Signature> for Signer {
    async fn sign(&self, msg: &[u8]) -> Result<Signature, signature::Error> {
        match self {
            Self::Ed25519(s) => Ok(Signature::from(VarsigSigner::sign(s, msg).await?)),
            #[cfg(feature = "es256")]
            Self::Es256(s) => Ok(Signature::from(VarsigSigner::sign(s, msg).await?)),
        }
    }
}

impl Issuer for Signer {
    type Signature = Signature;
}

/// A signer that presents an arbitrary DID while delegating the actual signing
/// to a wrapped key.
///
/// A [`Signer`] normally identifies as its key's `did:key`. `WithDid` overrides
/// that DID (for example a `did:web:` name) while keeping the underlying crypto
/// unchanged: the signature is produced by the wrapped signer, and a verifier
/// resolves the presented DID to that same key. This is how an identity signs
/// UCANs under a `did:web` (or any other method) name.
///
/// The presented DID is arbitrary; it is the caller's responsibility to make it
/// resolve to the wrapped key's public key (for `did:web`, by serving a DID
/// document whose verification method is that key).
#[derive(Debug, Clone)]
pub struct WithDid<S> {
    did: Did,
    signer: S,
}

impl<S> WithDid<S> {
    /// Wrap `signer` so it presents `did` instead of its key's `did:key`.
    pub const fn new(did: Did, signer: S) -> Self {
        Self { did, signer }
    }

    /// The wrapped signer.
    pub const fn signer(&self) -> &S {
        &self.signer
    }

    /// Consume the wrapper and return the inner signer.
    #[allow(clippy::missing_const_for_fn)]
    pub fn into_signer(self) -> S {
        self.signer
    }
}

impl<S> Principal for WithDid<S> {
    fn did(&self) -> Did {
        self.did.clone()
    }
}

impl<S> VarsigSigner<Signature> for WithDid<S>
where
    S: VarsigSigner<Signature> + ConditionalSync,
{
    async fn sign(&self, msg: &[u8]) -> Result<Signature, signature::Error> {
        self.signer.sign(msg).await
    }
}

impl<S> Issuer for WithDid<S>
where
    S: VarsigSigner<Signature> + ConditionalSync,
{
    type Signature = Signature;
}

impl Signer {
    /// Present this signer under an arbitrary DID (for example a `did:web:`
    /// name) while still signing with the underlying key.
    ///
    /// See [`WithDid`].
    #[must_use]
    pub const fn with_did(self, did: Did) -> WithDid<Self> {
        WithDid::new(did, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    async fn ed25519_sign_verify() {
        let signer = Signer::from(Ed25519Signer::generate().await.unwrap());
        let verifier = signer.verifier();
        let msg = b"agnostic";
        let sig = VarsigSigner::sign(&signer, msg).await.unwrap();
        assert_eq!(sig.algorithm(), AlgorithmTag::Ed25519);
        verifier.verify(msg, &sig).await.unwrap();
    }

    #[cfg(feature = "es256")]
    #[dialog_common::test]
    async fn es256_sign_verify() {
        let signer = Signer::from(Es256Signer::generate().await.unwrap());
        let verifier = signer.verifier();
        let msg = b"agnostic";
        let sig = VarsigSigner::sign(&signer, msg).await.unwrap();
        assert_eq!(sig.algorithm(), AlgorithmTag::Es256);
        verifier.verify(msg, &sig).await.unwrap();
    }

    #[cfg(feature = "es256")]
    #[dialog_common::test]
    async fn cross_algorithm_rejected() {
        // A signature from an ed25519 signer must not verify against an es256
        // verifier, even though both signature bodies are 64 bytes.
        let ed = Signer::from(Ed25519Signer::generate().await.unwrap());
        let es_verifier = Signer::from(Es256Signer::generate().await.unwrap()).verifier();
        let msg = b"agnostic";
        let sig = VarsigSigner::sign(&ed, msg).await.unwrap();
        assert!(es_verifier.verify(msg, &sig).await.is_err());
    }

    #[dialog_common::test]
    async fn verifier_did_key_roundtrip() {
        let signers = {
            let mut v = vec![Signer::from(Ed25519Signer::generate().await.unwrap())];
            #[cfg(feature = "es256")]
            v.push(Signer::from(Es256Signer::generate().await.unwrap()));
            v
        };
        for signer in signers {
            let verifier = signer.verifier();
            let did = verifier.did();
            let parsed = Verifier::from_did_key(did.as_str()).unwrap();
            assert_eq!(parsed.did(), did);
            assert_eq!(parsed.algorithm(), verifier.algorithm());
        }
    }

    #[dialog_common::test]
    async fn with_did_presents_custom_did() {
        let signer = Signer::from(Ed25519Signer::generate().await.unwrap());
        let key_did = signer.did();
        let web_did: Did = "did:web:issuer.example".parse().unwrap();

        let wrapped = signer.with_did(web_did.clone());

        // The wrapper presents the custom DID, not the key's did:key.
        assert_eq!(wrapped.did(), web_did);
        assert_ne!(wrapped.did(), key_did);
    }

    #[dialog_common::test]
    async fn with_did_signs_with_underlying_key() {
        let signer = Signer::from(Ed25519Signer::generate().await.unwrap());
        // The verifier is derived from the underlying key, before wrapping.
        let verifier = signer.verifier();
        let web_did: Did = "did:web:issuer.example".parse().unwrap();

        let wrapped = signer.with_did(web_did);
        let msg = b"signed under did:web";
        let sig = VarsigSigner::sign(&wrapped, msg).await.unwrap();

        // The signature was produced by the wrapped key, so the key's own
        // verifier accepts it even though the presented DID is did:web.
        verifier.verify(msg, &sig).await.unwrap();
    }

    #[cfg(feature = "es256")]
    #[dialog_common::test]
    async fn with_did_signs_with_underlying_p256_key() {
        let signer = Signer::from(Es256Signer::generate().await.unwrap());
        let verifier = signer.verifier();
        let web_did: Did = "did:web:issuer.example".parse().unwrap();

        let wrapped = signer.with_did(web_did);
        let msg = b"signed under did:web";
        let sig = VarsigSigner::sign(&wrapped, msg).await.unwrap();
        assert_eq!(sig.algorithm(), AlgorithmTag::Es256);
        verifier.verify(msg, &sig).await.unwrap();
    }
}
