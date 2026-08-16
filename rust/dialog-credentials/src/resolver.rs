//! Algorithm-agnostic `did:key` resolver.
//!
//! Resolves a `did:key` string to the agnostic [`Verifier`], dispatching to the
//! right algorithm by the DID's multicodec. This is what verifies the
//! algorithm-agnostic [`Signature`](crate::Signature): a UCAN signed by an
//! ed25519 or a P-256 key resolves through the same resolver.
//!
//! This handles `did:key` only. Network methods (`did:web`, `did:plc`) are
//! separate resolvers that compose with this one.

use crate::{Ed25519Verifier, Verifier};
use dialog_varsig::eddsa::Ed25519Signature;
use dialog_varsig::{AnySignature, Did, resolver::Resolver};

/// Resolves `did:key` strings to a verifier, dispatching to the right algorithm
/// by the DID's multicodec.
///
/// It resolves the algorithm-agnostic [`AnySignature`] (returning the agnostic
/// [`Verifier`]), and also the concrete per-algorithm signature types, so the
/// same resolver serves both agnostic and concretely-typed verification paths.
#[derive(Debug, Clone, Copy, Default)]
pub struct DidKeyResolver;

/// Error returned when a `did:key` string cannot be resolved to a supported
/// verifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("could not resolve did:key to a supported verifier")]
pub struct DidKeyResolveError;

impl Resolver<AnySignature> for DidKeyResolver {
    type Error = DidKeyResolveError;

    async fn resolve(
        &self,
        did: &Did,
    ) -> Result<impl dialog_varsig::Verifier<AnySignature>, Self::Error> {
        Verifier::from_did_key(did.as_str()).map_err(|_| DidKeyResolveError)
    }
}

impl Resolver<Ed25519Signature> for DidKeyResolver {
    type Error = DidKeyResolveError;

    async fn resolve(
        &self,
        did: &Did,
    ) -> Result<impl dialog_varsig::Verifier<Ed25519Signature>, Self::Error> {
        let verifier: Ed25519Verifier = did.as_str().parse().map_err(|_| DidKeyResolveError)?;
        Ok(verifier)
    }
}

#[cfg(feature = "es256")]
impl Resolver<dialog_varsig::ecdsa::Es256Signature> for DidKeyResolver {
    type Error = DidKeyResolveError;

    async fn resolve(
        &self,
        did: &Did,
    ) -> Result<impl dialog_varsig::Verifier<dialog_varsig::ecdsa::Es256Signature>, Self::Error>
    {
        let verifier: crate::Es256Verifier =
            did.as_str().parse().map_err(|_| DidKeyResolveError)?;
        Ok(verifier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Ed25519Signer, Signer};
    use dialog_varsig::{
        AnySignature, Principal, Signer as VarsigSigner, Verifier as VarsigVerifier,
    };

    #[dialog_common::test]
    async fn it_resolves_ed25519_did_key() {
        let signer = Signer::from(Ed25519Signer::generate().await.unwrap());
        let did = signer.did();
        let msg = b"resolve me";
        let sig = VarsigSigner::sign(&signer, msg).await.unwrap();

        let verifier = Resolver::<AnySignature>::resolve(&DidKeyResolver, &did)
            .await
            .unwrap();
        verifier.verify(msg, &sig).await.unwrap();
    }

    #[cfg(feature = "es256")]
    #[dialog_common::test]
    async fn it_resolves_es256_did_key() {
        use crate::Es256Signer;
        let signer = Signer::from(Es256Signer::generate().await.unwrap());
        let did = signer.did();
        let msg = b"resolve me";
        let sig = VarsigSigner::sign(&signer, msg).await.unwrap();

        let verifier = Resolver::<AnySignature>::resolve(&DidKeyResolver, &did)
            .await
            .unwrap();
        verifier.verify(msg, &sig).await.unwrap();
    }
}
