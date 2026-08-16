//! RSA DID key resolver.

use super::{error::RsaResolveError, verifier::RsaVerifier};
use dialog_varsig::{AnySignature, Did, Verifier};

/// Resolves `did:key` strings to RSA verifiers.
#[derive(Debug, Clone, Copy)]
pub struct RsaKeyResolver;

impl dialog_varsig::resolver::Resolver<AnySignature> for RsaKeyResolver {
    type Error = RsaResolveError;

    async fn resolve(&self, did: &Did) -> Result<impl Verifier<AnySignature>, Self::Error> {
        let rsa_did: RsaVerifier = did.as_str().parse()?;
        Ok(rsa_did)
    }
}
