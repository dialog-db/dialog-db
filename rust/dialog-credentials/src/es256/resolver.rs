//! ES256 DID key resolver.

use super::{error::Es256ResolveError, verifier::Es256Verifier};
use dialog_varsig::{Did, Verifier, ecdsa::Es256Signature};

/// Resolves `did:key` strings to ES256 verifiers.
#[derive(Debug, Clone, Copy)]
pub struct Es256KeyResolver;

impl dialog_varsig::resolver::Resolver<Es256Signature> for Es256KeyResolver {
    type Error = Es256ResolveError;

    async fn resolve(&self, did: &Did) -> Result<impl Verifier<Es256Signature>, Self::Error> {
        let es_did: Es256Verifier = did.as_str().parse()?;
        Ok(es_did)
    }
}
