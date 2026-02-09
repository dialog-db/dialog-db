//! Ed25519 DID key resolver.

use super::{error::Ed25519ResolveError, verifier::Ed25519Principal};
use varsig::{eddsa::Ed25519Signature, Did, Verifier};

/// Resolves `did:key` strings to Ed25519 verifiers.
#[derive(Debug, Clone, Copy)]
pub struct Ed25519KeyResolver;

impl varsig::resolver::Resolver<Ed25519Signature> for Ed25519KeyResolver {
    type Error = Ed25519ResolveError;

    async fn resolve(&self, did: &Did) -> Result<impl Verifier<Ed25519Signature>, Self::Error> {
        let ed_did: Ed25519Principal = did.as_str().parse()?;
        Ok(ed_did)
    }
}
