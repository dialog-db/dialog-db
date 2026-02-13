//! WebAuthn P-256 DID key resolver.

use super::{error::WebAuthnResolveError, verifier::WebAuthnVerifier};
use dialog_varsig::{Did, Verifier, webauthn::WebAuthnSignature};

/// Resolves `did:key` strings to WebAuthn P-256 verifiers.
#[derive(Debug, Clone, Copy)]
pub struct WebAuthnKeyResolver;

impl dialog_varsig::resolver::Resolver<WebAuthnSignature> for WebAuthnKeyResolver {
    type Error = WebAuthnResolveError;

    async fn resolve(
        &self,
        did: &Did,
    ) -> Result<impl Verifier<WebAuthnSignature>, Self::Error> {
        let verifier: WebAuthnVerifier = did.as_str().parse()?;
        Ok(verifier)
    }
}
