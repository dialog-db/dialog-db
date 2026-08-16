//! Bridge from the [`Resolve`](crate::Resolve) capability to the varsig
//! [`Resolver`](dialog_varsig::resolver::Resolver) trait.
//!
//! The UCAN verification path ([`chain.verify`]) is written against the varsig
//! `Resolver<AnySignature>` trait: it asks a resolver for a verifier per DID. To
//! keep that seam while routing resolution through the ambient [`Resolve`]
//! capability, [`PerformingResolver`] wraps any `Provider<Resolve>` and, on each
//! `resolve`, performs a `Resolve` against it.
//!
//! This is the one adapter that lets a caller pick a resolution policy (local
//! `did:key`, network `did:web`, a cache, or all three) as a provider and hand
//! it to code that only knows the `Resolver` trait.
//!
//! [`chain.verify`]: dialog_ucan_core::InvocationChain::verify

use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_varsig::{AnySignature, Did, resolver::Resolver};

use crate::error::ResolveError;
use crate::resolve::Resolve;
use crate::verifier::MultiVerifier;

/// Adapts a [`Resolve`](crate::Resolve) provider into a varsig
/// [`Resolver`](dialog_varsig::resolver::Resolver).
///
/// Borrows the provider env, so the env must outlive the verification it is
/// handed to. Construct one per verification with [`PerformingResolver::new`].
#[derive(Debug, Clone, Copy)]
pub struct PerformingResolver<'env, Env> {
    env: &'env Env,
}

impl<'env, Env> PerformingResolver<'env, Env> {
    /// Adapt `env` (a `Provider<Resolve>`) into a varsig resolver.
    #[must_use]
    pub fn new(env: &'env Env) -> Self {
        Self { env }
    }
}

impl<Env> Resolver<AnySignature> for PerformingResolver<'_, Env>
where
    Env: Provider<Resolve> + ConditionalSync,
{
    type Error = ResolveError;

    async fn resolve(
        &self,
        did: &Did,
    ) -> Result<impl dialog_varsig::Verifier<AnySignature>, ResolveError> {
        let verifier: MultiVerifier = Resolve::new(did.clone()).perform(self.env).await?;
        Ok(verifier)
    }
}

#[cfg(all(test, feature = "test-fetch"))]
mod tests {
    use super::*;
    use crate::document::DidDocument;
    use crate::fetch::MapFetch;
    use crate::provider::{DidWebProvider, MethodResolver};
    use dialog_credentials::{Ed25519Signer, Signer};
    use dialog_varsig::{
        AnySignature, Principal, Signer as VarsigSigner, Verifier as VarsigVerifier,
    };

    /// A `did.json` naming the given signer's `did:key` public key as its sole
    /// verification method, served for the given `did:web` host.
    fn did_web_document(web_did: &str, key_did: &str) -> String {
        format!(
            r#"{{
                "id": "{web_did}",
                "verificationMethod": [
                    {{
                        "id": "{web_did}#owner",
                        "type": "Multikey",
                        "controller": "{web_did}",
                        "publicKeyMultibase": "{multibase}"
                    }}
                ]
            }}"#,
            multibase = key_did.strip_prefix("did:key:").unwrap()
        )
    }

    #[dialog_common::test]
    async fn it_resolves_and_verifies_a_did_web_signature() {
        // A signer whose did:key we will publish under a did:web identity.
        let signer = Signer::from(Ed25519Signer::generate().await.unwrap());
        let key_did = signer.did();

        let web_did_str = "did:web:example.com";
        let web_did: Did = web_did_str.parse().unwrap();

        let doc = did_web_document(web_did_str, key_did.as_str());
        let fetch =
            MapFetch::new().with("https://example.com/.well-known/did.json", doc.into_bytes());

        // A method resolver that fetches did:web via the mock.
        let env = MethodResolver::with_providers(
            crate::provider::DidKeyProvider,
            DidWebProvider::with_fetch(fetch),
        );
        let resolver = PerformingResolver::new(&env);

        // Sign with the underlying key, verify through the did:web-resolved
        // verifier.
        let msg = b"resolve me over the web";
        let sig: AnySignature = VarsigSigner::sign(&signer, msg).await.unwrap();

        let verifier = Resolver::<AnySignature>::resolve(&resolver, &web_did)
            .await
            .expect("did:web resolution should succeed");
        verifier
            .verify(msg, &sig)
            .await
            .expect("signature should verify under the resolved verifier");

        // The document parse is round-trippable (guards the test fixture).
        let _doc: DidDocument =
            serde_json::from_str(&did_web_document(web_did_str, key_did.as_str())).unwrap();
    }

    #[dialog_common::test]
    async fn it_surfaces_fetch_failure_as_resolve_error() {
        let web_did: Did = "did:web:missing.example".parse().unwrap();
        let env = MethodResolver::with_providers(
            crate::provider::DidKeyProvider,
            DidWebProvider::with_fetch(MapFetch::new()),
        );
        let resolver = PerformingResolver::new(&env);

        let outcome = Resolver::<AnySignature>::resolve(&resolver, &web_did).await;
        assert!(
            matches!(outcome, Err(ResolveError::Fetch(_))),
            "an unreachable did:web host must surface as a Fetch error"
        );
    }
}
