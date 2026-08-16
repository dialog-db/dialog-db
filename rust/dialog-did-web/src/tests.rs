//! Tests for the `Resolve` capability and its providers.
//!
//! Network access is mocked through [`MapFetch`], so no test touches the
//! network. `did:web` documents are built from freshly generated keys, and the
//! agnostic verifier that resolution recovers is checked by actually verifying a
//! signature the matching signer produced.

use dialog_credentials::{Ed25519Signer, Es256Signer, Signer};
use dialog_varsig::{Did, Principal, Signer as VarsigSigner, Verifier as VarsigVerifier};

use crate::document::DidDocument;
use crate::fetch::MapFetch;
use crate::provider::{DidKeyProvider, DidWebProvider, MethodResolver};
use crate::resolve::Resolve;
use crate::url::did_web_url;
use crate::{CachingResolver, ResolveError};

/// The `did:key` multibase tail (`z...`) for a signer's public key.
fn multibase_of(signer: &Signer) -> String {
    let did = signer.did();
    did.as_str()
        .strip_prefix("did:key:")
        .expect("did:key")
        .to_string()
}

/// A did.json for a signer, keyed with `publicKeyMultibase`.
fn did_document_multibase(did_web: &str, signer: &Signer) -> String {
    format!(
        r#"{{
            "id": "{did_web}",
            "verificationMethod": [{{
                "id": "{did_web}#key-1",
                "type": "Multikey",
                "controller": "{did_web}",
                "publicKeyMultibase": "{multibase}"
            }}]
        }}"#,
        multibase = multibase_of(signer)
    )
}

async fn resolves_algorithm(signer: Signer) {
    let did_web = "did:web:example.com";
    let doc = did_document_multibase(did_web, &signer);
    let fetch = MapFetch::new().with("https://example.com/.well-known/did.json", doc.into_bytes());
    let provider = DidWebProvider::with_fetch(fetch);

    let did: Did = did_web.parse().unwrap();
    let verifier = Resolve::new(did).perform(&provider).await.unwrap();

    let msg = b"resolve me over the web";
    let sig = VarsigSigner::sign(&signer, msg).await.unwrap();
    verifier.verify(msg, &sig).await.unwrap();
}

#[dialog_common::test]
async fn it_resolves_ed25519_did_web() {
    let signer = Signer::from(Ed25519Signer::generate().await.unwrap());
    resolves_algorithm(signer).await;
}

#[dialog_common::test]
async fn it_resolves_es256_did_web() {
    let signer = Signer::from(Es256Signer::generate().await.unwrap());
    resolves_algorithm(signer).await;
}

#[dialog_common::test]
async fn it_resolves_jwk_ed25519() {
    let signer = Signer::from(Ed25519Signer::generate().await.unwrap());
    let key_bytes = signer
        .verifier()
        .as_ed25519()
        .unwrap()
        .0
        .to_bytes()
        .to_vec();
    let x = base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        &key_bytes,
    );
    let did_web = "did:web:jwk.example";
    let doc = format!(
        r#"{{
            "id": "{did_web}",
            "verificationMethod": [{{
                "id": "{did_web}#0",
                "type": "JsonWebKey2020",
                "publicKeyJwk": {{ "kty": "OKP", "crv": "Ed25519", "x": "{x}" }}
            }}]
        }}"#
    );
    let fetch = MapFetch::new().with("https://jwk.example/.well-known/did.json", doc.into_bytes());
    let provider = DidWebProvider::with_fetch(fetch);

    let did: Did = did_web.parse().unwrap();
    let verifier = Resolve::new(did).perform(&provider).await.unwrap();
    let msg = b"jwk verify";
    let sig = VarsigSigner::sign(&signer, msg).await.unwrap();
    verifier.verify(msg, &sig).await.unwrap();
}

#[dialog_common::test]
async fn it_refuses_missing_document() {
    let provider = DidWebProvider::with_fetch(MapFetch::new());
    let did: Did = "did:web:missing.example".parse().unwrap();
    let err = Resolve::new(did).perform(&provider).await.unwrap_err();
    assert!(matches!(err, ResolveError::Fetch(_)), "got {err:?}");
}

#[dialog_common::test]
async fn it_refuses_malformed_document() {
    let fetch = MapFetch::new().with(
        "https://bad.example/.well-known/did.json",
        b"not json".to_vec(),
    );
    let provider = DidWebProvider::with_fetch(fetch);
    let did: Did = "did:web:bad.example".parse().unwrap();
    let err = Resolve::new(did).perform(&provider).await.unwrap_err();
    assert!(
        matches!(err, ResolveError::MalformedDocument(_)),
        "got {err:?}"
    );
}

#[dialog_common::test]
async fn it_refuses_no_verification_method() {
    let doc = r#"{ "id": "did:web:empty.example", "verificationMethod": [] }"#;
    let fetch = MapFetch::new().with(
        "https://empty.example/.well-known/did.json",
        doc.as_bytes().to_vec(),
    );
    let provider = DidWebProvider::with_fetch(fetch);
    let did: Did = "did:web:empty.example".parse().unwrap();
    let err = Resolve::new(did).perform(&provider).await.unwrap_err();
    assert!(
        matches!(err, ResolveError::NoSupportedVerificationMethod),
        "got {err:?}"
    );
}

#[dialog_common::test]
async fn it_refuses_unsupported_key_type() {
    let doc = r#"{
        "id": "did:web:rsa.example",
        "verificationMethod": [{
            "id": "did:web:rsa.example#0",
            "type": "JsonWebKey2020",
            "publicKeyJwk": { "kty": "RSA", "crv": "RSA", "x": "AQAB" }
        }]
    }"#;
    let fetch = MapFetch::new().with(
        "https://rsa.example/.well-known/did.json",
        doc.as_bytes().to_vec(),
    );
    let provider = DidWebProvider::with_fetch(fetch);
    let did: Did = "did:web:rsa.example".parse().unwrap();
    let err = Resolve::new(did).perform(&provider).await.unwrap_err();
    assert!(
        matches!(err, ResolveError::UnsupportedKey(_)),
        "got {err:?}"
    );
}

#[dialog_common::test]
async fn method_dispatch_resolves_did_key_without_fetching() {
    let signer = Signer::from(Ed25519Signer::generate().await.unwrap());
    let fetch = MapFetch::new();
    let resolver =
        MethodResolver::with_providers(DidKeyProvider, DidWebProvider::with_fetch(fetch.clone()));

    let did = signer.did();
    let verifier = Resolve::new(did).perform(&resolver).await.unwrap();
    let msg = b"local";
    let sig = VarsigSigner::sign(&signer, msg).await.unwrap();
    verifier.verify(msg, &sig).await.unwrap();

    assert_eq!(fetch.calls(), 0, "did:key must not hit the network");
}

#[dialog_common::test]
async fn method_dispatch_routes_did_web_to_fetcher() {
    let signer = Signer::from(Ed25519Signer::generate().await.unwrap());
    let did_web = "did:web:route.example";
    let doc = did_document_multibase(did_web, &signer);
    let fetch = MapFetch::new().with(
        "https://route.example/.well-known/did.json",
        doc.into_bytes(),
    );
    let resolver =
        MethodResolver::with_providers(DidKeyProvider, DidWebProvider::with_fetch(fetch.clone()));

    let did: Did = did_web.parse().unwrap();
    Resolve::new(did).perform(&resolver).await.unwrap();
    assert_eq!(fetch.calls(), 1, "did:web must hit the fetcher");
}

#[dialog_common::test]
async fn caching_serves_second_resolution_from_cache() {
    let signer = Signer::from(Ed25519Signer::generate().await.unwrap());
    let did_web = "did:web:cache.example";
    let doc = did_document_multibase(did_web, &signer);
    let fetch = MapFetch::new().with(
        "https://cache.example/.well-known/did.json",
        doc.into_bytes(),
    );
    let cached = CachingResolver::new(DidWebProvider::with_fetch(fetch.clone()));

    let did: Did = did_web.parse().unwrap();
    Resolve::new(did.clone()).perform(&cached).await.unwrap();
    Resolve::new(did).perform(&cached).await.unwrap();

    assert_eq!(fetch.calls(), 1, "second resolution must come from cache");
}

#[dialog_common::test]
async fn caching_remembers_failure_briefly() {
    let fetch = MapFetch::new();
    let cached = CachingResolver::new(DidWebProvider::with_fetch(fetch.clone()));

    let did: Did = "did:web:down.example".parse().unwrap();
    assert!(Resolve::new(did.clone()).perform(&cached).await.is_err());
    assert!(Resolve::new(did).perform(&cached).await.is_err());

    assert_eq!(fetch.calls(), 1, "a cached failure must not refetch");
}

#[test]
fn document_selects_by_fragment() {
    let doc: DidDocument = serde_json::from_str(
        r#"{
            "id": "did:web:frag.example",
            "verificationMethod": [
                { "id": "did:web:frag.example#other", "publicKeyMultibase": "zBadKeyValue" },
                { "id": "did:web:frag.example#key-1", "publicKeyMultibase": "not-a-key" }
            ]
        }"#,
    )
    .unwrap();
    // Selecting a fragment restricts to that method; both keys here are junk, so
    // it fails as an unsupported key rather than silently using another method.
    let err = doc.verifier(Some("key-1")).unwrap_err();
    assert!(
        matches!(err, ResolveError::UnsupportedKey(_)),
        "got {err:?}"
    );
}

#[test]
fn url_derivation_is_wired() {
    assert_eq!(
        did_web_url("did:web:example.com:users:alice").unwrap(),
        "https://example.com/users/alice/did.json"
    );
}

/// Build a self-issued UCAN invocation whose issuer signs under a `did:web`
/// name (via [`WithDid`]), then verify it through a `did:web`-resolving
/// resolver. This is the full loop: an identity SIGNS a UCAN under
/// `did:web:issuer.example`, resolution fetches that identity's DID document,
/// and VERIFICATION recovers the underlying key and checks the
/// header-declared-algorithm signature over the invocation the resolved key
/// produced. It exercises the same `InvocationChain::verify` path (and its
/// `verify_signature` step, which resolves the invocation issuer) the authorizer
/// uses.
async fn signs_and_verifies_under_did_web(key: Signer) {
    use crate::PerformingResolver;
    use dialog_ucan_core::{InvocationBuilder, InvocationChain};
    use std::collections::HashMap;

    // The did:web identity: the underlying key signs, but it presents
    // did:web:issuer.example as its DID.
    let web_did: Did = "did:web:issuer.example".parse().unwrap();
    let issuer = key.clone().with_did(web_did.clone());
    assert_eq!(
        issuer.did(),
        web_did,
        "issuer must present its did:web name"
    );

    // A self-issued invocation: issuer and subject are both did:web:issuer.example,
    // so verification resolves that did:web DID and checks the signature the
    // underlying key produced.
    let invocation = InvocationBuilder::new()
        .issuer(issuer)
        .audience(&web_did)
        .subject(&web_did)
        .command(vec!["storage".to_string(), "get".to_string()])
        .proofs(vec![])
        .try_build()
        .await
        .expect("invocation signed under did:web should build");
    let chain = InvocationChain::new(invocation, HashMap::new());

    // Publish the issuer's key under did:web:issuer.example, served by the mock.
    let doc = did_document_multibase(web_did.as_str(), &key);
    let fetch = MapFetch::new().with(
        "https://issuer.example/.well-known/did.json",
        doc.into_bytes(),
    );
    let env = MethodResolver::with_providers(DidKeyProvider, DidWebProvider::with_fetch(fetch));

    let resolver = PerformingResolver::new(&env);
    chain
        .verify(&resolver)
        .await
        .expect("a did:web-issued invocation should verify");
}

#[dialog_common::test]
async fn it_signs_and_verifies_under_did_web_ed25519() {
    let key = Signer::from(Ed25519Signer::generate().await.unwrap());
    signs_and_verifies_under_did_web(key).await;
}

#[cfg(feature = "es256")]
#[dialog_common::test]
async fn it_signs_and_verifies_under_did_web_p256() {
    let key = Signer::from(Es256Signer::generate().await.unwrap());
    signs_and_verifies_under_did_web(key).await;
}

/// Negative: if the `did:web` document publishes the WRONG key, a UCAN signed
/// under that `did:web` name must NOT verify. Resolution recovers a key that did
/// not produce the signature, so `verify_signature` fails.
#[dialog_common::test]
async fn it_refuses_did_web_with_wrong_key() {
    use crate::PerformingResolver;
    use dialog_ucan_core::{InvocationBuilder, InvocationChain};
    use std::collections::HashMap;

    let web_did: Did = "did:web:issuer.example".parse().unwrap();
    let key = Signer::from(Ed25519Signer::generate().await.unwrap());
    let issuer = key.with_did(web_did.clone());

    let invocation = InvocationBuilder::new()
        .issuer(issuer)
        .audience(&web_did)
        .subject(&web_did)
        .command(vec!["storage".to_string(), "get".to_string()])
        .proofs(vec![])
        .try_build()
        .await
        .expect("invocation should build");
    let chain = InvocationChain::new(invocation, HashMap::new());

    // Publish a DIFFERENT key under did:web:issuer.example.
    let wrong_key = Signer::from(Ed25519Signer::generate().await.unwrap());
    let doc = did_document_multibase(web_did.as_str(), &wrong_key);
    let fetch = MapFetch::new().with(
        "https://issuer.example/.well-known/did.json",
        doc.into_bytes(),
    );
    let env = MethodResolver::with_providers(DidKeyProvider, DidWebProvider::with_fetch(fetch));

    let resolver = PerformingResolver::new(&env);
    assert!(
        chain.verify(&resolver).await.is_err(),
        "a did:web document with the wrong key must not verify"
    );
}
