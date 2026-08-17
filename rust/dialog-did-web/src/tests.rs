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

/// A did.json naming two verification methods, `#key-1` and `#key-2`, keyed by
/// two signers' `publicKeyMultibase`.
fn did_document_two_keys(did_web: &str, first: &Signer, second: &Signer) -> String {
    format!(
        r#"{{
            "id": "{did_web}",
            "verificationMethod": [
                {{
                    "id": "{did_web}#key-1",
                    "type": "Multikey",
                    "controller": "{did_web}",
                    "publicKeyMultibase": "{first}"
                }},
                {{
                    "id": "{did_web}#key-2",
                    "type": "Multikey",
                    "controller": "{did_web}",
                    "publicKeyMultibase": "{second}"
                }}
            ]
        }}"#,
        first = multibase_of(first),
        second = multibase_of(second),
    )
}

/// A did:web document with two keys must verify a signature made with EITHER
/// key. This is the multi-key any-match fix: before it, only the first key's
/// signatures verified, so a signature by the SECOND key was refused.
#[dialog_common::test]
async fn it_verifies_a_signature_by_any_document_key() {
    let key1 = Signer::from(Ed25519Signer::generate().await.unwrap());
    let key2 = Signer::from(Es256Signer::generate().await.unwrap());
    let did_web = "did:web:multi.example";
    let doc = did_document_two_keys(did_web, &key1, &key2);
    let fetch = MapFetch::new().with(
        "https://multi.example/.well-known/did.json",
        doc.into_bytes(),
    );
    let provider = DidWebProvider::with_fetch(fetch);

    let did: Did = did_web.parse().unwrap();
    let verifier = Resolve::new(did).perform(&provider).await.unwrap();

    // The verifier reports the did:web DID, not either member key's did:key.
    assert_eq!(verifier.did().as_str(), did_web);

    let msg = b"any key may have signed";

    // A signature by the SECOND key must verify (the bug being fixed).
    let sig2 = VarsigSigner::sign(&key2, msg).await.unwrap();
    verifier.verify(msg, &sig2).await.unwrap();

    // A signature by the FIRST key must also verify.
    let sig1 = VarsigSigner::sign(&key1, msg).await.unwrap();
    verifier.verify(msg, &sig1).await.unwrap();
}

/// A signature made with a key that is NOT in the document must be refused,
/// even though the document names two other keys.
#[dialog_common::test]
async fn it_refuses_a_signature_by_a_key_not_in_the_document() {
    let key1 = Signer::from(Ed25519Signer::generate().await.unwrap());
    let key2 = Signer::from(Ed25519Signer::generate().await.unwrap());
    let did_web = "did:web:strangers.example";
    let doc = did_document_two_keys(did_web, &key1, &key2);
    let fetch = MapFetch::new().with(
        "https://strangers.example/.well-known/did.json",
        doc.into_bytes(),
    );
    let provider = DidWebProvider::with_fetch(fetch);

    let did: Did = did_web.parse().unwrap();
    let verifier = Resolve::new(did).perform(&provider).await.unwrap();

    let outsider = Signer::from(Ed25519Signer::generate().await.unwrap());
    let msg = b"i am not in the document";
    let sig = VarsigSigner::sign(&outsider, msg).await.unwrap();
    assert!(
        verifier.verify(msg, &sig).await.is_err(),
        "a signature by a key absent from the document must not verify"
    );
}

/// A `#fragment` in the resolved DID selects exactly that verification method:
/// the kid-hint path. A signature by `key-2` verifies through `#key-2`, while a
/// signature by `key-1` is refused (its key is not in the selected set).
#[dialog_common::test]
async fn it_selects_a_single_key_by_fragment() {
    let key1 = Signer::from(Ed25519Signer::generate().await.unwrap());
    let key2 = Signer::from(Ed25519Signer::generate().await.unwrap());
    let did_web = "did:web:pick.example";
    let doc = did_document_two_keys(did_web, &key1, &key2);
    let fetch = MapFetch::new().with(
        "https://pick.example/.well-known/did.json",
        doc.into_bytes(),
    );
    let provider = DidWebProvider::with_fetch(fetch);

    // Resolve did:web:pick.example#key-2: only key-2 is in the verifier.
    let did: Did = format!("{did_web}#key-2").parse().unwrap();
    let verifier = Resolve::new(did).perform(&provider).await.unwrap();

    // The verifier's identity is the base did:web DID (fragment stripped).
    assert_eq!(verifier.did().as_str(), did_web);

    let msg = b"selected by fragment";
    let sig2 = VarsigSigner::sign(&key2, msg).await.unwrap();
    verifier.verify(msg, &sig2).await.unwrap();

    let sig1 = VarsigSigner::sign(&key1, msg).await.unwrap();
    assert!(
        verifier.verify(msg, &sig1).await.is_err(),
        "a fragment-selected verifier must refuse a signature by another key"
    );
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

/// The resolved document's `id` must equal the DID being resolved. Here the
/// document served for `did:web:victim.example` claims to be
/// `did:web:attacker.example` and carries the attacker's key. A correct
/// resolver refuses (the document is not this DID's document); without the
/// check it binds the attacker's key to the victim DID, so any signature the
/// attacker produces would verify as the victim. Combined with the URL-injection
/// surface (see `url.rs`), a redirecting or shared host makes this reachable
/// from remote input.
///
/// did:web spec: resolution MUST confirm the document `id` matches the DID.
#[dialog_common::test]
async fn it_refuses_document_whose_id_is_a_different_did() {
    let attacker = Signer::from(Ed25519Signer::generate().await.unwrap());
    let victim_did = "did:web:victim.example";
    // The document is served at the victim's URL but its `id` (and its only
    // key) belong to the attacker.
    let doc = did_document_multibase("did:web:attacker.example", &attacker);
    let fetch = MapFetch::new().with(
        "https://victim.example/.well-known/did.json",
        doc.into_bytes(),
    );
    let provider = DidWebProvider::with_fetch(fetch);

    let did: Did = victim_did.parse().unwrap();
    let outcome = Resolve::new(did).perform(&provider).await;

    // Must refuse: the served document is not the victim DID's document.
    let Err(err) = &outcome else {
        let verifier = outcome.unwrap();
        let msg = b"attacker speaks as victim";
        let sig = VarsigSigner::sign(&attacker, msg).await.unwrap();
        let forged = verifier.verify(msg, &sig).await.is_ok();
        panic!(
            "resolving {victim_did} accepted a document with id=did:web:attacker.example; \
             attacker key verifies as victim = {forged}"
        );
    };
    assert!(
        matches!(err, ResolveError::MalformedDocument(_)),
        "expected an id-mismatch refusal, got {err:?}"
    );
}

/// A verification method whose `controller` is a *different* DID must not
/// contribute a key to this DID's verifier. The document for
/// `did:web:subject.example` lists a method controlled by
/// `did:web:other.example`; a correct resolver excludes it (the subject never
/// authorized that key), so a signature by that key is refused.
#[dialog_common::test]
async fn it_excludes_a_method_controlled_by_another_did() {
    let foreign = Signer::from(Ed25519Signer::generate().await.unwrap());
    let subject_did = "did:web:subject.example";
    let doc = format!(
        r#"{{
            "id": "{subject_did}",
            "verificationMethod": [{{
                "id": "{subject_did}#key-1",
                "type": "Multikey",
                "controller": "did:web:other.example",
                "publicKeyMultibase": "{key}"
            }}]
        }}"#,
        key = multibase_of(&foreign)
    );
    let fetch = MapFetch::new().with(
        "https://subject.example/.well-known/did.json",
        doc.into_bytes(),
    );
    let provider = DidWebProvider::with_fetch(fetch);

    let did: Did = subject_did.parse().unwrap();
    let outcome = Resolve::new(did).perform(&provider).await;

    // The only key is controlled by another DID, so no usable key remains.
    match outcome {
        Err(ResolveError::NoSupportedVerificationMethod) => {}
        Err(other) => panic!("expected NoSupportedVerificationMethod, got {other:?}"),
        Ok(verifier) => {
            let msg = b"foreign-controlled key signs";
            let sig = VarsigSigner::sign(&foreign, msg).await.unwrap();
            assert!(
                verifier.verify(msg, &sig).await.is_err(),
                "a method controlled by did:web:other.example must not verify for {subject_did}"
            );
        }
    }
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
    let did: Did = "did:web:frag.example".parse().unwrap();
    let err = doc.verifier(&did, Some("key-1")).unwrap_err();
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
