//! End-to-end `did:mailto` verification tests, built test-vector-first.
//!
//! The DKIM-signed proof emails here are **self-signed** with a known RSA or
//! ed25519 key, and the domain's DKIM key is served through a mocked
//! DNS-over-HTTPS response (no real network). Dropping in a real Gmail `.eml`
//! later means adding a fixture and a `MapFetch` route carrying its real `p=`;
//! no code here changes.

use std::time::Duration;

use base64::Engine;
use dialog_dkim::{BodyCanon, Canonicalization, DkimPublicKey, Header, HeaderCanon, SignedEmail};
use dialog_varsig::{
    AnySignature, Did, Principal, Signer as VarsigSigner, Verifier as VarsigVerifier,
};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::EncodePublicKey;
use rsa::sha2::Sha256;
use rsa::signature::{SignatureEncoding, Signer};
use rsa::{RsaPrivateKey, RsaPublicKey};

use crate::fetch::MapFetch;
use crate::mailto::did::MailtoDid;
use crate::mailto::key_provider::DkimKeyProvider;
use crate::mailto::verifier::{multi_verifier_from_bindings, verify_mailto_proof};

/// The cached 2048-bit RSA key shared across the workspace.
fn rsa_key() -> (SigningKey<Sha256>, DkimPublicKey, String) {
    let der = include_bytes!("../fixtures/rsa_test_2048.pkcs1.der");
    let private_key = RsaPrivateKey::from_pkcs1_der(der).unwrap();
    let public_key = RsaPublicKey::from(&private_key);
    let spki_der = public_key.to_public_key_der().unwrap().as_bytes().to_vec();
    let p = base64::engine::general_purpose::STANDARD.encode(&spki_der);
    (
        SigningKey::<Sha256>::new(private_key),
        DkimPublicKey::rsa_from_spki_der(spki_der),
        p,
    )
}

/// A deterministic ed25519 key and its DNS `p=` value.
fn ed25519_key() -> (ed25519_dalek::SigningKey, DkimPublicKey, String) {
    let signing = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
    let public = signing.verifying_key().to_bytes();
    let p = base64::engine::general_purpose::STANDARD.encode(public);
    (signing, DkimPublicKey::ed25519_from_bytes(public), p)
}

enum InnerKey<'a> {
    Rsa(&'a SigningKey<Sha256>),
    Ed25519(&'a ed25519_dalek::SigningKey),
}

fn header(name: &str, value: &str) -> Header {
    Header {
        name: name.to_string(),
        raw_value: format!(" {value}"),
    }
}

/// Build a DKIM-signed "I am also known as {did:key}" email.
#[allow(clippy::too_many_arguments)]
fn build_binding_eml(
    from: &str,
    subject_did_key: &str,
    domain: &str,
    selector: &str,
    canon: Canonicalization,
    inner: InnerKey<'_>,
) -> Vec<u8> {
    let headers = vec![
        header("From", from),
        header("To", "verify@service.example"),
        header("Subject", &format!("I am also known as {subject_did_key}")),
        header("Date", "Mon, 16 Aug 2026 12:00:00 +0000"),
        header("Message-ID", "<abc123@example.com>"),
    ];
    let signed_names = ["From", "To", "Subject", "Date", "Message-ID"];

    let algorithm = match inner {
        InnerKey::Rsa(_) => "rsa-sha256",
        InnerKey::Ed25519(_) => "ed25519-sha256",
    };
    let canon_str = format!(
        "{}/simple",
        match canon.header {
            HeaderCanon::Simple => "simple",
            HeaderCanon::Relaxed => "relaxed",
        }
    );
    let body = "hello body";
    let body_hash = base64::engine::general_purpose::STANDARD
        .encode(<Sha256 as rsa::sha2::Digest>::digest(body.as_bytes()));
    let h_list = signed_names.join(":");

    let dkim_value_unsigned = format!(
        " v=1; a={algorithm}; c={canon_str}; d={domain}; s={selector};\r\n \
         h={h_list}; bh={body_hash};\r\n b="
    );

    let selected: Vec<Header> = signed_names
        .iter()
        .map(|name| {
            headers
                .iter()
                .find(|h| h.name_eq_ignore_case(name))
                .unwrap()
                .clone()
        })
        .collect();

    let mut signed_data = Vec::new();
    for h in &selected {
        signed_data.extend_from_slice(&canon.canonicalize_header(h));
    }
    signed_data.extend_from_slice(&canon.canonicalize_dkim_signature(&dkim_value_unsigned));

    let b_value = match inner {
        InnerKey::Rsa(key) => {
            base64::engine::general_purpose::STANDARD.encode(key.sign(&signed_data).to_bytes())
        }
        InnerKey::Ed25519(key) => {
            use ed25519_dalek::Signer as _;
            use sha2::{Digest, Sha256 as PlainSha256};
            let digest = PlainSha256::digest(&signed_data);
            base64::engine::general_purpose::STANDARD.encode(key.sign(&digest).to_bytes())
        }
    };
    let dkim_value_signed = format!("{dkim_value_unsigned}{b_value}");

    let mut eml = String::new();
    eml.push_str("DKIM-Signature:");
    eml.push_str(&dkim_value_signed);
    eml.push_str("\r\n");
    for h in &headers {
        eml.push_str(&h.name);
        eml.push(':');
        eml.push_str(&h.raw_value);
        eml.push_str("\r\n");
    }
    eml.push_str("\r\n");
    eml.push_str(body);
    eml.into_bytes()
}

const RELAXED: Canonicalization = Canonicalization {
    header: HeaderCanon::Relaxed,
    body: BodyCanon::Simple,
};

/// A DoH JSON response body carrying the given DKIM record.
fn doh_body(record: &str) -> Vec<u8> {
    format!(r#"{{"Answer":[{{"data":"\"{record}\""}}]}}"#).into_bytes()
}

/// A key provider whose DoH endpoint serves the given `(selector, domain)` key.
fn key_provider_for(selector: &str, domain: &str, record: &str) -> DkimKeyProvider<MapFetch> {
    let url = format!("https://dns.google/resolve?name={selector}._domainkey.{domain}&type=TXT");
    let fetch = MapFetch::new().with(url, doh_body(record));
    DkimKeyProvider::with_fetch(fetch)
}

#[dialog_common::test]
async fn rsa_binding_verifies_and_extracts_key() {
    let (signing, _key, p) = rsa_key();
    let did_key = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    let eml = build_binding_eml(
        "Alice <alice@example.com>",
        did_key,
        "example.com",
        "sel",
        RELAXED,
        InnerKey::Rsa(&signing),
    );
    let proof = SignedEmail::from_raw_eml(&eml).unwrap();
    let provider = key_provider_for("sel", "example.com", &format!("v=DKIM1; k=rsa; p={p}"));
    let identity: Did = "did:mailto:example.com:alice".parse().unwrap();

    let binding = verify_mailto_proof(&identity, &proof, &provider)
        .await
        .unwrap();
    assert_eq!(binding.authorized_key.did().as_str(), did_key);
    assert_eq!(binding.identity, identity);
}

#[dialog_common::test]
async fn ed25519_binding_verifies() {
    let (signing, _key, p) = ed25519_key();
    let did_key = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    let eml = build_binding_eml(
        "alice@example.com",
        did_key,
        "example.com",
        "sel",
        RELAXED,
        InnerKey::Ed25519(&signing),
    );
    let proof = SignedEmail::from_raw_eml(&eml).unwrap();
    let provider = key_provider_for("sel", "example.com", &format!("v=DKIM1; k=ed25519; p={p}"));
    let identity: Did = "did:mailto:example.com:alice".parse().unwrap();

    verify_mailto_proof(&identity, &proof, &provider)
        .await
        .unwrap();
}

#[dialog_common::test]
async fn simple_canonicalization_binding_verifies() {
    let (signing, _key, p) = rsa_key();
    let did_key = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    let eml = build_binding_eml(
        "alice@example.com",
        did_key,
        "example.com",
        "sel",
        Canonicalization {
            header: HeaderCanon::Simple,
            body: BodyCanon::Simple,
        },
        InnerKey::Rsa(&signing),
    );
    let proof = SignedEmail::from_raw_eml(&eml).unwrap();
    let provider = key_provider_for("sel", "example.com", &format!("v=DKIM1; k=rsa; p={p}"));
    let identity: Did = "did:mailto:example.com:alice".parse().unwrap();
    verify_mailto_proof(&identity, &proof, &provider)
        .await
        .unwrap();
}

#[dialog_common::test]
async fn tampered_subject_is_rejected() {
    let (signing, _key, p) = rsa_key();
    let did_key = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    let eml = build_binding_eml(
        "alice@example.com",
        did_key,
        "example.com",
        "sel",
        RELAXED,
        InnerKey::Rsa(&signing),
    );
    // Swap the bound did:key in the (signed) subject after signing.
    let tampered = String::from_utf8(eml).unwrap().replace(
        did_key,
        "did:key:z6MkOTHERKEYtampered000000000000000000000000000",
    );
    let proof = SignedEmail::from_raw_eml(tampered.as_bytes()).unwrap();
    let provider = key_provider_for("sel", "example.com", &format!("v=DKIM1; k=rsa; p={p}"));
    let identity: Did = "did:mailto:example.com:alice".parse().unwrap();
    assert!(
        verify_mailto_proof(&identity, &proof, &provider)
            .await
            .is_err()
    );
}

#[dialog_common::test]
async fn mismatched_domain_is_rejected() {
    let (signing, _key, p) = rsa_key();
    let did_key = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    // The email is from example.com but the identity claims other.com.
    let eml = build_binding_eml(
        "alice@example.com",
        did_key,
        "example.com",
        "sel",
        RELAXED,
        InnerKey::Rsa(&signing),
    );
    let proof = SignedEmail::from_raw_eml(&eml).unwrap();
    let provider = key_provider_for("sel", "example.com", &format!("v=DKIM1; k=rsa; p={p}"));
    let identity: Did = "did:mailto:other.com:alice".parse().unwrap();
    assert!(
        verify_mailto_proof(&identity, &proof, &provider)
            .await
            .is_err()
    );
}

#[dialog_common::test]
async fn mismatched_from_local_is_rejected() {
    let (signing, _key, p) = rsa_key();
    let did_key = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    // Signed From is bob@, but the identity is alice@.
    let eml = build_binding_eml(
        "bob@example.com",
        did_key,
        "example.com",
        "sel",
        RELAXED,
        InnerKey::Rsa(&signing),
    );
    let proof = SignedEmail::from_raw_eml(&eml).unwrap();
    let provider = key_provider_for("sel", "example.com", &format!("v=DKIM1; k=rsa; p={p}"));
    let identity: Did = "did:mailto:example.com:alice".parse().unwrap();
    assert!(
        verify_mailto_proof(&identity, &proof, &provider)
            .await
            .is_err()
    );
}

#[dialog_common::test]
async fn non_binding_subject_is_rejected() {
    let (signing, _key, p) = rsa_key();
    // A well-formed DKIM email whose subject is not the binding template.
    let eml = build_binding_eml(
        "alice@example.com",
        // build_binding_eml wraps this in "I am also known as ..."; use a
        // non-did:key value to make the extracted token invalid.
        "not-a-did-key",
        "example.com",
        "sel",
        RELAXED,
        InnerKey::Rsa(&signing),
    );
    let proof = SignedEmail::from_raw_eml(&eml).unwrap();
    let provider = key_provider_for("sel", "example.com", &format!("v=DKIM1; k=rsa; p={p}"));
    let identity: Did = "did:mailto:example.com:alice".parse().unwrap();
    assert!(
        verify_mailto_proof(&identity, &proof, &provider)
            .await
            .is_err()
    );
}

#[dialog_common::test]
async fn dkim_key_lookup_is_cached() {
    let (signing, _key, p) = rsa_key();
    let did_key = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    let eml = build_binding_eml(
        "alice@example.com",
        did_key,
        "example.com",
        "sel",
        RELAXED,
        InnerKey::Rsa(&signing),
    );
    let proof = SignedEmail::from_raw_eml(&eml).unwrap();

    let url = "https://dns.google/resolve?name=sel._domainkey.example.com&type=TXT";
    // MapFetch is Clone with a shared call counter (Arc), so the clone handed to
    // the provider and this handle observe the same count.
    let fetch = MapFetch::new().with(url, doh_body(&format!("v=DKIM1; k=rsa; p={p}")));
    let fetch_handle = fetch.clone();
    let provider = DkimKeyProvider::with_fetch(fetch).with_ttl(Duration::from_secs(3600));
    let identity: Did = "did:mailto:example.com:alice".parse().unwrap();

    verify_mailto_proof(&identity, &proof, &provider)
        .await
        .unwrap();
    verify_mailto_proof(&identity, &proof, &provider)
        .await
        .unwrap();
    // The second verification hit the cache, so only one DoH fetch happened.
    assert_eq!(fetch_handle.calls(), 1);
}

#[dialog_common::test]
async fn multiple_bindings_compose_into_multiverifier() {
    // Two "I am also known as" emails naming two different keys yield a
    // MultiVerifier that accepts a signature from either key.
    let (signing, _key, p) = rsa_key();
    let identity: Did = "did:mailto:example.com:alice".parse().unwrap();
    let provider = key_provider_for("sel", "example.com", &format!("v=DKIM1; k=rsa; p={p}"));

    // First bound key: a real ed25519 signer whose signature we will check.
    let bound_signer = dialog_credentials::Signer::from(
        dialog_credentials::Ed25519Signer::generate().await.unwrap(),
    );
    let bound_did = bound_signer.did();

    let eml1 = build_binding_eml(
        "alice@example.com",
        bound_did.as_str(),
        "example.com",
        "sel",
        RELAXED,
        InnerKey::Rsa(&signing),
    );
    let proof1 = SignedEmail::from_raw_eml(&eml1).unwrap();
    let binding1 = verify_mailto_proof(&identity, &proof1, &provider)
        .await
        .unwrap();

    // A second, unrelated bound key (not used to sign, just to grow the set).
    let other_did = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    let eml2 = build_binding_eml(
        "alice@example.com",
        other_did,
        "example.com",
        "sel",
        RELAXED,
        InnerKey::Rsa(&signing),
    );
    let proof2 = SignedEmail::from_raw_eml(&eml2).unwrap();
    let binding2 = verify_mailto_proof(&identity, &proof2, &provider)
        .await
        .unwrap();

    let multi = multi_verifier_from_bindings(&identity, vec![binding1, binding2]).unwrap();
    assert_eq!(multi.keys().len(), 2);
    assert_eq!(multi.did(), identity);

    // A UCAN-style signature from the first bound key verifies through the set.
    let msg = b"a payload the bound key signs";
    let sig: AnySignature = VarsigSigner::sign(&bound_signer, msg).await.unwrap();
    multi.verify(msg, &sig).await.unwrap();
}

#[dialog_common::test]
fn did_mailto_parses() {
    let did = MailtoDid::parse("did:mailto:example.com:alice").unwrap();
    assert_eq!(did.email(), "alice@example.com");
}
