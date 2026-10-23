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

/// Build a binding email that signs **two** `Subject:` headers, the shape an
/// oversigning signer (`h=...:subject:subject`) produces.
///
/// `displayed` is the top-most Subject, which is what a mail client shows the
/// sender. `signed_first` is the one below it, which RFC 6376 section 5.4.2's
/// bottom-up selection puts first in the signed set.
fn build_doubled_subject_eml(
    from: &str,
    displayed: &str,
    signed_first: &str,
    domain: &str,
    selector: &str,
    signing: &SigningKey<Sha256>,
) -> Vec<u8> {
    let headers = vec![
        header("From", from),
        header("Subject", displayed),
        header("Subject", signed_first),
        header("Date", "Mon, 16 Aug 2026 12:00:00 +0000"),
    ];
    let signed_names = ["From", "Subject", "Subject", "Date"];

    let body = "hello body";
    let body_hash = base64::engine::general_purpose::STANDARD
        .encode(<Sha256 as rsa::sha2::Digest>::digest(body.as_bytes()));
    let h_list = signed_names.join(":");
    let dkim_value_unsigned = format!(
        " v=1; a=rsa-sha256; c=relaxed/simple; d={domain}; s={selector};\r\n \
         h={h_list}; bh={body_hash};\r\n b="
    );

    // Bottom-up selection with a per-name consumed count, matching the verifier.
    let mut consumed: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let selected: Vec<Header> = signed_names
        .iter()
        .map(|name| {
            let already = consumed.entry(name.to_ascii_lowercase()).or_insert(0);
            let found = headers
                .iter()
                .rev()
                .filter(|h| h.name_eq_ignore_case(name))
                .nth(*already)
                .unwrap()
                .clone();
            *already += 1;
            found
        })
        .collect();

    let mut signed_data = Vec::new();
    for h in &selected {
        signed_data.extend_from_slice(&RELAXED.canonicalize_header(h));
    }
    signed_data.extend_from_slice(&RELAXED.canonicalize_dkim_signature(&dkim_value_unsigned));

    let b_value =
        base64::engine::general_purpose::STANDARD.encode(signing.sign(&signed_data).to_bytes());
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

/// A proof signing two `Subject:` headers must be refused, because which one
/// is authoritative diverges between the verifier and the human.
///
/// RFC 6376 section 5.4.2 consumes repeated `h=` names bottom-up, so the first
/// signed `Subject:` is the bottom-most header, while every mail client
/// displays the top-most. Picking by first match would therefore authorize a
/// key from a subject the sender never read. Oversigning `h=...:subject:subject`
/// is standard anti-replay practice (RFC 6376 section 8.15), so this is a
/// normal signer configuration rather than an exotic one.
///
/// The DKIM signature itself is valid here: the signer really did sign both
/// subjects. The refusal belongs at the binding layer, which is what this pins.
#[dialog_common::test]
async fn duplicate_signed_subject_is_refused() {
    let (signing, _key, p) = rsa_key();
    let attacker_key = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";

    let eml = build_doubled_subject_eml(
        "Alice <alice@example.com>",
        // What Alice saw in her mail client.
        "Hello, this is a totally normal email",
        // What first-match selection would have read instead.
        &format!("I am also known as {attacker_key}"),
        "example.com",
        "sel",
        &signing,
    );

    let proof = SignedEmail::from_raw_eml(&eml).unwrap();

    // Confirm the divergence is real: the first signed Subject is the one Alice
    // never saw.
    let first_subject = proof
        .signed_headers
        .iter()
        .find(|h| h.name_eq_ignore_case("subject"))
        .expect("a signed Subject");
    assert!(
        first_subject.raw_value.contains("I am also known as"),
        "bottom-up selection must put the hidden Subject first"
    );

    let provider = key_provider_for("sel", "example.com", &format!("v=DKIM1; k=rsa; p={p}"));
    let identity: Did = "did:mailto:example.com:alice".parse().unwrap();

    let outcome = verify_mailto_proof(&identity, &proof, &provider).await;
    assert!(
        outcome.is_err(),
        "a proof signing two Subject: headers must be refused: the header the \
         verifier reads is not the one the sender saw"
    );
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

/// Replaying a captured proof is inert, because the subject names the audience.
///
/// A DKIM signature is public and permanent: it rides in headers every relay,
/// the recipient's provider, and anyone the mail is forwarded to can read. So
/// the proof *will* be replayed, and the question is what a replayer gains.
///
/// The answer is nothing, because the binding's output is the `did:key` written
/// in the signed subject. A replayer cannot substitute their own key, since
/// changing the subject breaks `b=`; whoever presents the proof, to whichever
/// relying party, it authorizes the same key it always named. Re-delivering it
/// only restates a true statement.
///
/// This is why `did:mailto` needs no nonce or audience binding: the subject
/// *is* the audience. It is a structural property worth pinning, because
/// removing the key from the subject (say, moving it to an unsigned header)
/// would silently turn a public, permanent proof into a bearer token.
#[dialog_common::test]
async fn a_replayed_proof_still_authorizes_only_the_subjects_key() {
    let (signing, _key, p) = rsa_key();
    let alice_key = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    let eml = build_binding_eml(
        "alice@example.com",
        alice_key,
        "example.com",
        "sel",
        RELAXED,
        InnerKey::Rsa(&signing),
    );

    // Whoever holds the bytes can verify them: the proof is public material.
    let proof = SignedEmail::from_raw_eml(&eml).unwrap();
    let provider = key_provider_for("sel", "example.com", &format!("v=DKIM1; k=rsa; p={p}"));
    let identity: Did = "did:mailto:example.com:alice".parse().unwrap();

    // A third party replays it to a relying party that never received the mail.
    let binding = verify_mailto_proof(&identity, &proof, &provider)
        .await
        .expect("a captured proof verifies for anyone holding it");

    // And what they get is Alice's key, for Alice's identity. The replayer
    // gained no authority they did not already have by reading the email.
    assert_eq!(binding.authorized_key.did().as_str(), alice_key);
    assert_eq!(binding.identity, identity);
}

/// The corollary: the key in the subject cannot be swapped for an attacker's.
///
/// The subject is inside `h=`, so `b=` covers it. Rewriting the key to one the
/// attacker controls invalidates the signature, which is what makes the
/// audience binding above load-bearing rather than incidental.
#[dialog_common::test]
async fn rewriting_the_subject_key_breaks_the_signature() {
    let (signing, _key, p) = rsa_key();
    let alice_key = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    let eml = build_binding_eml(
        "alice@example.com",
        alice_key,
        "example.com",
        "sel",
        RELAXED,
        InnerKey::Rsa(&signing),
    );

    // Swap the subject's key for the attacker's, leaving everything else.
    let attacker_key = "did:key:z6MkfQhLHBSFMuR7bQXTQeqe5kYUW51HpfZeaymgy1zkP2jM";
    let tampered = String::from_utf8(eml)
        .unwrap()
        .replace(alice_key, attacker_key);

    let proof = SignedEmail::from_raw_eml(tampered.as_bytes()).unwrap();
    let provider = key_provider_for("sel", "example.com", &format!("v=DKIM1; k=rsa; p={p}"));
    let identity: Did = "did:mailto:example.com:alice".parse().unwrap();

    assert!(
        verify_mailto_proof(&identity, &proof, &provider)
            .await
            .is_err(),
        "the subject is signed, so substituting the audience key must not verify"
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

/// The DKIM key cache must not grow without bound on attacker-chosen keys.
///
/// The `(selector, domain)` pair comes from the proof's `s=`/`d=` tags, and any
/// real DKIM record on the internet (every provider selector times every
/// domain) makes a valid entry, so a hostile stream of proofs can otherwise pin
/// unbounded memory in the verifying server. This pins that the map stays
/// inside its bound while resolving far more distinct pairs than it can hold.
#[dialog_common::test]
async fn dkim_key_cache_is_bounded() {
    const CAPACITY: usize = 4;

    let (_signing, _key, p) = rsa_key();
    let record = format!("v=DKIM1; k=rsa; p={p}");

    // One MapFetch serving a distinct DoH route per (selector, domain) pair.
    let mut fetch = MapFetch::new();
    for i in 0..(CAPACITY * 10) {
        let url =
            format!("https://dns.google/resolve?name=s{i}._domainkey.host{i}.example&type=TXT");
        fetch = fetch.with(url, doh_body(&record));
    }
    let provider = DkimKeyProvider::with_fetch(fetch).with_capacity(CAPACITY);

    for i in 0..(CAPACITY * 10) {
        provider
            .resolve_key(&format!("s{i}"), &format!("host{i}.example"))
            .await
            .unwrap();
    }

    assert!(
        provider.cached_keys() <= CAPACITY,
        "the key cache must stay within its bound, held {} with a cap of {CAPACITY}",
        provider.cached_keys()
    );
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
