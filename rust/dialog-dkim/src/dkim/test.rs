//! DKIM verification tests, built test-vector-first.
//!
//! A real Gmail `.eml` is not available yet, so the vectors here are
//! **self-signed**: a known RSA (and ed25519) key signs a constructed
//! DKIM email, and the verify path checks the signature against that same key.
//! This exercises canonicalization, header reconstruction, and inner-signature
//! verification end to end against a signature produced with a known key.
//!
//! # Dropping in a real `.eml` later
//!
//! When a real DKIM-signed email arrives, add its bytes as a fixture file under
//! `fixtures/` and its domain key (the DNS `p=` value) and call
//! [`run_full_verify`] with `(raw_eml_bytes, key)`. No code here needs to
//! change: [`run_full_verify`] is the exact `(raw_eml, domain_key) -> result`
//! entry point a real vector plugs into. See [`real_eml_drop_in_point`] for the
//! placeholder test that becomes the real one.

#![cfg(feature = "dkim")]

use super::canonicalize::{Canonicalization, HeaderCanon};
use super::error::DkimError;
use super::key::DkimPublicKey;
use super::message::Header;
use super::signature::DkimSignatureHeader;
use super::verify::{SignedEmail, verify, verify_with_key};

use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::EncodePublicKey;
use rsa::sha2::Sha256;
use rsa::signature::{SignatureEncoding, Signer};
use rsa::{RsaPrivateKey, RsaPublicKey};

/// The cached 2048-bit RSA key shared with the rest of the workspace. Generating
/// one per test is far too slow.
fn rsa_signing_key() -> (SigningKey<Sha256>, DkimPublicKey) {
    let der = include_bytes!("../../fixtures/test_2048.pkcs1.der");
    let private_key = RsaPrivateKey::from_pkcs1_der(der).unwrap();
    let public_key = RsaPublicKey::from(&private_key);
    let spki_der = public_key.to_public_key_der().unwrap().as_bytes().to_vec();
    (
        SigningKey::<Sha256>::new(private_key),
        DkimPublicKey::rsa_from_spki_der(spki_der),
    )
}

/// A deterministic ed25519 key pair for ed25519-sha256 vectors.
fn ed25519_signing_key() -> (ed25519_dalek::SigningKey, DkimPublicKey) {
    let signing = ed25519_dalek::SigningKey::from_bytes(&[42u8; 32]);
    let public = signing.verifying_key().to_bytes();
    (signing, DkimPublicKey::ed25519_from_bytes(public))
}

/// A header as it would appear in the email (name plus raw value, the raw value
/// beginning with the space after the colon).
fn header(name: &str, value: &str) -> Header {
    Header {
        name: name.to_string(),
        raw_value: format!(" {value}"),
    }
}

/// The inner algorithm to sign a generated vector with.
enum InnerKey<'a> {
    Rsa(&'a SigningKey<Sha256>),
    Ed25519(&'a ed25519_dalek::SigningKey),
}

/// Build a complete DKIM-signed raw email from a set of headers, signing with
/// the given key and canonicalization. Returns the raw `.eml` bytes.
///
/// This is the test-vector generator: it reproduces the signer's side of RFC
/// 6376 (canonicalize the signed headers, canonicalize the DKIM-Signature header
/// with an empty `b=`, sign, then fill `b=`), so a successful round-trip through
/// [`verify`] proves the verifier reconstructs exactly what a signer produced.
fn build_signed_eml(
    headers: &[Header],
    signed_header_names: &[&str],
    domain: &str,
    selector: &str,
    canon: Canonicalization,
    inner: InnerKey<'_>,
    body: &str,
) -> Vec<u8> {
    let algorithm = match inner {
        InnerKey::Rsa(_) => "rsa-sha256",
        InnerKey::Ed25519(_) => "ed25519-sha256",
    };
    let canon_str = format!(
        "{}/{}",
        match canon.header {
            HeaderCanon::Simple => "simple",
            HeaderCanon::Relaxed => "relaxed",
        },
        "simple"
    );

    // A plausible bh=; its exact value is irrelevant to b= verification because
    // the body is never re-hashed, but it must be present and stable.
    let body_hash = base64::engine::general_purpose::STANDARD
        .encode(<Sha256 as rsa::sha2::Digest>::digest(body.as_bytes()));

    let h_list = signed_header_names.join(":");

    // The DKIM-Signature value WITH an empty b= (what the signer hashes over).
    let dkim_value_unsigned = format!(
        " v=1; a={algorithm}; c={canon_str}; d={domain}; s={selector};\r\n \
         h={h_list}; bh={body_hash};\r\n b="
    );

    // Reconstruct the signed data exactly as the verifier will: selected signed
    // headers canonicalized, then the DKIM-Signature header (empty b=) with no
    // trailing CRLF.
    let selected: Vec<Header> = signed_header_names
        .iter()
        .map(|name| {
            headers
                .iter()
                .find(|h| h.name_eq_ignore_case(name))
                .expect("signed header present")
                .clone()
        })
        .collect();

    let mut signed_data = Vec::new();
    for h in &selected {
        signed_data.extend_from_slice(&canon.canonicalize_header(h));
    }
    signed_data.extend_from_slice(&canon.canonicalize_dkim_signature(&dkim_value_unsigned));

    // Sign and base64 the signature.
    let b_value = match inner {
        InnerKey::Rsa(key) => {
            let sig = key.sign(&signed_data);
            base64::engine::general_purpose::STANDARD.encode(sig.to_bytes())
        }
        InnerKey::Ed25519(key) => {
            use ed25519_dalek::Signer as _;
            use sha2::{Digest, Sha256 as PlainSha256};
            let digest = PlainSha256::digest(&signed_data);
            let sig = key.sign(&digest);
            base64::engine::general_purpose::STANDARD.encode(sig.to_bytes())
        }
    };

    // The final DKIM-Signature header value, now with b= filled in. The bytes
    // before b= must be byte-identical to `dkim_value_unsigned` for the empty-b
    // reconstruction to match, so we append the signature after the trailing
    // `b=`.
    let dkim_value_signed = format!("{dkim_value_unsigned}{b_value}");

    // Assemble the raw email: DKIM-Signature first, then the other headers, a
    // blank line, and the body.
    let mut eml = String::new();
    eml.push_str("DKIM-Signature:");
    eml.push_str(&dkim_value_signed);
    eml.push_str("\r\n");
    for h in headers {
        eml.push_str(&h.name);
        eml.push(':');
        eml.push_str(&h.raw_value);
        eml.push_str("\r\n");
    }
    eml.push_str("\r\n");
    eml.push_str(body);
    eml.into_bytes()
}

/// The drop-in entry point for a real `.eml`: parse, extract the proof, and
/// verify against the given domain key. A real Gmail vector needs only to call
/// this with `(raw_eml_bytes, key)`.
///
/// # Errors
///
/// Propagates any [`DkimError`] from parsing or verification.
pub fn run_full_verify(
    raw_eml: &[u8],
    key: &DkimPublicKey,
) -> Result<DkimSignatureHeader, DkimError> {
    verify(raw_eml, key)
}

fn sample_headers() -> Vec<Header> {
    vec![
        header("From", "Alice <alice@example.com>"),
        header("To", "verify@service.example"),
        header(
            "Subject",
            "I am also known as did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
        ),
        header("Date", "Mon, 16 Aug 2026 12:00:00 +0000"),
        header("Message-ID", "<abc123@example.com>"),
    ]
}

const SIGNED_NAMES: &[&str] = &["From", "To", "Subject", "Date", "Message-ID"];

#[test]
fn rsa_sha256_relaxed_verifies() {
    let (signing, key) = rsa_signing_key();
    let headers = sample_headers();
    let eml = build_signed_eml(
        &headers,
        SIGNED_NAMES,
        "example.com",
        "sel",
        Canonicalization {
            header: HeaderCanon::Relaxed,
            body: super::canonicalize::BodyCanon::Simple,
        },
        InnerKey::Rsa(&signing),
        "hello body",
    );
    let sig = verify(&eml, &key).unwrap();
    assert_eq!(sig.domain, "example.com");
    assert_eq!(sig.selector, "sel");
}

#[test]
fn rsa_sha256_simple_verifies() {
    let (signing, key) = rsa_signing_key();
    let headers = sample_headers();
    let eml = build_signed_eml(
        &headers,
        SIGNED_NAMES,
        "example.com",
        "sel",
        Canonicalization {
            header: HeaderCanon::Simple,
            body: super::canonicalize::BodyCanon::Simple,
        },
        InnerKey::Rsa(&signing),
        "hello body",
    );
    verify(&eml, &key).unwrap();
}

#[test]
fn ed25519_sha256_relaxed_verifies() {
    let (signing, key) = ed25519_signing_key();
    let headers = sample_headers();
    let eml = build_signed_eml(
        &headers,
        SIGNED_NAMES,
        "example.com",
        "sel",
        Canonicalization {
            header: HeaderCanon::Relaxed,
            body: super::canonicalize::BodyCanon::Simple,
        },
        InnerKey::Ed25519(&signing),
        "hello body",
    );
    verify(&eml, &key).unwrap();
}

#[test]
fn ed25519_sha256_simple_verifies() {
    let (signing, key) = ed25519_signing_key();
    let headers = sample_headers();
    let eml = build_signed_eml(
        &headers,
        SIGNED_NAMES,
        "example.com",
        "sel",
        Canonicalization {
            header: HeaderCanon::Simple,
            body: super::canonicalize::BodyCanon::Simple,
        },
        InnerKey::Ed25519(&signing),
        "hello body",
    );
    verify(&eml, &key).unwrap();
}

#[test]
fn tampered_signed_header_fails() {
    let (signing, key) = rsa_signing_key();
    let headers = sample_headers();
    let eml = build_signed_eml(
        &headers,
        SIGNED_NAMES,
        "example.com",
        "sel",
        Canonicalization {
            header: HeaderCanon::Relaxed,
            body: super::canonicalize::BodyCanon::Simple,
        },
        InnerKey::Rsa(&signing),
        "hello body",
    );
    // Flip a byte in the signed Subject value.
    let tampered = String::from_utf8(eml)
        .unwrap()
        .replace("I am also known as", "I am ALSO known as");
    let outcome = verify(tampered.as_bytes(), &key);
    assert!(matches!(outcome, Err(DkimError::VerificationFailed)));
}

#[test]
fn wrong_key_fails() {
    let (signing, _correct) = rsa_signing_key();
    let (_other_signing, wrong_key) = ed25519_signing_key();
    let headers = sample_headers();
    let eml = build_signed_eml(
        &headers,
        SIGNED_NAMES,
        "example.com",
        "sel",
        Canonicalization {
            header: HeaderCanon::Relaxed,
            body: super::canonicalize::BodyCanon::Simple,
        },
        InnerKey::Rsa(&signing),
        "hello body",
    );
    // An ed25519 key cannot verify an rsa-sha256 signature: algorithm mismatch.
    assert!(matches!(
        verify(&eml, &wrong_key),
        Err(DkimError::KeyAlgorithmMismatch)
    ));
}

#[test]
fn captured_proof_verifies_offline() {
    // The portable proof (no body) verifies with only the domain key.
    let (signing, key) = rsa_signing_key();
    let headers = sample_headers();
    let eml = build_signed_eml(
        &headers,
        SIGNED_NAMES,
        "example.com",
        "sel",
        Canonicalization {
            header: HeaderCanon::Relaxed,
            body: super::canonicalize::BodyCanon::Simple,
        },
        InnerKey::Rsa(&signing),
        "a body that the proof does not carry",
    );
    let proof = SignedEmail::from_raw_eml(&eml).unwrap();
    // The proof carries the signed headers but not the body.
    assert!(proof.signed_headers.iter().any(|h| h.name == "Subject"));
    verify_with_key(&proof, &key).unwrap();
}

#[test]
fn dns_record_roundtrips_ed25519_key() {
    // The DNS p= form of an ed25519 key parses back to the same key that
    // verifies a signature from its private half.
    let (signing, direct_key) = ed25519_signing_key();
    let p = base64::engine::general_purpose::STANDARD.encode(signing.verifying_key().to_bytes());
    let record = format!("v=DKIM1; k=ed25519; p={p}");
    let parsed = DkimPublicKey::from_dns_txt(&record).unwrap();
    assert_eq!(parsed, direct_key);
}

/// Build a signed email whose `DKIM-Signature` carries extra tags (`extra_tags`,
/// e.g. `"t=1000; x=2000;"`) inserted into the signed header block, so the tags
/// are covered by `b=`. Mirrors [`build_signed_eml`] with RSA + relaxed canon.
fn build_signed_eml_with_extra_tags(
    headers: &[Header],
    signed_header_names: &[&str],
    domain: &str,
    selector: &str,
    extra_tags: &str,
    signing: &SigningKey<Sha256>,
) -> Vec<u8> {
    let canon = Canonicalization {
        header: HeaderCanon::Relaxed,
        body: super::canonicalize::BodyCanon::Simple,
    };
    let body = "hello body";
    let body_hash = base64::engine::general_purpose::STANDARD
        .encode(<Sha256 as rsa::sha2::Digest>::digest(body.as_bytes()));
    let h_list = signed_header_names.join(":");

    // The extra tags ride inside the signed DKIM-Signature value (before b=), so
    // b= commits to them: a real signer's t=/x= are covered the same way.
    let dkim_value_unsigned = format!(
        " v=1; a=rsa-sha256; c=relaxed/simple; d={domain}; s={selector}; {extra_tags}\r\n \
         h={h_list}; bh={body_hash};\r\n b="
    );

    let selected: Vec<Header> = signed_header_names
        .iter()
        .map(|name| {
            headers
                .iter()
                .find(|h| h.name_eq_ignore_case(name))
                .expect("signed header present")
                .clone()
        })
        .collect();

    let mut signed_data = Vec::new();
    for h in &selected {
        signed_data.extend_from_slice(&canon.canonicalize_header(h));
    }
    signed_data.extend_from_slice(&canon.canonicalize_dkim_signature(&dkim_value_unsigned));

    let b_value =
        base64::engine::general_purpose::STANDARD.encode(signing.sign(&signed_data).to_bytes());
    let dkim_value_signed = format!("{dkim_value_unsigned}{b_value}");

    let mut eml = String::new();
    eml.push_str("DKIM-Signature:");
    eml.push_str(&dkim_value_signed);
    eml.push_str("\r\n");
    for h in headers {
        eml.push_str(&h.name);
        eml.push(':');
        eml.push_str(&h.raw_value);
        eml.push_str("\r\n");
    }
    eml.push_str("\r\n");
    eml.push_str(body);
    eml.into_bytes()
}

/// Build a signed email whose `h=` names a header more than once, so the
/// captured proof carries two `Subject:` values.
///
/// Unlike [`build_signed_eml`], which resolves each `h=` name by first match,
/// this reproduces the signer's real behavior for repeated names: RFC 6376
/// section 5.4.2 consumes them bottom-up, so `h=...:subject:subject` signs the
/// bottom-most `Subject:` first and the one above it second.
fn build_signed_eml_with_repeated_names(
    headers: &[Header],
    signed_header_names: &[&str],
    domain: &str,
    selector: &str,
    signing: &SigningKey<Sha256>,
) -> Vec<u8> {
    let canon = Canonicalization {
        header: HeaderCanon::Relaxed,
        body: super::canonicalize::BodyCanon::Simple,
    };
    let body = "hello body";
    let body_hash = base64::engine::general_purpose::STANDARD
        .encode(<Sha256 as rsa::sha2::Digest>::digest(body.as_bytes()));
    let h_list = signed_header_names.join(":");

    let dkim_value_unsigned = format!(
        " v=1; a=rsa-sha256; c=relaxed/simple; d={domain}; s={selector};\r\n \
         h={h_list}; bh={body_hash};\r\n b="
    );

    // Bottom-up selection with a per-name consumed count, matching the verifier.
    let mut consumed: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let selected: Vec<Header> = signed_header_names
        .iter()
        .map(|name| {
            let already = consumed.entry(name.to_ascii_lowercase()).or_insert(0);
            let found = headers
                .iter()
                .rev()
                .filter(|h| h.name_eq_ignore_case(name))
                .nth(*already)
                .expect("signed header present")
                .clone();
            *already += 1;
            found
        })
        .collect();

    let mut signed_data = Vec::new();
    for h in &selected {
        signed_data.extend_from_slice(&canon.canonicalize_header(h));
    }
    signed_data.extend_from_slice(&canon.canonicalize_dkim_signature(&dkim_value_unsigned));

    let b_value =
        base64::engine::general_purpose::STANDARD.encode(signing.sign(&signed_data).to_bytes());
    let dkim_value_signed = format!("{dkim_value_unsigned}{b_value}");

    let mut eml = String::new();
    eml.push_str("DKIM-Signature:");
    eml.push_str(&dkim_value_signed);
    eml.push_str("\r\n");
    for h in headers {
        eml.push_str(&h.name);
        eml.push(':');
        eml.push_str(&h.raw_value);
        eml.push_str("\r\n");
    }
    eml.push_str("\r\n");
    eml.push_str(body);
    eml.into_bytes()
}

/// A proof must not carry two `Subject:` headers, because which one is
/// authoritative diverges between the verifier and the human.
///
/// `select_signed_headers` consumes repeated `h=` names bottom-up (RFC 6376
/// section 5.4.2), so `signed_headers[0]` for a doubled name is the
/// *bottom-most* `Subject:`. Every mail client displays the *top-most* one, and
/// the `did:mailto` binding reads the first match. So the subject the user read
/// and the subject that authorizes a key are different headers.
///
/// Oversigning (`h=from:subject:subject`) is standard anti-replay practice
/// (RFC 6376 section 8.15), so this is a normal configuration, not an exotic
/// one. The attack: get the victim to send one message with a doubled Subject,
/// and the key bound is one they never saw.
///
/// Refusing a duplicate outright is the fix; a binding proof has no legitimate
/// reason to sign two subjects.
#[dialog_common::test]
fn duplicate_signed_subject_is_rejected() {
    let (signing, key) = rsa_signing_key();

    let headers = vec![
        header("From", "Alice <alice@example.com>"),
        // What a mail client shows the user.
        header("Subject", "Hello, this is a totally normal email"),
        // What the verifier reads: signed first under bottom-up selection.
        header(
            "Subject",
            "I am also known as did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
        ),
        header("Date", "Mon, 16 Aug 2026 12:00:00 +0000"),
    ];

    let eml = build_signed_eml_with_repeated_names(
        &headers,
        &["From", "Subject", "Subject", "Date"],
        "example.com",
        "sel",
        &signing,
    );

    // Sanity: the vector is a correctly signed email.
    assert!(
        verify(&eml, &key).is_ok(),
        "test vector must be a validly signed email"
    );

    let proof = SignedEmail::from_raw_eml(&eml).unwrap();

    // Confirm the divergence is real before asserting the fix: the first signed
    // Subject is the bottom one, not the one a client would display.
    let first_subject = proof
        .signed_headers
        .iter()
        .find(|h| h.name_eq_ignore_case("subject"))
        .expect("a signed Subject");
    assert!(
        first_subject.raw_value.contains("I am also known as"),
        "the first signed Subject is the bottom-most one, which the user never saw"
    );

    assert!(
        verify_with_key(&proof, &key).is_err(),
        "a proof signing two Subject: headers must be refused: the header the \
         verifier reads is not the header the sender saw"
    );
}

/// A DKIM key below the RFC 8301 floor must not verify a binding.
///
/// `verify_rsa_sha256` checks no modulus size, and the `rsa` crate happily
/// parses a 512-bit key. 512-bit RSA is factorable on commodity hardware in
/// hours, and 512-bit DKIM keys are still published in the wild. Anyone who
/// factors a domain's weak DKIM key mints an "I am also known as" binding for
/// every mailbox at that domain, so the whole `did:mailto` identity for that
/// domain falls to an offline computation.
///
/// RFC 8301 requires at least 1024 bits and recommends 2048.
#[dialog_common::test]
fn undersized_rsa_dkim_key_is_rejected() {
    use rsa::pkcs1v15::SigningKey as Pkcs1SigningKey;

    // 512 bits: far below the RFC 8301 floor, but a well-formed RSA key.
    let mut rng = rand::thread_rng();
    let weak_private = RsaPrivateKey::new(&mut rng, 512).expect("512-bit key");
    let weak_public = RsaPublicKey::from(&weak_private);
    let weak_key = DkimPublicKey::rsa_from_spki_der(
        weak_public.to_public_key_der().unwrap().as_bytes().to_vec(),
    );
    let weak_signing = Pkcs1SigningKey::<Sha256>::new(weak_private);

    let headers = sample_headers();
    let eml = build_signed_eml(
        &headers,
        SIGNED_NAMES,
        "example.com",
        "sel",
        Canonicalization {
            header: HeaderCanon::Relaxed,
            body: super::canonicalize::BodyCanon::Simple,
        },
        InnerKey::Rsa(&weak_signing),
        "hello body",
    );

    let proof = SignedEmail::from_raw_eml(&eml).unwrap();
    assert!(
        verify_with_key(&proof, &weak_key).is_err(),
        "a 512-bit RSA DKIM key is below the RFC 8301 floor and must not \
         authorize a binding, even though the signature itself is valid"
    );
}

/// A DKIM signature whose `x=` (signature expiration, RFC 6376 section 3.5) is
/// in the past must not verify. `x=` is the signer's own assertion that the
/// signature is no longer valid after a point in time; honoring it is stricter
/// than any delegation-level policy. This proof is cryptographically valid (the
/// signature covers a well-formed, in-the-past `x=`), so verification passes
/// today: `x=` is never parsed or enforced. This pins that gap.
#[test]
fn expired_signature_is_rejected() {
    let (signing, key) = rsa_signing_key();
    let headers = sample_headers();
    // t= well in the past, x= just after it: expired for any plausible clock.
    let eml = build_signed_eml_with_extra_tags(
        &headers,
        SIGNED_NAMES,
        "example.com",
        "sel",
        "t=1000; x=2000;",
        &signing,
    );
    // Sanity: the signature itself is valid (the vector is correctly signed).
    assert!(
        verify(&eml, &key).is_ok(),
        "test vector must be a validly signed email"
    );

    // The real assertion: an expired signature must be refused. Fails today
    // because x= is ignored.
    let proof = SignedEmail::from_raw_eml(&eml).unwrap();
    assert!(
        verify_with_key(&proof, &key).is_err(),
        "a DKIM signature whose x= expiration is in the past must not verify"
    );
}

/// An unsigned second `From:` prepended to a captured proof must not change
/// which `From:` the verifier trusts. DKIM signs only the headers named in `h=`
/// (bottom-up for repeats); the injected top `From:` is outside the signed set.
/// This is a defense-in-depth regression pin: it must PASS (the guarantee holds
/// today) and stay passing.
#[test]
fn unsigned_injected_from_header_does_not_displace_signed_from() {
    let (signing, key) = rsa_signing_key();
    let headers = sample_headers();
    let eml = build_signed_eml(
        &headers,
        SIGNED_NAMES,
        "example.com",
        "sel",
        Canonicalization {
            header: HeaderCanon::Relaxed,
            body: super::canonicalize::BodyCanon::Simple,
        },
        InnerKey::Rsa(&signing),
        "hello body",
    );

    // Inject an attacker From: at the very top, outside the signature.
    let mut tampered = Vec::new();
    tampered.extend_from_slice(b"From: attacker@evil.example\r\n");
    tampered.extend_from_slice(&eml);

    // The signature still verifies (the injected header is not in h=), and the
    // captured signed From: is the original one, not the attacker's.
    let proof = SignedEmail::from_raw_eml(&tampered).unwrap();
    verify_with_key(&proof, &key).unwrap();
    let signed_from = proof
        .signed_headers
        .iter()
        .find(|h| h.name_eq_ignore_case("from"))
        .expect("From is signed");
    assert!(
        signed_from.raw_value.contains("alice@example.com"),
        "the signed From must be the original, got {:?}",
        signed_from.raw_value
    );
    assert!(
        !signed_from.raw_value.contains("attacker@evil.example"),
        "the injected unsigned From must not be the trusted one"
    );
}

/// Placeholder for the real Gmail `.eml` vector.
///
/// When a real DKIM-signed email is available:
/// 1. Save it as `fixtures/real_gmail.eml`.
/// 2. Obtain the domain's DKIM key (the DNS `p=` value at
///    `<selector>._domainkey.<domain>`) and build a [`DkimPublicKey`] via
///    [`DkimPublicKey::from_dns_txt`].
/// 3. Replace the body of this test with a call that reads the fixture via
///    `include_bytes!("../../fixtures/real_gmail.eml")`, builds the key via
///    `DkimPublicKey::from_dns_txt("v=DKIM1; k=rsa; p=...")`, calls
///    [`run_full_verify`] with the two, and asserts on the returned
///    `sig.domain` (e.g. `"gmail.com"`).
///
/// No production code changes are needed; only this fixture + assertion.
#[test]
fn real_eml_drop_in_point() {
    // Until a real vector exists, prove the drop-in helper is wired to the same
    // verify path a real vector will use, by round-tripping a generated vector
    // through it.
    let (signing, key) = rsa_signing_key();
    let headers = sample_headers();
    let eml = build_signed_eml(
        &headers,
        SIGNED_NAMES,
        "example.com",
        "sel",
        Canonicalization {
            header: HeaderCanon::Relaxed,
            body: super::canonicalize::BodyCanon::Simple,
        },
        InnerKey::Rsa(&signing),
        "hello body",
    );
    let sig = run_full_verify(&eml, &key).unwrap();
    assert_eq!(sig.domain, "example.com");
}
