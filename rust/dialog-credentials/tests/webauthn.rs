//! WebAuthn integration tests.
//!
//! These construct WebAuthn-like signatures programmatically with a P-256
//! signing key, mimicking what a real authenticator produces, and exercise the
//! full verification pipeline: challenge validation, authenticator-data
//! binding, and inner ECDSA verification. A browser is not required; a true
//! end-to-end `navigator.credentials` assertion needs an interactive user
//! gesture, so it cannot be automated here.
//!
//! They also cover the algorithm-agnostic path: a WebAuthn-tagged
//! [`AnySignature`] verifies through the agnostic [`Verifier`]'s WebAuthn arm,
//! and the varsig header distinguishes WebAuthn from plain ES256.

#![cfg(feature = "webauthn")]

use base64::Engine;
use dialog_credentials::Verifier as AnyVerifier;
use dialog_credentials::webauthn::{WebAuthnVerifier, WebAuthnVerifyError};
use dialog_varsig::{
    AlgorithmTag, AnySignature, Principal, SignatureAlgorithm, Verifier,
    webauthn::WebAuthnSignature,
};
use p256::ecdsa::{SigningKey, signature::Signer as _};
use sha2::{Digest, Sha256};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Build valid `clientDataJSON` with a SHA-256 multihash challenge.
fn build_client_data_json(payload: &[u8]) -> Vec<u8> {
    let payload_hash = Sha256::digest(payload);
    let mut multihash = Vec::with_capacity(34);
    multihash.push(0x12);
    multihash.push(0x20);
    multihash.extend_from_slice(&payload_hash);

    let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&multihash);
    serde_json::to_vec(&serde_json::json!({
        "type": "webauthn.get",
        "challenge": challenge,
        "origin": "https://example.com",
        "crossOrigin": false
    }))
    .unwrap()
}

/// Build minimal valid authenticator data (37 bytes).
fn build_authenticator_data() -> Vec<u8> {
    let rp_id_hash = Sha256::digest(b"example.com");
    let mut auth_data = Vec::with_capacity(37);
    auth_data.extend_from_slice(&rp_id_hash);
    auth_data.push(0x05); // UP + UV flags
    auth_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
    auth_data
}

/// Create a complete signed WebAuthn test fixture.
fn sign_webauthn(sk: &SigningKey, payload: &[u8]) -> (WebAuthnVerifier, WebAuthnSignature) {
    let vk =
        WebAuthnVerifier::from_sec1_bytes(sk.verifying_key().to_encoded_point(true).as_bytes())
            .unwrap();

    let client_data_json = build_client_data_json(payload);
    let authenticator_data = build_authenticator_data();

    let client_data_hash = Sha256::digest(&client_data_json);
    let mut signed_data = Vec::new();
    signed_data.extend_from_slice(&authenticator_data);
    signed_data.extend_from_slice(&client_data_hash);

    // The inner signature is DER-encoded, exactly as a browser authenticator
    // emits it; the verifier parses it as DER.
    let ecdsa_sig: p256::ecdsa::DerSignature = sk.sign(&signed_data);
    let sig = WebAuthnSignature::new(
        client_data_json,
        authenticator_data,
        ecdsa_sig.to_bytes().to_vec(),
    );

    (vk, sig)
}

#[dialog_common::test]
async fn end_to_end_sign_and_verify() {
    let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
    let payload = b"integration test payload";

    let (verifier, sig) = sign_webauthn(&sk, payload);

    <WebAuthnVerifier as Verifier<WebAuthnSignature>>::verify(&verifier, payload, &sig)
        .await
        .expect("valid signature should verify");
}

#[dialog_common::test]
async fn did_roundtrip_then_verify() {
    let sk = SigningKey::from_bytes(&[7u8; 32].into()).unwrap();
    let payload = b"did roundtrip payload";

    let (verifier, sig) = sign_webauthn(&sk, payload);

    let did_str = verifier.to_string();
    let restored: WebAuthnVerifier = did_str.parse().unwrap();
    assert_eq!(restored, verifier);

    <WebAuthnVerifier as Verifier<WebAuthnSignature>>::verify(&restored, payload, &sig)
        .await
        .expect("restored verifier should accept signature");
}

#[dialog_common::test]
async fn signature_serialization_roundtrip() {
    let sk = SigningKey::from_bytes(&[99u8; 32].into()).unwrap();
    let payload = b"serialization test";

    let (verifier, sig) = sign_webauthn(&sk, payload);

    let encoded = sig.to_vec();
    let decoded = WebAuthnSignature::from_bytes(&encoded).unwrap();

    assert_eq!(decoded.client_data_json, sig.client_data_json);
    assert_eq!(decoded.authenticator_data, sig.authenticator_data);
    assert_eq!(decoded.signature, sig.signature);

    <WebAuthnVerifier as Verifier<WebAuthnSignature>>::verify(&verifier, payload, &decoded)
        .await
        .expect("decoded signature should verify");
}

#[dialog_common::test]
async fn cross_key_verification_fails() {
    let sk1 = SigningKey::from_bytes(&[1u8; 32].into()).unwrap();
    let sk2 = SigningKey::from_bytes(&[2u8; 32].into()).unwrap();
    let payload = b"cross-key test";

    let (_, sig) = sign_webauthn(&sk1, payload);
    let (verifier2, _) = sign_webauthn(&sk2, payload);

    let result =
        <WebAuthnVerifier as Verifier<WebAuthnSignature>>::verify(&verifier2, payload, &sig).await;
    assert!(result.is_err(), "wrong key should not verify");
}

#[dialog_common::test]
async fn verify_rejects_altered_challenge() {
    let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
    let payload = b"challenge test";

    let (verifier, sig) = sign_webauthn(&sk, payload);

    let result = verifier.verify_webauthn(b"different payload", &sig);
    assert!(matches!(
        result.unwrap_err(),
        WebAuthnVerifyError::ChallengeMismatch
    ));

    verifier.verify_webauthn(payload, &sig).unwrap();
}

#[dialog_common::test]
async fn principal_produces_valid_did() {
    let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
    let (verifier, _) = sign_webauthn(&sk, b"any");

    let did = verifier.did();
    assert!(did.as_str().starts_with("did:key:z"));
    assert_eq!(did.method(), "key");
}

#[dialog_common::test]
async fn webauthn_tagged_any_signature_verifies_through_agnostic_verifier() {
    let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
    let payload = b"agnostic webauthn";

    let (verifier, sig) = sign_webauthn(&sk, payload);

    // Wrap the WebAuthn signature as an algorithm-agnostic AnySignature and
    // route it through the agnostic Verifier's WebAuthn arm.
    let any_sig = AnySignature::from(sig);
    assert_eq!(any_sig.algorithm(), AlgorithmTag::WebAuthn);

    let any_verifier: AnyVerifier = verifier.did().as_str().parse().unwrap();
    assert_eq!(any_verifier.algorithm(), AlgorithmTag::WebAuthn);

    Verifier::<AnySignature>::verify(&any_verifier, payload, &any_sig)
        .await
        .expect("WebAuthn-tagged AnySignature should verify through the agnostic verifier");
}

#[dialog_common::test]
async fn agnostic_verifier_rejects_cross_algorithm_tag() {
    // An ed25519-tagged agnostic signature must not verify against a WebAuthn
    // verifier: the tags differ and the agnostic dispatch rejects it before any
    // crypto runs.
    let sk = SigningKey::from_bytes(&[9u8; 32].into()).unwrap();
    let (verifier, _) = sign_webauthn(&sk, b"payload");
    let any_verifier: AnyVerifier = verifier.did().as_str().parse().unwrap();

    let ed_signer = dialog_credentials::Signer::from(
        dialog_credentials::Ed25519Signer::generate().await.unwrap(),
    );
    let ed_sig = dialog_varsig::Signer::sign(&ed_signer, b"payload")
        .await
        .unwrap();
    assert_eq!(ed_sig.algorithm(), AlgorithmTag::Ed25519);

    assert!(
        Verifier::<AnySignature>::verify(&any_verifier, b"payload", &ed_sig)
            .await
            .is_err()
    );
}

#[dialog_common::test]
fn webauthn_header_differs_from_es256() {
    use dialog_varsig::ecdsa::Es256;
    use dialog_varsig::webauthn::{WEBAUTHN_MARKER, WebAuthnP256};

    // The WebAuthn header is the ES256 header followed by the 0x300001 marker.
    let mut wa = vec![WebAuthnP256::default().prefix()];
    wa.extend(WebAuthnP256::default().config_tags());

    let mut es = vec![Es256::default().prefix()];
    es.extend(Es256::default().config_tags());

    assert_ne!(wa, es);
    assert_eq!(*wa.last().unwrap(), WEBAUTHN_MARKER);
    // The bare ES256 header does not parse as WebAuthn.
    assert!(WebAuthnP256::try_from_tags(&es).is_none());
}
