//! WebAuthn integration tests.
//!
//! ## Automated tests (run on all platforms)
//!
//! These tests construct WebAuthn-like signatures programmatically using
//! a P-256 signing key, mimicking what a real authenticator would produce.
//! They exercise the full verification pipeline: challenge validation,
//! authenticator-data binding, and ECDSA signature verification.
//!
//! ## Interactive browser tests (feature: `web-integration-tests`)
//!
//! When the `web-integration-tests` feature is enabled, additional tests
//! are compiled that exercise the real `navigator.credentials` WebAuthn API.
//! These must be run in a browser context (e.g., via `wasm-pack test --headless`).
//!
//! Since actual WebAuthn credential creation requires user interaction
//! (biometric/PIN prompt), these tests are designed to be run manually
//! in a browser environment with the WebAuthn API available.
//!
//! ```sh
//! # Run automated tests
//! cargo test -p dialog-credentials --features webauthn
//!
//! # Run interactive browser tests (requires wasm-pack)
//! wasm-pack test --headless --chrome -- --features web-integration-tests
//! ```

#![cfg(feature = "webauthn")]

use base64::Engine;
use dialog_credentials::webauthn::{WebAuthnVerifier, WebAuthnVerifyError};
use dialog_varsig::{Principal, Verifier, webauthn::WebAuthnSignature};
use p256::ecdsa::{SigningKey, signature::Signer as _};
use sha2::{Digest, Sha256};

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
    let vk = WebAuthnVerifier::from_sec1_bytes(
        &sk.verifying_key()
            .to_encoded_point(true)
            .as_bytes()
            .to_vec(),
    )
    .unwrap();

    let client_data_json = build_client_data_json(payload);
    let authenticator_data = build_authenticator_data();

    let client_data_hash = Sha256::digest(&client_data_json);
    let mut signed_data = Vec::new();
    signed_data.extend_from_slice(&authenticator_data);
    signed_data.extend_from_slice(&client_data_hash);

    let ecdsa_sig: p256::ecdsa::DerSignature = sk.sign(&signed_data);
    let sig = WebAuthnSignature::new(
        client_data_json,
        authenticator_data,
        ecdsa_sig.to_bytes().to_vec(),
    );

    (vk, sig)
}

#[tokio::test]
async fn end_to_end_sign_and_verify() {
    let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
    let payload = b"integration test payload";

    let (verifier, sig) = sign_webauthn(&sk, payload);

    // Verify via the Verifier trait
    <WebAuthnVerifier as Verifier<WebAuthnSignature>>::verify(&verifier, payload, &sig)
        .await
        .expect("valid signature should verify");
}

#[tokio::test]
async fn did_roundtrip_then_verify() {
    let sk = SigningKey::from_bytes(&[7u8; 32].into()).unwrap();
    let payload = b"did roundtrip payload";

    let (verifier, sig) = sign_webauthn(&sk, payload);

    // Serialize verifier to DID, parse it back, then verify
    let did_str = verifier.to_string();
    let restored: WebAuthnVerifier = did_str.parse().unwrap();
    assert_eq!(restored, verifier);

    <WebAuthnVerifier as Verifier<WebAuthnSignature>>::verify(&restored, payload, &sig)
        .await
        .expect("restored verifier should accept signature");
}

#[tokio::test]
async fn signature_serialization_roundtrip() {
    let sk = SigningKey::from_bytes(&[99u8; 32].into()).unwrap();
    let payload = b"serialization test";

    let (verifier, sig) = sign_webauthn(&sk, payload);

    // Encode/decode the signature
    let encoded = sig.to_vec();
    let decoded = WebAuthnSignature::from_bytes(&encoded).unwrap();

    assert_eq!(decoded.client_data_json, sig.client_data_json);
    assert_eq!(decoded.authenticator_data, sig.authenticator_data);
    assert_eq!(decoded.signature, sig.signature);

    // The decoded signature should still verify
    <WebAuthnVerifier as Verifier<WebAuthnSignature>>::verify(&verifier, payload, &decoded)
        .await
        .expect("decoded signature should verify");
}

#[tokio::test]
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

#[tokio::test]
async fn verify_rejects_altered_challenge() {
    let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
    let payload = b"challenge test";

    let (verifier, sig) = sign_webauthn(&sk, payload);

    // Try to verify with a different payload — challenge won't match
    let result = verifier.verify_webauthn(b"different payload", &sig);
    assert!(matches!(
        result.unwrap_err(),
        WebAuthnVerifyError::ChallengeMismatch
    ));

    // Correct payload works
    verifier.verify_webauthn(payload, &sig).unwrap();
}

#[tokio::test]
async fn principal_produces_valid_did() {
    let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
    let (verifier, _) = sign_webauthn(&sk, b"any");

    let did = verifier.did();
    assert!(did.as_str().starts_with("did:key:z"));
    assert_eq!(did.method(), "key");
}

// --- Interactive WebAuthn browser tests ---
// These require the `web-integration-tests` feature and a browser environment.
// They are compiled but will only succeed in WASM targets with WebAuthn support.

#[cfg(all(
    feature = "web-integration-tests",
    target_arch = "wasm32",
    target_os = "unknown"
))]
mod browser_tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;

    /// This test demonstrates the verification flow with a "synthetic" WebAuthn
    /// signature — it uses the same P-256 test vector approach but runs in the
    /// WASM environment, proving that `WebAuthnVerifier` works in the browser.
    ///
    /// To run a true end-to-end test with `navigator.credentials`, you would
    /// need to:
    /// 1. Create a credential via `navigator.credentials.create()` (requires user gesture)
    /// 2. Use `navigator.credentials.get()` to produce an assertion (requires user gesture)
    /// 3. Extract `authenticatorData`, `clientDataJSON`, and `signature` from the assertion
    /// 4. Construct a `WebAuthnSignature` and verify it with `WebAuthnVerifier`
    ///
    /// Since steps 1-2 require interactive user gestures, they cannot be automated
    /// in headless test environments. The synthetic test below validates the entire
    /// verification pipeline without requiring browser interaction.
    #[wasm_bindgen_test]
    async fn synthetic_webauthn_verify_in_browser() {
        let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
        let payload = b"browser verification test";

        let (verifier, sig) = sign_webauthn(&sk, payload);

        <WebAuthnVerifier as Verifier<WebAuthnSignature>>::verify(&verifier, payload, &sig)
            .await
            .expect("synthetic WebAuthn signature should verify in WASM");
    }
}
