//! WebAuthn P-256 verifier.
//!
//! Verifies WebAuthn signatures on any platform by:
//! 1. Parsing `clientDataJSON` to extract and validate the challenge
//! 2. Computing `authenticatorData || SHA-256(clientDataJSON)` as the signed message
//! 3. Verifying the inner ECDSA P-256 signature against that message

use super::error::{WebAuthnDidFromStrError, WebAuthnVerifyError};
use base58::ToBase58;
use dialog_varsig::{Did, Principal, Verifier, webauthn::WebAuthnSignature};
use p256::ecdsa::signature::Verifier as _;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::str::FromStr;

/// Multicodec prefix for P-256 public keys: `[0x80, 0x24]` is the varint
/// encoding of `0x1200` (p256-pub).
const P256_MULTICODEC: [u8; 2] = [0x80, 0x24];

/// A WebAuthn P-256 `did:key` verifier.
///
/// Wraps a P-256 ECDSA verifying key and can verify [`WebAuthnSignature`]s
/// on any platform (native or WASM).
#[derive(Debug, Clone)]
pub struct WebAuthnVerifier {
    key: p256::ecdsa::VerifyingKey,
}

impl WebAuthnVerifier {
    /// Create a verifier from raw compressed-point bytes (33 bytes).
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a valid P-256 compressed point.
    pub fn from_sec1_bytes(bytes: &[u8]) -> Result<Self, WebAuthnDidFromStrError> {
        let key = p256::ecdsa::VerifyingKey::from_sec1_bytes(bytes)
            .map_err(|_| WebAuthnDidFromStrError::InvalidKey)?;
        Ok(Self { key })
    }

    /// Get the inner P-256 verifying key.
    #[must_use]
    pub const fn verifying_key(&self) -> &p256::ecdsa::VerifyingKey {
        &self.key
    }

    /// Get the compressed SEC1 public key bytes (33 bytes).
    #[must_use]
    pub fn to_sec1_bytes(&self) -> Vec<u8> {
        self.key.to_encoded_point(true).as_bytes().to_vec()
    }

    /// Verify a WebAuthn signature against a payload.
    ///
    /// This performs the full WebAuthn verification flow:
    /// 1. Parse `clientDataJSON` and validate the challenge matches `SHA-256(payload)`
    /// 2. Compute the verification message: `authenticatorData || SHA-256(clientDataJSON)`
    /// 3. Verify the inner ECDSA P-256 signature
    ///
    /// # Errors
    ///
    /// Returns an error if any step of the verification fails.
    pub fn verify_webauthn(
        &self,
        payload: &[u8],
        sig: &WebAuthnSignature,
    ) -> Result<(), WebAuthnVerifyError> {
        // Step 1: Validate the challenge
        self.validate_challenge(payload, &sig.client_data_json)?;

        // Step 2: Compute the signed message
        // Per WebAuthn spec: signedData = authenticatorData || SHA-256(clientDataJSON)
        let client_data_hash = Sha256::digest(&sig.client_data_json);
        let mut signed_data = Vec::with_capacity(sig.authenticator_data.len() + 32);
        signed_data.extend_from_slice(&sig.authenticator_data);
        signed_data.extend_from_slice(&client_data_hash);

        // Step 3: Verify the ECDSA signature
        let ecdsa_sig = p256::ecdsa::DerSignature::from_bytes(&sig.signature)
            .map_err(|e| WebAuthnVerifyError::InvalidSignature(e.to_string()))?;
        self.key
            .verify(&signed_data, &ecdsa_sig)
            .map_err(|e| WebAuthnVerifyError::InvalidSignature(e.to_string()))
    }

    /// Validate the challenge in `clientDataJSON`.
    ///
    /// The challenge is expected to be `base64url(SHA-256(payload))`.
    fn validate_challenge(
        &self,
        payload: &[u8],
        client_data_json: &[u8],
    ) -> Result<(), WebAuthnVerifyError> {
        #[derive(serde::Deserialize)]
        struct ClientData {
            challenge: String,
        }

        let client_data: ClientData = serde_json::from_slice(client_data_json)
            .map_err(|e| WebAuthnVerifyError::InvalidClientData(e.to_string()))?;

        // Decode the base64url challenge
        use base64::Engine;
        let challenge_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&client_data.challenge)
            .map_err(|e| WebAuthnVerifyError::InvalidClientData(e.to_string()))?;

        // Compute the expected challenge: SHA-256 multihash of the payload
        // Multihash format: [hash_function_code, digest_size, digest...]
        // For SHA-256: [0x12, 0x20, <32 bytes of SHA-256 digest>]
        let payload_hash = Sha256::digest(payload);
        let mut expected_multihash = Vec::with_capacity(34);
        expected_multihash.push(0x12); // SHA-256 multicodec
        expected_multihash.push(0x20); // 32 bytes
        expected_multihash.extend_from_slice(&payload_hash);

        if challenge_bytes != expected_multihash {
            return Err(WebAuthnVerifyError::ChallengeMismatch);
        }

        Ok(())
    }
}

impl PartialEq for WebAuthnVerifier {
    fn eq(&self, other: &Self) -> bool {
        self.to_sec1_bytes() == other.to_sec1_bytes()
    }
}

impl Eq for WebAuthnVerifier {}

impl std::fmt::Display for WebAuthnVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sec1 = self.to_sec1_bytes();
        let mut raw_bytes = Vec::with_capacity(2 + sec1.len());
        raw_bytes.extend_from_slice(&P256_MULTICODEC);
        raw_bytes.extend_from_slice(&sec1);
        let b58 = ToBase58::to_base58(raw_bytes.as_slice());
        write!(f, "did:key:z{b58}")
    }
}

impl FromStr for WebAuthnVerifier {
    type Err = WebAuthnDidFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        let did_tag = *parts
            .first()
            .ok_or(WebAuthnDidFromStrError::InvalidDidHeader)?;
        let key_tag = *parts
            .get(1)
            .ok_or(WebAuthnDidFromStrError::InvalidDidHeader)?;

        if parts.len() != 3 || did_tag != "did" || key_tag != "key" {
            return Err(WebAuthnDidFromStrError::InvalidDidHeader);
        }

        let b58 = parts
            .get(2)
            .ok_or(WebAuthnDidFromStrError::InvalidDidHeader)?
            .strip_prefix('z')
            .ok_or(WebAuthnDidFromStrError::MissingBase58Prefix)?;

        let key_bytes =
            base58::FromBase58::from_base58(b58).map_err(|_| WebAuthnDidFromStrError::InvalidKey)?;

        // Expect: 2-byte varint multicodec prefix + 33-byte compressed point
        if key_bytes.len() != 35 {
            return Err(WebAuthnDidFromStrError::InvalidKey);
        }
        if key_bytes[0..2] != P256_MULTICODEC {
            return Err(WebAuthnDidFromStrError::InvalidKey);
        }

        let point_bytes = &key_bytes[2..];
        Self::from_sec1_bytes(point_bytes)
    }
}

impl Verifier<WebAuthnSignature> for WebAuthnVerifier {
    async fn verify(
        &self,
        payload: &[u8],
        signature: &WebAuthnSignature,
    ) -> Result<(), signature::Error> {
        self.verify_webauthn(payload, signature)
            .map_err(|_| signature::Error::new())
    }
}

impl Principal for WebAuthnVerifier {
    fn did(&self) -> Did {
        self.to_string().parse().expect("valid DID string")
    }
}

impl Serialize for WebAuthnVerifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for WebAuthnVerifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DidKeyVisitor;

        impl serde::de::Visitor<'_> for DidKeyVisitor {
            type Value = WebAuthnVerifier;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a did:key string containing a P-256 public key")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_str(DidKeyVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use p256::ecdsa::{SigningKey, signature::Signer as _};
    use sha2::{Digest, Sha256};

    /// Build a valid `clientDataJSON` with the challenge set to `base64url(multihash-sha256(payload))`.
    fn build_client_data_json(payload: &[u8]) -> Vec<u8> {
        let payload_hash = Sha256::digest(payload);
        let mut multihash = Vec::with_capacity(34);
        multihash.push(0x12); // SHA-256 code
        multihash.push(0x20); // 32-byte digest length
        multihash.extend_from_slice(&payload_hash);

        let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&multihash);

        let json = serde_json::json!({
            "type": "webauthn.get",
            "challenge": challenge,
            "origin": "https://example.com",
            "crossOrigin": false
        });
        serde_json::to_vec(&json).unwrap()
    }

    /// Build a minimal valid authenticator data (37 bytes minimum).
    ///
    /// Layout: rpIdHash (32) | flags (1) | signCount (4)
    fn build_authenticator_data() -> Vec<u8> {
        let rp_id_hash = Sha256::digest(b"example.com");
        let mut auth_data = Vec::with_capacity(37);
        auth_data.extend_from_slice(&rp_id_hash); // 32 bytes
        auth_data.push(0x05); // flags: UP (0x01) + UV (0x04)
        auth_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // sign count = 1
        auth_data
    }

    /// Create a complete WebAuthn test fixture: signing key, verifier, and valid signature.
    fn create_test_fixture(payload: &[u8]) -> (SigningKey, WebAuthnVerifier, WebAuthnSignature) {
        let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
        let vk = WebAuthnVerifier {
            key: *sk.verifying_key(),
        };

        let client_data_json = build_client_data_json(payload);
        let authenticator_data = build_authenticator_data();

        // Compute the signed message per WebAuthn spec
        let client_data_hash = Sha256::digest(&client_data_json);
        let mut signed_data = Vec::new();
        signed_data.extend_from_slice(&authenticator_data);
        signed_data.extend_from_slice(&client_data_hash);

        // Sign with P-256
        let ecdsa_sig: p256::ecdsa::DerSignature = sk.sign(&signed_data);
        let sig = WebAuthnSignature::new(
            client_data_json,
            authenticator_data,
            ecdsa_sig.to_bytes().to_vec(),
        );

        (sk, vk, sig)
    }

    #[dialog_common::test]
    async fn webauthn_verify_valid_signature() {
        let payload = b"hello webauthn";
        let (_, verifier, sig) = create_test_fixture(payload);

        verifier.verify_webauthn(payload, &sig).unwrap();
    }

    #[dialog_common::test]
    async fn webauthn_verify_via_trait() {
        let payload = b"trait-based verification";
        let (_, verifier, sig) = create_test_fixture(payload);

        <WebAuthnVerifier as Verifier<WebAuthnSignature>>::verify(&verifier, payload, &sig)
            .await
            .unwrap();
    }

    #[dialog_common::test]
    async fn webauthn_verify_wrong_payload_fails() {
        let payload = b"original payload";
        let (_, verifier, sig) = create_test_fixture(payload);

        let result = verifier.verify_webauthn(b"tampered payload", &sig);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), WebAuthnVerifyError::ChallengeMismatch));
    }

    #[dialog_common::test]
    async fn webauthn_verify_wrong_key_fails() {
        let payload = b"key mismatch test";
        let (_, _, sig) = create_test_fixture(payload);

        // Different key
        let other_sk = SigningKey::from_bytes(&[99u8; 32].into()).unwrap();
        let other_verifier = WebAuthnVerifier {
            key: *other_sk.verifying_key(),
        };

        let result = other_verifier.verify_webauthn(payload, &sig);
        assert!(result.is_err());
    }

    #[dialog_common::test]
    async fn webauthn_verify_tampered_authenticator_data_fails() {
        let payload = b"auth data tamper test";
        let (_, verifier, mut sig) = create_test_fixture(payload);

        // Flip a byte in authenticator data
        sig.authenticator_data[0] ^= 0xff;

        let result = verifier.verify_webauthn(payload, &sig);
        assert!(result.is_err());
    }

    #[dialog_common::test]
    async fn webauthn_verify_tampered_client_data_fails() {
        let payload = b"client data tamper test";
        let (_, verifier, mut sig) = create_test_fixture(payload);

        // Replace the client data with different JSON
        sig.client_data_json = build_client_data_json(b"different payload");

        let result = verifier.verify_webauthn(payload, &sig);
        assert!(result.is_err());
    }

    #[dialog_common::test]
    async fn webauthn_verify_invalid_client_data_json_fails() {
        let payload = b"bad json test";
        let (_, verifier, mut sig) = create_test_fixture(payload);

        sig.client_data_json = b"not json".to_vec();

        let result = verifier.verify_webauthn(payload, &sig);
        assert!(matches!(
            result.unwrap_err(),
            WebAuthnVerifyError::InvalidClientData(_)
        ));
    }

    #[dialog_common::test]
    fn webauthn_did_display_roundtrip() {
        let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
        let verifier = WebAuthnVerifier {
            key: *sk.verifying_key(),
        };

        let did_string = verifier.to_string();
        assert!(did_string.starts_with("did:key:z"));

        let parsed: WebAuthnVerifier = did_string.parse().unwrap();
        assert_eq!(parsed, verifier);
    }

    #[dialog_common::test]
    fn webauthn_did_from_str_invalid_header() {
        let result: Result<WebAuthnVerifier, _> = "not:a:did".parse();
        assert!(matches!(
            result,
            Err(WebAuthnDidFromStrError::InvalidDidHeader)
        ));
    }

    #[dialog_common::test]
    fn webauthn_did_from_str_missing_prefix() {
        let result: Result<WebAuthnVerifier, _> = "did:key:abc".parse();
        assert!(matches!(
            result,
            Err(WebAuthnDidFromStrError::MissingBase58Prefix)
        ));
    }

    #[dialog_common::test]
    fn webauthn_sec1_bytes_roundtrip() {
        let sk = SigningKey::from_bytes(&[7u8; 32].into()).unwrap();
        let verifier = WebAuthnVerifier {
            key: *sk.verifying_key(),
        };

        let sec1 = verifier.to_sec1_bytes();
        assert_eq!(sec1.len(), 33); // compressed point

        let restored = WebAuthnVerifier::from_sec1_bytes(&sec1).unwrap();
        assert_eq!(restored, verifier);
    }

    #[dialog_common::test]
    fn webauthn_principal_did() {
        let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
        let verifier = WebAuthnVerifier {
            key: *sk.verifying_key(),
        };

        let did = verifier.did();
        assert!(did.as_str().starts_with("did:key:z"));
    }

    #[dialog_common::test]
    async fn webauthn_different_payloads_need_different_signatures() {
        let payload1 = b"payload one";
        let payload2 = b"payload two";
        let (_, verifier, sig1) = create_test_fixture(payload1);

        // sig1 is valid for payload1
        verifier.verify_webauthn(payload1, &sig1).unwrap();

        // sig1 is NOT valid for payload2 (challenge mismatch)
        let result = verifier.verify_webauthn(payload2, &sig1);
        assert!(matches!(result.unwrap_err(), WebAuthnVerifyError::ChallengeMismatch));
    }
}
