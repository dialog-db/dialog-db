//! WebAuthn P-256 verifier.
//!
//! Verifies WebAuthn assertions on any platform by checking the ceremony type,
//! the challenge, the authenticator's flags and relying party, and the inner
//! ECDSA P-256 signature over `authenticatorData || SHA-256(clientDataJSON)`.
//! See [`WebAuthnVerifier`] for the full list and the reason for each check.
//!
//! A WebAuthn credential is presented as a distinct `did:key` variant: the key
//! bytes are a plain 33-byte compressed P-256 point, but the multicodec prefix
//! is the private-use [`WEBAUTHN_P256_MULTICODEC`] tag rather than `p256-pub`.
//! That prefix is what routes resolution to a [`WebAuthnVerifier`] (which checks
//! the WebAuthn assertion structure) instead of a plain
//! [`Es256Verifier`](crate::Es256Verifier).

use super::error::{WebAuthnDidFromStrError, WebAuthnVerifyError};
use base58::ToBase58;
use dialog_varsig::{Did, Principal, Verifier, webauthn::WebAuthnSignature};
use p256::ecdsa::signature::Verifier as _;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::str::FromStr;

/// Private-use multicodec prefix identifying a WebAuthn P-256 `did:key`.
///
/// This is the unsigned-varint encoding of `0x300001`, the same private-use tag
/// the varsig header uses to mark a WebAuthn wrapper. There is no
/// standard-registered multicodec for "WebAuthn P-256"; this private-use value
/// is what distinguishes a passkey `did:key` from a plain `p256-pub`
/// (`0x1200`) `did:key`, so that resolution builds a [`WebAuthnVerifier`]. The
/// key bytes that follow are an ordinary 33-byte compressed P-256 point.
pub const WEBAUTHN_P256_MULTICODEC: [u8; 4] = [0x81, 0x80, 0xc0, 0x01];

/// The smallest valid `authenticatorData`: a 32-byte rpIdHash, a flags byte,
/// and a 4-byte signature counter.
const MIN_AUTHENTICATOR_DATA_LEN: usize = 37;

/// The offset of the flags byte in `authenticatorData`.
const FLAGS_OFFSET: usize = 32;

/// The User Present bit of the flags byte: a human performed a gesture.
const FLAG_USER_PRESENT: u8 = 0x01;

/// The User Verified bit of the flags byte: the authenticator verified the
/// human's identity (PIN, biometric), not merely their presence.
const FLAG_USER_VERIFIED: u8 = 0x04;

/// The `clientData.type` an *assertion* carries. A registration carries
/// `"webauthn.create"` and must never be accepted as an authorization.
const CLIENT_DATA_TYPE_GET: &str = "webauthn.get";

/// A WebAuthn P-256 `did:key` verifier.
///
/// Wraps a P-256 ECDSA verifying key and verifies [`WebAuthnSignature`]s on any
/// platform (native or WASM).
///
/// # What verification checks
///
/// A valid inner ECDSA signature is necessary but nowhere near sufficient: the
/// same key signs both WebAuthn ceremonies, and the bytes it signs are
/// structured data whose meaning lives in fields the signature alone does not
/// interpret. So [`verify_webauthn`](Self::verify_webauthn) checks, in order:
///
/// 1. `clientData.type` is `webauthn.get`. A registration
///    (`navigator.credentials.create()`) signs the same
///    `authenticatorData || SHA-256(clientDataJSON)` shape, so without this a
///    site that prompts "register a passkey with us", having set the challenge
///    to the hash of a payload it chose, converts a registration gesture into
///    an authorization over that payload.
/// 2. The challenge is the multihash of the payload being authorized.
/// 3. `authenticatorData` is long enough to hold the fields it must.
/// 4. The User Present *and* User Verified flags are set, so a human gestured
///    and the authenticator verified who they are (PIN, biometric). The signer
///    asks the browser for `userVerification: "required"`, but that is a
///    signing-time hint; an attacker assembling an assertion never goes
///    through it, so both bits are enforced here.
/// 5. `rpIdHash` matches the expected relying party, *when one is known* (see
///    [`expecting_rp_id`](Self::expecting_rp_id)).
/// 6. The inner ECDSA signature verifies, in its canonical low-S form.
///
/// # Why the relying party is optional
///
/// A passkey is presented as a `did:key`, which carries a public key and
/// nothing else. Resolution can therefore recover the key but not the origin
/// the credential was registered for, so a verifier built from a DID alone
/// cannot know which `rpIdHash` to expect. Where the caller does know it (a
/// server verifying assertions for its own origin), [`expecting_rp_id`] binds
/// it and the check becomes mandatory. Where it does not, the remaining checks
/// still hold, and the key itself remains the authorization boundary.
///
/// [`expecting_rp_id`]: Self::expecting_rp_id
#[derive(Debug, Clone)]
pub struct WebAuthnVerifier {
    key: p256::ecdsa::VerifyingKey,
    /// SHA-256 of the expected relying-party id, when the caller knows it.
    rp_id_hash: Option<[u8; 32]>,
}

impl WebAuthnVerifier {
    /// Create a verifier from raw SEC1 point bytes (33-byte compressed or
    /// 65-byte uncompressed).
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a valid P-256 point.
    pub fn from_sec1_bytes(bytes: &[u8]) -> Result<Self, WebAuthnDidFromStrError> {
        let key = p256::ecdsa::VerifyingKey::from_sec1_bytes(bytes)
            .map_err(|_| WebAuthnDidFromStrError::InvalidKey)?;
        Ok(Self {
            key,
            rp_id_hash: None,
        })
    }

    /// Require assertions to name `rp_id` as their relying party.
    ///
    /// Binds the `rpIdHash` check, which is otherwise skipped because a
    /// `did:key` cannot carry an origin. A caller that knows which relying
    /// party should have produced these assertions should always set it: it is
    /// what stops an assertion harvested from another origin the same
    /// authenticator serves from being replayed here.
    #[must_use]
    pub fn expecting_rp_id(mut self, rp_id: &str) -> Self {
        self.rp_id_hash = Some(Sha256::digest(rp_id.as_bytes()).into());
        self
    }

    /// The expected relying-party id hash, if one was bound.
    #[must_use]
    pub const fn rp_id_hash(&self) -> Option<&[u8; 32]> {
        self.rp_id_hash.as_ref()
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
    /// See the [type docs](Self) for the full list of checks and why each one
    /// is needed.
    ///
    /// # Errors
    ///
    /// Returns an error if any step of the verification fails.
    pub fn verify_webauthn(
        &self,
        payload: &[u8],
        sig: &WebAuthnSignature,
    ) -> Result<(), WebAuthnVerifyError> {
        self.validate_client_data(payload, &sig.client_data_json)?;
        self.validate_authenticator_data(&sig.authenticator_data)?;

        // Per WebAuthn: signedData = authenticatorData || SHA-256(clientDataJSON).
        let client_data_hash = Sha256::digest(&sig.client_data_json);
        let mut signed_data = Vec::with_capacity(sig.authenticator_data.len() + 32);
        signed_data.extend_from_slice(&sig.authenticator_data);
        signed_data.extend_from_slice(&client_data_hash);

        // The inner signature is DER-encoded, exactly as a browser authenticator
        // emits it. This is the DER-vs-raw reconciliation: WebAuthn keeps DER on
        // the wire and parses it here, rather than converting to raw r||s.
        let ecdsa_sig = p256::ecdsa::DerSignature::from_bytes(&sig.signature)
            .map_err(|e| WebAuthnVerifyError::InvalidSignature(e.to_string()))?;

        // ECDSA signatures are malleable: negating s yields a second, distinct
        // encoding that verifies against the same key and message. Normalize to
        // the canonical low-S form before verifying, so the two encodings of one
        // authorization become one value and nothing downstream that keys on a
        // signature can be fooled by a duplicate.
        //
        // Normalizing rather than rejecting is deliberate. Real authenticators
        // do emit high-S signatures, and refusing them would break genuine
        // passkeys; the malleability only matters because two byte strings map
        // to one authorization, and collapsing them fixes exactly that.
        let scalars = p256::ecdsa::Signature::try_from(ecdsa_sig)
            .map_err(|e| WebAuthnVerifyError::InvalidSignature(e.to_string()))?;
        let canonical = scalars.normalize_s().unwrap_or(scalars);

        self.key
            .verify(&signed_data, &canonical)
            .map_err(|e| WebAuthnVerifyError::InvalidSignature(e.to_string()))
    }

    /// Validate `clientDataJSON`: it must be an *assertion* whose challenge is
    /// `base64url(multihash-sha256(payload))`.
    fn validate_client_data(
        &self,
        payload: &[u8],
        client_data_json: &[u8],
    ) -> Result<(), WebAuthnVerifyError> {
        // `type` is required, not optional: a clientDataJSON without one is not
        // a WebAuthn ceremony, and serde would otherwise ignore its absence.
        #[derive(serde::Deserialize)]
        struct ClientData {
            #[serde(rename = "type")]
            type_: String,
            challenge: String,
        }

        let client_data: ClientData = serde_json::from_slice(client_data_json)
            .map_err(|e| WebAuthnVerifyError::InvalidClientData(e.to_string()))?;

        if client_data.type_ != CLIENT_DATA_TYPE_GET {
            return Err(WebAuthnVerifyError::InvalidClientData(format!(
                "expected a {CLIENT_DATA_TYPE_GET} assertion, got {}",
                client_data.type_
            )));
        }

        use base64::Engine;
        let challenge_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&client_data.challenge)
            .map_err(|e| WebAuthnVerifyError::InvalidClientData(e.to_string()))?;

        // Expected challenge: the SHA-256 multihash of the payload,
        // `[0x12, 0x20, <32-byte digest>]`.
        let payload_hash = Sha256::digest(payload);
        let mut expected_multihash = Vec::with_capacity(34);
        expected_multihash.push(0x12);
        expected_multihash.push(0x20);
        expected_multihash.extend_from_slice(&payload_hash);

        if challenge_bytes != expected_multihash {
            return Err(WebAuthnVerifyError::ChallengeMismatch);
        }

        Ok(())
    }

    /// Validate `authenticatorData`: long enough, gestured for, and (when the
    /// caller bound one) naming the expected relying party.
    fn validate_authenticator_data(
        &self,
        authenticator_data: &[u8],
    ) -> Result<(), WebAuthnVerifyError> {
        if authenticator_data.len() < MIN_AUTHENTICATOR_DATA_LEN {
            return Err(WebAuthnVerifyError::InvalidAuthenticatorData(format!(
                "authenticatorData must be at least {MIN_AUTHENTICATOR_DATA_LEN} bytes, got {}",
                authenticator_data.len()
            )));
        }

        // A human must have gestured. Without this an assertion assembled
        // without anyone touching the authenticator verifies.
        if authenticator_data[FLAGS_OFFSET] & FLAG_USER_PRESENT == 0 {
            return Err(WebAuthnVerifyError::InvalidAuthenticatorData(
                "the User Present flag is not set".into(),
            ));
        }

        // The human must also have been *verified* (PIN, biometric), not just
        // present. The signer requests `userVerification: "required"`, so every
        // assertion this stack produces carries UV; per WebAuthn, a relying
        // party that requires verification must check the bit, because the
        // request option is only a hint the verifier never sees. Without this,
        // whoever *holds* the authenticator (a stolen security key with no PIN,
        // or a client that asks for `userVerification: "discouraged"`) can mint
        // presence-only assertions that verify.
        if authenticator_data[FLAGS_OFFSET] & FLAG_USER_VERIFIED == 0 {
            return Err(WebAuthnVerifyError::InvalidAuthenticatorData(
                "the User Verified flag is not set".into(),
            ));
        }

        // Bind the assertion to its origin, when the caller knows which one to
        // expect. A did:key carries no origin, so this is skipped otherwise.
        if let Some(expected) = &self.rp_id_hash
            && &authenticator_data[..FLAGS_OFFSET] != expected.as_slice()
        {
            return Err(WebAuthnVerifyError::InvalidAuthenticatorData(
                "rpIdHash does not match the expected relying party".into(),
            ));
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
        let mut raw_bytes = Vec::with_capacity(WEBAUTHN_P256_MULTICODEC.len() + sec1.len());
        raw_bytes.extend_from_slice(&WEBAUTHN_P256_MULTICODEC);
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

        let key_bytes = base58::FromBase58::from_base58(b58)
            .map_err(|_| WebAuthnDidFromStrError::InvalidBase58)?;

        // Expect: 4-byte varint multicodec prefix + 33-byte compressed point.
        let prefix_len = WEBAUTHN_P256_MULTICODEC.len();
        if key_bytes.len() != prefix_len + 33 {
            return Err(WebAuthnDidFromStrError::WrongMulticodec);
        }
        if key_bytes[0..prefix_len] != WEBAUTHN_P256_MULTICODEC {
            return Err(WebAuthnDidFromStrError::WrongMulticodec);
        }

        Self::from_sec1_bytes(&key_bytes[prefix_len..])
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
                f.write_str("a WebAuthn did:key string containing a P-256 public key")
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

    /// Build valid `clientDataJSON` with challenge `base64url(multihash-sha256(payload))`.
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

    /// Build minimal valid authenticator data (37 bytes: rpIdHash | flags | signCount).
    fn build_authenticator_data() -> Vec<u8> {
        let rp_id_hash = Sha256::digest(b"example.com");
        let mut auth_data = Vec::with_capacity(37);
        auth_data.extend_from_slice(&rp_id_hash);
        auth_data.push(0x05); // UP + UV flags
        auth_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
        auth_data
    }

    fn create_test_fixture(payload: &[u8]) -> (SigningKey, WebAuthnVerifier, WebAuthnSignature) {
        let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
        let vk = WebAuthnVerifier {
            key: *sk.verifying_key(),
            rp_id_hash: None,
        };

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
        assert!(matches!(
            result.unwrap_err(),
            WebAuthnVerifyError::ChallengeMismatch
        ));
    }

    #[dialog_common::test]
    async fn webauthn_verify_wrong_key_fails() {
        let payload = b"key mismatch test";
        let (_, _, sig) = create_test_fixture(payload);
        let other_sk = SigningKey::from_bytes(&[99u8; 32].into()).unwrap();
        let other_verifier = WebAuthnVerifier {
            key: *other_sk.verifying_key(),
            rp_id_hash: None,
        };
        assert!(other_verifier.verify_webauthn(payload, &sig).is_err());
    }

    #[dialog_common::test]
    async fn webauthn_verify_tampered_authenticator_data_fails() {
        let payload = b"auth data tamper test";
        let (_, verifier, mut sig) = create_test_fixture(payload);
        sig.authenticator_data[0] ^= 0xff;
        assert!(verifier.verify_webauthn(payload, &sig).is_err());
    }

    #[dialog_common::test]
    async fn webauthn_verify_tampered_client_data_fails() {
        let payload = b"client data tamper test";
        let (_, verifier, mut sig) = create_test_fixture(payload);
        sig.client_data_json = build_client_data_json(b"different payload");
        assert!(verifier.verify_webauthn(payload, &sig).is_err());
    }

    #[dialog_common::test]
    async fn webauthn_verify_invalid_client_data_json_fails() {
        let payload = b"bad json test";
        let (_, verifier, mut sig) = create_test_fixture(payload);
        sig.client_data_json = b"not json".to_vec();
        assert!(matches!(
            verifier.verify_webauthn(payload, &sig).unwrap_err(),
            WebAuthnVerifyError::InvalidClientData(_)
        ));
    }

    #[dialog_common::test]
    fn webauthn_did_display_roundtrip() {
        let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
        let verifier = WebAuthnVerifier {
            key: *sk.verifying_key(),
            rp_id_hash: None,
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
            rp_id_hash: None,
        };
        let sec1 = verifier.to_sec1_bytes();
        assert_eq!(sec1.len(), 33);
        let restored = WebAuthnVerifier::from_sec1_bytes(&sec1).unwrap();
        assert_eq!(restored, verifier);
    }

    #[dialog_common::test]
    fn webauthn_did_distinct_from_p256_did_key() {
        // A WebAuthn did:key must not be a plain p256-pub did:key: the multicodec
        // prefix differs, so a p256-pub did:key must not parse as WebAuthn.
        let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
        let compressed = sk.verifying_key().to_encoded_point(true);
        let mut p256_raw = vec![0x80u8, 0x24]; // p256-pub multicodec
        p256_raw.extend_from_slice(compressed.as_bytes());
        let p256_did = format!("did:key:z{}", p256_raw.as_slice().to_base58());
        let parsed: Result<WebAuthnVerifier, _> = p256_did.parse();
        assert!(matches!(
            parsed,
            Err(WebAuthnDidFromStrError::WrongMulticodec)
        ));
    }

    #[dialog_common::test]
    async fn webauthn_different_payloads_need_different_signatures() {
        let payload1 = b"payload one";
        let payload2 = b"payload two";
        let (_, verifier, sig1) = create_test_fixture(payload1);
        verifier.verify_webauthn(payload1, &sig1).unwrap();
        assert!(matches!(
            verifier.verify_webauthn(payload2, &sig1).unwrap_err(),
            WebAuthnVerifyError::ChallengeMismatch
        ));
    }

    /// Build a fixture with attacker-chosen `clientDataJSON` and
    /// `authenticatorData`, signed by the same key. The challenge is still the
    /// correct multihash of the payload, so only the *unchecked* fields differ
    /// from the honest fixture. This is what an attacker who can drive an
    /// authenticator (rather than go through [`WebAuthnSigner`]) produces.
    fn fixture_with(
        payload: &[u8],
        client_data: &serde_json::Value,
        authenticator_data: Vec<u8>,
    ) -> (WebAuthnVerifier, WebAuthnSignature) {
        let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
        let vk = WebAuthnVerifier {
            key: *sk.verifying_key(),
            rp_id_hash: None,
        };

        // The challenge is honest: this test is about the fields *around* it.
        let payload_hash = Sha256::digest(payload);
        let mut multihash = vec![0x12, 0x20];
        multihash.extend_from_slice(&payload_hash);
        let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&multihash);

        let mut client_data = client_data.clone();
        client_data["challenge"] = serde_json::Value::String(challenge);
        let client_data_json = serde_json::to_vec(&client_data).unwrap();

        let client_data_hash = Sha256::digest(&client_data_json);
        let mut signed_data = authenticator_data.clone();
        signed_data.extend_from_slice(&client_data_hash);

        let ecdsa_sig: p256::ecdsa::DerSignature = sk.sign(&signed_data);
        let sig = WebAuthnSignature::new(
            client_data_json,
            authenticator_data,
            ecdsa_sig.to_bytes().to_vec(),
        );
        (vk, sig)
    }

    /// WebAuthn requires the verifier to check `clientData.type`. An assertion
    /// carries `"webauthn.get"`; a *registration* carries `"webauthn.create"`.
    /// Both ceremonies sign the same `authenticatorData || SHA-256(clientDataJSON)`
    /// structure, so without a type check a credential-creation signature is
    /// accepted as an authorization.
    ///
    /// The attack: a site prompts "register a passkey with us" via
    /// `navigator.credentials.create()` with `challenge` set to
    /// `multihash-sha256(target_payload)`. The attestation that comes back
    /// verifies here as a signature over `target_payload`, so a registration
    /// gesture is silently converted into a signed UCAN.
    #[dialog_common::test]
    async fn webauthn_refuses_a_registration_ceremony_signature() {
        let payload = b"authorize the attacker";
        let (verifier, sig) = fixture_with(
            payload,
            &serde_json::json!({
                "type": "webauthn.create",
                "origin": "https://example.com",
                "crossOrigin": false
            }),
            build_authenticator_data(),
        );

        assert!(
            verifier.verify_webauthn(payload, &sig).is_err(),
            "a clientDataJSON of type webauthn.create is a registration, not an \
             assertion, and must not authorize a payload"
        );
    }

    /// `clientData.type` must be *present*. `ClientData` deserializes only
    /// `challenge`, and serde ignores unknown fields, so a clientDataJSON with
    /// no `type` at all parses happily. A verifier that never reads the field
    /// cannot distinguish a well-formed assertion from an arbitrary JSON blob
    /// an attacker composed.
    #[dialog_common::test]
    async fn webauthn_refuses_client_data_without_a_type() {
        let payload = b"no type field";
        let (verifier, sig) = fixture_with(
            payload,
            &serde_json::json!({ "origin": "https://example.com" }),
            build_authenticator_data(),
        );

        assert!(
            verifier.verify_webauthn(payload, &sig).is_err(),
            "clientDataJSON without a `type` field is not a WebAuthn assertion"
        );
    }

    /// The first 32 bytes of `authenticatorData` are the SHA-256 of the relying
    /// party id. Binding a signature to one origin is the entire point of that
    /// field.
    ///
    /// The attack: a passkey the user registered at `evil.example` produces
    /// assertions that would otherwise verify here as authorizations for this
    /// identity, so any relying party the same authenticator serves could
    /// harvest a signature and replay it into this system.
    ///
    /// A `did:key` carries no origin, so a verifier built from a DID alone
    /// cannot know which rpIdHash to expect; a caller that does know binds it
    /// with [`WebAuthnVerifier::expecting_rp_id`], and the check applies.
    #[dialog_common::test]
    async fn webauthn_refuses_an_assertion_for_a_different_relying_party() {
        let payload = b"signed at the wrong origin";
        let mut foreign_auth_data = Sha256::digest(b"evil.example").to_vec();
        foreign_auth_data.push(0x05); // UP + UV
        foreign_auth_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);

        let (verifier, sig) = fixture_with(
            payload,
            &serde_json::json!({
                "type": "webauthn.get",
                "origin": "https://evil.example",
                "crossOrigin": false
            }),
            foreign_auth_data,
        );

        // Bound to this relying party, the foreign assertion is refused.
        let bound = verifier.clone().expecting_rp_id("example.com");
        assert!(
            bound.verify_webauthn(payload, &sig).is_err(),
            "an assertion whose rpIdHash is another relying party's must not verify"
        );

        // Bound to the party that actually produced it, the same assertion
        // verifies: the check is a real comparison, not a blanket refusal.
        let matching = verifier.expecting_rp_id("evil.example");
        matching
            .verify_webauthn(payload, &sig)
            .expect("an assertion matching its bound relying party must verify");
    }

    /// An assertion whose `authenticatorData` is well-formed but whose relying
    /// party is simply unknown to the verifier still authorizes: a `did:key`
    /// carries no origin, so binding one is the caller's choice. The key
    /// remains the authorization boundary in that case.
    #[dialog_common::test]
    async fn webauthn_without_a_bound_relying_party_still_verifies() {
        let payload = b"no relying party bound";
        let (_, verifier, sig) = create_test_fixture(payload);
        assert!(verifier.rp_id_hash().is_none());
        verifier
            .verify_webauthn(payload, &sig)
            .expect("an unbound verifier checks everything but the relying party");
    }

    /// Bit 0 of `authenticatorData[32]` is User Present: the authenticator
    /// asserts a human performed a gesture. With the flag clear, no one touched
    /// the key. The signer requests `userVerification: "required"`, but that is
    /// a hint to the browser at *signing* time; an attacker assembling an
    /// assertion directly never goes through it, so the check has to happen
    /// here.
    #[dialog_common::test]
    async fn webauthn_refuses_an_assertion_with_no_user_presence() {
        let payload = b"nobody touched the key";
        let mut auth_data = Sha256::digest(b"example.com").to_vec();
        auth_data.push(0x00); // UP and UV both clear
        auth_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);

        let (verifier, sig) = fixture_with(
            payload,
            &serde_json::json!({
                "type": "webauthn.get",
                "origin": "https://example.com",
                "crossOrigin": false
            }),
            auth_data,
        );

        assert!(
            verifier.verify_webauthn(payload, &sig).is_err(),
            "an assertion with the User Present flag clear must not verify"
        );
    }

    /// User *presence* alone is not enough: presence is a touch, verification
    /// is an identity check (PIN, biometric). The signer always requests
    /// `userVerification: "required"`, so honest assertions carry UV; an
    /// assertion with UP set but UV clear is one minted by whoever merely
    /// *holds* the authenticator — a stolen security key with no PIN, or a
    /// client that asked for `userVerification: "discouraged"` — and must be
    /// refused.
    #[dialog_common::test]
    async fn webauthn_refuses_an_assertion_without_user_verification() {
        let payload = b"present but never verified";
        let mut auth_data = Sha256::digest(b"example.com").to_vec();
        auth_data.push(FLAG_USER_PRESENT); // UP set, UV clear
        auth_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);

        let (verifier, sig) = fixture_with(
            payload,
            &serde_json::json!({
                "type": "webauthn.get",
                "origin": "https://example.com",
                "crossOrigin": false
            }),
            auth_data,
        );

        assert!(
            verifier.verify_webauthn(payload, &sig).is_err(),
            "an assertion with the User Verified flag clear must not verify"
        );
    }

    /// `authenticatorData` must be at least 37 bytes (32 rpIdHash + 1 flags +
    /// 4 signCount). Nothing checks the length before it is concatenated, so a
    /// truncated or empty value is accepted as long as the ECDSA signature
    /// covers it. Any rpIdHash or flag check added later must not index into a
    /// short slice, so pin the length requirement itself.
    #[dialog_common::test]
    async fn webauthn_refuses_truncated_authenticator_data() {
        let payload = b"truncated authenticator data";
        let (verifier, sig) = fixture_with(
            payload,
            &serde_json::json!({
                "type": "webauthn.get",
                "origin": "https://example.com",
                "crossOrigin": false
            }),
            Vec::new(),
        );

        assert!(
            verifier.verify_webauthn(payload, &sig).is_err(),
            "authenticatorData shorter than 37 bytes is not a WebAuthn assertion"
        );
    }

    /// ECDSA signatures are malleable: negating `s` yields a second, distinct
    /// DER encoding that verifies against the same key and message, so one
    /// logical authorization would otherwise have two valid byte encodings.
    ///
    /// Verification normalizes to the canonical low-S form rather than refusing
    /// high-S, because real authenticators do emit high-S and refusing would
    /// break genuine passkeys. What matters is that the two encodings collapse
    /// to one value, so nothing downstream that keys on a signature can be
    /// fooled by a duplicate. This pins that collapse: both encodings verify,
    /// and both normalize to the same scalars.
    #[dialog_common::test]
    async fn webauthn_normalizes_a_high_s_malleated_signature() {
        let payload = b"malleable signature";
        let (_, verifier, sig) = create_test_fixture(payload);
        verifier.verify_webauthn(payload, &sig).unwrap();

        // Re-encode the same signature with s negated (high-S form).
        let der = p256::ecdsa::DerSignature::from_bytes(&sig.signature).unwrap();
        let original = p256::ecdsa::Signature::try_from(der).unwrap();
        let (r, s) = (original.r(), original.s());
        let high_s = -*s;
        let malleated =
            p256::ecdsa::Signature::from_scalars(*r, high_s).expect("valid malleated scalars");

        let mut forged = sig.clone();
        forged.signature = malleated.to_der().to_bytes().to_vec();
        assert_ne!(
            forged.signature, sig.signature,
            "the malleated encoding must differ from the original"
        );

        // Both encodings verify (an authenticator may emit either)...
        verifier
            .verify_webauthn(payload, &forged)
            .expect("a high-S encoding is normalized, not refused");

        // ...and both name the same canonical signature, so the two byte
        // strings are one authorization rather than two.
        let canonical = |s: p256::ecdsa::Signature| s.normalize_s().unwrap_or(s);
        assert_eq!(
            canonical(original),
            canonical(malleated),
            "both encodings must normalize to the same signature"
        );
    }
}
