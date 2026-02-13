//! WebAuthn P-256 signer using the browser's Web Authentication API.
//!
//! The [`WebAuthnSigner`] wraps a WebAuthn credential and, on WASM targets,
//! uses `navigator.credentials` to produce [`WebAuthnSignature`]s.
//!
//! # Platforms
//!
//! - The struct and reconstruction (`from_raw_parts`) are available everywhere.
//! - Registration (`register`) and signing (`Signer` impl) require a browser
//!   environment (`wasm32-unknown-unknown`).

use super::verifier::WebAuthnVerifier;
use dialog_varsig::{Did, Principal};
use serde::Serialize;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use dialog_varsig::{Signer, webauthn::WebAuthnSignature};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use js_sys::{Object, Reflect, Uint8Array};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use sha2::{Digest, Sha256};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::prelude::*;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_futures::JsFuture;

/// Errors from WebAuthn signing operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WebAuthnSignerError {
    /// The WebAuthn API is not available in this environment.
    #[error("WebAuthn API not available: {0}")]
    NotAvailable(String),

    /// Credential registration failed.
    #[error("registration failed: {0}")]
    RegistrationFailed(String),

    /// Assertion (signing) failed.
    #[error("assertion failed: {0}")]
    AssertionFailed(String),

    /// The public key is invalid or unsupported.
    #[error("invalid public key: {0}")]
    InvalidPublicKey(String),

    /// A JavaScript interop error occurred.
    #[error("JS error: {0}")]
    JsError(String),
}

/// Configuration for registering a new WebAuthn credential.
pub struct RegistrationOptions {
    /// The relying party identifier (typically the domain, e.g. `"example.com"`).
    pub rp_id: String,
    /// A human-readable relying party name.
    pub rp_name: String,
    /// An opaque user identifier (unique per user on this RP).
    pub user_id: Vec<u8>,
    /// The user's account name (e.g. `"user@example.com"`).
    pub user_name: String,
    /// A human-readable display name for the user.
    pub user_display_name: String,
}

/// A WebAuthn P-256 signer.
///
/// On WASM targets this uses `navigator.credentials.get()` to sign payloads.
/// On all platforms it can be reconstructed from stored parts for DID/Principal
/// use, but the [`Signer`] implementation is only available on WASM.
///
/// # Creating a signer
///
/// ## Register a new passkey (WASM only)
///
/// ```ignore
/// let signer = WebAuthnSigner::register(RegistrationOptions {
///     rp_id: "example.com".into(),
///     rp_name: "Example".into(),
///     user_id: b"user-123".to_vec(),
///     user_name: "user@example.com".into(),
///     user_display_name: "User".into(),
/// }).await?;
/// ```
///
/// ## Reconstruct from stored credential
///
/// ```ignore
/// let signer = WebAuthnSigner::from_raw_parts(
///     credential_id,
///     "example.com",
///     &public_key_sec1_bytes,
/// )?;
/// ```
#[derive(Debug, Clone)]
pub struct WebAuthnSigner {
    /// The credential ID returned by the authenticator during registration.
    credential_id: Vec<u8>,
    /// The relying party identifier (domain).
    rp_id: String,
    /// The P-256 verifier derived from the credential's public key.
    verifier: WebAuthnVerifier,
}

impl WebAuthnSigner {
    /// Reconstruct a signer from a previously-stored credential ID and public key.
    ///
    /// `public_key_sec1` must be a SEC1-encoded P-256 point (33 bytes compressed
    /// or 65 bytes uncompressed).
    ///
    /// # Errors
    ///
    /// Returns an error if `public_key_sec1` is not a valid P-256 point.
    pub fn from_raw_parts(
        credential_id: Vec<u8>,
        rp_id: impl Into<String>,
        public_key_sec1: &[u8],
    ) -> Result<Self, WebAuthnSignerError> {
        let verifier = WebAuthnVerifier::from_sec1_bytes(public_key_sec1)
            .map_err(|e| WebAuthnSignerError::InvalidPublicKey(e.to_string()))?;
        Ok(Self {
            credential_id,
            rp_id: rp_id.into(),
            verifier,
        })
    }

    /// Get the raw credential ID.
    #[must_use]
    pub fn credential_id(&self) -> &[u8] {
        &self.credential_id
    }

    /// Get the relying party ID.
    #[must_use]
    pub fn rp_id(&self) -> &str {
        &self.rp_id
    }

    /// Get the associated [`WebAuthnVerifier`].
    #[must_use]
    pub const fn webauthn_did(&self) -> &WebAuthnVerifier {
        &self.verifier
    }

    /// Get the compressed SEC1 public key bytes (33 bytes).
    #[must_use]
    pub fn to_public_key_bytes(&self) -> Vec<u8> {
        self.verifier.to_sec1_bytes()
    }
}

impl std::fmt::Display for WebAuthnSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.verifier)
    }
}

impl Principal for WebAuthnSigner {
    fn did(&self) -> Did {
        self.verifier.did()
    }
}

impl Serialize for WebAuthnSigner {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.verifier.serialize(serializer)
    }
}

// ============================================================================
// WASM-only: registration, signing, Authority
// ============================================================================

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl WebAuthnSigner {
    /// Register a new WebAuthn credential (passkey) and return a signer.
    ///
    /// Calls `navigator.credentials.create()`, which triggers a browser prompt
    /// for the user to create a passkey (biometric / PIN / security key).
    /// Only P-256 (ES256, COSE algorithm -7) credentials are accepted.
    ///
    /// # Errors
    ///
    /// Returns an error if the WebAuthn API is unavailable, the user cancels
    /// the prompt, or the authenticator returns a non-P-256 credential.
    pub async fn register(opts: RegistrationOptions) -> Result<Self, WebAuthnSignerError> {
        let public_key_opts = Object::new();

        // Random challenge for the registration ceremony
        let challenge = random_challenge(32)?;
        let challenge_array = Uint8Array::from(challenge.as_slice());
        js_set(&public_key_opts, "challenge", &challenge_array)?;

        // Relying party
        let rp = Object::new();
        js_set(&rp, "id", &JsValue::from_str(&opts.rp_id))?;
        js_set(&rp, "name", &JsValue::from_str(&opts.rp_name))?;
        js_set(&public_key_opts, "rp", &rp)?;

        // User
        let user = Object::new();
        let user_id_array = Uint8Array::from(opts.user_id.as_slice());
        js_set(&user, "id", &user_id_array)?;
        js_set(&user, "name", &JsValue::from_str(&opts.user_name))?;
        js_set(
            &user,
            "displayName",
            &JsValue::from_str(&opts.user_display_name),
        )?;
        js_set(&public_key_opts, "user", &user)?;

        // Only allow ES256 (P-256)
        let params = js_sys::Array::new();
        let param = Object::new();
        js_set(&param, "type", &JsValue::from_str("public-key"))?;
        js_set(&param, "alg", &JsValue::from_f64(-7.0))?; // ES256
        params.push(&param);
        js_set(&public_key_opts, "pubKeyCredParams", &params)?;

        // Authenticator selection
        let auth_selection = Object::new();
        js_set(
            &auth_selection,
            "residentKey",
            &JsValue::from_str("required"),
        )?;
        js_set(
            &auth_selection,
            "userVerification",
            &JsValue::from_str("required"),
        )?;
        js_set(&public_key_opts, "authenticatorSelection", &auth_selection)?;

        js_set(&public_key_opts, "timeout", &JsValue::from_f64(60_000.0))?;

        // Wrap in CredentialCreationOptions
        let options = Object::new();
        js_set(&options, "publicKey", &public_key_opts)?;

        // navigator.credentials.create(options)
        let credentials = get_credentials_container()?;
        let create_fn: js_sys::Function = js_get(&credentials, "create")?.unchecked_into();
        let promise: js_sys::Promise = create_fn
            .call1(&credentials, &options)
            .map_err(|e| WebAuthnSignerError::RegistrationFailed(format!("{e:?}")))?
            .unchecked_into();
        let credential = JsFuture::from(promise)
            .await
            .map_err(|e| WebAuthnSignerError::RegistrationFailed(format!("{e:?}")))?;

        // Extract credential ID
        let raw_id = js_get(&credential, "rawId")?;
        let credential_id = array_buffer_to_vec(&raw_id);

        // Extract and validate the public key
        let response = js_get(&credential, "response")?;

        let get_algo_fn: js_sys::Function = js_get(&response, "getPublicKeyAlgorithm")?
            .dyn_into()
            .map_err(|_| {
                WebAuthnSignerError::RegistrationFailed(
                    "getPublicKeyAlgorithm not supported".into(),
                )
            })?;
        let algo = get_algo_fn
            .call0(&response)
            .map_err(|e| WebAuthnSignerError::RegistrationFailed(format!("{e:?}")))?;
        let algo_num = algo.as_f64().unwrap_or(0.0) as i64;
        if algo_num != -7 {
            return Err(WebAuthnSignerError::InvalidPublicKey(format!(
                "expected ES256 (alg -7), got alg {algo_num}"
            )));
        }

        let get_pk_fn: js_sys::Function =
            js_get(&response, "getPublicKey")?.dyn_into().map_err(|_| {
                WebAuthnSignerError::RegistrationFailed("getPublicKey not supported".into())
            })?;
        let spki_buffer = get_pk_fn
            .call0(&response)
            .map_err(|e| WebAuthnSignerError::RegistrationFailed(format!("{e:?}")))?;
        if spki_buffer.is_null() || spki_buffer.is_undefined() {
            return Err(WebAuthnSignerError::RegistrationFailed(
                "authenticator did not return a public key".into(),
            ));
        }
        let spki_bytes = array_buffer_to_vec(&spki_buffer);
        let point_bytes = parse_p256_spki(&spki_bytes)?;

        let verifier = WebAuthnVerifier::from_sec1_bytes(point_bytes)
            .map_err(|e| WebAuthnSignerError::InvalidPublicKey(e.to_string()))?;

        Ok(Self {
            credential_id,
            rp_id: opts.rp_id,
            verifier,
        })
    }

    /// Perform a WebAuthn assertion to sign the given payload.
    ///
    /// Calls `navigator.credentials.get()` with a challenge derived from
    /// `SHA-256(payload)` encoded as a multihash. The browser will prompt
    /// the user for a gesture (biometric / PIN).
    async fn sign_webauthn(
        &self,
        payload: &[u8],
    ) -> Result<WebAuthnSignature, WebAuthnSignerError> {
        // Challenge = raw multihash bytes; the browser base64url-encodes them
        // into clientDataJSON.challenge automatically.
        let payload_hash = Sha256::digest(payload);
        let mut challenge = Vec::with_capacity(34);
        challenge.push(0x12); // SHA-256 multicodec
        challenge.push(0x20); // 32-byte digest
        challenge.extend_from_slice(&payload_hash);

        let public_key_opts = Object::new();

        let challenge_array = Uint8Array::from(challenge.as_slice());
        js_set(&public_key_opts, "challenge", &challenge_array)?;
        js_set(&public_key_opts, "rpId", &JsValue::from_str(&self.rp_id))?;
        js_set(
            &public_key_opts,
            "userVerification",
            &JsValue::from_str("required"),
        )?;
        js_set(&public_key_opts, "timeout", &JsValue::from_f64(60_000.0))?;

        // allowCredentials — restrict to our stored credential
        let allow_creds = js_sys::Array::new();
        let descriptor = Object::new();
        js_set(&descriptor, "type", &JsValue::from_str("public-key"))?;
        let cred_id_array = Uint8Array::from(self.credential_id.as_slice());
        js_set(&descriptor, "id", &cred_id_array)?;
        allow_creds.push(&descriptor);
        js_set(&public_key_opts, "allowCredentials", &allow_creds)?;

        // Wrap in CredentialRequestOptions
        let options = Object::new();
        js_set(&options, "publicKey", &public_key_opts)?;

        // navigator.credentials.get(options)
        let credentials = get_credentials_container()?;
        let get_fn: js_sys::Function = js_get(&credentials, "get")?.unchecked_into();
        let promise: js_sys::Promise = get_fn
            .call1(&credentials, &options)
            .map_err(|e| WebAuthnSignerError::AssertionFailed(format!("{e:?}")))?
            .unchecked_into();
        let credential = JsFuture::from(promise)
            .await
            .map_err(|e| WebAuthnSignerError::AssertionFailed(format!("{e:?}")))?;

        // Extract authenticator response fields
        let response = js_get(&credential, "response")?;
        let client_data_json = extract_response_field(&response, "clientDataJSON")?;
        let authenticator_data = extract_response_field(&response, "authenticatorData")?;
        let sig_bytes = extract_response_field(&response, "signature")?;

        Ok(WebAuthnSignature::new(
            client_data_json,
            authenticator_data,
            sig_bytes,
        ))
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl Signer<WebAuthnSignature> for WebAuthnSigner {
    async fn sign(&self, payload: &[u8]) -> Result<WebAuthnSignature, signature::Error> {
        self.sign_webauthn(payload)
            .await
            .map_err(|e| signature::Error::from_source(e))
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl dialog_capability::Authority for WebAuthnSigner {
    type Signature = WebAuthnSignature;
}

// ============================================================================
// WASM helpers
// ============================================================================

/// Expected SPKI header for a P-256 uncompressed public key (26 bytes).
///
/// ```text
/// SEQUENCE (89 bytes)
///   SEQUENCE (19 bytes)
///     OID 1.2.840.10045.2.1 (ecPublicKey)
///     OID 1.2.840.10045.3.1.7 (prime256v1 / P-256)
///   BIT STRING (66 bytes, 0 unused bits)
///     04 || x || y  (65-byte uncompressed point)
/// ```
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
const P256_SPKI_HEADER: [u8; 26] = [
    0x30, 0x59, // SEQUENCE, 89 bytes
    0x30, 0x13, // SEQUENCE, 19 bytes
    0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, // OID ecPublicKey
    0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, // OID prime256v1
    0x03, 0x42, // BIT STRING, 66 bytes
    0x00, // 0 unused bits
];

/// Parse a P-256 public key from SPKI (SubjectPublicKeyInfo) DER encoding.
///
/// Returns the uncompressed SEC1 point bytes (65 bytes: `04 || x || y`).
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn parse_p256_spki(spki: &[u8]) -> Result<&[u8], WebAuthnSignerError> {
    const EXPECTED_LEN: usize = 91; // 26-byte header + 65-byte uncompressed point
    if spki.len() != EXPECTED_LEN {
        return Err(WebAuthnSignerError::InvalidPublicKey(format!(
            "expected {EXPECTED_LEN}-byte SPKI, got {} bytes",
            spki.len()
        )));
    }
    if spki[..P256_SPKI_HEADER.len()] != P256_SPKI_HEADER {
        return Err(WebAuthnSignerError::InvalidPublicKey(
            "SPKI header does not match P-256".into(),
        ));
    }
    Ok(&spki[P256_SPKI_HEADER.len()..])
}

/// Get `navigator.credentials`.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn get_credentials_container() -> Result<JsValue, WebAuthnSignerError> {
    let global = js_sys::global();
    let navigator = Reflect::get(&global, &"navigator".into())
        .map_err(|_| WebAuthnSignerError::NotAvailable("navigator not found".into()))?;
    if navigator.is_undefined() {
        return Err(WebAuthnSignerError::NotAvailable(
            "navigator is undefined".into(),
        ));
    }
    let credentials = Reflect::get(&navigator, &"credentials".into())
        .map_err(|_| WebAuthnSignerError::NotAvailable("credentials not found".into()))?;
    if credentials.is_undefined() {
        return Err(WebAuthnSignerError::NotAvailable(
            "navigator.credentials is undefined".into(),
        ));
    }
    Ok(credentials)
}

/// Generate random bytes via `crypto.getRandomValues()`.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn random_challenge(len: usize) -> Result<Vec<u8>, WebAuthnSignerError> {
    let global = js_sys::global();
    let crypto = Reflect::get(&global, &"crypto".into())
        .map_err(|_| WebAuthnSignerError::NotAvailable("crypto not found".into()))?;
    let array = Uint8Array::new_with_length(len as u32);
    let get_random_values: js_sys::Function = Reflect::get(&crypto, &"getRandomValues".into())
        .map_err(|e| WebAuthnSignerError::JsError(format!("{e:?}")))?
        .unchecked_into();
    get_random_values
        .call1(&crypto, &array)
        .map_err(|e| WebAuthnSignerError::JsError(format!("{e:?}")))?;
    let mut bytes = vec![0u8; len];
    array.copy_to(&mut bytes);
    Ok(bytes)
}

/// Shorthand for `Reflect::get` with a string key.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn js_get(obj: &JsValue, key: &str) -> Result<JsValue, WebAuthnSignerError> {
    Reflect::get(obj, &JsValue::from_str(key))
        .map_err(|e| WebAuthnSignerError::JsError(format!("failed to get '{key}': {e:?}")))
}

/// Shorthand for `Reflect::set` with a string key.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn js_set(obj: &Object, key: &str, value: &JsValue) -> Result<(), WebAuthnSignerError> {
    Reflect::set(obj, &JsValue::from_str(key), value)
        .map_err(|e| WebAuthnSignerError::JsError(format!("failed to set '{key}': {e:?}")))?;
    Ok(())
}

/// Read an `ArrayBuffer` property from a JS object as `Vec<u8>`.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn extract_response_field(obj: &JsValue, key: &str) -> Result<Vec<u8>, WebAuthnSignerError> {
    let buffer = Reflect::get(obj, &JsValue::from_str(key))
        .map_err(|e| WebAuthnSignerError::AssertionFailed(format!("missing '{key}': {e:?}")))?;
    Ok(array_buffer_to_vec(&buffer))
}

/// Convert a JS `ArrayBuffer` (or typed-array view) to `Vec<u8>`.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn array_buffer_to_vec(value: &JsValue) -> Vec<u8> {
    let array = Uint8Array::new(value);
    let mut bytes = vec![0u8; array.length() as usize];
    array.copy_to(&mut bytes);
    bytes
}

// ============================================================================
// Tests (cross-platform)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_varsig::Principal;
    use p256::ecdsa::SigningKey;

    fn test_signer(seed: u8) -> WebAuthnSigner {
        let sk = SigningKey::from_bytes(&[seed; 32].into()).unwrap();
        let sec1 = sk.verifying_key().to_encoded_point(true);
        WebAuthnSigner::from_raw_parts(vec![seed; 16], "example.com", sec1.as_bytes()).unwrap()
    }

    #[dialog_common::test]
    fn from_raw_parts_compressed_point() {
        let signer = test_signer(42);
        assert_eq!(signer.credential_id(), &[42u8; 16]);
        assert_eq!(signer.rp_id(), "example.com");
        assert_eq!(signer.to_public_key_bytes().len(), 33);
    }

    #[dialog_common::test]
    fn from_raw_parts_uncompressed_point() {
        let sk = SigningKey::from_bytes(&[7u8; 32].into()).unwrap();
        let uncompressed = sk.verifying_key().to_encoded_point(false);
        let signer =
            WebAuthnSigner::from_raw_parts(vec![1, 2, 3], "test.example", uncompressed.as_bytes())
                .unwrap();
        assert_eq!(signer.rp_id(), "test.example");
    }

    #[dialog_common::test]
    fn from_raw_parts_invalid_key_fails() {
        let result = WebAuthnSigner::from_raw_parts(vec![1], "x.com", &[0u8; 10]);
        assert!(result.is_err());
    }

    #[dialog_common::test]
    fn display_is_did_key() {
        let signer = test_signer(42);
        let display = signer.to_string();
        assert!(display.starts_with("did:key:z"));
    }

    #[dialog_common::test]
    fn principal_did_matches_verifier() {
        let signer = test_signer(42);
        assert_eq!(signer.did(), signer.webauthn_did().did());
    }

    #[dialog_common::test]
    fn different_seeds_produce_different_dids() {
        let s1 = test_signer(1);
        let s2 = test_signer(2);
        assert_ne!(s1.did(), s2.did());
    }

    #[dialog_common::test]
    fn serde_roundtrip() {
        let signer = test_signer(42);
        let json = serde_json::to_string(&signer).unwrap();
        // Serializes as the DID string
        assert!(json.contains("did:key:z"));
        // Can be deserialized as a WebAuthnVerifier
        let verifier: WebAuthnVerifier = serde_json::from_str(&json).unwrap();
        assert_eq!(verifier, *signer.webauthn_did());
    }
}
