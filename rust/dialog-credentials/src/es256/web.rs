//! WebCrypto-based ES256 (ECDSA P-256) signing implementation.
//!
//! This module provides P-256 signing for browser WASM environments using
//! the Web Crypto API (`SubtleCrypto` with `ECDSA` over `P-256`). It supports
//! non-extractable keys for enhanced security, matching the ed25519 web arm.
//!
//! # Security
//!
//! By default, keys are created as **non-extractable**, meaning the private
//! key material cannot be read from JavaScript/WASM code. This is the same
//! defense-in-depth posture the ed25519 WebCrypto arm uses.
//!
//! # Why P-256 on the web matters
//!
//! Browser `SubtleCrypto` supports ECDSA P-256 broadly, and WebAuthn/passkeys
//! produce P-256 keys, so a passkey-derived signer belongs on this arm.

use crate::key::KeyExport;
use dialog_varsig::ecdsa::Es256Signature;
use js_sys::{Object, Reflect, Uint8Array};
use p256::elliptic_curve::sec1::ToEncodedPoint;
use p256::pkcs8::EncodePrivateKey;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{CryptoKey, SubtleCrypto};

pub use crate::key::{ExtractableKey, WebCryptoError};

/// WebCrypto-based ES256 signing key.
///
/// Wraps a WebCrypto `CryptoKey` pair, non-extractable by default. The cached
/// public key bytes are the 33-byte SEC1 compressed point, matching the native
/// arm's `to_compressed_bytes` so that the `did:key` encoding is identical on
/// both arms.
#[derive(Debug, Clone)]
pub struct SigningKey {
    private_key: CryptoKey,
    public_key: CryptoKey,
    public_key_bytes: [u8; 33],
}

impl SigningKey {
    fn new(private_key: CryptoKey, public_key: CryptoKey, public_key_bytes: [u8; 33]) -> Self {
        Self {
            private_key,
            public_key,
            public_key_bytes,
        }
    }

    /// Generate a new P-256 keypair using WebCrypto.
    ///
    /// The private key is created as **non-extractable** for security.
    ///
    /// # Errors
    ///
    /// Returns an error if key generation fails or the browser does not
    /// support ECDSA P-256.
    pub async fn generate() -> Result<Self, WebCryptoError> {
        generate(false).await
    }

    /// Import a keypair from a [`KeyExport`].
    ///
    /// - `Extractable(bytes)` treats the bytes as a 32-byte P-256 scalar,
    ///   imports it via PKCS#8 (non-extractable), and derives the public key.
    /// - `NonExtractable { private_key, public_key }` wraps the supplied
    ///   `CryptoKey`s and re-exports the public key to cache its bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the scalar is invalid or the import fails.
    pub async fn import(key: impl Into<KeyExport>) -> Result<Self, WebCryptoError> {
        let key = key.into();
        match key {
            KeyExport::Extractable(ref bytes) => {
                let scalar: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                    WebCryptoError::KeyImport(format!(
                        "expected 32 scalar bytes, got {}",
                        bytes.len()
                    ))
                })?;
                import(&scalar, false).await
            }
            KeyExport::NonExtractable {
                private_key,
                public_key,
                // ES256 is an ECDSA key with no X25519 derivation, so it never
                // carries an agreement component.
                agreement: _,
            } => {
                let subtle = get_subtle_crypto()?;
                let public_key_bytes = export_public_key_compressed(&subtle, &public_key).await?;
                Ok(SigningKey::new(private_key, public_key, public_key_bytes))
            }
        }
    }

    /// Export the key material.
    ///
    /// If the private key is extractable, returns `KeyExport::Extractable`
    /// with the raw 32-byte scalar recovered from the PKCS#8 encoding.
    /// Otherwise returns `KeyExport::NonExtractable` with both `CryptoKey`s.
    ///
    /// # Errors
    ///
    /// Returns an error if the PKCS#8 export fails.
    pub async fn export(&self) -> Result<KeyExport, WebCryptoError> {
        if self.private_key.extractable() {
            let subtle = get_subtle_crypto()?;
            let promise = subtle
                .export_key("pkcs8", &self.private_key)
                .map_err(|e| WebCryptoError::KeyExport(format!("{e:?}")))?;
            let exported = JsFuture::from(promise)
                .await
                .map_err(|e| WebCryptoError::KeyExport(format!("{e:?}")))?;
            let array = Uint8Array::new(&exported);
            let mut pkcs8_bytes = vec![0u8; array.length() as usize];
            array.copy_to(&mut pkcs8_bytes);
            let scalar = scalar_from_pkcs8(&pkcs8_bytes)?;
            Ok(KeyExport::Extractable(scalar.to_vec()))
        } else {
            Ok(KeyExport::NonExtractable {
                private_key: self.private_key.clone(),
                public_key: self.public_key.clone(),
                agreement: None,
            })
        }
    }

    /// Get the verifying (public) key.
    #[must_use]
    pub fn verifying_key(&self) -> VerifyingKey {
        VerifyingKey::new(self.public_key.clone(), self.public_key_bytes)
    }

    /// Sign a message using the WebCrypto API.
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if signing fails.
    pub async fn sign_bytes(&self, msg: &[u8]) -> Result<Es256Signature, signature::Error> {
        sign(&self.private_key, msg).await
    }
}

/// Generate a keypair with the specified extractability.
async fn generate(extractable: bool) -> Result<SigningKey, WebCryptoError> {
    let subtle = get_subtle_crypto()?;

    let algorithm = ecdsa_key_algorithm()?;

    let key_usages = js_sys::Array::new();
    key_usages.push(&"sign".into());
    key_usages.push(&"verify".into());

    let promise = subtle
        .generate_key_with_object(&algorithm, extractable, &key_usages)
        .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;

    let key_pair = JsFuture::from(promise)
        .await
        .map_err(|e| WebCryptoError::KeyGeneration(format!("{e:?}")))?;

    let private_key: CryptoKey = Reflect::get(&key_pair, &"privateKey".into())
        .map_err(|e| WebCryptoError::KeyGeneration(format!("failed to get privateKey: {e:?}")))?
        .unchecked_into();

    let public_key: CryptoKey = Reflect::get(&key_pair, &"publicKey".into())
        .map_err(|e| WebCryptoError::KeyGeneration(format!("failed to get publicKey: {e:?}")))?
        .unchecked_into();

    let public_key_bytes = export_public_key_compressed(&subtle, &public_key).await?;

    Ok(SigningKey::new(private_key, public_key, public_key_bytes))
}

/// Import a keypair from a 32-byte scalar with the specified extractability.
async fn import(scalar: &[u8; 32], extractable: bool) -> Result<SigningKey, WebCryptoError> {
    let subtle = get_subtle_crypto()?;

    let secret = p256::SecretKey::from_bytes(scalar.into())
        .map_err(|e| WebCryptoError::KeyImport(format!("invalid P-256 scalar: {e}")))?;
    let pkcs8 = secret
        .to_pkcs8_der()
        .map_err(|e| WebCryptoError::KeyImport(format!("PKCS#8 encode failed: {e}")))?;

    let algorithm = ecdsa_key_algorithm()?;

    let key_usages = js_sys::Array::new();
    key_usages.push(&"sign".into());

    let pkcs8_array = Uint8Array::from(pkcs8.as_bytes());

    let promise = subtle
        .import_key_with_object(
            "pkcs8",
            &pkcs8_array.buffer(),
            &algorithm,
            extractable,
            &key_usages,
        )
        .map_err(|e| WebCryptoError::KeyImport(format!("{e:?}")))?;

    let private_key: CryptoKey = JsFuture::from(promise)
        .await
        .map_err(|e| WebCryptoError::KeyImport(format!("{e:?}")))?
        .unchecked_into();

    let public_key_bytes = compressed_public_from_secret(&secret);
    let public_key = import_public_key_compressed(&subtle, &public_key_bytes).await?;

    Ok(SigningKey::new(private_key, public_key, public_key_bytes))
}

/// Sign a message using WebCrypto ECDSA P-256 with SHA-256.
async fn sign(key: &CryptoKey, msg: &[u8]) -> Result<Es256Signature, signature::Error> {
    let subtle = subtle_from_global()?;

    let algorithm = ecdsa_sign_algorithm()
        .map_err(|e| signature::Error::from_source(format!("algorithm setup failed: {e}")))?;

    let msg_array = Uint8Array::from(msg);

    let promise = subtle
        .sign_with_object_and_buffer_source(&algorithm, key, &msg_array)
        .map_err(|e| signature::Error::from_source(format!("sign failed: {e:?}")))?;

    let signature_buffer = JsFuture::from(promise)
        .await
        .map_err(|e| signature::Error::from_source(format!("sign await failed: {e:?}")))?;

    let signature_array = Uint8Array::new(&signature_buffer);

    if signature_array.length() != 64 {
        return Err(signature::Error::from_source(format!(
            "expected 64 signature bytes (r||s), got {}",
            signature_array.length()
        )));
    }

    let mut signature_bytes = [0u8; 64];
    signature_array.copy_to(&mut signature_bytes);

    Ok(Es256Signature::from_bytes(signature_bytes))
}

/// Verify a signature using WebCrypto ECDSA P-256 with SHA-256.
pub(crate) async fn verify(
    key: &CryptoKey,
    msg: &[u8],
    sig: &Es256Signature,
) -> Result<(), signature::Error> {
    let subtle = subtle_from_global()?;

    let algorithm = ecdsa_sign_algorithm()
        .map_err(|e| signature::Error::from_source(format!("algorithm setup failed: {e}")))?;

    let sig_array = Uint8Array::from(sig.to_bytes().as_slice());
    let msg_array = Uint8Array::from(msg);

    let promise = subtle
        .verify_with_object_and_buffer_source_and_buffer_source(
            &algorithm, key, &sig_array, &msg_array,
        )
        .map_err(|e| signature::Error::from_source(format!("verify failed: {e:?}")))?;

    let result = JsFuture::from(promise)
        .await
        .map_err(|e| signature::Error::from_source(format!("verify await failed: {e:?}")))?;

    if result.as_bool() == Some(true) {
        Ok(())
    } else {
        Err(signature::Error::new())
    }
}

/// WebCrypto-based ES256 verifying key.
///
/// Wraps a WebCrypto public `CryptoKey` alongside a cached 33-byte SEC1
/// compressed point for synchronous `did:key` encoding.
#[derive(Debug, Clone)]
pub struct VerifyingKey {
    crypto_key: CryptoKey,
    public_key_bytes: [u8; 33],
}

impl VerifyingKey {
    fn new(crypto_key: CryptoKey, public_key_bytes: [u8; 33]) -> Self {
        Self {
            crypto_key,
            public_key_bytes,
        }
    }

    /// Get a reference to the inner `CryptoKey`.
    #[must_use]
    pub const fn crypto_key(&self) -> &CryptoKey {
        &self.crypto_key
    }

    /// Get the 33-byte SEC1 compressed public key bytes.
    #[must_use]
    pub const fn to_compressed_bytes(&self) -> [u8; 33] {
        self.public_key_bytes
    }

    /// Create a `VerifyingKey` from a WebCrypto `CryptoKey`.
    ///
    /// Validates that the key is an ECDSA P-256 key with `verify` usage, and
    /// exports the compressed public point for `did:key` encoding.
    ///
    /// # Errors
    ///
    /// Returns an error if the key's algorithm or curve is not ECDSA P-256,
    /// the key lacks the `verify` usage, or the export fails.
    pub async fn from_crypto_key(key: CryptoKey) -> Result<Self, WebCryptoError> {
        let algo = key
            .algorithm()
            .map_err(|e| WebCryptoError::InvalidPublicKey(format!("{e:?}")))?;

        let name = Reflect::get(&algo, &"name".into())
            .ok()
            .and_then(|v| v.as_string());
        if name.as_deref() != Some("ECDSA") {
            return Err(WebCryptoError::InvalidPublicKey(format!(
                "expected ECDSA algorithm, got {name:?}"
            )));
        }

        let curve = Reflect::get(&algo, &"namedCurve".into())
            .ok()
            .and_then(|v| v.as_string());
        if curve.as_deref() != Some("P-256") {
            return Err(WebCryptoError::InvalidPublicKey(format!(
                "expected P-256 curve, got {curve:?}"
            )));
        }

        let usages = key.usages();
        if !usages.includes(&"verify".into(), 0) {
            return Err(WebCryptoError::InvalidPublicKey(
                "key does not have 'verify' usage".into(),
            ));
        }

        let subtle = get_subtle_crypto()?;
        let public_key_bytes = export_public_key_compressed(&subtle, &key).await?;

        Ok(Self {
            crypto_key: key,
            public_key_bytes,
        })
    }

    /// Verify a signature for the given message.
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if verification fails.
    pub async fn verify_signature(
        &self,
        msg: &[u8],
        signature: &Es256Signature,
    ) -> Result<(), signature::Error> {
        verify(&self.crypto_key, msg, signature).await
    }
}

/// Build the `{ name: "ECDSA", namedCurve: "P-256" }` algorithm object used for
/// key generation and import.
fn ecdsa_key_algorithm() -> Result<Object, WebCryptoError> {
    let algorithm = Object::new();
    Reflect::set(&algorithm, &"name".into(), &"ECDSA".into())
        .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;
    Reflect::set(&algorithm, &"namedCurve".into(), &"P-256".into())
        .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;
    Ok(algorithm)
}

/// Build the `{ name: "ECDSA", hash: "SHA-256" }` algorithm object used for
/// signing and verification.
fn ecdsa_sign_algorithm() -> Result<Object, WebCryptoError> {
    let algorithm = Object::new();
    Reflect::set(&algorithm, &"name".into(), &"ECDSA".into())
        .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;
    Reflect::set(&algorithm, &"hash".into(), &"SHA-256".into())
        .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;
    Ok(algorithm)
}

/// Get the `SubtleCrypto` interface, returning a `WebCryptoError` on failure.
fn get_subtle_crypto() -> Result<SubtleCrypto, WebCryptoError> {
    let global = js_sys::global();

    let crypto = Reflect::get(&global, &"crypto".into())
        .map_err(|_| WebCryptoError::NotAvailable("crypto not found on global".into()))?;

    if crypto.is_undefined() {
        return Err(WebCryptoError::NotAvailable("crypto is undefined".into()));
    }

    let subtle = Reflect::get(&crypto, &"subtle".into())
        .map_err(|_| WebCryptoError::NotAvailable("subtle not found on crypto".into()))?;

    if subtle.is_undefined() {
        return Err(WebCryptoError::NotAvailable(
            "crypto.subtle is undefined".into(),
        ));
    }

    Ok(subtle.unchecked_into())
}

/// Get the `SubtleCrypto` interface, returning a `signature::Error` on failure
/// (used on the sign/verify paths).
fn subtle_from_global() -> Result<SubtleCrypto, signature::Error> {
    let global = js_sys::global();

    let crypto = Reflect::get(&global, &"crypto".into())
        .map_err(|e| signature::Error::from_source(format!("crypto not found: {e:?}")))?;

    let subtle = Reflect::get(&crypto, &"subtle".into())
        .map_err(|e| signature::Error::from_source(format!("subtle not found: {e:?}")))?;

    Ok(subtle.unchecked_into())
}

/// Export a public `CryptoKey` and return its 33-byte SEC1 compressed point.
///
/// WebCrypto exports ECDSA public keys as a 65-byte uncompressed SEC1 point
/// (`0x04 || X || Y`); we compress it via the `p256` crate so the bytes match
/// the native arm's `did:key` encoding exactly.
async fn export_public_key_compressed(
    subtle: &SubtleCrypto,
    public_key: &CryptoKey,
) -> Result<[u8; 33], WebCryptoError> {
    let promise = subtle
        .export_key("raw", public_key)
        .map_err(|e| WebCryptoError::KeyExport(format!("{e:?}")))?;

    let exported = JsFuture::from(promise)
        .await
        .map_err(|e| WebCryptoError::KeyExport(format!("{e:?}")))?;

    let array = Uint8Array::new(&exported);
    let mut raw = vec![0u8; array.length() as usize];
    array.copy_to(&mut raw);

    compress_sec1(&raw)
}

/// Import a 33-byte compressed public point as a verify-only WebCrypto
/// `CryptoKey`. WebCrypto's `raw` import accepts both compressed and
/// uncompressed SEC1 points for ECDSA.
async fn import_public_key_compressed(
    subtle: &SubtleCrypto,
    compressed: &[u8; 33],
) -> Result<CryptoKey, WebCryptoError> {
    let algorithm = ecdsa_key_algorithm()?;

    let key_usages = js_sys::Array::new();
    key_usages.push(&"verify".into());

    let key_data = Uint8Array::from(compressed.as_slice());

    let promise = subtle
        .import_key_with_object("raw", &key_data.buffer(), &algorithm, true, &key_usages)
        .map_err(|e| WebCryptoError::KeyImport(format!("{e:?}")))?;

    let key = JsFuture::from(promise)
        .await
        .map_err(|e| WebCryptoError::KeyImport(format!("{e:?}")))?;

    Ok(key.unchecked_into())
}

/// Compress a SEC1 public point (accepts a 65-byte uncompressed or an
/// already-33-byte compressed point) to its 33-byte compressed form.
fn compress_sec1(bytes: &[u8]) -> Result<[u8; 33], WebCryptoError> {
    let public = p256::PublicKey::from_sec1_bytes(bytes)
        .map_err(|e| WebCryptoError::InvalidPublicKey(format!("invalid SEC1 point: {e}")))?;
    let point = public.to_encoded_point(true);
    let mut out = [0u8; 33];
    let point_bytes = point.as_bytes();
    if point_bytes.len() != 33 {
        return Err(WebCryptoError::InvalidPublicKey(format!(
            "expected 33 compressed bytes, got {}",
            point_bytes.len()
        )));
    }
    out.copy_from_slice(point_bytes);
    Ok(out)
}

/// Derive the 33-byte compressed public point from a P-256 secret key.
fn compressed_public_from_secret(secret: &p256::SecretKey) -> [u8; 33] {
    let point = secret.public_key().to_encoded_point(true);
    let mut out = [0u8; 33];
    out.copy_from_slice(point.as_bytes());
    out
}

/// Recover the 32-byte P-256 scalar from a PKCS#8 DER private key.
fn scalar_from_pkcs8(der: &[u8]) -> Result<[u8; 32], WebCryptoError> {
    use p256::pkcs8::DecodePrivateKey;
    let secret = p256::SecretKey::from_pkcs8_der(der)
        .map_err(|e| WebCryptoError::KeyExport(format!("PKCS#8 decode failed: {e}")))?;
    Ok(secret.to_bytes().into())
}

impl ExtractableKey for SigningKey {
    async fn generate() -> Result<Self, WebCryptoError> {
        generate(true).await
    }

    async fn import(key: impl Into<KeyExport>) -> Result<Self, WebCryptoError> {
        let key = key.into();
        match key {
            KeyExport::Extractable(ref bytes) => {
                let scalar: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                    WebCryptoError::KeyImport(format!(
                        "expected 32 scalar bytes, got {}",
                        bytes.len()
                    ))
                })?;
                import(&scalar, true).await
            }
            KeyExport::NonExtractable {
                private_key,
                public_key,
                // ES256 is an ECDSA key with no X25519 derivation, so it never
                // carries an agreement component.
                agreement: _,
            } => {
                let subtle = get_subtle_crypto()?;
                let public_key_bytes = export_public_key_compressed(&subtle, &public_key).await?;
                Ok(SigningKey::new(private_key, public_key, public_key_bytes))
            }
        }
    }

    async fn export(&self) -> Result<KeyExport, WebCryptoError> {
        self.export().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::es256::Es256Verifier;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    #[dialog_common::test]
    async fn web_generate_sign_verify_roundtrip() {
        let signing = SigningKey::generate().await.unwrap();
        let verifying = signing.verifying_key();
        let msg = b"hello webcrypto p256";

        let sig = signing.sign_bytes(msg).await.unwrap();
        verifying.verify_signature(msg, &sig).await.unwrap();
        assert!(verifying.verify_signature(b"other", &sig).await.is_err());
    }

    #[dialog_common::test]
    async fn web_did_key_matches_native_encoding() {
        // A key imported from a fixed scalar must produce the same did:key on
        // the web arm as the native arm would, since both encode the 33-byte
        // compressed point behind the same multicodec.
        let scalar = [7u8; 32];
        let web_key = import(&scalar, true).await.unwrap();
        let web_compressed = web_key.verifying_key().to_compressed_bytes();

        let native = p256::ecdsa::SigningKey::from_slice(&scalar).unwrap();
        let native_verifier = Es256Verifier::from(*native.verifying_key());
        let native_compressed =
            crate::es256::Es256VerifyingKey::from(*native.verifying_key()).to_compressed_bytes();

        assert_eq!(
            web_compressed, native_compressed,
            "web and native must agree on the compressed public point"
        );

        // And therefore on the did:key string.
        let web_verifier = Es256Verifier::from(crate::es256::Es256VerifyingKey::WebCrypto(
            web_key.verifying_key(),
        ));
        assert_eq!(web_verifier.to_string(), native_verifier.to_string());
    }

    #[dialog_common::test]
    async fn web_export_import_preserves_did() {
        // Extractable web key: export the scalar and re-import; the DID is stable.
        let signing = <SigningKey as ExtractableKey>::generate().await.unwrap();
        let did_before = Es256Verifier::from(crate::es256::Es256VerifyingKey::WebCrypto(
            signing.verifying_key(),
        ))
        .to_string();

        let export = signing.export().await.unwrap();
        let restored = <SigningKey as ExtractableKey>::import(export)
            .await
            .unwrap();
        let did_after = Es256Verifier::from(crate::es256::Es256VerifyingKey::WebCrypto(
            restored.verifying_key(),
        ))
        .to_string();

        assert_eq!(did_before, did_after);
    }
}
