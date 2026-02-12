//! WebCrypto-based Ed25519 signing implementation.
//!
//! This module provides Ed25519 signing for WASM environments using
//! the Web Crypto API. It supports non-extractable keys for enhanced
//! security in browser and service worker environments.
//!
//! # Security
//!
//! By default, all keys are created as **non-extractable**, meaning the private
//! key material cannot be accessed directly from JavaScript/WASM code. This
//! provides defense-in-depth: even if an attacker gains code execution in your
//! service worker, they cannot exfiltrate the private key material.

use crate::key::KeyExport;
use js_sys::{Object, Reflect, Uint8Array};
use varsig::eddsa::Ed25519Signature;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{CryptoKey, SubtleCrypto};

// Re-export for backwards compatibility
pub use crate::key::{ExtractableKey, WebCryptoError};

/// WebCrypto-based Ed25519 signing key.
///
/// This wraps a WebCrypto `CryptoKey` which is non-extractable by default,
/// meaning the private key material cannot be accessed directly from
/// JavaScript/WASM code.
///
/// # Creating Keys
///
/// Use [`SigningKey::generate()`] to create a new non-extractable keypair:
///
/// ```ignore
/// // Generate a new non-extractable key (secure default)
/// let key = SigningKey::generate().await?;
///
/// // Import from seed bytes (non-extractable)
/// let key = SigningKey::import(&seed).await?;
/// ```
#[derive(Debug, Clone)]
pub struct SigningKey {
    /// The WebCrypto private key.
    private_key: CryptoKey,
    /// The WebCrypto public key.
    public_key: CryptoKey,
    /// Cached raw public key bytes.
    public_key_bytes: [u8; 32],
}

impl SigningKey {
    /// Create a `SigningKey` from private and public `CryptoKey`s and cached public key bytes.
    fn new(private_key: CryptoKey, public_key: CryptoKey, public_key_bytes: [u8; 32]) -> Self {
        Self {
            private_key,
            public_key,
            public_key_bytes,
        }
    }

    /// Generate a new Ed25519 keypair using WebCrypto.
    ///
    /// The private key is created as **non-extractable** for security.
    ///
    /// # Errors
    ///
    /// Returns an error if key generation fails or the browser doesn't support Ed25519.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let key = SigningKey::generate().await?;
    /// ```
    pub async fn generate() -> Result<Self, WebCryptoError> {
        generate(false).await
    }

    /// Import a keypair from a [`KeyExport`].
    ///
    /// - `Extractable(bytes)` — converts to a seed, imports via PKCS#8 (non-extractable),
    ///   and derives the public key.
    /// - `NonExtractable { private_key, public_key }` — exports the public key raw bytes
    ///   and constructs a `SigningKey` with both `CryptoKey`s and cached bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the seed bytes are invalid or import fails.
    pub async fn import(key: impl Into<KeyExport>) -> Result<Self, WebCryptoError> {
        let key = key.into();
        match key {
            KeyExport::Extractable(ref bytes) => {
                let seed: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                    WebCryptoError::KeyImport(format!(
                        "expected 32 seed bytes, got {}",
                        bytes.len()
                    ))
                })?;
                import(&seed, false).await
            }
            KeyExport::NonExtractable {
                private_key,
                public_key,
            } => {
                let subtle = get_subtle_crypto()?;
                let public_key_bytes = export_public_key_raw(&subtle, &public_key).await?;
                Ok(SigningKey::new(private_key, public_key, public_key_bytes))
            }
        }
    }

    /// Export the key material.
    ///
    /// If the private key is extractable, returns `KeyExport::Extractable` with the
    /// 32-byte seed extracted from the PKCS#8 encoding (bytes `[16..48]`).
    /// Otherwise, returns `KeyExport::NonExtractable` with clones of both `CryptoKey`s.
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
            // PKCS#8 for Ed25519: 16-byte header, then 32-byte seed
            if pkcs8_bytes.len() < 48 {
                return Err(WebCryptoError::KeyExport(format!(
                    "PKCS#8 too short: expected >= 48 bytes, got {}",
                    pkcs8_bytes.len()
                )));
            }
            let seed = pkcs8_bytes[16..48].to_vec();
            Ok(KeyExport::Extractable(seed))
        } else {
            Ok(KeyExport::NonExtractable {
                private_key: self.private_key.clone(),
                public_key: self.public_key.clone(),
            })
        }
    }

    /// Get the verifying (public) key.
    #[must_use]
    pub fn verifying_key(&self) -> VerifyingKey {
        VerifyingKey::new(self.public_key.clone(), self.public_key_bytes)
    }
}

impl SigningKey {
    /// Sign a message using the WebCrypto API.
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if signing fails.
    pub async fn sign_bytes(&self, msg: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        sign(&self.private_key, msg).await
    }
}

// ============================================================================
// Internal implementation
// ============================================================================

/// Generate a keypair with the specified extractability.
async fn generate(extractable: bool) -> Result<SigningKey, WebCryptoError> {
    let subtle = get_subtle_crypto()?;

    // Create algorithm parameters for Ed25519
    let algorithm = Object::new();
    Reflect::set(&algorithm, &"name".into(), &"Ed25519".into())
        .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;

    // Generate the key pair
    let key_usages = js_sys::Array::new();
    key_usages.push(&"sign".into());
    key_usages.push(&"verify".into());

    let promise = subtle
        .generate_key_with_object(&algorithm, extractable, &key_usages)
        .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;

    let key_pair = JsFuture::from(promise)
        .await
        .map_err(|e| WebCryptoError::KeyGeneration(format!("{e:?}")))?;

    // Extract private and public keys from the key pair object
    let private_key: CryptoKey = Reflect::get(&key_pair, &"privateKey".into())
        .map_err(|e| WebCryptoError::KeyGeneration(format!("failed to get privateKey: {e:?}")))?
        .unchecked_into();

    let public_key: CryptoKey = Reflect::get(&key_pair, &"publicKey".into())
        .map_err(|e| WebCryptoError::KeyGeneration(format!("failed to get publicKey: {e:?}")))?
        .unchecked_into();

    let public_key_bytes = export_public_key_raw(&subtle, &public_key).await?;

    Ok(SigningKey::new(private_key, public_key, public_key_bytes))
}

/// Import a keypair from seed bytes with the specified extractability.
async fn import(seed: &[u8; 32], extractable: bool) -> Result<SigningKey, WebCryptoError> {
    let subtle = get_subtle_crypto()?;

    // Import as PKCS#8 - Ed25519 private keys need proper formatting
    let pkcs8 = Pkcs8::from(seed);

    let algorithm = Object::new();
    Reflect::set(&algorithm, &"name".into(), &"Ed25519".into())
        .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;

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

    // Derive public key bytes from seed, then import into WebCrypto
    let public_key_bytes = ed25519_dalek::SigningKey::from_bytes(seed)
        .verifying_key()
        .to_bytes();
    let public_key = import_public_key_raw(&subtle, &public_key_bytes).await?;

    Ok(SigningKey::new(private_key, public_key, public_key_bytes))
}

/// Sign a message using WebCrypto.
async fn sign(key: &CryptoKey, msg: &[u8]) -> Result<Ed25519Signature, signature::Error> {
    let global = js_sys::global();

    let crypto = Reflect::get(&global, &"crypto".into())
        .map_err(|e| signature::Error::from_source(format!("crypto not found: {e:?}")))?;

    let subtle: SubtleCrypto = Reflect::get(&crypto, &"subtle".into())
        .map_err(|e| signature::Error::from_source(format!("subtle not found: {e:?}")))?
        .unchecked_into();

    let algorithm = Object::new();
    Reflect::set(&algorithm, &"name".into(), &"Ed25519".into())
        .map_err(|e| signature::Error::from_source(format!("failed to set algorithm: {e:?}")))?;

    let msg_array = Uint8Array::from(msg);

    let promise = subtle
        .sign_with_object_and_buffer_source(&algorithm, key, &msg_array)
        .map_err(|e| signature::Error::from_source(format!("sign failed: {e:?}")))?;

    let signature_buffer = JsFuture::from(promise)
        .await
        .map_err(|e| signature::Error::from_source(format!("sign await failed: {e:?}")))?;

    let signature_array = Uint8Array::new(&signature_buffer);
    let mut signature_bytes = [0u8; 64];

    if signature_array.length() != 64 {
        return Err(signature::Error::from_source(format!(
            "expected 64 bytes, got {}",
            signature_array.length()
        )));
    }

    signature_array.copy_to(&mut signature_bytes);

    Ok(Ed25519Signature::from_bytes(signature_bytes))
}

/// Verify a signature using WebCrypto.
pub(crate) async fn verify(
    key: &CryptoKey,
    msg: &[u8],
    sig: &Ed25519Signature,
) -> Result<(), signature::Error> {
    let global = js_sys::global();

    let crypto = Reflect::get(&global, &"crypto".into())
        .map_err(|e| signature::Error::from_source(format!("crypto not found: {e:?}")))?;

    let subtle: SubtleCrypto = Reflect::get(&crypto, &"subtle".into())
        .map_err(|e| signature::Error::from_source(format!("subtle not found: {e:?}")))?
        .unchecked_into();

    let algorithm = Object::new();
    Reflect::set(&algorithm, &"name".into(), &"Ed25519".into())
        .map_err(|e| signature::Error::from_source(format!("failed to set algorithm: {e:?}")))?;

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

/// WebCrypto-based Ed25519 verifying key.
///
/// This wraps a WebCrypto `CryptoKey` for signature verification,
/// alongside a cached copy of the raw public key bytes for synchronous
/// DID encoding.
#[derive(Debug, Clone)]
pub struct VerifyingKey {
    /// The WebCrypto public key (used for async verification).
    crypto_key: CryptoKey,
    /// Cached raw public key bytes (used for DID encoding).
    public_key_bytes: [u8; 32],
}

impl VerifyingKey {
    /// Create a `VerifyingKey` from a `CryptoKey` and its raw public key bytes.
    fn new(crypto_key: CryptoKey, public_key_bytes: [u8; 32]) -> Self {
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

    /// Get the raw public key bytes.
    #[must_use]
    pub const fn to_bytes(&self) -> [u8; 32] {
        self.public_key_bytes
    }
}

impl VerifyingKey {
    /// Create a `VerifyingKey` from a WebCrypto `CryptoKey`.
    ///
    /// Validates that the key is an Ed25519 key with `verify` usage,
    /// and exports the raw public key bytes for synchronous DID encoding.
    ///
    /// # Errors
    ///
    /// Returns an error if the key's algorithm is not Ed25519,
    /// the key does not have the `verify` usage, or the raw key
    /// export fails.
    pub async fn from_crypto_key(key: CryptoKey) -> Result<Self, WebCryptoError> {
        let name = key
            .algorithm()
            .ok()
            .and_then(|algo| Reflect::get(&algo, &"name".into()).ok())
            .and_then(|v| v.as_string());

        if name.as_deref() != Some("Ed25519") {
            return Err(WebCryptoError::InvalidPublicKey(format!(
                "expected Ed25519 algorithm, got {:?}",
                name
            )));
        }

        let usages = key.usages();
        if !usages.includes(&"verify".into(), 0) {
            return Err(WebCryptoError::InvalidPublicKey(
                "key does not have 'verify' usage".into(),
            ));
        }

        let subtle = get_subtle_crypto()?;
        let public_key_bytes = export_public_key_raw(&subtle, &key).await?;

        Ok(Self {
            crypto_key: key,
            public_key_bytes,
        })
    }
}

/// Get the SubtleCrypto interface.
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

/// Export a public key as raw bytes.
async fn export_public_key_raw(
    subtle: &SubtleCrypto,
    public_key: &CryptoKey,
) -> Result<[u8; 32], WebCryptoError> {
    let promise = subtle
        .export_key("raw", public_key)
        .map_err(|e| WebCryptoError::KeyExport(format!("{e:?}")))?;

    let exported = JsFuture::from(promise)
        .await
        .map_err(|e| WebCryptoError::KeyExport(format!("{e:?}")))?;

    let array = Uint8Array::new(&exported);
    let mut bytes = [0u8; 32];

    if array.length() != 32 {
        return Err(WebCryptoError::KeyExport(format!(
            "expected 32 bytes, got {}",
            array.length()
        )));
    }

    array.copy_to(&mut bytes);
    Ok(bytes)
}

/// Import raw public key bytes as a WebCrypto `CryptoKey`.
async fn import_public_key_raw(
    subtle: &SubtleCrypto,
    bytes: &[u8; 32],
) -> Result<CryptoKey, WebCryptoError> {
    let algorithm = Object::new();
    Reflect::set(&algorithm, &"name".into(), &"Ed25519".into())
        .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;

    let key_usages = js_sys::Array::new();
    key_usages.push(&"verify".into());

    let key_data = Uint8Array::from(bytes.as_slice());

    let promise = subtle
        .import_key_with_object("raw", &key_data.buffer(), &algorithm, true, &key_usages)
        .map_err(|e| WebCryptoError::KeyImport(format!("{e:?}")))?;

    let key = JsFuture::from(promise)
        .await
        .map_err(|e| WebCryptoError::KeyImport(format!("{e:?}")))?;

    Ok(key.unchecked_into())
}

/// PKCS#8 wrapper for Ed25519 private key.
///
/// WebCrypto requires PKCS#8 format for importing Ed25519 private keys.
/// This type wraps the DER-encoded PKCS#8 structure.
struct Pkcs8([u8; 48]);

impl Pkcs8 {
    /// Get the PKCS#8 bytes as a slice.
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8; 32]> for Pkcs8 {
    fn from(seed: &[u8; 32]) -> Self {
        // PKCS#8 header for Ed25519:
        // SEQUENCE {
        //   INTEGER 0 (version)
        //   SEQUENCE {
        //     OBJECT IDENTIFIER 1.3.101.112 (Ed25519)
        //   }
        //   OCTET STRING {
        //     OCTET STRING (the 32-byte seed)
        //   }
        // }
        let mut pkcs8 = [0u8; 48];
        // Header (16 bytes)
        pkcs8[..16].copy_from_slice(&[
            0x30, 0x2e, // SEQUENCE, 46 bytes
            0x02, 0x01, 0x00, // INTEGER 0 (version)
            0x30, 0x05, // SEQUENCE, 5 bytes
            0x06, 0x03, 0x2b, 0x65, 0x70, // OID 1.3.101.112 (Ed25519)
            0x04, 0x22, // OCTET STRING, 34 bytes
            0x04, 0x20, // OCTET STRING, 32 bytes (the seed)
        ]);
        // Seed (32 bytes)
        pkcs8[16..].copy_from_slice(seed);
        Self(pkcs8)
    }
}

// ============================================================================
// Extractable key support
// ============================================================================

impl ExtractableKey for SigningKey {
    async fn generate() -> Result<Self, WebCryptoError> {
        generate(true).await
    }

    async fn import(key: impl Into<KeyExport>) -> Result<Self, WebCryptoError> {
        let key = key.into();
        match key {
            KeyExport::Extractable(ref bytes) => {
                let seed: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                    WebCryptoError::KeyImport(format!(
                        "expected 32 seed bytes, got {}",
                        bytes.len()
                    ))
                })?;
                import(&seed, true).await
            }
            KeyExport::NonExtractable {
                private_key,
                public_key,
            } => {
                let subtle = get_subtle_crypto()?;
                let public_key_bytes = export_public_key_raw(&subtle, &public_key).await?;
                Ok(SigningKey::new(private_key, public_key, public_key_bytes))
            }
        }
    }

    async fn export(&self) -> Result<KeyExport, WebCryptoError> {
        self.export().await
    }
}
