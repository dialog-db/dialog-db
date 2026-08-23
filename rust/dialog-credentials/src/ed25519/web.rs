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
use dialog_varsig::eddsa::Ed25519Signature;
use js_sys::{Object, Reflect, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{CryptoKey, SubtleCrypto};

use crate::key::AgreementKeyPair;

// Re-export for backwards compatibility
pub use crate::key::{ExtractableAgreementKey, ExtractableKey, WebCryptoError};

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
    /// The X25519 agreement key derived from the same seed.
    ///
    /// Held rather than derived on demand because a non-extractable signing key
    /// never yields its seed again. `None` only when the key was restored from
    /// an archive that predates the agreement component.
    agreement: Option<AgreementSecretKey>,
}

impl SigningKey {
    /// Create a `SigningKey` from private and public `CryptoKey`s and cached public key bytes.
    fn new(private_key: CryptoKey, public_key: CryptoKey, public_key_bytes: [u8; 32]) -> Self {
        Self {
            private_key,
            public_key,
            public_key_bytes,
            agreement: None,
        }
    }

    /// Attach a derived X25519 agreement key.
    fn with_agreement(mut self, agreement: AgreementSecretKey) -> Self {
        self.agreement = Some(agreement);
        self
    }

    /// Get the derived X25519 agreement key, if this key carries one.
    #[must_use]
    pub const fn agreement_key(&self) -> Option<&AgreementSecretKey> {
        self.agreement.as_ref()
    }

    /// Whether the underlying private `CryptoKey` is extractable.
    #[must_use]
    pub fn private_key_is_extractable(&self) -> bool {
        self.private_key.extractable()
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
        // WebCrypto cannot derive X25519 from an Ed25519 key, and a
        // non-extractable key never yields its seed -- so a key generated
        // directly as non-extractable could never grow an agreement key.
        // Instead generate an *extractable* key, read its seed once, and
        // import both the signing key and the derived agreement key from that
        // seed as non-extractable keys. The extractable original is dropped
        // here and the seed never leaves this function.
        let extractable = generate(true).await?;
        let seed = extractable.export_seed().await?;
        import_with_agreement(&seed, false).await
    }

    /// Export the raw 32-byte seed from an extractable private key.
    async fn export_seed(&self) -> Result<[u8; 32], WebCryptoError> {
        if !self.private_key.extractable() {
            return Err(WebCryptoError::KeyExport(
                "cannot export seed from a non-extractable key".into(),
            ));
        }

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

        // PKCS#8 for Ed25519: 16-byte header, then the 32-byte seed
        let seed: [u8; 32] = pkcs8_bytes
            .get(16..48)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| {
                WebCryptoError::KeyExport(format!(
                    "PKCS#8 too short: expected >= 48 bytes, got {}",
                    pkcs8_bytes.len()
                ))
            })?;

        Ok(seed)
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
                import_with_agreement(&seed, false).await
            }
            KeyExport::NonExtractable {
                private_key,
                public_key,
                agreement,
            } => {
                let subtle = get_subtle_crypto()?;
                let public_key_bytes = export_public_key_raw(&subtle, &public_key).await?;
                let key = SigningKey::new(private_key, public_key, public_key_bytes);

                // The seed is gone, so the agreement key can only come back
                // from the archive. Without one the signing key still works;
                // only key agreement is unavailable.
                match agreement {
                    Some(pair) => {
                        let agreement =
                            AgreementSecretKey::from_crypto_keys(pair.private_key, pair.public_key)
                                .await?;
                        Ok(key.with_agreement(agreement))
                    }
                    None => Ok(key),
                }
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
                agreement: self.agreement.as_ref().map(|key| AgreementKeyPair {
                    private_key: key.private_key().clone(),
                    public_key: key.public_key().clone(),
                }),
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

/// Import a signing key from a seed and derive its X25519 agreement key.
///
/// The pair is what gets archived together: the seed is the only thing either
/// key can be derived from, and it is not recoverable from a non-extractable
/// `CryptoKey`.
async fn import_with_agreement(
    seed: &[u8; 32],
    extractable: bool,
) -> Result<SigningKey, WebCryptoError> {
    let signing_key = import(seed, extractable).await?;
    let agreement = if extractable {
        <AgreementSecretKey as ExtractableAgreementKey>::from_ed25519_seed(seed).await?
    } else {
        AgreementSecretKey::from_ed25519_seed(seed).await?
    };
    Ok(signing_key.with_agreement(agreement))
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
        // Same extractable-first flow as `SigningKey::generate`, but both the
        // signing key and the agreement key stay extractable.
        let key = generate(true).await?;
        let seed = key.export_seed().await?;
        import_with_agreement(&seed, true).await
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
                import_with_agreement(&seed, true).await
            }
            KeyExport::NonExtractable {
                private_key,
                public_key,
                agreement,
            } => {
                let subtle = get_subtle_crypto()?;
                let public_key_bytes = export_public_key_raw(&subtle, &public_key).await?;
                let key = SigningKey::new(private_key, public_key, public_key_bytes);
                match agreement {
                    Some(pair) => {
                        let agreement =
                            AgreementSecretKey::from_crypto_keys(pair.private_key, pair.public_key)
                                .await?;
                        Ok(key.with_agreement(agreement))
                    }
                    None => Ok(key),
                }
            }
        }
    }

    async fn export(&self) -> Result<KeyExport, WebCryptoError> {
        self.export().await
    }
}

/// WebCrypto-based X25519 secret key.
///
/// WebCrypto cannot derive an X25519 key from an Ed25519 key, and a
/// non-extractable Ed25519 `CryptoKey` never yields its seed. So the derivation
/// is done in Rust from the seed (see [`AgreementSecretKey::from_ed25519_seed`])
/// and the resulting X25519 secret is imported into WebCrypto as a
/// non-extractable `CryptoKey`, which can then be stored alongside the Ed25519
/// key and restored later.
#[derive(Debug, Clone)]
pub struct AgreementSecretKey {
    /// The WebCrypto X25519 private key.
    private_key: CryptoKey,
    /// The WebCrypto X25519 public key.
    public_key: CryptoKey,
    /// Cached raw public key bytes.
    public_key_bytes: [u8; 32],
}

impl AgreementSecretKey {
    /// Create from private/public `CryptoKey`s and cached public key bytes.
    fn new(private_key: CryptoKey, public_key: CryptoKey, public_key_bytes: [u8; 32]) -> Self {
        Self {
            private_key,
            public_key,
            public_key_bytes,
        }
    }

    /// Derive an X25519 key from a 32-byte Ed25519 seed and import it into
    /// WebCrypto as a **non-extractable** key.
    ///
    /// The derivation matches the native path exactly, so the same Ed25519 seed
    /// yields the same X25519 public key on both platforms.
    ///
    /// # Errors
    ///
    /// Returns an error if the WebCrypto import fails or the browser does not
    /// support X25519.
    pub async fn from_ed25519_seed(seed: &[u8; 32]) -> Result<Self, WebCryptoError> {
        let secret = super::agreement_secret_bytes(seed);
        Self::import_secret(&secret, false).await
    }

    /// Import a raw X25519 secret as a non-extractable key.
    ///
    /// Used for ephemeral sender keys, whose secret is generated directly
    /// rather than derived from an Ed25519 seed.
    pub(crate) async fn from_secret_bytes(secret: &[u8; 32]) -> Result<Self, WebCryptoError> {
        Self::import_secret(secret, false).await
    }

    /// Import a raw X25519 secret with the given extractability.
    async fn import_secret(secret: &[u8; 32], extractable: bool) -> Result<Self, WebCryptoError> {
        let subtle = get_subtle_crypto()?;

        let pkcs8 = X25519Pkcs8::from(secret);

        let algorithm = Object::new();
        Reflect::set(&algorithm, &"name".into(), &"X25519".into())
            .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;

        let key_usages = js_sys::Array::new();
        key_usages.push(&"deriveBits".into());

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
            .map_err(|e| WebCryptoError::KeyImport(format!("X25519 import failed: {e:?}")))?
            .unchecked_into();

        // The public key is the Montgomery point for this secret. Derive it in
        // Rust (WebCrypto cannot export it from a non-extractable private key)
        // and import it so callers can hand it to `deriveBits` as a peer key.
        let public_key_bytes = super::agreement_public_bytes(secret);
        let public_key = import_agreement_public_key_raw(&subtle, &public_key_bytes).await?;

        Ok(Self::new(private_key, public_key, public_key_bytes))
    }

    /// Reconstruct from stored `CryptoKey`s.
    ///
    /// Used when restoring an archived credential whose X25519 component was
    /// persisted as an opaque WebCrypto key pair.
    ///
    /// # Errors
    ///
    /// Returns an error if the public key's raw bytes cannot be exported.
    pub async fn from_crypto_keys(
        private_key: CryptoKey,
        public_key: CryptoKey,
    ) -> Result<Self, WebCryptoError> {
        let subtle = get_subtle_crypto()?;
        let public_key_bytes = export_public_key_raw(&subtle, &public_key).await?;
        Ok(Self::new(private_key, public_key, public_key_bytes))
    }

    /// Get the WebCrypto private key.
    #[must_use]
    pub const fn private_key(&self) -> &CryptoKey {
        &self.private_key
    }

    /// Get the WebCrypto public key.
    #[must_use]
    pub const fn public_key(&self) -> &CryptoKey {
        &self.public_key
    }

    /// Get the public (agreement) key.
    #[must_use]
    pub fn agreement_public_key(&self) -> AgreementPublicKey {
        AgreementPublicKey::new(self.public_key.clone(), self.public_key_bytes)
    }

    /// Perform X25519 key agreement with `peer`, returning the raw shared secret.
    ///
    /// # Errors
    ///
    /// Returns an error if `deriveBits` fails.
    pub async fn diffie_hellman(
        &self,
        peer: &AgreementPublicKey,
    ) -> Result<[u8; 32], WebCryptoError> {
        let subtle = get_subtle_crypto()?;

        let algorithm = Object::new();
        Reflect::set(&algorithm, &"name".into(), &"X25519".into())
            .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;
        Reflect::set(&algorithm, &"public".into(), peer.crypto_key())
            .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;

        let promise = subtle
            .derive_bits_with_object(&algorithm, &self.private_key, 256)
            .map_err(|e| WebCryptoError::JsError(format!("deriveBits failed: {e:?}")))?;

        let derived = JsFuture::from(promise)
            .await
            .map_err(|e| WebCryptoError::JsError(format!("deriveBits await failed: {e:?}")))?;

        let array = Uint8Array::new(&derived);
        if array.length() != 32 {
            return Err(WebCryptoError::JsError(format!(
                "expected 32 shared-secret bytes, got {}",
                array.length()
            )));
        }

        let mut bytes = [0u8; 32];
        array.copy_to(&mut bytes);
        Ok(bytes)
    }
}

/// WebCrypto-based X25519 public key.
///
/// Wraps a WebCrypto `CryptoKey` for use as a `deriveBits` peer key, alongside
/// a cached copy of the raw public key bytes.
#[derive(Debug, Clone)]
pub struct AgreementPublicKey {
    /// The WebCrypto public key.
    crypto_key: CryptoKey,
    /// Cached raw public key bytes.
    public_key_bytes: [u8; 32],
}

impl AgreementPublicKey {
    /// Create from a `CryptoKey` and its raw bytes.
    fn new(crypto_key: CryptoKey, public_key_bytes: [u8; 32]) -> Self {
        Self {
            crypto_key,
            public_key_bytes,
        }
    }

    /// Import raw X25519 public key bytes into WebCrypto.
    ///
    /// # Errors
    ///
    /// Returns an error if the import fails.
    pub async fn from_bytes(bytes: &[u8; 32]) -> Result<Self, WebCryptoError> {
        let subtle = get_subtle_crypto()?;
        let crypto_key = import_agreement_public_key_raw(&subtle, bytes).await?;
        Ok(Self::new(crypto_key, *bytes))
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

/// Import raw X25519 public key bytes as a WebCrypto `CryptoKey`.
///
/// Peer public keys carry no usages of their own; the usages belong to the
/// private key that calls `deriveBits`.
async fn import_agreement_public_key_raw(
    subtle: &SubtleCrypto,
    bytes: &[u8; 32],
) -> Result<CryptoKey, WebCryptoError> {
    let algorithm = Object::new();
    Reflect::set(&algorithm, &"name".into(), &"X25519".into())
        .map_err(|e| WebCryptoError::JsError(format!("{e:?}")))?;

    let key_usages = js_sys::Array::new();
    let key_data = Uint8Array::from(bytes.as_slice());

    let promise = subtle
        .import_key_with_object("raw", &key_data.buffer(), &algorithm, true, &key_usages)
        .map_err(|e| WebCryptoError::KeyImport(format!("{e:?}")))?;

    let key = JsFuture::from(promise)
        .await
        .map_err(|e| WebCryptoError::KeyImport(format!("X25519 public import: {e:?}")))?;

    Ok(key.unchecked_into())
}

/// PKCS#8 wrapper for an X25519 private key.
///
/// Same DER shape as the Ed25519 wrapper, with OID 1.3.101.110 (X25519).
struct X25519Pkcs8([u8; 48]);

impl X25519Pkcs8 {
    /// Get the PKCS#8 bytes as a slice.
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8; 32]> for X25519Pkcs8 {
    fn from(secret: &[u8; 32]) -> Self {
        let mut pkcs8 = [0u8; 48];
        pkcs8[..16].copy_from_slice(&[
            0x30, 0x2e, // SEQUENCE, 46 bytes
            0x02, 0x01, 0x00, // INTEGER 0 (version)
            0x30, 0x05, // SEQUENCE, 5 bytes
            0x06, 0x03, 0x2b, 0x65, 0x6e, // OID 1.3.101.110 (X25519)
            0x04, 0x22, // OCTET STRING, 34 bytes
            0x04, 0x20, // OCTET STRING, 32 bytes (the secret)
        ]);
        pkcs8[16..].copy_from_slice(secret);
        Self(pkcs8)
    }
}

impl ExtractableAgreementKey for AgreementSecretKey {
    async fn from_ed25519_seed(seed: &[u8; 32]) -> Result<Self, WebCryptoError> {
        let secret = super::agreement_secret_bytes(seed);
        Self::import_secret(&secret, true).await
    }
}
