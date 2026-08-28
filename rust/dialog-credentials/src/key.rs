//! Algorithm-agnostic key export types and credential storage formats.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use js_sys::{Object, Reflect, Uint8Array};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use thiserror::Error;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::{JsCast, JsValue};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use web_sys::{CryptoKey, CryptoKeyPair};

/// Key material for import/export.
///
/// On native platforms, only the `Extractable` variant is available.
/// On WASM (`wasm32-unknown-unknown`), a `NonExtractable` variant is also
/// available for opaque `WebCrypto` key pairs whose key material cannot be read.
#[derive(Debug, Clone)]
pub enum KeyExport {
    /// Raw seed bytes — the key material is accessible.
    Extractable(Vec<u8>),

    /// Opaque WebCrypto key pair — key material is NOT accessible.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    NonExtractable {
        /// The WebCrypto private key.
        private_key: CryptoKey,
        /// The WebCrypto public key.
        public_key: CryptoKey,
        /// The derived X25519 agreement key, when one was archived.
        ///
        /// A non-extractable signing key never yields its seed, so the X25519
        /// key it was derived from cannot be recovered later. Archiving it here
        /// as a third component is what makes the agreement key restorable.
        /// `None` for exports written before the agreement key existed, or for
        /// keys that never had one.
        agreement: Option<AgreementArchive>,
    },
}

/// How an X25519 agreement key is archived next to its signing key.
///
/// The key is opaque WebCrypto material either way; the two forms differ only
/// in how it crosses the structured-clone boundary into storage.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Debug, Clone)]
pub enum AgreementArchive {
    /// The X25519 `CryptoKey`s themselves, serialized as
    /// `{ privateKey, publicKey }`. The form used wherever the browser can
    /// clone them.
    Keys(AgreementKeyPair),

    /// The private key wrapped under an AES-KW key, serialized as
    /// `{ wrappingKey, wrappedKey, publicKey }`.
    ///
    /// WebKit (Safari 27) serializes an X25519 `CryptoKey` but cannot
    /// deserialize one: `structuredClone` throws and IndexedDB hands back
    /// `null` for any value that contains one, so a `Keys` archive written
    /// there is lost on the next read. AES-KW keys do round-trip, so the
    /// private key is archived as the AES-KW ciphertext of its PKCS#8 form
    /// under a non-extractable `CryptoKey`, and the public key as raw bytes.
    /// The wrap adds no secrecy, since whoever can use the wrapping key can
    /// unwrap; it is a serialization the browser can read back, and the key
    /// it unwraps to is non-extractable again.
    Wrapped(WrappedAgreementKey),
}

/// An opaque WebCrypto X25519 key pair, archived alongside a signing key.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Debug, Clone)]
pub struct AgreementKeyPair {
    /// The WebCrypto X25519 private key.
    pub private_key: CryptoKey,
    /// The WebCrypto X25519 public key.
    pub public_key: CryptoKey,
}

/// An X25519 private key wrapped for archival, with its public half.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Debug, Clone)]
pub struct WrappedAgreementKey {
    /// The non-extractable AES-KW key the private key is wrapped under.
    pub wrapping_key: CryptoKey,
    /// The private key's PKCS#8 form, wrapped: `wrapKey("pkcs8", …, "AES-KW")`.
    pub wrapped_key: Vec<u8>,
    /// The raw 32-byte X25519 public key.
    pub public_key: [u8; 32],
}

impl From<&[u8; 32]> for KeyExport {
    fn from(seed: &[u8; 32]) -> Self {
        KeyExport::Extractable(seed.to_vec())
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<CryptoKeyPair> for KeyExport {
    fn from(pair: CryptoKeyPair) -> Self {
        KeyExport::NonExtractable {
            private_key: pair.get_private_key(),
            public_key: pair.get_public_key(),
            agreement: None,
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<KeyExport> for JsValue {
    fn from(export: KeyExport) -> Self {
        match export {
            KeyExport::Extractable(bytes) => Uint8Array::from(bytes.as_slice()).into(),
            KeyExport::NonExtractable {
                private_key,
                public_key,
                agreement,
            } => {
                let pair = CryptoKeyPair::new(&private_key, &public_key);
                // The X25519 key rides along as an extra property so the
                // archived value stays a plain `{ privateKey, publicKey }`
                // object for readers that predate the agreement key.
                if let Some(agreement) = agreement {
                    let value = JsValue::from(agreement);
                    // Set failures here would silently drop the agreement key,
                    // so surface them as a dropped property rather than a
                    // half-written object: `Reflect::set` on a fresh object
                    // only fails if the object is frozen, which it is not.
                    let _ = Reflect::set(&pair, &AGREEMENT_KEY.into(), &value);
                }
                pair.into()
            }
        }
    }
}

/// Property name carrying the archived X25519 key on a serialized key export.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
const AGREEMENT_KEY: &str = "agreementKey";
/// Property names of the wrapped archive shape.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
const WRAPPING_KEY: &str = "wrappingKey";
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
const WRAPPED_KEY: &str = "wrappedKey";
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
const PUBLIC_KEY: &str = "publicKey";
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
const PRIVATE_KEY: &str = "privateKey";

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<AgreementArchive> for JsValue {
    fn from(archive: AgreementArchive) -> Self {
        match archive {
            AgreementArchive::Keys(pair) => {
                CryptoKeyPair::new(&pair.private_key, &pair.public_key).into()
            }
            AgreementArchive::Wrapped(wrapped) => {
                let value = Object::new();
                // As above: a fresh object is never frozen, so these cannot fail.
                let _ = Reflect::set(&value, &WRAPPING_KEY.into(), &wrapped.wrapping_key);
                let _ = Reflect::set(
                    &value,
                    &WRAPPED_KEY.into(),
                    &Uint8Array::from(wrapped.wrapped_key.as_slice()),
                );
                let _ = Reflect::set(
                    &value,
                    &PUBLIC_KEY.into(),
                    &Uint8Array::from(wrapped.public_key.as_slice()),
                );
                value.into()
            }
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl AgreementArchive {
    /// Read either archive shape back; `None` when the value is neither.
    fn from_js(value: &JsValue) -> Option<Self> {
        if let (Some(private_key), Some(public_key)) = (
            crypto_key_property(value, PRIVATE_KEY),
            crypto_key_property(value, PUBLIC_KEY),
        ) {
            return Some(Self::Keys(AgreementKeyPair {
                private_key,
                public_key,
            }));
        }
        let wrapping_key = crypto_key_property(value, WRAPPING_KEY)?;
        let wrapped_key = bytes_property(value, WRAPPED_KEY)?;
        let public_key = bytes_property(value, PUBLIC_KEY)?.try_into().ok()?;
        Some(Self::Wrapped(WrappedAgreementKey {
            wrapping_key,
            wrapped_key,
            public_key,
        }))
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn crypto_key_property(value: &JsValue, name: &str) -> Option<CryptoKey> {
    Reflect::get(value, &name.into()).ok()?.dyn_into().ok()
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn bytes_property(value: &JsValue, name: &str) -> Option<Vec<u8>> {
    Reflect::get(value, &name.into())
        .ok()?
        .dyn_ref::<Uint8Array>()
        .map(Uint8Array::to_vec)
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl TryFrom<JsValue> for KeyExport {
    type Error = WebCryptoError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        // If it's a Uint8Array, treat as extractable bytes
        if let Some(array) = value.dyn_ref::<Uint8Array>() {
            return Ok(KeyExport::Extractable(array.to_vec()));
        }

        // Otherwise treat as { privateKey, publicKey } object
        let private_key: CryptoKey = Reflect::get(&value, &"privateKey".into())
            .map_err(|_| WebCryptoError::KeyImport("missing privateKey".into()))?
            .dyn_into()
            .map_err(|_| WebCryptoError::KeyImport("invalid privateKey".into()))?;

        let public_key: CryptoKey = Reflect::get(&value, &"publicKey".into())
            .map_err(|_| WebCryptoError::KeyImport("missing publicKey".into()))?
            .dyn_into()
            .map_err(|_| WebCryptoError::KeyImport("invalid publicKey".into()))?;

        // Absent or malformed agreement key is not an error: exports written
        // before the X25519 component existed simply do not carry one.
        let agreement = Reflect::get(&value, &AGREEMENT_KEY.into())
            .ok()
            .filter(|v| !v.is_undefined() && !v.is_null())
            .and_then(|v| AgreementArchive::from_js(&v));

        Ok(KeyExport::NonExtractable {
            private_key,
            public_key,
            agreement,
        })
    }
}

/// Errors that can occur when using WebCrypto operations.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Debug, Clone, Error)]
pub enum WebCryptoError {
    /// WebCrypto API is not available.
    #[error("WebCrypto not available: {0}")]
    NotAvailable(String),

    /// Key generation failed.
    #[error("key generation failed: {0}")]
    KeyGeneration(String),

    /// Key import failed.
    #[error("key import failed: {0}")]
    KeyImport(String),

    /// Key export failed.
    #[error("key export failed: {0}")]
    KeyExport(String),

    /// Invalid public key.
    #[error("invalid public key: {0}")]
    InvalidPublicKey(String),

    /// JavaScript error.
    #[error("JS error: {0}")]
    JsError(String),
}

/// Trait for creating WebCrypto keys with extractable private key material.
///
/// By default, key generation and import create **non-extractable** keys for
/// security. Use this trait when you need extractable keys (e.g., for key
/// backup or export).
///
/// # Security Warning
///
/// Extractable keys allow the private key material to be exported from
/// WebCrypto. Only use extractable keys when you have a specific need
/// for key export functionality.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait ExtractableKey: Sized {
    /// Generate a new keypair with extractable private key.
    fn generate() -> impl std::future::Future<Output = Result<Self, WebCryptoError>>;

    /// Import a keypair from a [`KeyExport`] with extractable private key.
    fn import(
        key: impl Into<KeyExport>,
    ) -> impl std::future::Future<Output = Result<Self, WebCryptoError>>;

    /// Export the key material.
    fn export(&self) -> impl std::future::Future<Output = Result<KeyExport, WebCryptoError>>;
}

/// Trait for creating WebCrypto X25519 keys with extractable key material.
///
/// The counterpart to [`ExtractableKey`] for agreement keys: derivation from an
/// Ed25519 seed normally produces a **non-extractable** `CryptoKey`, and this
/// trait opts into an extractable one for backup or export.
///
/// # Security Warning
///
/// Extractable keys allow the private key material to be exported from
/// WebCrypto. Only use them when you have a specific need for key export.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait ExtractableAgreementKey: Sized {
    /// Derive an X25519 key from an Ed25519 seed with an extractable secret.
    fn from_ed25519_seed(
        seed: &[u8; 32],
    ) -> impl std::future::Future<Output = Result<Self, WebCryptoError>>;
}

// Re-export credential types for backward compatibility.
pub use crate::credential::{
    Credential, CredentialExport, CredentialExportError, SignerCredential, SignerCredentialExport,
    VerifierCredential, VerifierCredentialExport,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_roundtrips_extractable_key_through_bytes() {
        let original = KeyExport::Extractable(vec![1, 2, 3, 4, 5]);
        let bytes = match &original {
            KeyExport::Extractable(b) => b.clone(),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            _ => panic!("expected extractable"),
        };
        let restored = KeyExport::Extractable(bytes);
        match (&original, &restored) {
            (KeyExport::Extractable(a), KeyExport::Extractable(b)) => assert_eq!(a, b),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            _ => panic!("expected extractable"),
        }
    }
}

// Web-only: `JsValue` conversions and `NonExtractable` (WebCrypto) roundtrips
// do not exist on native, so these tests cannot run outside `wasm32-unknown`.
#[cfg(all(test, target_arch = "wasm32", target_os = "unknown"))]
mod web_tests {
    use super::*;
    use crate::Ed25519Signer;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    #[dialog_common::test]
    fn it_converts_extractable_key_to_uint8array() {
        let export = KeyExport::Extractable(vec![10, 20, 30]);
        let js_val: JsValue = export.into();
        assert!(js_val.is_instance_of::<Uint8Array>());

        let array = Uint8Array::from(js_val);
        assert_eq!(array.to_vec(), vec![10, 20, 30]);
    }

    #[dialog_common::test]
    fn it_roundtrips_extractable_key_through_jsvalue() {
        let original = KeyExport::Extractable(vec![42; 32]);
        let js_val: JsValue = original.into();
        let restored = KeyExport::try_from(js_val).unwrap();

        match restored {
            KeyExport::Extractable(bytes) => assert_eq!(bytes, vec![42; 32]),
            _ => panic!("expected Extractable variant"),
        }
    }

    #[dialog_common::test]
    async fn non_extractable_roundtrip_through_jsvalue() {
        let signer = Ed25519Signer::generate().await.unwrap();
        let export = signer.export().await.unwrap();

        // Should be NonExtractable on web
        assert!(
            matches!(&export, KeyExport::NonExtractable { .. }),
            "default generate should produce non-extractable key"
        );

        let js_val: JsValue = export.into();

        // Should be a JS object with privateKey and publicKey
        assert!(js_val.is_object());
        let private = Reflect::get(&js_val, &"privateKey".into()).unwrap();
        let public = Reflect::get(&js_val, &"publicKey".into()).unwrap();
        assert!(private.is_instance_of::<CryptoKey>());
        assert!(public.is_instance_of::<CryptoKey>());

        // Roundtrip back to KeyExport
        let restored = KeyExport::try_from(js_val).unwrap();
        assert!(matches!(restored, KeyExport::NonExtractable { .. }));

        // Should be importable back into a signer
        let restored_signer = Ed25519Signer::import(restored).await.unwrap();
        assert_eq!(
            dialog_varsig::Principal::did(&signer),
            dialog_varsig::Principal::did(&restored_signer),
            "roundtripped signer should have same DID"
        );
    }

    #[dialog_common::test]
    fn it_fails_to_convert_invalid_jsvalue() {
        let result = KeyExport::try_from(JsValue::from_str("not a key"));
        assert!(result.is_err());
    }

    /// Both archive shapes read back as their variant; anything else reads
    /// as no archive, like an export written before agreement keys existed.
    #[dialog_common::test]
    async fn it_reads_both_agreement_archive_shapes() {
        let signer = Ed25519Signer::generate().await.unwrap();
        let export: JsValue = signer.export().await.unwrap().into();
        let pair = Reflect::get(&export, &"privateKey".into()).unwrap();
        let public = Reflect::get(&export, &"publicKey".into()).unwrap();

        let keys = Object::new();
        Reflect::set(&keys, &"privateKey".into(), &pair).unwrap();
        Reflect::set(&keys, &"publicKey".into(), &public).unwrap();
        assert!(matches!(
            AgreementArchive::from_js(&keys),
            Some(AgreementArchive::Keys(_))
        ));

        let wrapped = Object::new();
        Reflect::set(&wrapped, &"wrappingKey".into(), &pair).unwrap();
        Reflect::set(
            &wrapped,
            &"wrappedKey".into(),
            &Uint8Array::from([1u8; 56].as_slice()),
        )
        .unwrap();
        Reflect::set(
            &wrapped,
            &"publicKey".into(),
            &Uint8Array::from([2u8; 32].as_slice()),
        )
        .unwrap();
        match AgreementArchive::from_js(&wrapped) {
            Some(AgreementArchive::Wrapped(archive)) => {
                assert_eq!(archive.wrapped_key, vec![1u8; 56]);
                assert_eq!(archive.public_key, [2u8; 32]);
            }
            other => panic!("expected a wrapped archive, got {other:?}"),
        }

        let short = Object::new();
        Reflect::set(&short, &"wrappingKey".into(), &pair).unwrap();
        Reflect::set(
            &short,
            &"wrappedKey".into(),
            &Uint8Array::from([1u8; 56].as_slice()),
        )
        .unwrap();
        Reflect::set(
            &short,
            &"publicKey".into(),
            &Uint8Array::from([2u8; 31].as_slice()),
        )
        .unwrap();
        assert!(
            AgreementArchive::from_js(&short).is_none(),
            "a public key of the wrong length is not an archive"
        );
        assert!(AgreementArchive::from_js(&JsValue::from_str("nope")).is_none());
    }
}
