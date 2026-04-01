//! Algorithm-agnostic key export types and credential storage formats.

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
        private_key: web_sys::CryptoKey,
        /// The WebCrypto public key.
        public_key: web_sys::CryptoKey,
    },
}

impl From<&[u8; 32]> for KeyExport {
    fn from(seed: &[u8; 32]) -> Self {
        KeyExport::Extractable(seed.to_vec())
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<web_sys::CryptoKeyPair> for KeyExport {
    fn from(pair: web_sys::CryptoKeyPair) -> Self {
        KeyExport::NonExtractable {
            private_key: pair.get_private_key(),
            public_key: pair.get_public_key(),
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<KeyExport> for wasm_bindgen::JsValue {
    fn from(export: KeyExport) -> Self {
        match export {
            KeyExport::Extractable(bytes) => js_sys::Uint8Array::from(bytes.as_slice()).into(),
            KeyExport::NonExtractable {
                private_key,
                public_key,
            } => web_sys::CryptoKeyPair::new(&private_key, &public_key).into(),
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl TryFrom<wasm_bindgen::JsValue> for KeyExport {
    type Error = WebCryptoError;

    fn try_from(value: wasm_bindgen::JsValue) -> Result<Self, Self::Error> {
        use wasm_bindgen::JsCast;

        // If it's a Uint8Array, treat as extractable bytes
        if let Some(array) = value.dyn_ref::<js_sys::Uint8Array>() {
            return Ok(KeyExport::Extractable(array.to_vec()));
        }

        // Otherwise treat as { privateKey, publicKey } object
        let private_key: web_sys::CryptoKey = js_sys::Reflect::get(&value, &"privateKey".into())
            .map_err(|_| WebCryptoError::KeyImport("missing privateKey".into()))?
            .dyn_into()
            .map_err(|_| WebCryptoError::KeyImport("invalid privateKey".into()))?;

        let public_key: web_sys::CryptoKey = js_sys::Reflect::get(&value, &"publicKey".into())
            .map_err(|_| WebCryptoError::KeyImport("missing publicKey".into()))?
            .dyn_into()
            .map_err(|_| WebCryptoError::KeyImport("invalid publicKey".into()))?;

        Ok(KeyExport::NonExtractable {
            private_key,
            public_key,
        })
    }
}

/// Errors that can occur when using WebCrypto operations.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Debug, Clone, thiserror::Error)]
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

// Re-export credential types for backward compatibility.
pub use crate::credential::{
    Credential, CredentialExport, CredentialExportError, SignerCredential, SignerCredentialExport,
    VerifierCredential, VerifierCredentialExport,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extractable_roundtrip_through_bytes() {
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

#[cfg(all(test, target_arch = "wasm32", target_os = "unknown"))]
mod wasm_tests {
    use super::*;
    use crate::Ed25519Signer;
    use wasm_bindgen::{JsCast, JsValue};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    #[dialog_common::test]
    fn extractable_to_jsvalue_produces_uint8array() {
        let export = KeyExport::Extractable(vec![10, 20, 30]);
        let js_val: JsValue = export.into();
        assert!(js_val.is_instance_of::<js_sys::Uint8Array>());

        let array = js_sys::Uint8Array::from(js_val);
        assert_eq!(array.to_vec(), vec![10, 20, 30]);
    }

    #[dialog_common::test]
    fn extractable_roundtrip_through_jsvalue() {
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
        let private = js_sys::Reflect::get(&js_val, &"privateKey".into()).unwrap();
        let public = js_sys::Reflect::get(&js_val, &"publicKey".into()).unwrap();
        assert!(private.is_instance_of::<web_sys::CryptoKey>());
        assert!(public.is_instance_of::<web_sys::CryptoKey>());

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
    fn try_from_invalid_jsvalue_fails() {
        let result = KeyExport::try_from(JsValue::from_str("not a key"));
        assert!(result.is_err());
    }
}
