//! Algorithm-agnostic key export types.

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
