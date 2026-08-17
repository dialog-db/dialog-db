//! Platform-specific credential export types.
//!
//! These are plain data containers for serialized credential material.
//! Import/export logic lives on the credential types themselves.
//!
//! On native the stored form is variable-length, multicodec-tagged bytes: the
//! leading tag names the algorithm, so ed25519 and p256 credentials round-trip
//! through the same container. ed25519's byte layout is unchanged.

use thiserror::Error;

#[cfg(not(target_arch = "wasm32"))]
use super::constants::{PRIVATE_TAG, PUBLIC_KEY_OFFSET, PUBLIC_TAG, SIGNER_EXPORT_SIZE};

#[cfg(not(target_arch = "wasm32"))]
use super::constants::VERIFIER_EXPORT_SIZE;
#[cfg(all(not(target_arch = "wasm32"), feature = "es256"))]
use super::constants::{
    ES256_PRIVATE_TAG, ES256_PUBLIC_KEY_OFFSET, ES256_PUBLIC_TAG, ES256_SIGNER_EXPORT_SIZE,
    ES256_VERIFIER_EXPORT_SIZE,
};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use js_sys::Uint8Array;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::{JsCast, JsValue};

/// Error type for credential export/import operations.
#[derive(Debug, Error)]
pub enum CredentialExportError {
    /// Key export/import operation failed.
    #[error("key operation failed: {0}")]
    Key(String),

    /// The stored data has an invalid format.
    #[error("invalid credential format: {0}")]
    InvalidFormat(String),
}

/// Platform-specific serialized form of a signer credential.
///
/// On native: variable-length multicodec-tagged bytes (68 bytes for ed25519,
/// 70 for p256).
/// On web: JsValue wrapping a CryptoKeyPair.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct SignerCredentialExport(pub Vec<u8>);

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Debug, Clone)]
pub struct SignerCredentialExport(pub JsValue);

/// Platform-specific serialized form of a verifier credential.
///
/// On native: variable-length multicodec-tagged bytes (34 bytes for ed25519,
/// 35 for p256).
/// On web: JsValue wrapping a Uint8Array of public key bytes.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct VerifierCredentialExport(pub Vec<u8>);

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Debug, Clone)]
pub struct VerifierCredentialExport(pub JsValue);

/// Platform-specific serialized form of a credential (signer or verifier).
#[derive(Debug, Clone)]
pub enum CredentialExport {
    /// A serialized signing credential.
    Signer(SignerCredentialExport),
    /// A serialized verifying credential.
    Verifier(VerifierCredentialExport),
}

#[cfg(not(target_arch = "wasm32"))]
impl From<Vec<u8>> for SignerCredentialExport {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<SignerCredentialExport> for Vec<u8> {
    fn from(export: SignerCredentialExport) -> Self {
        export.0
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<Vec<u8>> for VerifierCredentialExport {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<VerifierCredentialExport> for Vec<u8> {
    fn from(export: VerifierCredentialExport) -> Self {
        export.0
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl AsRef<[u8]> for SignerCredentialExport {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl AsRef<[u8]> for VerifierCredentialExport {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Whether these bytes are a signer export for some supported algorithm.
#[cfg(not(target_arch = "wasm32"))]
fn is_signer_export(bytes: &[u8]) -> bool {
    if bytes.len() == SIGNER_EXPORT_SIZE
        && bytes.starts_with(PRIVATE_TAG)
        && bytes[PUBLIC_KEY_OFFSET..].starts_with(PUBLIC_TAG)
    {
        return true;
    }
    #[cfg(feature = "es256")]
    if bytes.len() == ES256_SIGNER_EXPORT_SIZE
        && bytes.starts_with(ES256_PRIVATE_TAG)
        && bytes[ES256_PUBLIC_KEY_OFFSET..].starts_with(ES256_PUBLIC_TAG)
    {
        return true;
    }
    false
}

/// Whether these bytes are a verifier export for some supported algorithm.
#[cfg(not(target_arch = "wasm32"))]
fn is_verifier_export(bytes: &[u8]) -> bool {
    if bytes.len() == VERIFIER_EXPORT_SIZE && bytes.starts_with(PUBLIC_TAG) {
        return true;
    }
    #[cfg(feature = "es256")]
    if bytes.len() == ES256_VERIFIER_EXPORT_SIZE && bytes.starts_with(ES256_PUBLIC_TAG) {
        return true;
    }
    false
}

#[cfg(not(target_arch = "wasm32"))]
impl TryFrom<Vec<u8>> for CredentialExport {
    type Error = CredentialExportError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        if is_signer_export(&bytes) {
            Ok(Self::Signer(SignerCredentialExport(bytes)))
        } else if is_verifier_export(&bytes) {
            Ok(Self::Verifier(VerifierCredentialExport(bytes)))
        } else {
            Err(CredentialExportError::InvalidFormat(format!(
                "unrecognized credential format: length={}",
                bytes.len()
            )))
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl CredentialExport {
    /// Get the underlying bytes.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Signer(s) => s.as_ref(),
            Self::Verifier(v) => v.as_ref(),
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<JsValue> for CredentialExport {
    fn from(js: JsValue) -> Self {
        if js.is_instance_of::<Uint8Array>() {
            Self::Verifier(VerifierCredentialExport(js))
        } else {
            Self::Signer(SignerCredentialExport(js))
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<CredentialExport> for JsValue {
    fn from(export: CredentialExport) -> Self {
        match export {
            CredentialExport::Signer(s) => s.0,
            CredentialExport::Verifier(v) => v.0,
        }
    }
}
