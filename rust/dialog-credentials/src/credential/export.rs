//! Platform-specific credential export types.
//!
//! These are plain data containers for serialized credential material.
//! Import/export logic lives on the credential types themselves.

use thiserror::Error;

#[cfg(not(target_arch = "wasm32"))]
use super::constants::{
    ED25519_PRIV_TAG, ED25519_PUB_TAG, PUB_KEY_OFFSET, SIGNER_EXPORT_SIZE, VERIFIER_EXPORT_SIZE,
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
/// On native: multicodec-tagged fixed-size bytes (68 bytes).
/// On web: JsValue wrapping a CryptoKeyPair.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct SignerCredentialExport(pub [u8; SIGNER_EXPORT_SIZE]);

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Debug, Clone)]
pub struct SignerCredentialExport(pub JsValue);

/// Platform-specific serialized form of a verifier credential.
///
/// On native: multicodec-tagged fixed-size bytes (34 bytes).
/// On web: JsValue wrapping a Uint8Array of public key bytes.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct VerifierCredentialExport(pub [u8; VERIFIER_EXPORT_SIZE]);

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[derive(Debug, Clone)]
pub struct VerifierCredentialExport(pub JsValue);

/// Platform-specific serialized form of a credential (signer or verifier).
#[derive(Debug, Clone)]
pub enum CredentialExport {
    Signer(SignerCredentialExport),
    Verifier(VerifierCredentialExport),
}

/// Raw byte type for a serialized signer credential.
#[cfg(not(target_arch = "wasm32"))]
pub type SignerExport = [u8; SIGNER_EXPORT_SIZE];

/// Raw byte type for a serialized verifier credential.
#[cfg(not(target_arch = "wasm32"))]
pub type VerifierExport = [u8; VERIFIER_EXPORT_SIZE];

#[cfg(not(target_arch = "wasm32"))]
impl TryFrom<Vec<u8>> for SignerCredentialExport {
    type Error = CredentialExportError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let arr: SignerExport = bytes.try_into().map_err(|_| {
            CredentialExportError::InvalidFormat("invalid signer export length".into())
        })?;
        Ok(Self(arr))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl TryFrom<Vec<u8>> for VerifierCredentialExport {
    type Error = CredentialExportError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let arr: VerifierExport = bytes.try_into().map_err(|_| {
            CredentialExportError::InvalidFormat("invalid verifier export length".into())
        })?;
        Ok(Self(arr))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<SignerExport> for SignerCredentialExport {
    fn from(bytes: SignerExport) -> Self {
        Self(bytes)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<SignerCredentialExport> for SignerExport {
    fn from(export: SignerCredentialExport) -> Self {
        export.0
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<VerifierExport> for VerifierCredentialExport {
    fn from(bytes: VerifierExport) -> Self {
        Self(bytes)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<VerifierCredentialExport> for VerifierExport {
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

#[cfg(not(target_arch = "wasm32"))]
impl TryFrom<Vec<u8>> for CredentialExport {
    type Error = CredentialExportError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        if bytes.len() == SIGNER_EXPORT_SIZE
            && bytes.starts_with(ED25519_PRIV_TAG)
            && bytes[PUB_KEY_OFFSET..].starts_with(ED25519_PUB_TAG)
        {
            let arr: SignerExport = bytes.try_into().map_err(|_| {
                CredentialExportError::InvalidFormat("invalid signer length".into())
            })?;
            Ok(Self::Signer(arr.into()))
        } else if bytes.len() == VERIFIER_EXPORT_SIZE && bytes.starts_with(ED25519_PUB_TAG) {
            let arr: VerifierExport = bytes.try_into().map_err(|_| {
                CredentialExportError::InvalidFormat("invalid verifier length".into())
            })?;
            Ok(Self::Verifier(arr.into()))
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
