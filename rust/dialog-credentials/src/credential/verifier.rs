//! Verifier credential — wraps an Ed25519 public key.

use crate::Ed25519Verifier;
use crate::ed25519::Ed25519VerifyingKey;
use dialog_varsig::{Did, Principal};

use super::constants::KEY_SIZE;
#[cfg(not(target_arch = "wasm32"))]
use super::constants::{ED25519_PUB_TAG, PUB_TAG_SIZE, VERIFIER_EXPORT_SIZE};
use super::export::{CredentialExportError, VerifierCredentialExport};

/// A verifier credential — wraps an `Ed25519Verifier` (public key only).
#[derive(Debug, Clone)]
pub struct VerifierCredential(pub Ed25519Verifier);

impl From<Ed25519Verifier> for VerifierCredential {
    fn from(verifier: Ed25519Verifier) -> Self {
        Self(verifier)
    }
}

impl Principal for VerifierCredential {
    fn did(&self) -> Did {
        Principal::did(&self.0)
    }
}

impl From<VerifierCredential> for Did {
    fn from(credential: VerifierCredential) -> Self {
        credential.did()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl VerifierCredential {
    /// Export to multicodec-tagged bytes for native storage.
    pub fn export(&self) -> VerifierCredentialExport {
        let mut buf = [0u8; VERIFIER_EXPORT_SIZE];
        buf[..PUB_TAG_SIZE].copy_from_slice(ED25519_PUB_TAG);
        buf[PUB_TAG_SIZE..].copy_from_slice(&self.0.0.to_bytes());
        VerifierCredentialExport(buf)
    }

    /// Import from multicodec-tagged bytes.
    pub fn import(export: VerifierCredentialExport) -> Result<Self, CredentialExportError> {
        let data = &export.0;
        if !data.starts_with(ED25519_PUB_TAG) {
            return Err(CredentialExportError::InvalidFormat(
                "invalid ed25519-pub multicodec tag".into(),
            ));
        }

        let key_arr: &[u8; KEY_SIZE] = data[PUB_TAG_SIZE..]
            .try_into()
            .map_err(|_| CredentialExportError::InvalidFormat("invalid public key".into()))?;
        let vk = ed25519_dalek::VerifyingKey::from_bytes(key_arr)
            .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
        Ok(Self(Ed25519Verifier(Ed25519VerifyingKey::Native(vk))))
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use js_sys::Uint8Array;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::JsCast;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl VerifierCredential {
    /// Export to a JsValue (Uint8Array) for web storage.
    pub fn export(&self) -> VerifierCredentialExport {
        let bytes = self.0.0.to_bytes();
        VerifierCredentialExport(Uint8Array::from(bytes.as_slice()).into())
    }

    /// Import from a JsValue (Uint8Array).
    pub fn import(export: VerifierCredentialExport) -> Result<Self, CredentialExportError> {
        let array: &Uint8Array = export
            .0
            .dyn_ref()
            .ok_or_else(|| CredentialExportError::InvalidFormat("expected Uint8Array".into()))?;
        let bytes = array.to_vec();
        let key_arr: [u8; KEY_SIZE] = bytes.as_slice().try_into().map_err(|_| {
            CredentialExportError::InvalidFormat(format!("invalid verifier length={}", bytes.len()))
        })?;
        let vk = ed25519_dalek::VerifyingKey::from_bytes(&key_arr)
            .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
        Ok(Self(Ed25519Verifier(Ed25519VerifyingKey::Native(vk))))
    }
}
