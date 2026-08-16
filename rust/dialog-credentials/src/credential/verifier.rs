//! Verifier credential — wraps an algorithm-agnostic public key.

use crate::Verifier;
use dialog_varsig::{Did, Principal};

#[cfg(not(target_arch = "wasm32"))]
use super::constants::{KEY_SIZE, PUBLIC_TAG, PUBLIC_TAG_SIZE, VERIFIER_EXPORT_SIZE};
use super::export::{CredentialExportError, VerifierCredentialExport};

/// A verifier credential — wraps a [`Verifier`] (public key only).
#[derive(Debug, Clone)]
pub struct VerifierCredential(pub Verifier);

impl From<Verifier> for VerifierCredential {
    fn from(verifier: Verifier) -> Self {
        Self(verifier)
    }
}

impl From<crate::Ed25519Verifier> for VerifierCredential {
    fn from(verifier: crate::Ed25519Verifier) -> Self {
        Self(Verifier::Ed25519(verifier))
    }
}

#[cfg(feature = "es256")]
impl From<crate::Es256Verifier> for VerifierCredential {
    fn from(verifier: crate::Es256Verifier) -> Self {
        Self(Verifier::Es256(verifier))
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
        match &self.0 {
            Verifier::Ed25519(verifier) => {
                let mut buffer = vec![0u8; VERIFIER_EXPORT_SIZE];
                buffer[..PUBLIC_TAG_SIZE].copy_from_slice(PUBLIC_TAG);
                buffer[PUBLIC_TAG_SIZE..].copy_from_slice(&verifier.0.to_bytes());
                VerifierCredentialExport(buffer)
            }
            #[cfg(feature = "es256")]
            Verifier::Es256(verifier) => VerifierCredentialExport(es256_native::export(verifier)),
            #[cfg(feature = "webauthn")]
            Verifier::WebAuthn(verifier) => {
                VerifierCredentialExport(webauthn_native::export(verifier))
            }
        }
    }

    /// Import from multicodec-tagged bytes, dispatching on the leading tag.
    pub fn import(export: VerifierCredentialExport) -> Result<Self, CredentialExportError> {
        use crate::ed25519::Ed25519VerifyingKey;
        let data = &export.0;

        if data.len() == VERIFIER_EXPORT_SIZE && data.starts_with(PUBLIC_TAG) {
            let key_arr: &[u8; KEY_SIZE] = data[PUBLIC_TAG_SIZE..]
                .try_into()
                .map_err(|_| CredentialExportError::InvalidFormat("invalid public key".into()))?;
            let vk = ed25519_dalek::VerifyingKey::from_bytes(key_arr)
                .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
            return Ok(Self(Verifier::Ed25519(crate::Ed25519Verifier(
                Ed25519VerifyingKey::Native(vk),
            ))));
        }

        #[cfg(feature = "es256")]
        if let Some(verifier) = es256_native::try_import(data)? {
            return Ok(Self(Verifier::Es256(verifier)));
        }

        #[cfg(feature = "webauthn")]
        if let Some(verifier) = webauthn_native::try_import(data)? {
            return Ok(Self(Verifier::WebAuthn(verifier)));
        }

        Err(CredentialExportError::InvalidFormat(
            "unrecognized verifier credential tag".into(),
        ))
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "es256"))]
mod es256_native {
    use super::CredentialExportError;
    use crate::Es256Verifier;
    use crate::credential::constants::{
        ES256_PUBLIC_KEY_SIZE, ES256_PUBLIC_TAG, ES256_PUBLIC_TAG_SIZE, ES256_VERIFIER_EXPORT_SIZE,
    };
    use crate::es256::Es256VerifyingKey;

    pub(super) fn export(verifier: &Es256Verifier) -> Vec<u8> {
        let compressed = verifier.0.to_compressed_bytes();
        let mut buffer = vec![0u8; ES256_VERIFIER_EXPORT_SIZE];
        buffer[..ES256_PUBLIC_TAG_SIZE].copy_from_slice(ES256_PUBLIC_TAG);
        buffer[ES256_PUBLIC_TAG_SIZE..].copy_from_slice(&compressed);
        buffer
    }

    pub(super) fn try_import(data: &[u8]) -> Result<Option<Es256Verifier>, CredentialExportError> {
        if data.len() != ES256_VERIFIER_EXPORT_SIZE || !data.starts_with(ES256_PUBLIC_TAG) {
            return Ok(None);
        }
        let key_arr: &[u8; ES256_PUBLIC_KEY_SIZE] = data[ES256_PUBLIC_TAG_SIZE..]
            .try_into()
            .map_err(|_| CredentialExportError::InvalidFormat("invalid es256 public key".into()))?;
        let vk = p256::ecdsa::VerifyingKey::from_sec1_bytes(key_arr)
            .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
        Ok(Some(Es256Verifier(Es256VerifyingKey::Native(vk))))
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "webauthn"))]
mod webauthn_native {
    use super::CredentialExportError;
    use crate::credential::constants::{
        WEBAUTHN_PUBLIC_KEY_SIZE, WEBAUTHN_PUBLIC_TAG, WEBAUTHN_PUBLIC_TAG_SIZE,
        WEBAUTHN_VERIFIER_EXPORT_SIZE,
    };
    use crate::webauthn::WebAuthnVerifier;

    pub(super) fn export(verifier: &WebAuthnVerifier) -> Vec<u8> {
        let compressed = verifier.to_sec1_bytes();
        let mut buffer = vec![0u8; WEBAUTHN_VERIFIER_EXPORT_SIZE];
        buffer[..WEBAUTHN_PUBLIC_TAG_SIZE].copy_from_slice(WEBAUTHN_PUBLIC_TAG);
        buffer[WEBAUTHN_PUBLIC_TAG_SIZE..].copy_from_slice(&compressed);
        buffer
    }

    pub(super) fn try_import(
        data: &[u8],
    ) -> Result<Option<WebAuthnVerifier>, CredentialExportError> {
        if data.len() != WEBAUTHN_VERIFIER_EXPORT_SIZE || !data.starts_with(WEBAUTHN_PUBLIC_TAG) {
            return Ok(None);
        }
        let key = &data[WEBAUTHN_PUBLIC_TAG_SIZE..];
        debug_assert_eq!(key.len(), WEBAUTHN_PUBLIC_KEY_SIZE);
        let verifier = WebAuthnVerifier::from_sec1_bytes(key)
            .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
        Ok(Some(verifier))
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use js_sys::Uint8Array;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::JsCast;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl VerifierCredential {
    /// Export to a JsValue (Uint8Array) for web storage.
    ///
    /// The bytes are the algorithm-tagged public key form, so a reader can
    /// dispatch on the tag.
    pub fn export(&self) -> VerifierCredentialExport {
        let bytes = self.to_tagged_public_bytes();
        VerifierCredentialExport(Uint8Array::from(bytes.as_slice()).into())
    }

    /// Import from a JsValue (Uint8Array).
    pub fn import(export: VerifierCredentialExport) -> Result<Self, CredentialExportError> {
        let array: &Uint8Array = export
            .0
            .dyn_ref()
            .ok_or_else(|| CredentialExportError::InvalidFormat("expected Uint8Array".into()))?;
        let bytes = array.to_vec();
        Self::from_tagged_public_bytes(&bytes)
    }

    fn to_tagged_public_bytes(&self) -> Vec<u8> {
        use crate::credential::constants::PUBLIC_TAG;
        match &self.0 {
            Verifier::Ed25519(verifier) => {
                let mut buffer = Vec::with_capacity(PUBLIC_TAG.len() + 32);
                buffer.extend_from_slice(PUBLIC_TAG);
                buffer.extend_from_slice(&verifier.0.to_bytes());
                buffer
            }
            #[cfg(feature = "es256")]
            Verifier::Es256(verifier) => {
                use crate::credential::constants::ES256_PUBLIC_TAG;
                let compressed = verifier.0.to_compressed_bytes();
                let mut buffer = Vec::with_capacity(ES256_PUBLIC_TAG.len() + compressed.len());
                buffer.extend_from_slice(ES256_PUBLIC_TAG);
                buffer.extend_from_slice(&compressed);
                buffer
            }
            #[cfg(feature = "webauthn")]
            Verifier::WebAuthn(verifier) => {
                use crate::credential::constants::WEBAUTHN_PUBLIC_TAG;
                let compressed = verifier.to_sec1_bytes();
                let mut buffer = Vec::with_capacity(WEBAUTHN_PUBLIC_TAG.len() + compressed.len());
                buffer.extend_from_slice(WEBAUTHN_PUBLIC_TAG);
                buffer.extend_from_slice(&compressed);
                buffer
            }
        }
    }

    fn from_tagged_public_bytes(bytes: &[u8]) -> Result<Self, CredentialExportError> {
        use crate::credential::constants::PUBLIC_TAG;
        use crate::ed25519::Ed25519VerifyingKey;

        // Historic web verifier exports were raw 32-byte ed25519 keys with no
        // tag. Accept them for backward compatibility.
        if bytes.len() == 32 {
            let key_arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| CredentialExportError::InvalidFormat("invalid public key".into()))?;
            let vk = ed25519_dalek::VerifyingKey::from_bytes(&key_arr)
                .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
            return Ok(Self(Verifier::Ed25519(crate::Ed25519Verifier(
                Ed25519VerifyingKey::Native(vk),
            ))));
        }

        if bytes.starts_with(PUBLIC_TAG) && bytes.len() == PUBLIC_TAG.len() + 32 {
            let key_arr: [u8; 32] = bytes[PUBLIC_TAG.len()..]
                .try_into()
                .map_err(|_| CredentialExportError::InvalidFormat("invalid public key".into()))?;
            let vk = ed25519_dalek::VerifyingKey::from_bytes(&key_arr)
                .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
            return Ok(Self(Verifier::Ed25519(crate::Ed25519Verifier(
                Ed25519VerifyingKey::Native(vk),
            ))));
        }

        #[cfg(feature = "es256")]
        {
            use crate::credential::constants::ES256_PUBLIC_TAG;
            use crate::es256::Es256VerifyingKey;
            if bytes.starts_with(ES256_PUBLIC_TAG) && bytes.len() == ES256_PUBLIC_TAG.len() + 33 {
                let key = &bytes[ES256_PUBLIC_TAG.len()..];
                let vk = p256::ecdsa::VerifyingKey::from_sec1_bytes(key)
                    .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
                return Ok(Self(Verifier::Es256(crate::Es256Verifier(
                    Es256VerifyingKey::Native(vk),
                ))));
            }
        }

        #[cfg(feature = "webauthn")]
        {
            use crate::credential::constants::WEBAUTHN_PUBLIC_TAG;
            if bytes.starts_with(WEBAUTHN_PUBLIC_TAG)
                && bytes.len() == WEBAUTHN_PUBLIC_TAG.len() + 33
            {
                let key = &bytes[WEBAUTHN_PUBLIC_TAG.len()..];
                let verifier = crate::webauthn::WebAuthnVerifier::from_sec1_bytes(key)
                    .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
                return Ok(Self(Verifier::WebAuthn(verifier)));
            }
        }

        Err(CredentialExportError::InvalidFormat(format!(
            "invalid verifier bytes length={}",
            bytes.len()
        )))
    }
}
