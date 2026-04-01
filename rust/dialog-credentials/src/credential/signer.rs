//! Signer credential — wraps a full Ed25519 keypair.

use crate::Ed25519Signer;
use crate::key::KeyExport;
use dialog_capability::Issuer;
use dialog_varsig::eddsa::Ed25519Signature;
use dialog_varsig::{Did, Principal, Signer};

use super::export::{CredentialExportError, SignerCredentialExport};
#[cfg(not(target_arch = "wasm32"))]
use super::export::{
    ED25519_PRIV_TAG, ED25519_PUB_TAG, KEY_SIZE, PRIV_TAG_SIZE, PUB_KEY_OFFSET, PUB_TAG_SIZE,
    SIGNER_EXPORT_SIZE,
};

/// A signer credential — wraps an `Ed25519Signer` (full keypair).
#[derive(Debug, Clone)]
pub struct SignerCredential(pub Ed25519Signer);

impl From<Ed25519Signer> for SignerCredential {
    fn from(signer: Ed25519Signer) -> Self {
        Self(signer)
    }
}

impl Principal for SignerCredential {
    fn did(&self) -> Did {
        Principal::did(&self.0)
    }
}

impl From<SignerCredential> for Did {
    fn from(credential: SignerCredential) -> Self {
        credential.did()
    }
}

impl SignerCredential {
    /// Get a reference to the underlying signer.
    pub fn signer(&self) -> &Ed25519Signer {
        &self.0
    }

    /// Consume and return the underlying signer.
    pub fn into_signer(self) -> Ed25519Signer {
        self.0
    }
}

impl From<SignerCredential> for Ed25519Signer {
    fn from(credential: SignerCredential) -> Self {
        credential.0
    }
}

impl Signer<Ed25519Signature> for SignerCredential {
    async fn sign(&self, msg: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        Signer::sign(&self.0, msg).await
    }
}

impl Issuer for SignerCredential {
    type Signature = Ed25519Signature;
}

#[cfg(not(target_arch = "wasm32"))]
impl SignerCredential {
    /// Export to multicodec-tagged bytes for native storage.
    pub async fn export(&self) -> Result<SignerCredentialExport, CredentialExportError> {
        let KeyExport::Extractable(ref seed) = self
            .0
            .export()
            .await
            .map_err(|e| CredentialExportError::Key(e.to_string()))?;

        let public_key = self.0.ed25519_did().0.to_bytes();
        let mut buf = [0u8; SIGNER_EXPORT_SIZE];
        buf[..PRIV_TAG_SIZE].copy_from_slice(ED25519_PRIV_TAG);
        buf[PRIV_TAG_SIZE..PUB_KEY_OFFSET].copy_from_slice(seed);
        buf[PUB_KEY_OFFSET..PUB_KEY_OFFSET + PUB_TAG_SIZE].copy_from_slice(ED25519_PUB_TAG);
        buf[PUB_KEY_OFFSET + PUB_TAG_SIZE..].copy_from_slice(&public_key);
        Ok(SignerCredentialExport(buf))
    }

    /// Import from multicodec-tagged bytes.
    pub async fn import(export: SignerCredentialExport) -> Result<Self, CredentialExportError> {
        let data = &export.0;
        if !data.starts_with(ED25519_PRIV_TAG)
            || !data[PUB_KEY_OFFSET..].starts_with(ED25519_PUB_TAG)
        {
            return Err(CredentialExportError::InvalidFormat(
                "invalid multicodec tags".into(),
            ));
        }

        let seed: &[u8; KEY_SIZE] = data[PRIV_TAG_SIZE..PUB_KEY_OFFSET]
            .try_into()
            .map_err(|_| CredentialExportError::InvalidFormat("invalid seed".into()))?;
        let signer = Ed25519Signer::import(seed)
            .await
            .map_err(|e| CredentialExportError::Key(e.to_string()))?;
        Ok(Self(signer))
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl SignerCredential {
    /// Export to a JsValue (CryptoKeyPair) for web storage.
    pub async fn export(&self) -> Result<SignerCredentialExport, CredentialExportError> {
        let key_export = self
            .0
            .export()
            .await
            .map_err(|e| CredentialExportError::Key(e.to_string()))?;
        Ok(SignerCredentialExport(key_export.into()))
    }

    /// Import from a JsValue (CryptoKeyPair).
    pub async fn import(export: SignerCredentialExport) -> Result<Self, CredentialExportError> {
        let key_export = KeyExport::try_from(export.0)
            .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
        let signer = Ed25519Signer::import(key_export)
            .await
            .map_err(|e| CredentialExportError::Key(e.to_string()))?;
        Ok(Self(signer))
    }
}
