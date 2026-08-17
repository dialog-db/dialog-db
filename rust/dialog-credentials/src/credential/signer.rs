//! Signer credential — wraps an algorithm-agnostic signing key.

use crate::Signer;
use dialog_capability::Issuer;
use dialog_varsig::{Did, Principal};

#[cfg(not(target_arch = "wasm32"))]
use super::constants::{
    KEY_SIZE, PRIVATE_TAG, PRIVATE_TAG_SIZE, PUBLIC_KEY_OFFSET, PUBLIC_TAG, PUBLIC_TAG_SIZE,
    SIGNER_EXPORT_SIZE,
};
use super::export::{CredentialExportError, SignerCredentialExport};

/// A signer credential — wraps a [`Signer`] (a full keypair for some algorithm).
#[derive(Debug, Clone)]
pub struct SignerCredential(pub Signer);

impl From<Signer> for SignerCredential {
    fn from(signer: Signer) -> Self {
        Self(signer)
    }
}

impl From<crate::Ed25519Signer> for SignerCredential {
    fn from(signer: crate::Ed25519Signer) -> Self {
        Self(Signer::Ed25519(signer))
    }
}

#[cfg(feature = "es256")]
impl From<crate::Es256Signer> for SignerCredential {
    fn from(signer: crate::Es256Signer) -> Self {
        Self(Signer::Es256(signer))
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
    pub fn signer(&self) -> &Signer {
        &self.0
    }

    /// Consume and return the underlying signer.
    pub fn into_signer(self) -> Signer {
        self.0
    }
}

impl From<SignerCredential> for Signer {
    fn from(credential: SignerCredential) -> Self {
        credential.0
    }
}

impl dialog_varsig::Signer<crate::Signature> for SignerCredential {
    async fn sign(&self, msg: &[u8]) -> Result<crate::Signature, signature::Error> {
        dialog_varsig::Signer::sign(&self.0, msg).await
    }
}

impl Issuer for SignerCredential {
    type Signature = crate::Signature;
}

#[cfg(not(target_arch = "wasm32"))]
impl SignerCredential {
    /// Export to multicodec-tagged bytes for native storage.
    ///
    /// The layout is `{private_tag|seed|public_tag|pubkey}`, with the tags and
    /// key sizes chosen per algorithm. ed25519 is byte-identical to prior
    /// releases.
    pub async fn export(&self) -> Result<SignerCredentialExport, CredentialExportError> {
        match &self.0 {
            Signer::Ed25519(signer) => {
                let crate::key::KeyExport::Extractable(ref seed) = signer
                    .export()
                    .await
                    .map_err(|e| CredentialExportError::Key(e.to_string()))?;

                let public_key = signer.ed25519_did().0.to_bytes();
                let mut buffer = vec![0u8; SIGNER_EXPORT_SIZE];
                buffer[..PRIVATE_TAG_SIZE].copy_from_slice(PRIVATE_TAG);
                buffer[PRIVATE_TAG_SIZE..PUBLIC_KEY_OFFSET].copy_from_slice(seed);
                buffer[PUBLIC_KEY_OFFSET..PUBLIC_KEY_OFFSET + PUBLIC_TAG_SIZE]
                    .copy_from_slice(PUBLIC_TAG);
                buffer[PUBLIC_KEY_OFFSET + PUBLIC_TAG_SIZE..].copy_from_slice(&public_key);
                Ok(SignerCredentialExport(buffer))
            }
            #[cfg(feature = "es256")]
            Signer::Es256(signer) => {
                Ok(SignerCredentialExport(es256_native::export(signer).await?))
            }
        }
    }

    /// Import from multicodec-tagged bytes, dispatching on the leading tag.
    pub async fn import(export: SignerCredentialExport) -> Result<Self, CredentialExportError> {
        let data = &export.0;

        if data.len() == SIGNER_EXPORT_SIZE
            && data.starts_with(PRIVATE_TAG)
            && data[PUBLIC_KEY_OFFSET..].starts_with(PUBLIC_TAG)
        {
            let seed: &[u8; KEY_SIZE] = data[PRIVATE_TAG_SIZE..PUBLIC_KEY_OFFSET]
                .try_into()
                .map_err(|_| CredentialExportError::InvalidFormat("invalid seed".into()))?;
            let stored_pubkey: &[u8; KEY_SIZE] = data[PUBLIC_KEY_OFFSET + PUBLIC_TAG_SIZE..]
                .try_into()
                .map_err(|_| CredentialExportError::InvalidFormat("invalid public key".into()))?;
            let signer = crate::Ed25519Signer::import(seed)
                .await
                .map_err(|e| CredentialExportError::Key(e.to_string()))?;

            // Verify the stored public key matches the one derived from the
            // seed. A mismatch indicates either corruption or tampering.
            let derived_pubkey = signer.ed25519_did().0.to_bytes();
            if *stored_pubkey != derived_pubkey {
                return Err(CredentialExportError::InvalidFormat(
                    "public key does not match seed".into(),
                ));
            }

            return Ok(Self(Signer::Ed25519(signer)));
        }

        #[cfg(feature = "es256")]
        if let Some(signer) = es256_native::try_import(data).await? {
            return Ok(Self(Signer::Es256(signer)));
        }

        Err(CredentialExportError::InvalidFormat(
            "unrecognized signer credential tags".into(),
        ))
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "es256"))]
mod es256_native {
    use super::CredentialExportError;
    use crate::Es256Signer;
    use crate::credential::constants::{
        ES256_PRIVATE_KEY_SIZE, ES256_PRIVATE_TAG, ES256_PRIVATE_TAG_SIZE, ES256_PUBLIC_KEY_OFFSET,
        ES256_PUBLIC_KEY_SIZE, ES256_PUBLIC_TAG, ES256_PUBLIC_TAG_SIZE, ES256_SIGNER_EXPORT_SIZE,
    };

    pub(super) async fn export(signer: &Es256Signer) -> Result<Vec<u8>, CredentialExportError> {
        let crate::key::KeyExport::Extractable(ref scalar) = signer
            .export()
            .await
            .map_err(|e| CredentialExportError::Key(e.to_string()))?;

        let public_key = signer.es256_did().0.to_compressed_bytes();
        let mut buffer = vec![0u8; ES256_SIGNER_EXPORT_SIZE];
        buffer[..ES256_PRIVATE_TAG_SIZE].copy_from_slice(ES256_PRIVATE_TAG);
        buffer[ES256_PRIVATE_TAG_SIZE..ES256_PUBLIC_KEY_OFFSET].copy_from_slice(scalar);
        buffer[ES256_PUBLIC_KEY_OFFSET..ES256_PUBLIC_KEY_OFFSET + ES256_PUBLIC_TAG_SIZE]
            .copy_from_slice(ES256_PUBLIC_TAG);
        buffer[ES256_PUBLIC_KEY_OFFSET + ES256_PUBLIC_TAG_SIZE..].copy_from_slice(&public_key);
        Ok(buffer)
    }

    pub(super) async fn try_import(
        data: &[u8],
    ) -> Result<Option<Es256Signer>, CredentialExportError> {
        if data.len() != ES256_SIGNER_EXPORT_SIZE
            || !data.starts_with(ES256_PRIVATE_TAG)
            || !data[ES256_PUBLIC_KEY_OFFSET..].starts_with(ES256_PUBLIC_TAG)
        {
            return Ok(None);
        }

        let scalar: &[u8; ES256_PRIVATE_KEY_SIZE] = data
            [ES256_PRIVATE_TAG_SIZE..ES256_PUBLIC_KEY_OFFSET]
            .try_into()
            .map_err(|_| CredentialExportError::InvalidFormat("invalid es256 scalar".into()))?;
        let stored_pubkey: &[u8; ES256_PUBLIC_KEY_SIZE] = data
            [ES256_PUBLIC_KEY_OFFSET + ES256_PUBLIC_TAG_SIZE..]
            .try_into()
            .map_err(|_| CredentialExportError::InvalidFormat("invalid es256 public key".into()))?;
        let signer = Es256Signer::import(scalar)
            .await
            .map_err(|e| CredentialExportError::Key(e.to_string()))?;

        let derived_pubkey = signer.es256_did().0.to_compressed_bytes();
        if *stored_pubkey != derived_pubkey {
            return Err(CredentialExportError::InvalidFormat(
                "es256 public key does not match scalar".into(),
            ));
        }
        Ok(Some(signer))
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl SignerCredential {
    /// Export to a JsValue (CryptoKeyPair) for web storage.
    pub async fn export(&self) -> Result<SignerCredentialExport, CredentialExportError> {
        match &self.0 {
            Signer::Ed25519(signer) => {
                let key_export = signer
                    .export()
                    .await
                    .map_err(|e| CredentialExportError::Key(e.to_string()))?;
                Ok(SignerCredentialExport(key_export.into()))
            }
            #[cfg(feature = "es256")]
            Signer::Es256(signer) => {
                let key_export = signer
                    .export()
                    .await
                    .map_err(|e| CredentialExportError::Key(e.to_string()))?;
                Ok(SignerCredentialExport(key_export.into()))
            }
        }
    }

    /// Import from a JsValue (CryptoKeyPair).
    ///
    /// The web `CryptoKeyPair` does not carry an algorithm tag we can read
    /// synchronously, so the ed25519 arm is tried; a future multi-algorithm web
    /// store would probe the key's algorithm before dispatch.
    pub async fn import(export: SignerCredentialExport) -> Result<Self, CredentialExportError> {
        let key_export = crate::key::KeyExport::try_from(export.0)
            .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
        let signer = crate::Ed25519Signer::import(key_export)
            .await
            .map_err(|e| CredentialExportError::Key(e.to_string()))?;
        Ok(Self(Signer::Ed25519(signer)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Ed25519Signer;

    #[dialog_common::test]
    async fn it_roundtrips_export_import() {
        let signer = Ed25519Signer::generate().await.unwrap();
        let original_did = Principal::did(&signer);
        let cred = SignerCredential::from(signer);

        let export = cred.export().await.unwrap();
        let imported = SignerCredential::import(export).await.unwrap();

        assert_eq!(imported.did(), original_did);
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "es256"))]
    #[dialog_common::test]
    async fn it_roundtrips_es256_export_import() {
        use crate::Es256Signer;
        let signer = Es256Signer::generate().await.unwrap();
        let original_did = Principal::did(&signer);
        let cred = SignerCredential::from(signer);

        let export = cred.export().await.unwrap();
        let imported = SignerCredential::import(export).await.unwrap();

        assert_eq!(imported.did(), original_did);
        assert_eq!(imported.0.algorithm(), crate::AlgorithmTag::Es256);
    }

    #[cfg(not(target_arch = "wasm32"))]
    mod native {
        use super::*;
        use crate::credential::constants::{
            PUBLIC_KEY_OFFSET, PUBLIC_TAG_SIZE, SIGNER_EXPORT_SIZE,
        };

        #[dialog_common::test]
        async fn it_rejects_mismatched_pubkey() {
            let signer = Ed25519Signer::generate().await.unwrap();
            let cred = SignerCredential::from(signer);
            let export = cred.export().await.unwrap();

            // Tamper with the public key bytes (flip all bits) while keeping
            // the seed and multicodec tags intact.
            let mut bytes = export.0;
            assert_eq!(bytes.len(), SIGNER_EXPORT_SIZE);
            for b in &mut bytes[PUBLIC_KEY_OFFSET + PUBLIC_TAG_SIZE..] {
                *b ^= 0xff;
            }

            let result = SignerCredential::import(bytes.into()).await;
            assert!(
                result.is_err(),
                "should reject credential where public key doesn't match seed"
            );
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("does not match seed"),
                "error should mention mismatch: {err}"
            );
        }

        #[dialog_common::test]
        async fn it_rejects_invalid_tags() {
            let mut bytes = vec![0u8; SIGNER_EXPORT_SIZE];
            // Wrong private key tag
            bytes[0] = 0x00;
            bytes[1] = 0x00;

            let result = SignerCredential::import(bytes.into()).await;
            assert!(result.is_err(), "should reject invalid multicodec tags");
        }

        // The exact bytes an ed25519 signer credential serialized to before the
        // algorithm-agnostic change: {0x80,0x26 | 32-byte seed | 0xed,0x01 |
        // 32-byte pubkey}. Deterministic seed of all 0x07 so the vector is
        // stable. This asserts old on-disk credentials still import.
        #[dialog_common::test]
        async fn it_imports_legacy_ed25519_credential() {
            let seed = [0x07u8; 32];
            let signer = Ed25519Signer::import(&seed).await.unwrap();
            let expected_did = Principal::did(&signer);
            let pubkey = signer.ed25519_did().0.to_bytes();

            let mut legacy = Vec::new();
            legacy.extend_from_slice(&[0x80, 0x26]);
            legacy.extend_from_slice(&seed);
            legacy.extend_from_slice(&[0xed, 0x01]);
            legacy.extend_from_slice(&pubkey);
            assert_eq!(legacy.len(), SIGNER_EXPORT_SIZE);

            let imported = SignerCredential::import(legacy.into()).await.unwrap();
            assert_eq!(imported.did(), expected_did);
        }
    }

    #[cfg(target_arch = "wasm32")]
    mod web {
        use super::*;
        use crate::credential::export::SignerCredentialExport;
        use js_sys::Object;
        use wasm_bindgen::JsValue;

        wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

        #[dialog_common::test]
        async fn it_rejects_garbage_jsvalue() {
            let garbage = SignerCredentialExport(JsValue::from_str("not a key"));
            let result = SignerCredential::import(garbage).await;
            assert!(result.is_err(), "should reject a string as credential");
        }

        #[dialog_common::test]
        async fn it_rejects_null() {
            let null = SignerCredentialExport(JsValue::NULL);
            let result = SignerCredential::import(null).await;
            assert!(result.is_err(), "should reject null as credential");
        }

        #[dialog_common::test]
        async fn it_rejects_random_object() {
            let obj = Object::new();
            let export = SignerCredentialExport(obj.into());
            let result = SignerCredential::import(export).await;
            assert!(result.is_err(), "should reject random object as credential");
        }
    }
}
