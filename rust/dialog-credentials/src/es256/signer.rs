//! ES256 signer implementation.

use super::{Es256SigningKey, error::Es256SignerError, verifier::Es256Verifier};
use crate::key::KeyExport;
use dialog_varsig::{Did, Principal, Signer, ecdsa::Es256Signature};
use serde::Serialize;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use super::web;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use crate::key::{ExtractableKey, WebCryptoError};

/// An `ES256` (`P-256`) `did:key` signer.
///
/// On native platforms this wraps a `p256::ecdsa::SigningKey`. On WASM it can
/// also wrap a `WebCrypto` `CryptoKey` for non-extractable key support.
#[derive(Debug, Clone)]
pub struct Es256Signer {
    did: Es256Verifier,
    signer: Es256SigningKey,
}

impl From<Es256SigningKey> for Es256Signer {
    fn from(signer: Es256SigningKey) -> Self {
        let did = Es256Verifier::from(signer.verifying_key());
        Self { did, signer }
    }
}

impl Es256Signer {
    /// Generate a new ES256 keypair.
    ///
    /// # Errors
    ///
    /// Returns an error if the RNG fails.
    pub async fn generate() -> Result<Self, Es256SignerError> {
        Ok(Es256SigningKey::generate().await?.into())
    }

    /// Import a keypair from a [`KeyExport`].
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a valid P-256 scalar.
    pub async fn import(key: impl Into<KeyExport>) -> Result<Self, Es256SignerError> {
        let signing_key = Es256SigningKey::import(key).await?;
        Ok(signing_key.into())
    }

    /// Export the key material.
    ///
    /// # Errors
    ///
    /// Currently infallible for the native arm.
    pub async fn export(&self) -> Result<KeyExport, Es256SignerError> {
        Ok(self.signer.export().await?)
    }

    /// Get the associated ES256 DID (verifier).
    #[must_use]
    pub const fn es256_did(&self) -> &Es256Verifier {
        &self.did
    }

    /// Get the inner signing key.
    #[must_use]
    pub const fn signing_key(&self) -> &Es256SigningKey {
        &self.signer
    }
}

impl From<p256::ecdsa::SigningKey> for Es256Signer {
    fn from(key: p256::ecdsa::SigningKey) -> Self {
        Es256SigningKey::from(key).into()
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<web::SigningKey> for Es256Signer {
    fn from(key: web::SigningKey) -> Self {
        Es256SigningKey::from(key).into()
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl ExtractableKey for Es256Signer {
    async fn generate() -> Result<Self, WebCryptoError> {
        let key = <web::SigningKey as ExtractableKey>::generate().await?;
        Ok(Es256SigningKey::from(key).into())
    }

    async fn import(key: impl Into<KeyExport>) -> Result<Self, WebCryptoError> {
        let key = <web::SigningKey as ExtractableKey>::import(key).await?;
        Ok(Es256SigningKey::from(key).into())
    }

    async fn export(&self) -> Result<KeyExport, WebCryptoError> {
        match &self.signer {
            Es256SigningKey::WebCrypto(key) => {
                <web::SigningKey as ExtractableKey>::export(key).await
            }
            Es256SigningKey::Native(key) => Ok(KeyExport::Extractable(key.to_bytes().to_vec())),
        }
    }
}

impl std::fmt::Display for Es256Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.did)
    }
}

impl Signer<Es256Signature> for Es256Signer {
    async fn sign(&self, msg: &[u8]) -> Result<Es256Signature, signature::Error> {
        self.signer.sign_bytes(msg).await
    }
}

impl Principal for Es256Signer {
    fn did(&self) -> Did {
        self.did.did()
    }
}

use dialog_capability::Issuer;

impl Issuer for Es256Signer {
    type Signature = Es256Signature;
}

impl Serialize for Es256Signer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.did.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    async fn es256_sign_verify_roundtrip() {
        use dialog_varsig::Verifier as _;

        let signer = Es256Signer::generate().await.unwrap();
        let verifier = signer.es256_did().clone();
        let msg = b"hello p256";

        let sig = Signer::sign(&signer, msg).await.unwrap();
        verifier.verify(msg, &sig).await.unwrap();

        // A tampered message must not verify.
        assert!(verifier.verify(b"other", &sig).await.is_err());
    }

    #[dialog_common::test]
    async fn es256_did_is_stable() {
        let signer = Es256Signer::generate().await.unwrap();
        let did1 = Principal::did(&signer);
        let did2 = signer.es256_did().did();
        assert_eq!(did1, did2);
    }

    #[dialog_common::test]
    async fn es256_export_import_roundtrip() {
        let signer = Es256Signer::generate().await.unwrap();
        let did = Principal::did(&signer);
        let export = signer.export().await.unwrap();
        let restored = Es256Signer::import(export).await.unwrap();
        assert_eq!(Principal::did(&restored), did);
    }
}
