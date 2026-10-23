//! Credential types for identity management.
//!
//! A [`Credential`] represents either a full signing keypair ([`SignerCredential`])
//! or a public-key-only verifier ([`VerifierCredential`]).

pub(crate) mod constants;
pub mod export;
pub mod signer;
pub mod verifier;

pub use export::{
    CredentialExport, CredentialExportError, SignerCredentialExport, VerifierCredentialExport,
};
pub use signer::SignerCredential;
pub use verifier::VerifierCredential;

use crate::{Signer, Verifier};
use dialog_varsig::{Did, Principal};
use serde::ser::Error as SerError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Either a signer or verifier credential.
///
/// # Serialization
///
/// Only the `Verifier` variant is serializable (as the DID string).
/// Serializing a `Signer` will fail to prevent accidental key leakage.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Credential {
    /// Full keypair — can sign as this identity.
    Signer(SignerCredential),
    /// Public key only — can verify but not sign.
    Verifier(VerifierCredential),
}

impl From<Signer> for Credential {
    fn from(signer: Signer) -> Self {
        Self::Signer(SignerCredential(signer))
    }
}

impl From<crate::Ed25519Signer> for Credential {
    fn from(signer: crate::Ed25519Signer) -> Self {
        Self::Signer(SignerCredential(Signer::Ed25519(signer)))
    }
}

#[cfg(feature = "es256")]
impl From<crate::Es256Signer> for Credential {
    fn from(signer: crate::Es256Signer) -> Self {
        Self::Signer(SignerCredential(Signer::Es256(signer)))
    }
}

impl From<Verifier> for Credential {
    fn from(verifier: Verifier) -> Self {
        Self::Verifier(VerifierCredential(verifier))
    }
}

impl From<crate::Ed25519Verifier> for Credential {
    fn from(verifier: crate::Ed25519Verifier) -> Self {
        Self::Verifier(VerifierCredential(Verifier::Ed25519(verifier)))
    }
}

#[cfg(feature = "es256")]
impl From<crate::Es256Verifier> for Credential {
    fn from(verifier: crate::Es256Verifier) -> Self {
        Self::Verifier(VerifierCredential(Verifier::Es256(verifier)))
    }
}

impl Principal for Credential {
    fn did(&self) -> Did {
        match self {
            Self::Signer(s) => s.did(),
            Self::Verifier(v) => v.did(),
        }
    }
}

impl From<Credential> for Did {
    fn from(credential: Credential) -> Self {
        credential.did()
    }
}

impl Credential {
    /// Get a reference to the signer, if this credential holds one.
    pub fn signer(&self) -> Option<&Signer> {
        match self {
            Self::Signer(s) => Some(&s.0),
            Self::Verifier(_) => None,
        }
    }

    /// Export to a platform-specific storage form.
    pub async fn export(&self) -> Result<CredentialExport, CredentialExportError> {
        match self {
            Self::Signer(s) => Ok(CredentialExport::Signer(s.export().await?)),
            Self::Verifier(v) => Ok(CredentialExport::Verifier(v.export())),
        }
    }

    /// Import from a platform-specific storage form.
    pub async fn import(export: CredentialExport) -> Result<Self, CredentialExportError> {
        match export {
            CredentialExport::Signer(s) => Ok(Self::Signer(SignerCredential::import(s).await?)),
            CredentialExport::Verifier(v) => Ok(Self::Verifier(VerifierCredential::import(v)?)),
        }
    }

    /// Recover a verifier-only credential (public key, hence DID) from the
    /// byte-compatible on-disk storage form, on any target.
    ///
    /// The stored form is multicodec-tagged bytes — `{public_tag|pubkey}` for a
    /// verifier, `{private_tag|priv|public_tag|pubkey}` for a signer, with the
    /// tags naming the algorithm. Only the public key is read, so this works in
    /// the browser too (no WebCrypto import of a non-extractable signing key).
    /// The result can verify and yields the credential's [`Did`], which is all
    /// that subject checks need; it cannot sign.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn identity(bytes: &[u8]) -> Result<Self, CredentialExportError> {
        Self::identity_native(bytes)
    }

    /// Recover a verifier-only credential from tagged bytes (any target).
    #[cfg(target_arch = "wasm32")]
    pub fn identity(bytes: &[u8]) -> Result<Self, CredentialExportError> {
        Self::identity_any(bytes)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn identity_native(bytes: &[u8]) -> Result<Self, CredentialExportError> {
        use constants::{PRIVATE_TAG, PUBLIC_KEY_OFFSET, PUBLIC_TAG, PUBLIC_TAG_SIZE};

        // ed25519 verifier form.
        if bytes.starts_with(PUBLIC_TAG) && bytes.len() == PUBLIC_TAG_SIZE + constants::KEY_SIZE {
            return Self::ed25519_verifier_from_pubkey(&bytes[PUBLIC_TAG_SIZE..]);
        }
        // ed25519 signer form: read only the trailing public key.
        if bytes.starts_with(PRIVATE_TAG)
            && bytes.len() == constants::SIGNER_EXPORT_SIZE
            && bytes[PUBLIC_KEY_OFFSET..].starts_with(PUBLIC_TAG)
        {
            return Self::ed25519_verifier_from_pubkey(
                &bytes[PUBLIC_KEY_OFFSET + PUBLIC_TAG_SIZE..],
            );
        }

        #[cfg(feature = "es256")]
        {
            use constants::{
                ES256_PRIVATE_TAG, ES256_PUBLIC_KEY_OFFSET, ES256_PUBLIC_TAG,
                ES256_PUBLIC_TAG_SIZE, ES256_SIGNER_EXPORT_SIZE, ES256_VERIFIER_EXPORT_SIZE,
            };
            if bytes.starts_with(ES256_PUBLIC_TAG) && bytes.len() == ES256_VERIFIER_EXPORT_SIZE {
                return Self::es256_verifier_from_pubkey(&bytes[ES256_PUBLIC_TAG_SIZE..]);
            }
            if bytes.starts_with(ES256_PRIVATE_TAG)
                && bytes.len() == ES256_SIGNER_EXPORT_SIZE
                && bytes[ES256_PUBLIC_KEY_OFFSET..].starts_with(ES256_PUBLIC_TAG)
            {
                return Self::es256_verifier_from_pubkey(
                    &bytes[ES256_PUBLIC_KEY_OFFSET + ES256_PUBLIC_TAG_SIZE..],
                );
            }
        }

        #[cfg(feature = "webauthn")]
        {
            use constants::{
                WEBAUTHN_PUBLIC_TAG, WEBAUTHN_PUBLIC_TAG_SIZE, WEBAUTHN_VERIFIER_EXPORT_SIZE,
            };
            if bytes.starts_with(WEBAUTHN_PUBLIC_TAG)
                && bytes.len() == WEBAUTHN_VERIFIER_EXPORT_SIZE
            {
                return Self::webauthn_verifier_from_pubkey(&bytes[WEBAUTHN_PUBLIC_TAG_SIZE..]);
            }
        }

        #[cfg(feature = "rsa")]
        {
            use constants::{
                RSA_PRIVATE_TAG, RSA_PRIVATE_TAG_SIZE, RSA_PUBLIC_TAG, RSA_PUBLIC_TAG_SIZE,
            };
            // RSA bodies are variable length, so dispatch on the tag prefix
            // alone. The private form yields the verifier via the derived key.
            if bytes.starts_with(RSA_PUBLIC_TAG) {
                return Self::rsa_verifier_from_pubkey(&bytes[RSA_PUBLIC_TAG_SIZE..]);
            }
            if bytes.starts_with(RSA_PRIVATE_TAG) {
                return Self::rsa_verifier_from_private(&bytes[RSA_PRIVATE_TAG_SIZE..]);
            }
        }

        Err(CredentialExportError::InvalidFormat(format!(
            "unrecognized credential format: length={}",
            bytes.len()
        )))
    }

    #[cfg(target_arch = "wasm32")]
    fn identity_any(bytes: &[u8]) -> Result<Self, CredentialExportError> {
        use constants::{PUBLIC_TAG, PUBLIC_TAG_SIZE};
        if bytes.starts_with(PUBLIC_TAG) && bytes.len() == PUBLIC_TAG_SIZE + constants::KEY_SIZE {
            return Self::ed25519_verifier_from_pubkey(&bytes[PUBLIC_TAG_SIZE..]);
        }
        #[cfg(feature = "es256")]
        {
            use constants::{ES256_PUBLIC_TAG, ES256_PUBLIC_TAG_SIZE, ES256_VERIFIER_EXPORT_SIZE};
            if bytes.starts_with(ES256_PUBLIC_TAG) && bytes.len() == ES256_VERIFIER_EXPORT_SIZE {
                return Self::es256_verifier_from_pubkey(&bytes[ES256_PUBLIC_TAG_SIZE..]);
            }
        }
        #[cfg(feature = "webauthn")]
        {
            use constants::{
                WEBAUTHN_PUBLIC_TAG, WEBAUTHN_PUBLIC_TAG_SIZE, WEBAUTHN_VERIFIER_EXPORT_SIZE,
            };
            if bytes.starts_with(WEBAUTHN_PUBLIC_TAG)
                && bytes.len() == WEBAUTHN_VERIFIER_EXPORT_SIZE
            {
                return Self::webauthn_verifier_from_pubkey(&bytes[WEBAUTHN_PUBLIC_TAG_SIZE..]);
            }
        }
        #[cfg(feature = "rsa")]
        {
            use constants::{
                RSA_PRIVATE_TAG, RSA_PRIVATE_TAG_SIZE, RSA_PUBLIC_TAG, RSA_PUBLIC_TAG_SIZE,
            };
            if bytes.starts_with(RSA_PUBLIC_TAG) {
                return Self::rsa_verifier_from_pubkey(&bytes[RSA_PUBLIC_TAG_SIZE..]);
            }
            if bytes.starts_with(RSA_PRIVATE_TAG) {
                return Self::rsa_verifier_from_private(&bytes[RSA_PRIVATE_TAG_SIZE..]);
            }
        }
        Err(CredentialExportError::InvalidFormat(format!(
            "unrecognized credential format: length={}",
            bytes.len()
        )))
    }

    fn ed25519_verifier_from_pubkey(pubkey: &[u8]) -> Result<Self, CredentialExportError> {
        let key: [u8; 32] = pubkey
            .try_into()
            .map_err(|_| CredentialExportError::InvalidFormat("invalid public key".into()))?;
        let vk = ed25519_dalek::VerifyingKey::from_bytes(&key)
            .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
        Ok(Self::Verifier(VerifierCredential(Verifier::Ed25519(
            crate::Ed25519Verifier(crate::ed25519::Ed25519VerifyingKey::Native(vk)),
        ))))
    }

    #[cfg(feature = "es256")]
    fn es256_verifier_from_pubkey(pubkey: &[u8]) -> Result<Self, CredentialExportError> {
        let vk = p256::ecdsa::VerifyingKey::from_sec1_bytes(pubkey)
            .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
        Ok(Self::Verifier(VerifierCredential(Verifier::Es256(
            crate::Es256Verifier(crate::es256::Es256VerifyingKey::Native(vk)),
        ))))
    }

    #[cfg(feature = "webauthn")]
    fn webauthn_verifier_from_pubkey(pubkey: &[u8]) -> Result<Self, CredentialExportError> {
        let verifier = crate::webauthn::WebAuthnVerifier::from_sec1_bytes(pubkey)
            .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
        Ok(Self::Verifier(VerifierCredential(Verifier::WebAuthn(
            verifier,
        ))))
    }

    #[cfg(feature = "rsa")]
    fn rsa_verifier_from_pubkey(pubkey: &[u8]) -> Result<Self, CredentialExportError> {
        let key = crate::rsa::RsaVerifyingKey::from_pkcs1_der(pubkey)
            .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
        Ok(Self::Verifier(VerifierCredential(Verifier::Rsa(
            crate::rsa::RsaVerifier(key),
        ))))
    }

    #[cfg(feature = "rsa")]
    fn rsa_verifier_from_private(private_der: &[u8]) -> Result<Self, CredentialExportError> {
        let signing_key = crate::rsa::RsaSigningKey::from_pkcs1_der(private_der)
            .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
        Ok(Self::Verifier(VerifierCredential(Verifier::Rsa(
            crate::rsa::RsaVerifier(signing_key.verifying_key()),
        ))))
    }

    /// The identity (public-key) bytes for this credential, in the
    /// byte-compatible verifier storage form `{public_tag|pubkey}`, on any
    /// target.
    ///
    /// This is the inverse of [`identity`](Self::identity): writing these bytes
    /// to a directory's `credential/key/self` is what makes that directory the
    /// space for this credential's [`Did`]. Only the public key is emitted, so
    /// it works in the browser too.
    pub fn to_identity_bytes(&self) -> Vec<u8> {
        let verifier = match self {
            Self::Signer(s) => s.0.verifier(),
            Self::Verifier(v) => v.0.clone(),
        };
        tagged_public_bytes(&verifier)
    }
}

/// Serialize a verifier's public key in the tagged `{public_tag|pubkey}` form.
fn tagged_public_bytes(verifier: &Verifier) -> Vec<u8> {
    use constants::PUBLIC_TAG;
    match verifier {
        Verifier::Ed25519(v) => {
            let mut buffer = Vec::with_capacity(PUBLIC_TAG.len() + 32);
            buffer.extend_from_slice(PUBLIC_TAG);
            buffer.extend_from_slice(&v.0.to_bytes());
            buffer
        }
        #[cfg(feature = "es256")]
        Verifier::Es256(v) => {
            use constants::ES256_PUBLIC_TAG;
            let compressed = v.0.to_compressed_bytes();
            let mut buffer = Vec::with_capacity(ES256_PUBLIC_TAG.len() + compressed.len());
            buffer.extend_from_slice(ES256_PUBLIC_TAG);
            buffer.extend_from_slice(&compressed);
            buffer
        }
        #[cfg(feature = "webauthn")]
        Verifier::WebAuthn(v) => {
            use constants::WEBAUTHN_PUBLIC_TAG;
            let compressed = v.to_sec1_bytes();
            let mut buffer = Vec::with_capacity(WEBAUTHN_PUBLIC_TAG.len() + compressed.len());
            buffer.extend_from_slice(WEBAUTHN_PUBLIC_TAG);
            buffer.extend_from_slice(&compressed);
            buffer
        }
        #[cfg(feature = "rsa")]
        Verifier::Rsa(v) => {
            use constants::RSA_PUBLIC_TAG;
            // The RSA public key is variable length (PKCS#1 DER), so unlike the
            // curve keys the body length is not fixed.
            let key_der = v.0.to_pkcs1_der();
            let mut buffer = Vec::with_capacity(RSA_PUBLIC_TAG.len() + key_der.len());
            buffer.extend_from_slice(RSA_PUBLIC_TAG);
            buffer.extend_from_slice(&key_der);
            buffer
        }
    }
}

impl Serialize for Credential {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Signer(_) => Err(SerError::custom(
                "Serialization of secret key material is not supported",
            )),
            Self::Verifier(v) => v.did().serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Credential {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let did = Did::deserialize(deserializer)?;
        let verifier = Verifier::from_did_key(did.as_str()).map_err(serde::de::Error::custom)?;
        Ok(Self::Verifier(VerifierCredential(verifier)))
    }
}
