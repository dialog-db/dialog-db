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

use crate::{Ed25519Signer, Ed25519Verifier};
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

impl From<Ed25519Signer> for Credential {
    fn from(signer: Ed25519Signer) -> Self {
        Self::Signer(SignerCredential(signer))
    }
}

impl From<Ed25519Verifier> for Credential {
    fn from(verifier: Ed25519Verifier) -> Self {
        Self::Verifier(VerifierCredential(verifier))
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
    pub fn signer(&self) -> Option<&Ed25519Signer> {
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
    /// The stored form is multicodec-tagged bytes — `{PUBLIC_TAG|pubkey}` for a
    /// verifier, `{PRIVATE_TAG|seed|PUBLIC_TAG|pubkey}` for a signer. Only the
    /// public key is read, so this works in the browser too (no WebCrypto
    /// import of a non-extractable signing key). The result can verify and
    /// yields the credential's [`Did`](dialog_varsig::Did), which is all that
    /// subject checks need; it cannot sign.
    pub fn identity(bytes: &[u8]) -> Result<Self, CredentialExportError> {
        use constants::{
            KEY_SIZE, PRIVATE_TAG, PUBLIC_KEY_OFFSET, PUBLIC_TAG, PUBLIC_TAG_SIZE,
            SIGNER_EXPORT_SIZE, VERIFIER_EXPORT_SIZE,
        };

        let pubkey: &[u8] = if bytes.len() == VERIFIER_EXPORT_SIZE && bytes.starts_with(PUBLIC_TAG)
        {
            &bytes[PUBLIC_TAG_SIZE..]
        } else if bytes.len() == SIGNER_EXPORT_SIZE
            && bytes.starts_with(PRIVATE_TAG)
            && bytes[PUBLIC_KEY_OFFSET..].starts_with(PUBLIC_TAG)
        {
            &bytes[PUBLIC_KEY_OFFSET + PUBLIC_TAG_SIZE..]
        } else {
            return Err(CredentialExportError::InvalidFormat(format!(
                "unrecognized credential format: length={}",
                bytes.len()
            )));
        };

        let key: [u8; KEY_SIZE] = pubkey
            .try_into()
            .map_err(|_| CredentialExportError::InvalidFormat("invalid public key".into()))?;
        let vk = ed25519_dalek::VerifyingKey::from_bytes(&key)
            .map_err(|e| CredentialExportError::InvalidFormat(e.to_string()))?;
        Ok(Self::Verifier(VerifierCredential::from(
            crate::Ed25519Verifier(crate::ed25519::Ed25519VerifyingKey::Native(vk)),
        )))
    }
}

impl Serialize for Credential {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Signer(_) => Err(SerError::custom(
                "Serialization of secret key material is not supported",
            )),
            Self::Verifier(v) => v.0.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Credential {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let verifier = Ed25519Verifier::deserialize(deserializer)?;
        Ok(Self::Verifier(VerifierCredential(verifier)))
    }
}
