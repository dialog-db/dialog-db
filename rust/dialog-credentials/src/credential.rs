//! Credential types for identity management.
//!
//! A [`Credential`] represents either a full signing keypair ([`SignerCredential`])
//! or a public-key-only verifier ([`VerifierCredential`]).

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
