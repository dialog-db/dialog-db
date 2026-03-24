//! Opened profile credentials.
//!
//! [`OpenedProfile`] holds the profile and operator signers and implements
//! the credential provider traits needed by the Environment.

use dialog_capability::credential::{self, CredentialError, Identity};
use dialog_capability::{Capability, Policy, Provider};
use dialog_varsig::{Did, Principal};

#[cfg(feature = "ed25519")]
use crate::Ed25519Signer;

/// An opened profile with profile and operator signers.
///
/// Created by opening a profile against a storage backend.
/// Implements the credential provider traits (`Provider<Identify>`,
/// `Provider<Sign>`, `Principal`, `Issuer`) needed by the Environment.
#[cfg(feature = "ed25519")]
#[derive(Debug, Clone)]
pub struct OpenedProfile {
    name: String,
    profile: Ed25519Signer,
    operator: Ed25519Signer,
    account: Option<Did>,
}

#[cfg(feature = "ed25519")]
impl OpenedProfile {
    /// Create an opened profile from existing signers.
    pub fn new(name: impl Into<String>, profile: Ed25519Signer, operator: Ed25519Signer) -> Self {
        Self {
            name: name.into(),
            profile,
            operator,
            account: None,
        }
    }

    /// Set the account DID.
    pub fn with_account(mut self, account: Did) -> Self {
        self.account = Some(account);
        self
    }

    /// Get the profile name.
    pub fn profile_name(&self) -> &str {
        &self.name
    }

    /// Get the profile DID.
    pub fn profile_did(&self) -> Did {
        Principal::did(&self.profile)
    }

    /// Get the operator DID.
    pub fn operator_did(&self) -> Did {
        Principal::did(&self.operator)
    }

    /// Get the account DID, if configured.
    pub fn account_did(&self) -> Option<&Did> {
        self.account.as_ref()
    }

    /// Get a reference to the profile signer.
    pub fn profile_signer(&self) -> &Ed25519Signer {
        &self.profile
    }

    /// Get a reference to the operator signer.
    pub fn operator_signer(&self) -> &Ed25519Signer {
        &self.operator
    }
}

#[cfg(feature = "ed25519")]
impl Principal for OpenedProfile {
    fn did(&self) -> Did {
        self.operator_did()
    }
}

#[cfg(feature = "ed25519")]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<credential::Identify> for OpenedProfile {
    async fn execute(
        &self,
        _input: Capability<credential::Identify>,
    ) -> Result<Identity, CredentialError> {
        Ok(Identity {
            profile: self.profile_did(),
            operator: self.operator_did(),
            account: self.account.clone(),
        })
    }
}

#[cfg(feature = "ed25519")]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<credential::Sign> for OpenedProfile {
    async fn execute(
        &self,
        input: Capability<credential::Sign>,
    ) -> Result<Vec<u8>, CredentialError> {
        use dialog_varsig::Signer;
        use dialog_varsig::eddsa::Ed25519Signature;

        let payload = credential::Sign::of(&input).payload.as_slice();
        let sig: Ed25519Signature = Signer::sign(&self.operator, payload)
            .await
            .map_err(|e| CredentialError::SigningFailed(e.to_string()))?;
        Ok(sig.to_bytes().to_vec())
    }
}

#[cfg(feature = "ed25519")]
impl dialog_capability::Issuer for OpenedProfile {
    type Signature = dialog_varsig::eddsa::Ed25519Signature;
}

#[cfg(feature = "ed25519")]
impl dialog_varsig::Signer<dialog_varsig::eddsa::Ed25519Signature> for OpenedProfile {
    async fn sign(
        &self,
        msg: &[u8],
    ) -> Result<dialog_varsig::eddsa::Ed25519Signature, signature::Error> {
        dialog_varsig::Signer::sign(&self.operator, msg).await
    }
}

#[cfg(feature = "ed25519")]
impl serde::Serialize for OpenedProfile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.operator.serialize(serializer)
    }
}
