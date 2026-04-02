//! Authority — opened profile with signers and authority chain.
//!
//! [`Authority`] holds the profile and operator signers and implements
//! the provider traits needed by `Operator` for identity and signing effects.

use dialog_capability::authority::{self, AuthorityError, Operator as AuthOperator};
use dialog_capability::{Capability, Policy, Provider, Subject};
use dialog_credentials::Ed25519Signer;
use dialog_varsig::eddsa::Ed25519Signature;
use dialog_varsig::{Did, Principal};

/// An opened profile with profile and operator signers.
///
/// Created by [`environment::open`](crate::environment::open).
/// Implements the credential provider traits (`Provider<authority::Identify>`,
/// `Provider<authority::Sign>`, `Principal`, `Issuer`) needed by the Environment.
#[derive(Debug, Clone)]
pub struct Authority {
    name: String,
    profile: Ed25519Signer,
    operator: Ed25519Signer,
    account: Option<Did>,
}

impl Authority {
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

    /// Build the authority chain for the given subject DID.
    pub fn build_authority(&self, subject: Did) -> Capability<AuthOperator> {
        Subject::from(subject)
            .attenuate(authority::Profile {
                profile: self.profile_did(),
                account: self.account.clone(),
            })
            .attenuate(authority::Operator {
                operator: self.operator_did(),
            })
    }
}

impl Principal for Authority {
    fn did(&self) -> Did {
        self.operator_did()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<authority::Identify> for Authority {
    async fn execute(
        &self,
        input: Capability<authority::Identify>,
    ) -> Result<Capability<AuthOperator>, AuthorityError> {
        let subject_did = input.subject().clone();
        Ok(self.build_authority(subject_did))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<authority::Sign> for Authority {
    async fn execute(&self, input: Capability<authority::Sign>) -> Result<Vec<u8>, AuthorityError> {
        let payload = authority::Sign::of(&input).payload.as_slice();
        let sig: Ed25519Signature = dialog_varsig::Signer::sign(&self.operator, payload)
            .await
            .map_err(|e| AuthorityError::SigningFailed(e.to_string()))?;
        Ok(sig.to_bytes().to_vec())
    }
}

impl dialog_capability::Issuer for Authority {
    type Signature = Ed25519Signature;
}

impl dialog_varsig::Signer<Ed25519Signature> for Authority {
    async fn sign(&self, msg: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        dialog_varsig::Signer::sign(&self.operator, msg).await
    }
}

impl serde::Serialize for Authority {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.operator.serialize(serializer)
    }
}
