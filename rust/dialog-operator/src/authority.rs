//! Authority — opened profile with signers and authority chain.
//!
//! [`Authority`] holds the profile and operator signers and implements
//! the provider traits needed by `Operator` for identity effects.

use dialog_capability::{Capability, Provider, Subject};
use dialog_credentials::Ed25519Signer;
use dialog_effects::authority::{self, AuthorityError, Operator as AuthOperator};
use dialog_varsig::{Did, Principal};

// Authority always answers for the current session, regardless of which
// repository we're operating on. We use the profile DID as the subject
// of the returned chain since that's the identity the chain describes.

/// An opened profile with profile and operator signers.
///
/// Implements `Provider<Identify>` and `Principal` so the capability
/// system can resolve identity.
/// Built by [`OperatorBuilder`](crate::operator::OperatorBuilder).
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
        _input: authority::Identify,
    ) -> Result<Capability<AuthOperator>, AuthorityError> {
        Ok(self.build_authority(self.profile_did()))
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
