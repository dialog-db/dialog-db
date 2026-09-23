//! Authority — the acting signer and the replica identity it acts for.
//!
//! [`Authority`] holds the acting signer and the DID of the replica the
//! chain describes, and implements the provider traits a peer needs for
//! identity effects.

use dialog_capability::{Capability, Provider, Subject};
use dialog_credentials::Signer;
use dialog_effects::authority::{self, AuthorityError, Operator as AuthOperator};
use dialog_varsig::{Did, Principal, Signer as _};

// Authority always answers for the current session, regardless of which
// repository we're operating on. We use the profile DID as the subject
// of the returned chain since that's the identity the chain describes.

/// The acting signer and the replica identity the chain describes.
///
/// `profile` is the replica identity: the DID of the peer whose repository
/// holds the acting key's state, and the DID every replica, line and
/// branch entity derives from. `operator` is the key that signs. For a
/// root peer they are the same principal; for a worker the profile is
/// its parent's DID.
///
/// Implements `Provider<Identify>` and `Principal` so the capability
/// system can resolve identity. Built by the peer builder in
/// `dialog-peer`, above this crate.
#[derive(Debug, Clone)]
pub struct Authority {
    name: String,
    profile: Did,
    operator: Signer,
    account: Option<Did>,
}

impl Authority {
    /// An authority acting as `operator` for the replica `profile`.
    pub fn new(
        name: impl Into<String>,
        profile: impl Into<Did>,
        operator: impl Into<Signer>,
    ) -> Self {
        Self {
            name: name.into(),
            profile: profile.into(),
            operator: operator.into(),
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

    /// The replica identity the chain describes.
    pub fn profile_did(&self) -> Did {
        self.profile.clone()
    }

    /// Get the operator DID.
    pub fn operator_did(&self) -> Did {
        Principal::did(&self.operator)
    }

    /// Get the account DID, if configured.
    pub fn account_did(&self) -> Option<&Did> {
        self.account.as_ref()
    }

    /// Get a reference to the operator signer.
    pub fn operator_signer(&self) -> &Signer {
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<authority::Attest> for Authority {
    async fn execute(&self, input: authority::Attest) -> Result<Vec<u8>, AuthorityError> {
        // Sign with the operator key: the session identity `Identify`
        // reports as the issuer, so verifiers can resolve the operator's
        // did to check the signature.
        let signature = self
            .operator
            .sign(&input.payload)
            .await
            .map_err(|error| AuthorityError::Attestation(format!("{error}")))?;
        Ok(signature.to_bytes().to_vec())
    }
}

impl serde::Serialize for Authority {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize as the operator DID, mirroring the operator signer's own
        // did:key serialization.
        self.operator_did().serialize(serializer)
    }
}
