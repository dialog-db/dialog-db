//! Profile configuration and credentials for opening an environment.
//!
//! [`Profile`] describes which identity to open — profile name and operator
//! strategy. [`Credentials`] is the result of opening a profile — it holds
//! the profile and operator signers and implements the credential provider
//! traits needed by the Environment.

pub mod open;
mod provider;

use dialog_capability::authority::{self, Authority, AuthorityError};
use dialog_capability::{Capability, Policy, Provider, Subject};
use dialog_credentials::Ed25519Signer;
use dialog_varsig::eddsa::Ed25519Signature;
use dialog_varsig::{Did, Principal};

/// How to create the operator key for a session.
pub enum Operator {
    /// Generate a random ephemeral keypair each time.
    Unique,
    /// Derive deterministically from the profile key + context.
    Derive(Vec<u8>),
}

impl Operator {
    /// Shorthand for `Operator::Unique`.
    pub fn unique() -> Self {
        Self::Unique
    }

    /// Shorthand for `Operator::Derive(context.into())`.
    pub fn derive(context: impl Into<Vec<u8>>) -> Self {
        Self::Derive(context.into())
    }
}

impl<T: Into<Vec<u8>>> From<T> for Operator {
    fn from(context: T) -> Self {
        Self::Derive(context.into())
    }
}

/// Describes which profile to open and how to create the operator.
///
/// This is a configuration type — pass it to [`environment::open`](crate::environment::open)
/// to materialize the actual credentials.
///
/// # Examples
///
/// ```no_run
/// use dialog_artifacts::Profile;
///
/// // Default profile with unique operator
/// let profile = Profile::default();
///
/// // Named profile with derived operator
/// let profile = Profile::named("work")
///     .operated_by(dialog_artifacts::Operator::derive(b"alice"));
/// ```
pub struct Profile {
    /// The profile name (e.g. "default", "work", "personal").
    pub name: String,
    /// How to create the operator key.
    pub operator: Operator,
}

impl Profile {
    /// Create a profile descriptor with the given name.
    ///
    /// Defaults to `Operator::Unique`. Use `.operated_by()` to change.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            operator: Operator::Unique,
        }
    }

    /// Set the operator strategy.
    ///
    /// Accepts `Operator` directly, or anything convertible to it
    /// (e.g. `b"context"` for derived operator).
    pub fn operated_by(mut self, operator: impl Into<Operator>) -> Self {
        self.operator = operator.into();
        self
    }
}

impl Default for Profile {
    fn default() -> Self {
        Self::named("default")
    }
}

/// An opened profile with profile and operator signers.
///
/// Created by [`environment::open`](crate::environment::open).
/// Implements the credential provider traits (`Provider<authority::Identify>`,
/// `Provider<authority::Sign>`, `Principal`, `Issuer`) needed by the Environment.
#[derive(Debug, Clone)]
pub struct Credentials {
    name: String,
    profile: Ed25519Signer,
    operator: Ed25519Signer,
    account: Option<Did>,
}

impl Credentials {
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
    fn build_authority(&self, subject: Did) -> Authority {
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

impl Principal for Credentials {
    fn did(&self) -> Did {
        self.operator_did()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<authority::Identify> for Credentials {
    async fn execute(
        &self,
        input: Capability<authority::Identify>,
    ) -> Result<Authority, AuthorityError> {
        let subject_did = input.subject().clone();
        Ok(self.build_authority(subject_did))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<authority::Sign> for Credentials {
    async fn execute(&self, input: Capability<authority::Sign>) -> Result<Vec<u8>, AuthorityError> {
        let payload = authority::Sign::of(&input).payload.as_slice();
        let sig: Ed25519Signature = dialog_varsig::Signer::sign(&self.operator, payload)
            .await
            .map_err(|e| AuthorityError::SigningFailed(e.to_string()))?;
        Ok(sig.to_bytes().to_vec())
    }
}

impl dialog_capability::Issuer for Credentials {
    type Signature = Ed25519Signature;
}

impl dialog_varsig::Signer<Ed25519Signature> for Credentials {
    async fn sign(&self, msg: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        dialog_varsig::Signer::sign(&self.operator, msg).await
    }
}

impl serde::Serialize for Credentials {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.operator.serialize(serializer)
    }
}
