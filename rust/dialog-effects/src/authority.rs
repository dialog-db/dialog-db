//! Authority capability hierarchy — identity chain.
//!
//! The authority chain encodes the identity hierarchy as a capability chain:
//!
//! ```text
//! Subject (repository DID)
//! └── Profile { profile: Did, account: Option<Did> }
//!     └── Operator { operator: Did }
//! ```
//!
//! [`Identify`] is a direct env command that returns the current
//! `Capability<Operator>` chain. It is not a capability itself — session
//! identity is ambient state, so we query it from the env rather than
//! pretending it scopes to a subject.

use dialog_capability::{Attenuation, Capability, Command, Did, Policy, Provider, Subject};
use dialog_common::ConditionalSync;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error type for authority operations.
#[derive(Debug, Error)]
pub enum AuthorityError {
    /// Identity resolution failed.
    #[error("Identity error: {0}")]
    Identity(String),
}

/// Device identity — attenuates from Subject.
///
/// A profile is a named user identity on a specific device, with its own
/// ed25519 keypair. The optional `account` links to a persistent identity
/// that survives device loss (None = local only, Some = linked/recovered).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Profile {
    /// The profile's DID (ed25519 public key).
    pub profile: Did,
    /// Optional account DID for cross-device recovery.
    pub account: Option<Did>,
}

impl Profile {
    /// Create a local profile (no account).
    pub fn local(profile: Did) -> Self {
        Self {
            profile,
            account: None,
        }
    }

    /// Create a linked profile with an account.
    pub fn linked(profile: Did, account: Did) -> Self {
        Self {
            profile,
            account: Some(account),
        }
    }
}

impl Attenuation for Profile {
    type Of = Subject;
}

/// Session key — attenuates from Profile.
///
/// An ephemeral key representing the immediate invoker of a capability
/// in a specific session or process context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Operator {
    /// The operator's DID (ephemeral session key).
    pub operator: Did,
}

impl Operator {
    /// Create a new operator.
    pub fn new(operator: Did) -> Self {
        Self { operator }
    }
}

impl Attenuation for Operator {
    type Of = Profile;
}

/// Identify command — returns the current session's authority chain.
///
/// Session identity is ambient: regardless of which repository we're
/// operating on, there is one current operator. `Identify` is a direct
/// env query for that ambient state rather than a capability invocation.
///
/// The returned `Capability<Operator>` encodes the identity hierarchy:
/// subject, profile, and operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identify;

impl Command for Identify {
    type Input = Self;
    type Output = Result<Capability<Operator>, AuthorityError>;
}

impl Identify {
    /// Perform this command against an env that can provide it.
    pub async fn perform<Env>(self, env: &Env) -> Result<Capability<Operator>, AuthorityError>
    where
        Env: Provider<Identify> + ConditionalSync,
    {
        env.execute(self).await
    }
}

/// Extension trait for `Capability<Operator>` providing convenient
/// access to the authority chain fields.
pub trait OperatorExt {
    /// The operator DID (ephemeral session key).
    fn did(&self) -> Did;

    /// The profile DID from the authority chain.
    fn profile(&self) -> &Did;

    /// The optional account DID from the authority chain.
    fn account(&self) -> &Option<Did>;
}

impl OperatorExt for Capability<Operator> {
    fn did(&self) -> Did {
        Operator::of(self).operator.clone()
    }

    fn profile(&self) -> &Did {
        &Profile::of(self).profile
    }

    fn account(&self) -> &Option<Did> {
        &Profile::of(self).account
    }
}
