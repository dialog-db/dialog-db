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
//! [`Identify`] is an effect on `Subject` that returns the current
//! `Capability<Operator>` chain.

use dialog_capability::{Attenuation, Capability, Did, Effect, Subject};
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

/// Identify operation — returns the current authority chain.
///
/// This is an effect directly on `Subject` — no intermediate attenuation.
/// The returned `Capability<Operator>` encodes the full identity hierarchy:
/// subject, profile, and operator.
#[derive(Debug, Clone, Serialize, Deserialize, dialog_capability::Claim)]
pub struct Identify;

impl Effect for Identify {
    type Of = Subject;
    type Output = Result<Capability<Operator>, AuthorityError>;
}
