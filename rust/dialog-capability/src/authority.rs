//! Authority capability hierarchy — identity chain and signing.
//!
//! The authority chain encodes the identity hierarchy as a capability chain:
//!
//! ```text
//! Subject (repository DID)
//! └── Profile { profile: Did, account: Option<Did> }
//!     └── Operator { operator: Did }
//!         └── Sign { payload } -> Effect -> Result<Vec<u8>, CredentialError>
//! ```
//!
//! [`Identify`] is an effect on `Subject` that returns the current
//! `Capability<Operator>` chain.

use crate::{Attenuation, Capability, Did, Effect, Policy, Subject};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error type for authority operations (identity and signing).
#[derive(Debug, Error)]
pub enum AuthorityError {
    /// Identity resolution failed.
    #[error("Identity error: {0}")]
    Identity(String),

    /// Signing operation failed.
    #[error("Signing failed: {0}")]
    SigningFailed(String),
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

/// Sign operation — signs a payload using the operator's key.
#[derive(Debug, Clone, Serialize, Deserialize, crate::Claim)]
pub struct Sign {
    /// The payload to sign.
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl Sign {
    /// Create a new Sign effect.
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            payload: payload.into(),
        }
    }
}

impl Effect for Sign {
    type Of = Operator;
    type Output = Result<Vec<u8>, AuthorityError>;
}

/// Extension trait for `Capability<Sign>` to access its fields.
pub trait SignCapability {
    /// Get the payload to sign.
    fn payload(&self) -> &[u8];
}

impl SignCapability for Capability<Sign> {
    fn payload(&self) -> &[u8] {
        &Sign::of(self).payload
    }
}

/// Identify operation — returns the current authority chain.
///
/// This is an effect directly on `Subject` — no intermediate attenuation.
/// The returned `Capability<Operator>` encodes the full identity hierarchy:
/// subject, profile, and operator.
#[derive(Debug, Clone, Serialize, Deserialize, crate::Claim)]
pub struct Identify;

impl Effect for Identify {
    type Of = Subject;
    type Output = Result<Capability<Operator>, AuthorityError>;
}
