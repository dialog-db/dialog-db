//! Credential capability hierarchy.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//!   └── Credential (ability: /credential)
//!         └── Address { address: String }
//!             ├── Save { credential } → Effect → Result<(), CredentialError>
//!             └── Load → Effect → Result<dialog_credentials::Credential, CredentialError>
//! ```

pub use dialog_capability::{Attenuation, Capability, Claim, Effect, Policy, Subject};
pub use dialog_credentials;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Root attenuation for memory operations.
///
/// Attaches to Subject and provides the `/memory` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Credential;

impl Attenuation for Credential {
    type Of = Subject;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Address {
    pub address: String,
}
impl Policy for Address {
    type Of = Credential;
}

#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Save {
    /// The content to save.
    pub credential: dialog_credentials::Credential,
}
impl Effect for Save {
    type Of = Address;
    type Output = Result<(), CredentialError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Load;
impl Effect for Load {
    type Of = Address;
    type Output = Result<dialog_credentials::Credential, CredentialError>;
}

/// Errors that can occur during memory operations.
#[derive(Debug, Error)]
pub enum CredentialError {
    // TODO
}
