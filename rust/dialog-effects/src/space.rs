//! Space capability hierarchy for operator-level space operations.
//!
//! Resolves space names relative to the operator's mounted base
//! directory. Used after bootstrap to load and create repositories.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject -> Space -> { name } -> Mount / Create
//! ```
//!
//! `Mount` resolves the name against the operator's base directory
//! and delegates to `storage::Load` internally.
//!
//! `Create` resolves the name and delegates to `storage::Create`.

use dialog_capability::{Attenuation, Did, Effect, Subject};
use serde::{Deserialize, Serialize};

use super::storage::StorageError;

/// Root attenuation for space operations.
///
/// Attaches to Subject and provides the `/space` ability path segment.
/// The operator's base directory determines where names resolve to.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Space {
    /// Space name, resolved relative to the operator's base directory.
    pub name: String,
}

impl Space {
    /// Create a new space attenuation.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Attenuation for Space {
    type Of = Subject;
}

/// Mount an existing space by name.
///
/// Resolves the name against the operator's base directory, reads the
/// identity, creates providers, and registers the DID in the routing
/// table. Returns the DID.
#[derive(Debug, Clone, Serialize, Deserialize, dialog_capability::Claim)]
pub struct Mount;

impl Effect for Mount {
    type Of = Space;
    type Output = Result<Did, StorageError>;
}

/// Create a new space by name with the given credential.
///
/// Resolves the name against the operator's base directory, writes
/// the credential, creates providers, and registers the DID in the
/// routing table. Returns the DID.
#[derive(Debug, Clone, Serialize, Deserialize, dialog_capability::Claim)]
pub struct Create {
    /// The credential to store at the new space.
    pub credential: dialog_credentials::Credential,
}

impl Create {
    /// Create a new space creation effect.
    pub fn new(credential: dialog_credentials::Credential) -> Self {
        Self { credential }
    }
}

impl Effect for Create {
    type Of = Space;
    type Output = Result<Did, StorageError>;
}
