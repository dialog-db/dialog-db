//! Space capability hierarchy for operator-level space operations.
//!
//! Resolves space names relative to the operator's mounted base
//! directory. Used after bootstrap to load and create repositories.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (profile DID) -> Space { name } -> Load / Create
//! ```
//!
//! `Load` resolves the name against the operator's base directory
//! and delegates to `storage::Load` internally.
//!
//! `Create` resolves the name and delegates to `storage::Create`.

use dialog_capability::{Attenuate, Attenuation, Capability, Effect, Subject};
use dialog_credentials::Credential;
use serde::{Deserialize, Serialize};

use super::storage::StorageError;

/// Attenuation for space operations scoped by name.
///
/// Attaches to Subject (profile DID) and carries the space name.
/// The operator resolves this name against its base directory.
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

/// Extension trait adding `.load()` and `.create()` sugar on Space capabilities.
pub trait SpaceExt {
    /// Load an existing space by name.
    fn load(self) -> Capability<Load>;

    /// Create a new space with the given credential.
    fn create(self, credential: Credential) -> Capability<Create>;
}

impl SpaceExt for Capability<Space> {
    fn load(self) -> Capability<Load> {
        self.invoke(Load)
    }

    fn create(self, credential: Credential) -> Capability<Create> {
        self.invoke(Create::new(credential))
    }
}

/// Load an existing space by name.
///
/// The operator resolves the name against its base directory,
/// loads the credential, mounts the space, and returns the credential.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Load;

impl Effect for Load {
    type Of = Space;
    type Output = Result<Credential, StorageError>;
}

/// Create a new space by name with the given credential.
///
/// The operator resolves the name against its base directory,
/// stores the credential, mounts the space, and returns the credential.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Create {
    /// The credential to store at the new space.
    pub credential: Credential,
}

impl Create {
    /// Create a new space creation effect.
    pub fn new(credential: Credential) -> Self {
        Self { credential }
    }
}

impl Effect for Create {
    type Of = Space;
    type Output = Result<Credential, StorageError>;
}
