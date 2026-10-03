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

use std::fmt;

use dialog_capability::{Attenuate, Attenuation, Capability, Did, Effect, Subject};
use dialog_credentials::{Credential, Ed25519Signer, Extractable};
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

/// Extension trait to start a space capability chain from a [`Subject`] or
/// [`Did`]. Attaches a [`Space`] attenuation for the named space.
pub trait SpaceSubjectExt {
    /// The resulting space chain type.
    type Space;
    /// Scope to a named space under this subject.
    fn space(self, name: impl Into<String>) -> Self::Space;
}

impl SpaceSubjectExt for Subject {
    type Space = Capability<Space>;
    fn space(self, name: impl Into<String>) -> Capability<Space> {
        self.attenuate(Space::new(name))
    }
}

impl SpaceSubjectExt for Did {
    type Space = Capability<Space>;
    fn space(self, name: impl Into<String>) -> Capability<Space> {
        Subject::from(self).attenuate(Space::new(name))
    }
}

/// Extension trait adding `.load()` and `.create()` sugar on Space capabilities.
pub trait SpaceExt {
    /// Load an existing space by name.
    fn load(self) -> Capability<Load>;

    /// Create a new space under a key the environment generates.
    fn create(self) -> Capability<Create>;

    /// Create a new space under `key`, which the environment seals and
    /// does not keep. The key must be extractable: sealing it takes its
    /// material.
    fn create_with(self, key: Ed25519Signer<Extractable>) -> Capability<Create>;
}

impl SpaceExt for Capability<Space> {
    fn load(self) -> Capability<Load> {
        self.invoke(Load)
    }

    fn create(self) -> Capability<Create> {
        self.invoke(Create::generated())
    }

    fn create_with(self, key: Ed25519Signer<Extractable>) -> Capability<Create> {
        self.invoke(Create::with(key))
    }
}

/// Load an existing space by name.
///
/// The operator resolves the name against its base directory,
/// loads the credential, mounts the space, and returns the credential.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Load;

impl Attenuation for Load {
    type Of = Space;
}

impl Effect for Load {
    type Output = Result<Credential, StorageError>;
}

/// Create a new space by name.
///
/// The environment resolves the name against its base directory and
/// mounts the space. The space's key -- given, or generated -- is sealed
/// to the account the environment acts for and not kept in the space;
/// the space delegates its authority to that account. The creator gets
/// the key back, in memory.
#[derive(Debug, Clone, Default, Serialize, Deserialize, Attenuate)]
pub struct Create {
    /// The key the space is created under, or none for the environment
    /// to generate one. Never serialized.
    #[serde(skip)]
    pub key: Option<SpaceKey>,
}

impl Create {
    /// Create a space under a key the environment generates.
    pub fn generated() -> Self {
        Self { key: None }
    }

    /// Create a space under `key`.
    pub fn with(key: Ed25519Signer<Extractable>) -> Self {
        Self {
            key: Some(SpaceKey(key)),
        }
    }
}

/// A space's key, while its space is being created. Extractable, since
/// sealing it takes its material; never serialized or printed.
#[derive(Clone)]
pub struct SpaceKey(pub Ed25519Signer<Extractable>);

impl fmt::Debug for SpaceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SpaceKey")
            .field(&self.0.ed25519_did().to_string())
            .finish()
    }
}

impl Attenuation for Create {
    type Of = Space;
}

impl Effect for Create {
    type Output = Result<Credential, StorageError>;
}
