//! Storage capability hierarchy for loading and creating spaces.
//!
//! A space is a self-contained storage unit with its own identity.
//! The storage hierarchy provides capabilities for loading existing
//! spaces and creating new ones at different locations.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject -> Storage -> Profile { name }      -> Load / Create
//!                    -> Space { name }        -> Load / Create
//!                    -> Location { uri }      -> Load / Create
//! ```
//!
//! After a space is loaded, its DID routes to archive, memory,
//! credential, and delegation capabilities.

use dialog_capability::{Attenuation, Capability, Constraint, Did, Effect, Subject};
use dialog_common::ConditionalSend;
use dialog_credentials::Credential;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// Root attenuation for storage operations.
///
/// Attaches to Subject and provides the `/storage` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Storage;

impl Attenuation for Storage {
    type Of = Subject;
}

impl Storage {
    /// Build a capability chain for a profile space.
    pub fn profile(name: impl Into<String>) -> Capability<Profile> {
        Subject::from(dialog_capability::did!("local:storage"))
            .attenuate(Storage)
            .attenuate(Profile::new(name))
    }

    /// Build a capability chain for a project space.
    pub fn space(name: impl Into<String>) -> Capability<Space> {
        Subject::from(dialog_capability::did!("local:storage"))
            .attenuate(Storage)
            .attenuate(Space::new(name))
    }

    /// Build a capability chain for a space at an explicit URI.
    pub fn location(uri: impl Into<String>) -> Capability<Location> {
        Subject::from(dialog_capability::did!("local:storage"))
            .attenuate(Storage)
            .attenuate(Location::new(uri))
    }
}

/// Profile space attenuation.
///
/// Resolves to platform-specific profile storage:
/// - FS: `~/Library/Application Support/dialog/{name}/` (macOS),
///   `~/.local/share/dialog/{name}/` (Linux)
/// - IDB: database `{name}.profile`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Profile {
    /// Profile name.
    pub name: String,
}

impl Profile {
    /// Create a new profile attenuation.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Attenuation for Profile {
    type Of = Storage;
}

/// Project space attenuation.
///
/// Resolves to platform-specific workspace storage:
/// - FS: `$PWD/{name}/.dialog/`
/// - IDB: database `{name}`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Space {
    /// Space name.
    pub name: String,
}

impl Space {
    /// Create a new space attenuation.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Attenuation for Space {
    type Of = Storage;
}

/// Explicit location attenuation.
///
/// Resolves to the address specified by the URI.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Location {
    /// URI identifying the storage location.
    pub uri: String,
}

impl Location {
    /// Create a new explicit location.
    pub fn new(uri: impl Into<String>) -> Self {
        Self { uri: uri.into() }
    }
}

impl Attenuation for Location {
    type Of = Storage;
}

/// Load an existing space by reading its identity from the location.
///
/// Generic over the attenuation type so `Load<Profile>`, `Load<Space>`,
/// and `Load<Location>` are distinct effect types with shared behavior.
///
/// The environment reads the credential at the resolved location,
/// registers the DID in its routing table, and returns the DID.
#[derive(Debug, Clone, Serialize, Deserialize, dialog_capability::Claim)]
pub struct Load<T> {
    #[serde(skip)]
    _marker: PhantomData<T>,
}

impl<T> Load<T> {
    /// Create a new load effect.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Default for Load<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Effect for Load<T>
where
    T: Constraint + ConditionalSend + 'static,
{
    type Of = T;
    type Output = Result<Did, StorageError>;
}

/// Create a new space with the given credential at a location.
///
/// Writes the credential to the resolved location, registers the
/// DID in the routing table, and returns the DID.
///
/// Errors if a space already exists at the location.
#[derive(Debug, Clone, Serialize, Deserialize, dialog_capability::Claim)]
pub struct Create<T> {
    /// The credential to store at the new space.
    pub credential: Credential,
    #[serde(skip)]
    _marker: PhantomData<T>,
}

impl<T> Create<T> {
    /// Create a new space creation effect.
    pub fn new(credential: Credential) -> Self {
        Self {
            credential,
            _marker: PhantomData,
        }
    }
}

impl<T> Effect for Create<T>
where
    T: Constraint + ConditionalSend + 'static,
{
    type Of = T;
    type Output = Result<Did, StorageError>;
}

/// Errors during storage operations (load, create).
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// No space found at the resolved location.
    #[error("Space not found: {0}")]
    NotFound(String),

    /// A space already exists at the resolved location.
    #[error("Space already exists: {0}")]
    AlreadyExists(String),

    /// Backend storage error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// The credential at the location is invalid.
    #[error("Invalid credential: {0}")]
    InvalidCredential(String),
}
