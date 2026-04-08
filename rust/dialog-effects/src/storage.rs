//! Storage capability hierarchy for bootstrap space operations.
//!
//! System-level capabilities for loading and creating spaces at
//! explicit locations. Used during bootstrap before the operator
//! is built. After bootstrap, use [`space`](super::space) capabilities
//! which resolve names relative to the operator's base directory.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject -> Storage -> Location { directory, name } -> Load / Create
//! ```

use dialog_capability::{Attenuation, Capability, Claim, Effect, Subject, did};
use dialog_credentials::Credential;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Root attenuation for storage operations.
///
/// Attaches to Subject and provides the `/storage` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Storage;

impl Attenuation for Storage {
    type Of = Subject;
}

/// Directory category for platform-specific address resolution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Directory {
    /// User profile storage.
    ///
    /// Resolves to:
    /// - FS: `~/Library/Application Support/dialog/` (macOS),
    ///   `~/.local/share/dialog/` (Linux)
    /// - IDB: database suffix `.profile`
    Profile,

    /// Working directory storage.
    ///
    /// Resolves to:
    /// - FS: `$PWD/`
    /// - IDB: no suffix
    Current,

    /// Temporary storage.
    ///
    /// Resolves to:
    /// - FS: platform temp dir
    /// - IDB: database prefix `temp.`
    Temp,

    /// Custom path.
    At(String),
}

/// A resolved location: directory + name.
///
/// Used as a policy in the storage capability chain. The provider
/// resolves this to a platform-specific address.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Location {
    /// The directory category.
    pub directory: Directory,
    /// The name within the directory.
    pub name: String,
}

impl Location {
    /// Create a location.
    pub fn new(directory: Directory, name: impl Into<String>) -> Self {
        Self {
            directory,
            name: name.into(),
        }
    }

    /// Profile location.
    pub fn profile(name: impl Into<String>) -> Self {
        Self::new(Directory::Profile, name)
    }

    /// Current directory location.
    pub fn current(name: impl Into<String>) -> Self {
        Self::new(Directory::Current, name)
    }

    /// Temp location.
    pub fn temp(name: impl Into<String>) -> Self {
        Self::new(Directory::Temp, name)
    }

    /// Explicit path location.
    pub fn at(path: impl Into<String>) -> Self {
        Self {
            directory: Directory::At(path.into()),
            name: String::new(),
        }
    }
}

impl Attenuation for Location {
    type Of = Storage;
}

/// Extension trait adding `.load()` and `.create()` sugar on Location capabilities.
pub trait LocationExt {
    /// Load an existing space from this location.
    fn load(self) -> Capability<Load>;

    /// Create a new space at this location with the given credential.
    fn create(self, credential: Credential) -> Capability<Create>;
}

impl LocationExt for Capability<Location> {
    fn load(self) -> Capability<Load> {
        self.invoke(Load)
    }

    fn create(self, credential: Credential) -> Capability<Create> {
        self.invoke(Create::new(credential))
    }
}

/// Load an existing space from a location.
///
/// Reads the credential from the resolved location, mounts the space,
/// and returns the credential.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Load;

impl Effect for Load {
    type Of = Location;
    type Output = Result<Credential, StorageError>;
}

/// Create a new space at a location with the given credential.
///
/// Writes the credential to the resolved location, mounts the space,
/// and returns the credential.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Create {
    /// The credential establishing the space's identity.
    pub credential: Credential,
}

impl Create {
    /// Create a new space creation effect.
    pub fn new(credential: Credential) -> Self {
        Self { credential }
    }
}

impl Effect for Create {
    type Of = Location;
    type Output = Result<Credential, StorageError>;
}

/// Errors during storage operations.
#[derive(Debug, Error)]
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

/// Sugar: build a storage capability chain for a profile.
impl Storage {
    /// Build a capability chain for loading/creating a profile space.
    pub fn profile(name: impl Into<String>) -> Capability<Location> {
        Subject::from(did!("local:storage"))
            .attenuate(Storage)
            .attenuate(Location::profile(name))
    }

    /// Build a capability chain for loading/creating a current-dir space.
    pub fn current(name: impl Into<String>) -> Capability<Location> {
        Subject::from(did!("local:storage"))
            .attenuate(Storage)
            .attenuate(Location::current(name))
    }

    /// Build a capability chain for loading/creating a temp space.
    pub fn temp(name: impl Into<String>) -> Capability<Location> {
        Subject::from(did!("local:storage"))
            .attenuate(Storage)
            .attenuate(Location::temp(name))
    }

    /// Build a capability chain for loading/creating at an explicit path.
    pub fn at(path: impl Into<String>) -> Capability<Location> {
        Subject::from(did!("local:storage"))
            .attenuate(Storage)
            .attenuate(Location::at(path))
    }
}
