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

use dialog_capability::{Attenuate, Attenuation, Capability, Effect, Subject, did};
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
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

    /// The location as a URI: an absolute directory as a `file://` URL,
    /// and a platform directory by its role (`file:profile/{name}`), since
    /// where a role resolves differs by device.
    pub fn uri(&self) -> String {
        let Self { directory, name } = self;
        match directory {
            Directory::At(path) => format!("file://{}/{name}", path.trim_end_matches('/')),
            Directory::Profile => format!("file:profile/{name}"),
            Directory::Current => format!("file:current/{name}"),
            Directory::Temp => format!("file:temp/{name}"),
        }
    }

    /// The location a URI written by [`uri`](Self::uri) names, or `None`
    /// for one it did not write. An absolute directory's name is its last
    /// path segment.
    pub fn from_uri(uri: &str) -> Option<Self> {
        if let Some(path) = uri.strip_prefix("file://") {
            let (directory, name) = path.rsplit_once('/')?;
            let directory = if directory.is_empty() { "/" } else { directory };
            return Some(Self::new(Directory::At(directory.to_string()), name));
        }
        let (role, name) = uri.strip_prefix("file:")?.split_once('/')?;
        let directory = match role {
            "profile" => Directory::Profile,
            "current" => Directory::Current,
            "temp" => Directory::Temp,
            _ => return None,
        };
        Some(Self::new(directory, name))
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
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Load;

impl Attenuation for Load {
    type Of = Location;
}

impl Effect for Load {
    type Output = Result<Credential, StorageError>;
}

/// Create a new space at a location with the given credential.
///
/// Writes the credential to the resolved location, mounts the space,
/// and returns the credential.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
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

impl Attenuation for Create {
    type Of = Location;
}

impl Effect for Create {
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

#[cfg(test)]
mod tests {
    use super::{Directory, Location};

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Every kind of location reads back from the URI it is written as.
    #[dialog_common::test]
    fn it_reads_a_location_back_from_its_uri() {
        for location in [
            Location::new(Directory::At("/var/dialog".into()), "notes"),
            Location::new(Directory::At("/".into()), "notes"),
            Location::profile("notes"),
            Location::current("notes"),
            Location::temp("notes"),
        ] {
            assert_eq!(Location::from_uri(&location.uri()), Some(location));
        }
        assert_eq!(
            Location::new(Directory::At("/var/dialog/".into()), "notes").uri(),
            "file:///var/dialog/notes"
        );
        assert_eq!(Location::profile("notes").uri(), "file:profile/notes");
        assert_eq!(Location::from_uri("https://example.com/notes"), None);
    }
}
