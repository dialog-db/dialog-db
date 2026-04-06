//! Logical storage location that resolves to platform-specific addresses.
//!
//! A [`Location`] describes where a space lives without specifying how
//! its stored. Each backend extracts its address via `From<&Location>`.

use serde::{Deserialize, Serialize};

/// A logical storage location.
///
/// Describes where a space lives. Platform-specific backends resolve
/// their concrete addresses via `From<&Location>` conversions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Location {
    /// User profile storage.
    ///
    /// Resolves to:
    /// - Native FS: `~/Library/Application Support/dialog/{name}` (macOS),
    ///   `~/.local/share/dialog/{name}` (Linux)
    /// - Web IDB: database `{name}.profile`
    /// - Web OPFS: `/dialog/profile/{name}`
    Profile(String),

    /// Working directory storage.
    ///
    /// Resolves to:
    /// - Native FS: `$PWD/.dialog/{name}`
    /// - Web IDB: database `{name}`
    /// - Web OPFS: `/dialog/workspace/{name}`
    Workspace(String),

    /// Temporary/ephemeral storage.
    ///
    /// Resolves to:
    /// - Native FS: `/tmp/.dialog/{name}` or platform temp dir
    /// - Web IDB: database `temp.{name}`
    /// - Web OPFS: `/dialog/temp/{name}`
    Temp(String),
}

impl Location {
    /// Create a profile location.
    pub fn profile(name: impl Into<String>) -> Self {
        Self::Profile(name.into())
    }

    /// Create a workspace location.
    pub fn workspace(name: impl Into<String>) -> Self {
        Self::Workspace(name.into())
    }

    /// Create a temporary location.
    pub fn temp(name: impl Into<String>) -> Self {
        Self::Temp(name.into())
    }

    /// The name portion of this location.
    pub fn name(&self) -> &str {
        match self {
            Self::Profile(n) | Self::Workspace(n) | Self::Temp(n) => n,
        }
    }
}

// From conversions for each backend address type

#[cfg(not(target_arch = "wasm32"))]
impl From<&Location> for super::fs::Address {
    fn from(loc: &Location) -> Self {
        match loc {
            Location::Profile(name) => super::fs::Address::profile()
                .resolve(name)
                .expect("valid profile name"),
            Location::Workspace(name) => super::fs::Address::current()
                .resolve(name)
                .expect("valid workspace name"),
            Location::Temp(name) => super::fs::Address::temp()
                .resolve(name)
                .expect("valid temp name"),
        }
    }
}

impl From<&Location> for super::volatile::Address {
    fn from(loc: &Location) -> Self {
        match loc {
            Location::Profile(name) => super::volatile::Address::new(format!("profile/{name}")),
            Location::Workspace(name) => super::volatile::Address::new(format!("workspace/{name}")),
            Location::Temp(name) => super::volatile::Address::new(format!("temp/{name}")),
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl From<&Location> for super::indexeddb::Address {
    fn from(loc: &Location) -> Self {
        match loc {
            Location::Profile(name) => super::indexeddb::Address::new(format!("{name}.profile")),
            Location::Workspace(name) => super::indexeddb::Address::new(name.clone()),
            Location::Temp(name) => super::indexeddb::Address::new(format!("temp.{name}")),
        }
    }
}
