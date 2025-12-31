//! Local site types.

use std::hash::Hash;

/// Address for a local site.
///
/// Identifies a local replica/repository by its DID or path.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Address {
    /// A repository identified by DID.
    Did(String),
    /// A repository identified by file path.
    Path(std::path::PathBuf),
}

impl Address {
    /// Create a new DID-based address.
    pub fn did(did: impl Into<String>) -> Self {
        Address::Did(did.into())
    }

    /// Create a new path-based address.
    pub fn path(path: impl Into<std::path::PathBuf>) -> Self {
        Address::Path(path.into())
    }

    /// Get a display name for this address (for logging/errors).
    pub fn name(&self) -> String {
        match self {
            Address::Did(did) => did.clone(),
            Address::Path(path) => path.display().to_string(),
        }
    }
}
