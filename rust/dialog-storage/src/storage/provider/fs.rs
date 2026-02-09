//! Filesystem-based storage provider for native environments.
//!
//! This provider implements the capability-based storage API using the local
//! filesystem as the underlying storage mechanism. Each subject DID maps to a
//! separate directory, with subdirectories for different storage types.
//!
//! # Directory Structure
//!
//! For each subject DID, a directory is created using the DID as a path segment
//! (URL-encoded, e.g., `did%3Akey%3Az6MkExample`). Within each subject directory:
//!
//! - `archive/{catalog}/{base58(digest)}` - Content-addressed blob storage
//! - `memory/{space}/{cell}` - Transactional memory storage
//!
//! # Transactional Memory
//!
//! Memory operations use PID-based file locking for cross-process coordination
//! and BLAKE3 content hashing for CAS (Compare-And-Swap) semantics. This provides
//! reliable optimistic concurrency for file-based storage with automatic stale
//! lock recovery.
//!
//! # Example
//!
//! ```no_run
//! use dialog_storage::provider::FileSystem;
//! use dialog_capability::{did, Did, Subject};
//! use dialog_effects::archive::{Archive, Catalog, Get};
//! use dialog_common::Blake3Hash;
//! use std::path::PathBuf;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let mut provider = FileSystem::mount("file:///tmp/storage")?;
//! let digest = Blake3Hash::hash(b"hello");
//!
//! let effect = Subject::from(did!("key:z6Mk..."))
//!     .attenuate(Archive)
//!     .attenuate(Catalog::new("index"))
//!     .invoke(Get::new(digest));
//!
//! let result = effect.perform(&mut provider).await?;
//! # Ok(())
//! # }
//! ```

mod archive;
mod error;
mod memory;

pub use error::FileSystemError;

use dialog_capability::Did;
use std::path::PathBuf;
use url::Url;

const ARCHIVE: &str = "archive";
const MEMORY: &str = "memory";

/// Filesystem-based storage provider.
///
/// A transparent wrapper over a [`Location`] that manages storage directories
/// keyed by subject DID. Each subject gets its own directory with subdirectories
/// for archive and memory operations.
///
/// Uses URL semantics for path joining, which provides automatic containment
/// validation - attempts to escape the root via `..` or absolute paths will fail.
///
/// Directories are created lazily on first access.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct FileSystem(Location);

impl TryFrom<Url> for FileSystem {
    type Error = FileSystemError;

    fn try_from(url: Url) -> Result<Self, Self::Error> {
        Ok(Self(url.try_into()?))
    }
}

impl TryFrom<String> for FileSystem {
    type Error = FileSystemError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Ok(Self(s.try_into()?))
    }
}

impl TryFrom<&str> for FileSystem {
    type Error = FileSystemError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Ok(Self(s.try_into()?))
    }
}

impl TryFrom<PathBuf> for FileSystem {
    type Error = FileSystemError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        Ok(Self(path.try_into()?))
    }
}

impl FileSystem {
    /// Mounts a filesystem provider at the given root.
    ///
    /// Accepts a `PathBuf`, `file:` URL string, or `Url`.
    pub fn mount(
        root: impl TryInto<Location, Error = FileSystemError>,
    ) -> Result<Self, FileSystemError> {
        Ok(Self(root.try_into()?))
    }

    /// Returns the location for a subject's archive storage.
    fn archive(&self, subject: &Did) -> Result<Location, FileSystemError> {
        self.0.resolve(subject.as_ref())?.resolve(ARCHIVE)
    }

    /// Returns the location for a subject's memory storage.
    fn memory(&self, subject: &Did) -> Result<Location, FileSystemError> {
        self.0.resolve(subject.as_ref())?.resolve(MEMORY)
    }
}

/// A location in the filesystem, represented as a `file:` URL.
///
/// Provides methods for resolving child paths with containment validation,
/// and converting to native filesystem paths.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Location(Url);

impl TryFrom<Url> for Location {
    type Error = FileSystemError;

    fn try_from(mut url: Url) -> Result<Self, Self::Error> {
        if url.scheme() != "file" {
            return Err(FileSystemError::Io(format!(
                "Expected file: URL, got {}:",
                url.scheme()
            )));
        }

        // Ensure trailing slash for directory semantics
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }

        Ok(Self(url))
    }
}

impl TryFrom<String> for Location {
    type Error = FileSystemError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let url = Url::parse(&s).map_err(|e| FileSystemError::Io(format!("Invalid URL: {e}")))?;
        url.try_into()
    }
}

impl TryFrom<&str> for Location {
    type Error = FileSystemError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(s).map_err(|e| FileSystemError::Io(format!("Invalid URL: {e}")))?;
        url.try_into()
    }
}

impl TryFrom<PathBuf> for Location {
    type Error = FileSystemError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        // Ensure the path is absolute for proper URL conversion
        let absolute = if path.is_absolute() {
            path
        } else {
            std::env::current_dir()
                .map_err(|e| FileSystemError::Io(e.to_string()))?
                .join(path)
        };

        // Convert to file: URL, ensuring trailing slash for directory
        let mut url = Url::from_file_path(&absolute)
            .map_err(|_| FileSystemError::Io("Invalid path for URL conversion".to_string()))?;

        // Ensure trailing slash so joins work correctly
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }

        Ok(Self(url))
    }
}

impl TryFrom<Location> for PathBuf {
    type Error = FileSystemError;

    fn try_from(location: Location) -> Result<Self, Self::Error> {
        location
            .0
            .to_file_path()
            .map_err(|_| FileSystemError::Io("Failed to convert URL to path".to_string()))
    }
}

impl Location {
    /// Returns the URL path component of this location.
    pub fn path(&self) -> &str {
        self.0.path()
    }

    /// Resolves a path segment relative to this location, validating containment.
    ///
    /// Returns an error if the resulting path escapes this location (e.g., via `..`).
    /// The segment is prefixed with `./` to ensure it's interpreted as a relative
    /// path, preventing segments containing `:` from being parsed as URL schemes.
    pub fn resolve(&self, segment: &str) -> Result<Self, FileSystemError> {
        // Normalize base to ensure it ends with '/' for correct directory semantics.
        // Without trailing slash, joining "baz" to "file:///foo/bar" gives "file:///foo/baz"
        // (a sibling), not "file:///foo/bar/baz" (a child).
        let normalized_base = if self.0.path().ends_with('/') {
            self.0.clone()
        } else {
            let mut url = self.0.clone();
            url.set_path(&format!("{}/", self.0.path()));
            url
        };

        // Prefix with "./" to ensure the segment is treated as a relative path.
        // Without this, "did:key:z6Mk" would be interpreted as a URL with scheme "did".
        let relative_segment = format!("./{}", segment);

        let joined = normalized_base
            .join(&relative_segment)
            .map_err(|e| FileSystemError::Io(format!("Invalid path segment: {e}")))?;

        // Containment check: joined URL path must start with base URL path
        let base_path = normalized_base.path();
        let joined_path = joined.path();

        if !joined_path.starts_with(base_path) {
            return Err(FileSystemError::Containment(format!(
                "Path '{}' escapes base '{}'",
                segment,
                normalized_base.as_str()
            )));
        }

        Ok(Self(joined))
    }

    /// Ensures this location exists as a directory.
    pub async fn ensure_dir(&self) -> Result<(), FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        tokio::fs::create_dir_all(&path)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::did;

    #[dialog_common::test]
    fn it_generates_correct_paths() {
        let provider: FileSystem = "file:///root/".try_into().unwrap();
        let subject = did!("key:z6MkTest");

        // Archive path
        let archive = provider.archive(&subject).unwrap();
        assert_eq!(archive.path(), "/root/did:key:z6MkTest/archive");

        // Archive with catalog
        let catalog = archive.resolve("index").unwrap();
        assert_eq!(catalog.path(), "/root/did:key:z6MkTest/archive/index");

        // Memory path
        let memory = provider.memory(&subject).unwrap();
        assert_eq!(memory.path(), "/root/did:key:z6MkTest/memory");

        // Memory with space
        let space = memory.resolve("local").unwrap();
        assert_eq!(space.path(), "/root/did:key:z6MkTest/memory/local");
    }

    #[dialog_common::test]
    fn it_allows_nested_paths() {
        let provider: FileSystem = "file:///root/".try_into().unwrap();
        let subject = did!("key:z6MkTest");

        // Nested space path should work
        let memory = provider.memory(&subject).unwrap();
        let nested = memory.resolve("foo/bar").unwrap();
        assert_eq!(nested.path(), "/root/did:key:z6MkTest/memory/foo/bar");

        // Nested catalog path should work
        let archive = provider.archive(&subject).unwrap();
        let nested = archive.resolve("deep/nested/catalog").unwrap();
        assert_eq!(
            nested.path(),
            "/root/did:key:z6MkTest/archive/deep/nested/catalog"
        );
    }

    #[dialog_common::test]
    fn it_prevents_containment_escape_via_dotdot() {
        let provider: FileSystem = "file:///root/".try_into().unwrap();
        let subject = did!("key:z6MkTest");

        // Attempt to escape via ..
        let memory = provider.memory(&subject).unwrap();
        let result = memory.resolve("../escape");
        assert!(result.is_err());
        assert!(matches!(result, Err(FileSystemError::Containment(_))));
    }

    #[dialog_common::test]
    fn it_handles_absolute_looking_path() {
        let provider: FileSystem = "file:///root/".try_into().unwrap();
        let subject = did!("key:z6MkTest");

        // With "./" prefix, "/etc/passwd" becomes ".//etc/passwd" which URL normalizes
        let archive = provider.archive(&subject).unwrap();
        let result = archive.resolve("/etc/passwd").unwrap();
        // The path should be under root and archive
        assert!(result.path().starts_with("/root/"));
        assert!(result.path().contains("/archive/"));
    }

    #[dialog_common::test]
    fn it_prevents_prefix_collision() {
        // Ensure "bar" segment doesn't allow access to "barbaz" sibling
        let base: Location = "file:///foo/bar/".try_into().unwrap();

        // This should work - it's under bar/
        let valid = base.resolve("baz").unwrap();
        assert_eq!(valid.path(), "/foo/bar/baz");

        // Base without trailing slash should still work correctly due to normalization
        let base_no_slash: Location = "file:///foo/bar".try_into().unwrap();
        let result = base_no_slash.resolve("baz").unwrap();
        assert_eq!(result.path(), "/foo/bar/baz");
    }

    #[dialog_common::test]
    fn it_prevents_escape_via_encoded_dotdot() {
        let provider: FileSystem = "file:///root/".try_into().unwrap();
        let subject = did!("key:z6MkTest");

        // URL decodes %2e%2e to .. during join, so this should be caught
        let memory = provider.memory(&subject).unwrap();
        let result = memory.resolve("%2e%2e/escape");
        assert!(result.is_err());
    }

    #[dialog_common::test]
    fn it_prevents_deep_escape() {
        let provider: FileSystem = "file:///root/".try_into().unwrap();
        let subject = did!("key:z6MkTest");

        // Try to escape multiple levels
        let memory = provider.memory(&subject).unwrap();
        let result = memory.resolve("foo/../../../../../../etc/passwd");
        assert!(result.is_err());
        assert!(matches!(result, Err(FileSystemError::Containment(_))));
    }
}
