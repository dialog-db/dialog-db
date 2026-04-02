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
//! use dialog_storage::provider::{FileSystem, FileStore, fs};
//! use dialog_capability::{did, Did, Subject};
//! use dialog_effects::archive::{Archive, Catalog, Get};
//! use dialog_common::Blake3Hash;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let provider = FileSystem::mount(&fs::Address::temp())?;
//! let digest = Blake3Hash::hash(b"hello");
//!
//! let effect = Subject::from(did!("key:z6Mk..."))
//!     .attenuate(Archive)
//!     .attenuate(Catalog::new("index"))
//!     .invoke(Get::new(digest));
//!
//! let result = effect.perform(&provider).await?;
//! # Ok(())
//! # }
//! ```

mod archive;
mod error;
mod memory;
mod mount;
mod storage;

pub use error::FileSystemError;

use dialog_capability::Did;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use url::Url;

const ARCHIVE: &str = "archive";
const MEMORY: &str = "memory";
const STORAGE: &str = "storage";

/// Address for filesystem-based storage.
///
/// Wraps a URL with scheme-based resolution:
/// - `profile://` → platform data directory
/// - `temp://` → system temp directory
/// - `storage://` → current working directory
///
/// Use `resolve()` to narrow the address to a sub-path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct Address(url::Url);

impl Address {
    /// Profile storage root.
    pub fn profile() -> Self {
        Self(url::Url::parse("profile:///").expect("valid URL"))
    }

    /// Temporary storage root.
    pub fn temp() -> Self {
        Self(url::Url::parse("temp:///").expect("valid URL"))
    }

    /// Current/working directory storage root.
    pub fn current() -> Self {
        Self(url::Url::parse("storage:///").expect("valid URL"))
    }

    /// The URL scheme (e.g. `"profile"`, `"temp"`, `"storage"`).
    pub fn scheme(&self) -> &str {
        self.0.scheme()
    }

    /// The path portion of the URL.
    pub fn path(&self) -> &str {
        self.0.path()
    }

    /// Resolve a sub-path under this address.
    ///
    /// Uses URL resolution to ensure the result is always nested
    /// under this address.
    pub fn resolve(&self, segment: &str) -> Result<Self, FileSystemError> {
        let mut base = self.0.clone();
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }

        let resolved = base
            .join(&format!("./{segment}"))
            .map_err(|e| FileSystemError::Io(format!("URL join failed: {e}")))?;

        if !resolved.path().starts_with(base.path()) {
            return Err(FileSystemError::Io(format!(
                "path '{segment}' escapes base '{}'",
                base.path()
            )));
        }

        Ok(Self(resolved))
    }
}

/// Stateless filesystem provider.
///
/// Provides `Mount`, `Load`, and `Save` capabilities by resolving
/// paths from the capability chain against the native filesystem.
#[derive(Debug, Clone, Copy, Default)]
pub struct FileSystem;

impl FileSystem {
    /// Mount a FileStore from an address.
    pub fn mount(address: &Address) -> Result<FileStore, FileSystemError> {
        let path = Self::resolve(address)?;
        FileStore::mount(path)
    }

    /// Resolve an address to a concrete filesystem path.
    fn resolve(address: &Address) -> Result<PathBuf, FileSystemError> {
        let base_path = match address.scheme() {
            "profile" => {
                let data_dir = dirs::data_dir().ok_or_else(|| {
                    FileSystemError::Io("could not determine platform data directory".into())
                })?;
                data_dir.join("dialog")
            }
            "temp" => std::env::temp_dir(),
            "storage" => std::env::current_dir().map_err(|e| FileSystemError::Io(e.to_string()))?,
            scheme => {
                return Err(FileSystemError::Io(format!(
                    "unsupported location scheme: {scheme}"
                )));
            }
        };

        let relative = address.path().trim_start_matches('/');
        if relative.is_empty() {
            Ok(base_path)
        } else {
            Ok(base_path.join(relative))
        }
    }
}

/// Mounted filesystem store at a specific root location.
///
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
pub struct FileStore(Location);

impl From<Location> for FileStore {
    fn from(location: Location) -> Self {
        Self(location)
    }
}

impl TryFrom<Url> for FileStore {
    type Error = FileSystemError;

    fn try_from(url: Url) -> Result<Self, Self::Error> {
        Ok(Self(url.try_into()?))
    }
}

impl TryFrom<String> for FileStore {
    type Error = FileSystemError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Ok(Self(s.try_into()?))
    }
}

impl TryFrom<&str> for FileStore {
    type Error = FileSystemError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Ok(Self(s.try_into()?))
    }
}

impl TryFrom<PathBuf> for FileStore {
    type Error = FileSystemError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        Ok(Self(path.try_into()?))
    }
}

impl FileStore {
    /// Mounts a filesystem provider at the given root.
    ///
    /// Accepts a `PathBuf`, `file:` URL string, or `Url`.
    pub fn mount(
        root: impl TryInto<Location, Error = FileSystemError>,
    ) -> Result<Self, FileSystemError> {
        Ok(Self(root.try_into()?))
    }

    /// Returns the location for profile key storage (old API, kept for compat).
    pub fn profile_location() -> Result<Location, FileSystemError> {
        let data_dir = dirs::data_dir().ok_or_else(|| {
            FileSystemError::Io("could not determine platform data directory".into())
        })?;
        let root: Location = data_dir.join("dialog").try_into()?;
        root.resolve("profile")
    }

    /// Resolves a path segment relative to this filesystem's root.
    pub fn resolve(&self, segment: &str) -> Result<Location, FileSystemError> {
        self.0.resolve(segment)
    }

    /// Returns the location for a subject's archive storage.
    fn archive(&self, subject: &Did) -> Result<Location, FileSystemError> {
        self.0.resolve(subject.as_ref())?.resolve(ARCHIVE)
    }

    /// Returns the location for a subject's memory storage.
    fn memory(&self, subject: &Did) -> Result<Location, FileSystemError> {
        self.0.resolve(subject.as_ref())?.resolve(MEMORY)
    }

    /// Returns the location for a subject's key-value storage.
    fn storage(&self, subject: &Did, store: &str) -> Result<Location, FileSystemError> {
        self.0
            .resolve(subject.as_ref())?
            .resolve(STORAGE)?
            .resolve(store)
    }
}

/// A location in the filesystem, represented as a `file:` URL.
///
/// Provides methods for resolving child paths with containment validation,
/// and converting to native filesystem paths.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(transparent)]
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

    /// Ensures the parent directory of this location exists.
    pub async fn ensure_parent(&self) -> Result<(), FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| FileSystemError::Io(e.to_string()))?;
        }
        Ok(())
    }

    /// Ensures this location exists as a directory.
    pub async fn ensure_dir(&self) -> Result<(), FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        tokio::fs::create_dir_all(&path)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))
    }

    /// Read the contents of this location as bytes.
    pub async fn read(&self) -> Result<Vec<u8>, FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        tokio::fs::read(&path)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))
    }

    /// Write bytes to this location, creating parent directories as needed.
    pub async fn write(&self, content: &[u8]) -> Result<(), FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| FileSystemError::Io(e.to_string()))?;
        }
        tokio::fs::write(&path, content)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))
    }

    /// Check whether this location exists on the filesystem.
    pub async fn exists(&self) -> bool {
        let Ok(path) = PathBuf::try_from(self.clone()) else {
            return false;
        };
        tokio::fs::try_exists(&path).await.unwrap_or(false)
    }

    /// Remove the file at this location.
    pub async fn remove(&self) -> Result<(), FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        tokio::fs::remove_file(&path)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))
    }

    /// Acquire a PID-based file lock at `{self}.lock`.
    ///
    /// Returns an RAII guard that releases the lock when dropped.
    /// Handles stale lock detection and recovery automatically.
    /// Fails immediately if the lock is held by an active process.
    pub fn lock(&self) -> Result<Lock, FileSystemError> {
        let mut path: PathBuf = self.clone().try_into()?;
        let mut name = path.file_name().unwrap_or_default().to_os_string();
        name.push(".lock");
        path.set_file_name(name);
        let lock_path = path;
        let path_str = lock_path
            .to_str()
            .ok_or_else(|| FileSystemError::Lock("Lock path is not valid UTF-8".into()))?;

        let mut pidlock = pidlock::Pidlock::new(path_str);

        loop {
            match pidlock.acquire() {
                Ok(()) => return Ok(Lock(pidlock)),
                Err(pidlock::PidlockError::LockExists) => {
                    match pidlock.get_owner() {
                        Some(pid) => {
                            return Err(FileSystemError::Lock(format!(
                                "Concurrent write in progress (lock held by pid {pid})"
                            )));
                        }
                        None => {
                            // Stale lock cleared, retry
                        }
                    }
                }
                Err(e) => {
                    return Err(FileSystemError::Lock(format!(
                        "Failed to acquire lock: {e:?}"
                    )));
                }
            }
        }
    }

    /// Atomically rename this location to the target location.
    pub async fn rename(&self, target: &Location) -> Result<(), FileSystemError> {
        let from: PathBuf = self.clone().try_into()?;
        let to: PathBuf = target.clone().try_into()?;
        tokio::fs::rename(&from, &to)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))
    }
}

/// RAII guard that holds a PID lock and releases it when dropped.
pub struct Lock(pidlock::Pidlock);

impl Drop for Lock {
    fn drop(&mut self) {
        let _ = self.0.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::did;

    #[dialog_common::test]
    fn it_generates_correct_paths() {
        let provider: FileStore = "file:///root/".try_into().unwrap();
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
        let provider: FileStore = "file:///root/".try_into().unwrap();
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
        let provider: FileStore = "file:///root/".try_into().unwrap();
        let subject = did!("key:z6MkTest");

        // Attempt to escape via ..
        let memory = provider.memory(&subject).unwrap();
        let result = memory.resolve("../escape");
        assert!(result.is_err());
        assert!(matches!(result, Err(FileSystemError::Containment(_))));
    }

    #[dialog_common::test]
    fn it_handles_absolute_looking_path() {
        let provider: FileStore = "file:///root/".try_into().unwrap();
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
        let provider: FileStore = "file:///root/".try_into().unwrap();
        let subject = did!("key:z6MkTest");

        // URL decodes %2e%2e to .. during join, so this should be caught
        let memory = provider.memory(&subject).unwrap();
        let result = memory.resolve("%2e%2e/escape");
        assert!(result.is_err());
    }

    #[dialog_common::test]
    fn it_prevents_deep_escape() {
        let provider: FileStore = "file:///root/".try_into().unwrap();
        let subject = did!("key:z6MkTest");

        // Try to escape multiple levels
        let memory = provider.memory(&subject).unwrap();
        let result = memory.resolve("foo/../../../../../../etc/passwd");
        assert!(result.is_err());
        assert!(matches!(result, Err(FileSystemError::Containment(_))));
    }
}
