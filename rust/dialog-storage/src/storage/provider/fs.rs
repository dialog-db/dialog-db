//! Filesystem-based storage provider for native environments.
//!
//! Each space is a directory with the following layout:
//!
//! ```text
//! {space_root}/
//!   archive/{catalog}/{base58(digest)}
//!   memory/{space}/{cell}
//!   credential/{address}
//! ```
//!
//! Compare-And-Swap (CAS) semantics are accomplished through PID-based file
//! locking for cross-process coordination and BLAKE3 content hashing for
//! enforcing ensuring invariantns.

mod archive;
mod credential;
mod error;
mod memory;

pub use error::FileSystemError;

use std::path::PathBuf;
use url::Url;

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
pub struct FileSystem(FileSystemHandle);

impl FileSystem {
    /// Resolve a path segment under this space's root.
    pub fn resolve(&self, segment: &str) -> Result<FileSystemHandle, FileSystemError> {
        self.0.resolve(segment)
    }
}

use crate::resource::Resource;
use dialog_effects::storage::{Directory, Location};

#[async_trait::async_trait]
impl Resource<Location> for FileSystem {
    type Error = FileSystemError;

    async fn open(location: &Location) -> Result<Self, FileSystemError> {
        Ok(Self(FileSystemHandle::try_from(location)?))
    }
}

/// Resolve a `Location` (Directory + name) to a filesystem `Location`.
///
/// In test mode, all directories resolve under the platform temp dir
/// to avoid polluting real profile or workspace directories.
#[cfg(not(test))]
impl TryFrom<&Location> for FileSystemHandle {
    type Error = FileSystemError;

    fn try_from(location: &Location) -> Result<Self, FileSystemError> {
        let base = match &location.directory {
            Directory::Profile => {
                let data_dir = dirs::data_dir().ok_or_else(|| {
                    FileSystemError::Io("could not determine platform data directory".into())
                })?;
                data_dir.join("dialog")
            }
            Directory::Current => {
                std::env::current_dir().map_err(|e| FileSystemError::Io(e.to_string()))?
            }
            Directory::Temp => std::env::temp_dir(),
            Directory::At(path) => PathBuf::from(path),
        };

        let path = if location.name.is_empty() {
            base
        } else {
            base.join(&location.name)
        };

        path.try_into()
    }
}

/// Test mode: all directories resolve under temp dir to avoid
/// polluting real profile or workspace directories.
#[cfg(test)]
impl TryFrom<&Location> for FileSystemHandle {
    type Error = FileSystemError;

    fn try_from(location: &Location) -> Result<Self, FileSystemError> {
        let base = std::env::temp_dir().join("dialog");
        let suffix = match &location.directory {
            Directory::Profile => ".profile",
            Directory::Current => ".space",
            _ => "",
        };

        let path = if location.name.is_empty() {
            base
        } else {
            base.join(format!("{}{suffix}", location.name))
        };

        path.try_into()
    }
}

/// A location in the filesystem, represented as a `file:` URL.
///
/// Provides methods for resolving child paths with containment validation,
/// and converting to native filesystem paths.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct FileSystemHandle(Url);

impl TryFrom<Url> for FileSystemHandle {
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

impl TryFrom<String> for FileSystemHandle {
    type Error = FileSystemError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let url = Url::parse(&s).map_err(|e| FileSystemError::Io(format!("Invalid URL: {e}")))?;
        url.try_into()
    }
}

impl TryFrom<&str> for FileSystemHandle {
    type Error = FileSystemError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(s).map_err(|e| FileSystemError::Io(format!("Invalid URL: {e}")))?;
        url.try_into()
    }
}

impl TryFrom<PathBuf> for FileSystemHandle {
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

impl TryFrom<FileSystemHandle> for PathBuf {
    type Error = FileSystemError;

    fn try_from(location: FileSystemHandle) -> Result<Self, Self::Error> {
        let path = location
            .0
            .to_file_path()
            .map_err(|_| FileSystemError::Io("Failed to convert URL to path".to_string()))?;

        // Strip trailing slash added by FileSystemHandle for URL semantics.
        // Filesystem operations (read, write, rename) need clean file paths.
        let s = path.to_string_lossy();
        if s.ends_with('/') && s.len() > 1 {
            Ok(PathBuf::from(s.trim_end_matches('/')))
        } else {
            Ok(path)
        }
    }
}

impl FileSystemHandle {
    /// Returns the underlying URL.
    pub fn url(&self) -> &Url {
        &self.0
    }

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

    /// Read the contents of the file at this location.
    pub async fn read(&self) -> Result<Vec<u8>, FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        tokio::fs::read(&path)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))
    }

    /// Read the contents of the file at this location, returning `None` if
    /// the file does not exist.
    pub async fn read_optional(&self) -> Result<Option<Vec<u8>>, FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        match tokio::fs::read(&path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(FileSystemError::Io(e.to_string())),
        }
    }

    /// Write contents to the file at this location, creating parent dirs.
    pub async fn write(&self, contents: &[u8]) -> Result<(), FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| FileSystemError::Io(e.to_string()))?;
        }
        tokio::fs::write(&path, contents)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))
    }

    /// Atomically rename this location to another.
    pub async fn rename(&self, to: &FileSystemHandle) -> Result<(), FileSystemError> {
        let from_path: PathBuf = self.clone().try_into()?;
        let to_path: PathBuf = to.clone().try_into()?;
        tokio::fs::rename(&from_path, &to_path)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))
    }

    /// Remove the file at this location, returning Ok if already absent.
    pub async fn remove(&self) -> Result<(), FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(FileSystemError::Io(e.to_string())),
        }
    }

    /// List file names in this directory. Returns an empty vec if the
    /// directory does not exist.
    pub async fn list(&self) -> Result<Vec<String>, FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        let mut entries = match tokio::fs::read_dir(&path).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(FileSystemError::Io(e.to_string())),
        };

        let mut names = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))?
        {
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }
        Ok(names)
    }

    /// Check if this location exists.
    pub async fn exists(&self) -> bool {
        let Ok(path) = <PathBuf as TryFrom<FileSystemHandle>>::try_from(self.clone()) else {
            return false;
        };
        path.exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::Resource;
    use dialog_effects::storage::{Directory, Location as StorageLocation};

    /// Create a test FileSystem at a temp location.
    async fn test_space(name: &str) -> FileSystem {
        let location = StorageLocation::new(Directory::Temp, name);
        FileSystem::open(&location).await.unwrap()
    }

    #[dialog_common::test]
    async fn it_generates_correct_layout() {
        let space = test_space("layout-test").await;

        // Archive resolves directly under root
        let archive = space.archive().unwrap();
        assert!(archive.path().ends_with("/archive"));

        let catalog = archive.resolve("index").unwrap();
        assert!(catalog.path().ends_with("/archive/index"));

        // Credential resolves under root
        let cred = space.credential("self").unwrap();
        assert!(cred.path().ends_with("/credential/self"));

        // Memory resolves directly under root
        let memory = space.memory().unwrap();
        assert!(memory.path().ends_with("/memory"));

        let cell = memory.resolve("local").unwrap();
        assert!(cell.path().ends_with("/memory/local"));
    }

    #[dialog_common::test]
    async fn it_allows_nested_paths() {
        let space = test_space("nested-test").await;

        let memory = space.memory().unwrap();
        let nested = memory.resolve("foo/bar").unwrap();
        assert!(nested.path().ends_with("/memory/foo/bar"));

        let archive = space.archive().unwrap();
        let nested = archive.resolve("deep/nested/catalog").unwrap();
        assert!(nested.path().ends_with("/archive/deep/nested/catalog"));
    }

    #[dialog_common::test]
    async fn it_prevents_containment_escape_via_dotdot() {
        let space = test_space("escape-test").await;

        let memory = space.memory().unwrap();
        let result = memory.resolve("../escape");
        assert!(result.is_err());
        assert!(matches!(result, Err(FileSystemError::Containment(_))));
    }

    #[dialog_common::test]
    async fn it_prevents_escape_via_encoded_dotdot() {
        let space = test_space("encoded-escape-test").await;

        let memory = space.memory().unwrap();
        let result = memory.resolve("%2e%2e/escape");
        assert!(result.is_err());
    }

    #[dialog_common::test]
    async fn it_prevents_deep_escape() {
        let space = test_space("deep-escape-test").await;

        let memory = space.memory().unwrap();
        let result = memory.resolve("foo/../../../../../../etc/passwd");
        assert!(result.is_err());
        assert!(matches!(result, Err(FileSystemError::Containment(_))));
    }

    #[dialog_common::test]
    async fn test_mode_resolves_under_temp_dir() {
        let location = StorageLocation::new(Directory::Profile, "test-alice");
        let space = FileSystem::open(&location).await.unwrap();
        let archive = space.archive().unwrap();
        let path = archive.path();
        assert!(
            path.contains("dialog") && path.contains("test-alice") && path.ends_with("/archive"),
            "profile location should resolve under temp/dialog with .profile suffix: {path}"
        );
    }

    fn unique_name(prefix: &str) -> String {
        use dialog_common::time;
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let ts = time::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{ts}-{seq}")
    }

    #[dialog_common::test]
    async fn it_writes_credential_to_expected_path() {
        use dialog_credentials::{Ed25519Signer, SignerCredential};
        use dialog_effects::prelude::*;
        use dialog_varsig::Principal;

        let name = unique_name("fs-layout-credential");
        let location = StorageLocation::new(Directory::Temp, &name);
        let provider = FileSystem::open(&location).await.unwrap();
        let root: PathBuf = provider.0.clone().try_into().unwrap();

        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);
        let cred = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        did.credential("self")
            .save(cred)
            .perform(&provider)
            .await
            .unwrap();

        // Credential should be at {root}/credential/self
        let expected = root.join("credential").join("self");
        assert!(
            expected.exists(),
            "credential file should exist at {expected:?}"
        );
        assert!(
            expected.is_file(),
            "credential should be a file, not a directory"
        );
    }

    #[dialog_common::test]
    async fn it_writes_archive_to_expected_path() {
        use dialog_common::Blake3Hash;
        use dialog_credentials::Ed25519Signer;
        use dialog_effects::prelude::*;
        use dialog_varsig::Principal;

        let name = unique_name("fs-layout-archive");
        let location = StorageLocation::new(Directory::Temp, &name);
        let provider = FileSystem::open(&location).await.unwrap();
        let root: PathBuf = provider.0.clone().try_into().unwrap();

        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);
        let content = b"hello archive layout".to_vec();
        let digest = Blake3Hash::hash(&content);

        did.archive()
            .catalog("index")
            .put(digest.clone(), content)
            .perform(&provider)
            .await
            .unwrap();

        // Archive should be at {root}/archive/index/{base58(digest)}
        let digest_key = base58::ToBase58::to_base58(digest.as_bytes().as_slice());
        let expected = root.join("archive").join("index").join(&digest_key);
        assert!(
            expected.exists(),
            "archive blob should exist at {expected:?}"
        );
    }

    #[dialog_common::test]
    async fn it_writes_memory_to_expected_path() {
        use dialog_credentials::Ed25519Signer;
        use dialog_effects::prelude::*;
        use dialog_varsig::Principal;

        let name = unique_name("fs-layout-memory");
        let location = StorageLocation::new(Directory::Temp, &name);
        let provider = FileSystem::open(&location).await.unwrap();
        let root: PathBuf = provider.0.clone().try_into().unwrap();

        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);

        did.memory()
            .space("local")
            .cell("head")
            .publish(b"cell content", None)
            .perform(&provider)
            .await
            .unwrap();

        // Memory should be at {root}/memory/local/head
        let expected = root.join("memory").join("local").join("head");
        assert!(
            expected.exists(),
            "memory cell should exist at {expected:?}"
        );
    }

    #[dialog_common::test]
    async fn it_loads_credential_from_prefabricated_path() {
        use dialog_credentials::{Ed25519Signer, SignerCredential};
        use dialog_effects::prelude::*;
        use dialog_varsig::Principal;

        let name = unique_name("fs-prefab-credential");
        let location = StorageLocation::new(Directory::Temp, &name);
        let provider = FileSystem::open(&location).await.unwrap();
        let root: PathBuf = provider.0.clone().try_into().unwrap();

        // Generate a credential and export it
        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);
        let cred = dialog_credentials::Credential::Signer(SignerCredential::from(signer));
        let export = cred.export().await.unwrap();

        // Manually write the exported bytes to the expected path
        let cred_dir = root.join("credential");
        std::fs::create_dir_all(&cred_dir).unwrap();
        std::fs::write(cred_dir.join("self"), export.as_bytes()).unwrap();

        // Provider should load it successfully
        let loaded = did
            .credential("self")
            .load()
            .perform(&provider)
            .await
            .unwrap();

        assert_eq!(loaded.did(), cred.did());
    }

    #[dialog_common::test]
    async fn it_rejects_corrupted_credential() {
        use dialog_credentials::Ed25519Signer;
        use dialog_effects::prelude::*;
        use dialog_varsig::Principal;

        let name = unique_name("fs-corrupt-credential");
        let location = StorageLocation::new(Directory::Temp, &name);
        let provider = FileSystem::open(&location).await.unwrap();
        let root: PathBuf = provider.0.clone().try_into().unwrap();

        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);

        // Write garbage to the credential path
        let cred_dir = root.join("credential");
        std::fs::create_dir_all(&cred_dir).unwrap();
        std::fs::write(cred_dir.join("self"), b"not a valid credential").unwrap();

        let result = did.credential("self").load().perform(&provider).await;

        assert!(result.is_err(), "should reject corrupted credential data");
    }

    #[dialog_common::test]
    async fn it_loads_archive_from_prefabricated_path() {
        use dialog_common::Blake3Hash;
        use dialog_credentials::Ed25519Signer;
        use dialog_effects::prelude::*;
        use dialog_varsig::Principal;

        let name = unique_name("fs-prefab-archive");
        let location = StorageLocation::new(Directory::Temp, &name);
        let provider = FileSystem::open(&location).await.unwrap();
        let root: PathBuf = provider.0.clone().try_into().unwrap();

        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);
        let content = b"prefabricated blob".to_vec();
        let digest = Blake3Hash::hash(&content);

        // Manually write content to the expected archive path
        let digest_key = base58::ToBase58::to_base58(digest.as_bytes().as_slice());
        let blob_dir = root.join("archive").join("index");
        std::fs::create_dir_all(&blob_dir).unwrap();
        std::fs::write(blob_dir.join(&digest_key), &content).unwrap();

        // Provider should find it
        let loaded = did
            .archive()
            .catalog("index")
            .get(digest)
            .perform(&provider)
            .await
            .unwrap();

        assert_eq!(loaded, Some(content));
    }

    #[dialog_common::test]
    async fn it_loads_memory_from_prefabricated_path() {
        use dialog_common::Blake3Hash;
        use dialog_credentials::Ed25519Signer;
        use dialog_effects::prelude::*;
        use dialog_varsig::Principal;

        let name = unique_name("fs-prefab-memory");
        let location = StorageLocation::new(Directory::Temp, &name);
        let provider = FileSystem::open(&location).await.unwrap();
        let root: PathBuf = provider.0.clone().try_into().unwrap();

        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);
        let content = b"prefabricated cell value".to_vec();

        // Manually write content to the expected memory path
        let cell_dir = root.join("memory").join("local");
        std::fs::create_dir_all(&cell_dir).unwrap();
        std::fs::write(cell_dir.join("head"), &content).unwrap();

        // Provider should resolve it with correct edition
        let resolved = did
            .memory()
            .space("local")
            .cell("head")
            .resolve()
            .perform(&provider)
            .await
            .unwrap();

        let publication = resolved.expect("should find prefabricated cell");
        assert_eq!(publication.content, content);

        let expected_edition = Blake3Hash::hash(&content).as_bytes().to_vec();
        assert_eq!(publication.edition, expected_edition);
    }
}
