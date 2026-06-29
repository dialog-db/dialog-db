//! Filesystem-based storage provider.
//!
//! This provider is isomorphic across targets: on native platforms it is
//! backed by [`tokio::fs`], and in the browser by the
//! [File System Access API][fsapi] (`web_sys::FileSystemDirectoryHandle`).
//! The capability implementations (archive, memory, credential, certificate)
//! are written once against [`FileSystemHandle`] and shared by both.
//!
//! Each space is a directory with the following layout, byte-identical on
//! both targets so a vault written on native can be read in the browser and
//! vice versa:
//!
//! ```text
//! {space_root}/
//!   archive/{catalog}/{base58(digest)}
//!   memory/{space}/{cell}
//!   credential/key/{address}
//!   certificate/{audience}/{subject}/{issuer}.{hash}
//! ```
//!
//! Compare-And-Swap (CAS) semantics are accomplished through cross-writer
//! locking (PID-based file locks on native, the [Web Locks API][weblocks] in
//! the browser) and BLAKE3 content hashing for enforcing edition invariants.
//!
//! [fsapi]: https://developer.mozilla.org/en-US/docs/Web/API/File_System_API
//! [weblocks]: https://developer.mozilla.org/en-US/docs/Web/API/Web_Locks_API

mod archive;
mod error;
mod memory;

// Full credential and certificate storage are native-only. On the web a signer
// credential is a non-extractable WebCrypto key handle with no byte
// serialization, so saving one byte-compatibly is impossible there; browsers
// persist credentials through the IndexedDb provider instead. The archive and
// memory providers above are isomorphic and are what the FS-remote sync use
// case actually needs.
#[cfg(not(target_arch = "wasm32"))]
mod certificate;
#[cfg(not(target_arch = "wasm32"))]
mod credential;

// On the web we still need to READ a directory's identity (its
// `credential/key/self` DID) to authorize against it — only the public key is
// needed, so this works without a WebCrypto import. A DID-only `Load<Credential>`.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod credential_web;

#[cfg(not(target_arch = "wasm32"))]
mod native;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod web;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use web::FileSystemDirectoryHandleExt;

pub use error::FileSystemError;

pub use backend::FileWriter;

use url::Url;

/// A streamed byte source over a file, yielding owned chunks. Boxed so both
/// the native and web backends return one type from
/// [`FileSystemHandle::reader`].
#[cfg(not(target_arch = "wasm32"))]
pub type FileReader =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Vec<u8>, FileSystemError>> + Send>>;
/// A streamed byte source over a file, yielding owned chunks.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub type FileReader =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Vec<u8>, FileSystemError>>>>;

/// Filesystem-based storage provider.
///
/// A transparent wrapper over a [`FileSystemHandle`] that manages storage
/// directories keyed by subject DID. Each subject gets its own directory with
/// subdirectories for archive and memory operations.
///
/// Uses URL semantics for path joining, which provides automatic containment
/// validation - attempts to escape the root via `..` or absolute paths will fail.
///
/// Directories are created lazily on first access.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct FileSystem(FileSystemHandle);

impl FileSystem {
    /// The handle for this provider's root location.
    pub fn handle(&self) -> &FileSystemHandle {
        &self.0
    }

    /// Resolve a path segment under this space's root.
    pub fn resolve(&self, segment: &str) -> Result<FileSystemHandle, FileSystemError> {
        self.handle().resolve(segment)
    }
}

impl From<FileSystemHandle> for FileSystem {
    fn from(handle: FileSystemHandle) -> Self {
        Self(handle)
    }
}

/// A location in the filesystem, represented as a `file:` URL.
///
/// The URL is the single source of truth for path layout and containment on
/// every target; child paths resolved through [`resolve`](Self::resolve) are
/// validated against the root so segments can never escape it. On the web a
/// `MountedDirectory` is carried alongside so the same URL path can be walked
/// through the File System Access API.
#[derive(Clone, Debug)]
pub struct FileSystemHandle {
    /// The `file:` URL describing this handle's position in the layout.
    url: Url,
    /// The mounted directory this handle navigates within. The URL path
    /// relative to that directory's base URL gives the segment list to walk
    /// through the FS Access API.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    root: web::MountedDirectory,
}

/// Constructing a handle from a bare `file:` URL is native-only; on the web a
/// handle must be created through `MountedDirectory` so it carries the JS directory
/// handle it navigates from.
#[cfg(not(target_arch = "wasm32"))]
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

        Ok(Self { url })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl TryFrom<String> for FileSystemHandle {
    type Error = FileSystemError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let url = Url::parse(&s).map_err(|e| FileSystemError::Io(format!("Invalid URL: {e}")))?;
        url.try_into()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl TryFrom<&str> for FileSystemHandle {
    type Error = FileSystemError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(s).map_err(|e| FileSystemError::Io(format!("Invalid URL: {e}")))?;
        url.try_into()
    }
}

/// The active I/O backend for the current target. `native` is backed by
/// [`tokio::fs`]; `web` by the File System Access API. Both expose the same
/// free functions taking a [`FileSystemHandle`], plus a `LockGuard` type and
/// `lock` function for CAS critical sections.
#[cfg(not(target_arch = "wasm32"))]
use native as backend;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use web as backend;

pub(crate) use backend::{LockGuard, lock};

impl FileSystemHandle {
    /// Returns the underlying URL.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Returns the URL path component of this location.
    pub fn path(&self) -> &str {
        self.url().path()
    }

    /// Build a child handle that shares this handle's web root (if any) but
    /// points at the given URL. Keeps the root attached across `resolve`.
    pub(super) fn with_url(&self, url: Url) -> Self {
        Self {
            url,
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            root: self.root.clone(),
        }
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
        let normalized_base = if self.path().ends_with('/') {
            self.url().clone()
        } else {
            let mut url = self.url().clone();
            url.set_path(&format!("{}/", self.path()));
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

        Ok(self.with_url(joined))
    }

    /// Ensures this location exists as a directory.
    pub async fn ensure_dir(&self) -> Result<(), FileSystemError> {
        backend::ensure_dir(self).await
    }

    /// Read the contents of the file at this location.
    pub async fn read(&self) -> Result<Vec<u8>, FileSystemError> {
        backend::read(self).await
    }

    /// Read the contents of the file at this location, returning `None` if
    /// the file does not exist.
    pub async fn read_optional(&self) -> Result<Option<Vec<u8>>, FileSystemError> {
        backend::read_optional(self).await
    }

    /// Write contents to the file at this location, creating parent dirs.
    pub async fn write(&self, contents: &[u8]) -> Result<(), FileSystemError> {
        backend::write(self, contents).await
    }

    /// Write contents to this location atomically: a concurrent reader sees
    /// either the old file or the complete new one, never a partial write.
    ///
    /// Each backend does the cheapest thing that gives that guarantee — on the
    /// web `createWritable().close()` already swaps the file atomically, so this
    /// is a direct write; on native it stages a temp file and `rename`s it into
    /// place (one atomic syscall). Callers that need atomicity should prefer
    /// this over a manual temp+rename, which on the web degrades to a full
    /// read+rewrite+delete.
    pub async fn write_atomic(&self, contents: &[u8]) -> Result<(), FileSystemError> {
        backend::write_atomic(self, contents).await
    }

    /// Open a streaming reader over the whole file.
    pub async fn reader(&self) -> Result<FileReader, FileSystemError> {
        backend::open_reader(self, 0, None).await
    }

    /// Open a streaming reader over a byte range: `len` bytes from `offset`
    /// (to end-of-file when `len` is `None`).
    pub async fn reader_range(
        &self,
        offset: u64,
        len: Option<u64>,
    ) -> Result<FileReader, FileSystemError> {
        backend::open_reader(self, offset, len).await
    }

    /// Open a streaming writer that commits atomically on
    /// [`finish`](FileWriter::finish), so large content is never buffered whole.
    pub async fn writer(&self) -> Result<FileWriter, FileSystemError> {
        backend::open_writer(self).await
    }

    /// Atomically rename this location to another.
    pub async fn rename(&self, to: &FileSystemHandle) -> Result<(), FileSystemError> {
        backend::rename(self, to).await
    }

    /// Remove the file at this location, returning Ok if already absent.
    pub async fn remove(&self) -> Result<(), FileSystemError> {
        backend::remove(self).await
    }

    /// List file names in this directory. Returns an empty vec if the
    /// directory does not exist.
    pub async fn list(&self) -> Result<Vec<String>, FileSystemError> {
        backend::list(self).await
    }

    /// Check if this location exists.
    pub async fn exists(&self) -> bool {
        backend::exists(self).await
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::native::STORAGE_NAMESPACE;
    use super::*;
    use crate::helpers::unique_name;
    use crate::resource::Resource;
    use dialog_effects::storage::{Directory, Location as StorageLocation};
    use std::env;
    use std::path::PathBuf;

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
        let cred = space.credential_key("self").unwrap();
        assert!(cred.path().ends_with("/credential/key/self"));

        // Memory resolves directly under root
        let memory = space.memory().unwrap();
        assert!(memory.path().ends_with("/memory"));

        let cell = memory.resolve("local").unwrap();
        assert!(cell.path().ends_with("/memory/local"));
    }

    #[dialog_common::test]
    async fn it_streams_write_then_read() {
        use futures_util::StreamExt;

        let space = test_space("stream-roundtrip").await;
        let handle = space.resolve("blob/sample").unwrap();

        // Streamed, atomically-committed write across multiple chunks.
        let mut writer = handle.writer().await.unwrap();
        writer.write_all(b"hello ").await.unwrap();
        writer.write_all(b"streaming world").await.unwrap();
        writer.finish().await.unwrap();

        // Whole-file streamed read.
        let mut reader = handle.reader().await.unwrap();
        let mut whole = Vec::new();
        while let Some(chunk) = reader.next().await {
            whole.extend(chunk.unwrap());
        }
        assert_eq!(whole, b"hello streaming world");

        // Ranged read: 9 bytes from offset 6 -> "streaming".
        let mut reader = handle.reader_range(6, Some(9)).await.unwrap();
        let mut ranged = Vec::new();
        while let Some(chunk) = reader.next().await {
            ranged.extend(chunk.unwrap());
        }
        assert_eq!(ranged, b"streaming");
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

    #[cfg(windows)]
    #[test]
    fn it_encodes_and_decodes_reserved_chars_reversibly() {
        let did = "did:key:z6MkExampleColonSegment";
        let encoded = encode_reserved(did);
        assert!(!encoded.contains(':'), "encoded name must not contain ':'");
        assert_eq!(encoded, "did%3Akey%3Az6MkExampleColonSegment");
        assert_eq!(decode_reserved(&encoded), did, "must round-trip");

        // Distinct inputs that a lossy sink-character mapping would collide.
        assert_ne!(encode_reserved("a:b"), encode_reserved("a|b"));
        assert_eq!(decode_reserved(&encode_reserved("a:b")), "a:b");
        // A literal '%' is preserved (encoded so decoding stays unambiguous).
        assert_eq!(decode_reserved(&encode_reserved("a%3Ab")), "a%3Ab");
        // Multi-byte UTF-8 survives the round trip untouched.
        assert_eq!(decode_reserved(&encode_reserved("café:名前")), "café:名前");
    }

    #[cfg(windows)]
    #[dialog_common::test]
    async fn it_round_trips_a_did_named_path_on_windows() {
        let space = test_space("did-path-round-trip").await;
        let did = "did:key:z6MkExampleColonSegment";

        // Write under a DID-named directory (would fail with os error 123
        // before the fix).
        let handle = space.certificate().unwrap().resolve(did).unwrap();
        handle.write(b"x").await.unwrap();

        // On disk the colon is escaped, never present.
        let names = space.certificate().unwrap().list().await.unwrap();
        assert_eq!(
            names,
            vec![did.to_string()],
            "list recovers the logical name"
        );

        // The decoded name resolves back to the same bytes, no double-encoding.
        let again = space.certificate().unwrap().resolve(&names[0]).unwrap();
        assert_eq!(again.read().await.unwrap(), b"x");
    }

    #[cfg(windows)]
    #[test]
    fn it_preserves_the_drive_letter_colon() {
        let path = env::temp_dir().join("dialog-file-handle.tmp");
        let handle = FileSystemHandle::try_from(path.clone()).unwrap();
        let round_trip: PathBuf = handle.try_into().unwrap();
        // Drive-letter colon (a Prefix component) is untouched; trailing
        // separators are dropped.
        assert_eq!(round_trip, path);
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
    async fn it_resolves_profile_under_platform_data_dir() {
        let name = unique_name("profile-resolve");
        let location = StorageLocation::new(Directory::Profile, &name);
        let space = FileSystem::open(&location).await.unwrap();

        let root: PathBuf = space.handle().clone().try_into().unwrap();
        let expected = dirs::data_dir()
            .expect("platform data dir is required for this test")
            .join(STORAGE_NAMESPACE)
            .join(&name);
        assert_eq!(root, expected);
    }

    #[dialog_common::test]
    async fn it_resolves_current_under_working_directory() {
        let name = unique_name("current-resolve");
        let location = StorageLocation::new(Directory::Current, &name);
        let space = FileSystem::open(&location).await.unwrap();

        let root: PathBuf = space.handle().clone().try_into().unwrap();
        let expected = env::current_dir()
            .expect("current working directory must be accessible")
            .join(&name);
        assert_eq!(root, expected);
    }

    #[dialog_common::test]
    async fn it_resolves_temp_under_platform_temp_dir() {
        let name = unique_name("temp-resolve");
        let location = StorageLocation::new(Directory::Temp, &name);
        let space = FileSystem::open(&location).await.unwrap();

        let root: PathBuf = space.handle().clone().try_into().unwrap();
        let expected = env::temp_dir().join(&name);
        assert_eq!(root, expected);
    }

    #[dialog_common::test]
    async fn it_resolves_at_under_explicit_path() {
        // Use a tempdir as the explicit base so the test doesn't depend
        // on any fixed filesystem layout.
        let base = env::temp_dir().join(unique_name("at-base"));
        let name = unique_name("at-resolve");
        let location = StorageLocation::new(Directory::At(base.to_string_lossy().into()), &name);
        let space = FileSystem::open(&location).await.unwrap();

        let root: PathBuf = space.handle().clone().try_into().unwrap();
        assert_eq!(root, base.join(&name));
    }

    #[dialog_common::test]
    async fn it_writes_credential_to_expected_path() {
        use dialog_credentials::{Ed25519Signer, SignerCredential};
        use dialog_effects::prelude::*;
        use dialog_varsig::Principal;

        let name = unique_name("fs-layout-credential");
        let location = StorageLocation::new(Directory::Temp, &name);
        let provider = FileSystem::open(&location).await.unwrap();
        let root: PathBuf = provider.handle().clone().try_into().unwrap();

        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);
        let cred = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        did.credential()
            .key("self")
            .save(cred)
            .perform(&provider)
            .await
            .unwrap();

        // Credential should be at {root}/credential/key/self
        let expected = root.join("credential").join("key").join("self");
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
        use dialog_common::{Blake3Hash, Buffer};
        use dialog_credentials::Ed25519Signer;
        use dialog_effects::prelude::*;
        use dialog_varsig::Principal;

        let name = unique_name("fs-layout-archive");
        let location = StorageLocation::new(Directory::Temp, &name);
        let provider = FileSystem::open(&location).await.unwrap();
        let root: PathBuf = provider.handle().clone().try_into().unwrap();

        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);
        let content = b"hello archive layout".to_vec();
        let digest = Blake3Hash::hash(&content);

        did.archive()
            .catalog("index")
            .put(Buffer::from(content))
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
        let root: PathBuf = provider.handle().clone().try_into().unwrap();

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
        let root: PathBuf = provider.handle().clone().try_into().unwrap();

        // Generate a credential and export it
        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);
        let cred = dialog_credentials::Credential::Signer(SignerCredential::from(signer));
        let export = cred.export().await.unwrap();

        // Manually write the exported bytes to the expected path
        let cred_dir = root.join("credential").join("key");
        std::fs::create_dir_all(&cred_dir).unwrap();
        std::fs::write(cred_dir.join("self"), export.as_bytes()).unwrap();

        // Provider should load it successfully
        let loaded = did
            .credential()
            .key("self")
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
        let root: PathBuf = provider.handle().clone().try_into().unwrap();

        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);

        // Write garbage to the credential path
        let cred_dir = root.join("credential").join("key");
        std::fs::create_dir_all(&cred_dir).unwrap();
        std::fs::write(cred_dir.join("self"), b"not a valid credential").unwrap();

        let result = did.credential().key("self").load().perform(&provider).await;

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
        let root: PathBuf = provider.handle().clone().try_into().unwrap();

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
        let root: PathBuf = provider.handle().clone().try_into().unwrap();

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

        let expected_version = dialog_effects::memory::Version::from(Blake3Hash::hash(&content));
        assert_eq!(publication.version, expected_version);
    }
}
