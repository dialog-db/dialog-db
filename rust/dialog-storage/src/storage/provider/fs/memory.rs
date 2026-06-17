//! Memory capability provider for filesystem.
//!
//! Layout: `{space_root}/memory/{space}/{cell}`
//!
//! Implements transactional cell storage with CAS (Compare-And-Swap) semantics.
//! Uses PID-based file locking for cross-process coordination and BLAKE3
//! content hashing for edition tracking.

use super::{FileSystem, FileSystemError, FileSystemHandle};
use base58::ToBase58;
use dialog_capability::{Capability, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::memory::prelude::{PublishExt, ResolveExt, RetractExt};
use dialog_effects::memory::{Edition, MemoryError, Publish, Resolve, Retract, Version};

const MEMORY: &str = "memory";

impl FileSystem {
    /// Returns the handle for this space's memory directory.
    pub fn memory(&self) -> Result<FileSystemHandle, FileSystemError> {
        self.resolve(MEMORY)
    }
}

/// A 32-byte content hash used as the edition for CAS operations.
type ContentHash = [u8; 32];

/// Compute BLAKE3 hash of content.
fn content_hash(content: &[u8]) -> ContentHash {
    blake3::hash(content).into()
}

impl From<FileSystemError> for MemoryError {
    fn from(e: FileSystemError) -> Self {
        MemoryError::Storage(e.to_string())
    }
}

/// Format edition bytes for error messages.
fn format_edition(edition: Option<&[u8]>) -> Option<Version> {
    edition.map(Version::from)
}

/// Helper methods for cell-related paths.
impl FileSystemHandle {
    fn cell(&self, name: &str) -> Result<Self, FileSystemError> {
        self.resolve(name)
    }

    /// A sibling handle to this one with `.{hash}.tmp` appended to the final
    /// path segment. The hash in the name keeps concurrent writers' temp files
    /// from colliding even if a previous writer's cleanup was skipped.
    fn temp(&self, hash: &[u8; 32]) -> Result<Self, FileSystemError> {
        let leaf = self
            .url()
            .path_segments()
            .and_then(|mut s| s.next_back())
            .filter(|s| !s.is_empty())
            .unwrap_or("cell");
        let tmp_name = format!("{leaf}.{}.tmp", hash.to_base58());

        let mut url = self.url().clone();
        url.path_segments_mut()
            .map_err(|_| FileSystemError::Io("handle URL cannot be a base".to_string()))?
            .pop()
            .push(&tmp_name);
        Ok(self.with_url(url))
    }

    /// Acquire a cross-writer lock on this handle for a CAS critical section.
    ///
    /// On native this is a PID-based file lock at `{path}.lock`; on the web it
    /// is a [Web Locks API][weblocks] lock keyed by this handle's URL. The
    /// guard releases the lock when dropped.
    ///
    /// [weblocks]: https://developer.mozilla.org/en-US/docs/Web/API/Web_Locks_API
    async fn lock(&self) -> Result<super::LockGuard, FileSystemError> {
        super::lock(self).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Resolve> for FileSystem {
    async fn execute(
        &self,
        effect: Capability<Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        let space = effect.space();
        let cell = effect.cell();

        let handle = self.memory()?.resolve(space)?.cell(cell)?;

        match handle.read_optional().await? {
            Some(bytes) => {
                let edition = Blake3Hash::hash(&bytes);
                Ok(Some(Edition {
                    content: bytes,
                    version: Version::from(edition.as_bytes()),
                }))
            }
            None => Ok(None),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Publish> for FileSystem {
    async fn execute(&self, effect: Capability<Publish>) -> Result<Version, MemoryError> {
        let space = effect.space();
        let cell = effect.cell();
        let content = effect.content().to_vec();
        let expected_edition = effect.when().map(|e| e.as_bytes().to_vec());

        let cell_handle = self.memory()?.resolve(space)?.cell(cell)?;

        // Acquire lock for exclusive access
        let _guard = cell_handle.lock().await?;

        // Read current value to check CAS condition
        let current_edition: Option<[u8; 32]> = cell_handle
            .read_optional()
            .await?
            .map(|bytes| content_hash(&bytes));

        // Compute new edition
        let new_edition = content_hash(&content);

        // If current value already matches desired value, succeed without writing
        if current_edition.as_ref() == Some(&new_edition) {
            return Ok(Version::from(new_edition.as_slice()));
        }

        // Check CAS condition
        match (expected_edition.as_deref(), &current_edition) {
            // Creating new: require cell doesn't exist
            (None, Some(current)) => {
                return Err(MemoryError::VersionMismatch {
                    expected: None,
                    actual: format_edition(Some(current.as_slice())),
                });
            }
            // Updating existing: require edition matches
            (Some(expected), Some(current)) => {
                if expected != current.as_slice() {
                    return Err(MemoryError::VersionMismatch {
                        expected: format_edition(Some(expected)),
                        actual: format_edition(Some(current.as_slice())),
                    });
                }
            }
            // Updating non-existent: fail
            (Some(expected), None) => {
                return Err(MemoryError::VersionMismatch {
                    expected: format_edition(Some(expected)),
                    actual: None,
                });
            }
            // Creating new when cell doesn't exist: valid
            (None, None) => {}
        }

        // Write to temp file (hash in name prevents conflicts if cleanup fails),
        // then atomic rename to final location. write() creates parent dirs.
        let tmp_handle = cell_handle.temp(&new_edition)?;
        tmp_handle.write(&content).await?;
        tmp_handle.rename(&cell_handle).await?;

        Ok(Version::from(new_edition.as_slice()))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Retract> for FileSystem {
    async fn execute(&self, effect: Capability<Retract>) -> Result<(), MemoryError> {
        let space = effect.space();
        let cell = effect.cell();
        let expected_edition = effect.when().as_bytes().to_vec();

        let cell_handle = self.memory()?.resolve(space)?.cell(cell)?;

        // If space directory doesn't exist, the cell doesn't exist either
        if !cell_handle.exists().await {
            return Ok(());
        }

        // Acquire lock for exclusive access
        let _guard = cell_handle.lock().await?;

        // Read current value to check CAS condition
        let current_bytes = match cell_handle.read_optional().await? {
            Some(bytes) => bytes,
            None => return Ok(()),
        };

        let current_edition = content_hash(&current_bytes);

        // Check CAS condition
        if expected_edition != current_edition.as_slice() {
            return Err(MemoryError::VersionMismatch {
                expected: format_edition(Some(&expected_edition)),
                actual: format_edition(Some(current_edition.as_slice())),
            });
        }

        // Delete the file
        cell_handle.remove().await?;
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::helpers::{unique_did, unique_name};
    use crate::resource::Resource;
    use dialog_effects::memory::Version;
    use dialog_effects::prelude::*;
    use dialog_effects::storage::{Directory, Location as StorageLocation};

    #[dialog_common::test]
    async fn it_resolves_non_existent_cell() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_resolves_non_existent_cell"),
        );
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        let effect = did.memory().space("local").cell("missing").resolve();

        let result = effect.perform(&provider).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_new_content() -> anyhow::Result<()> {
        let location =
            StorageLocation::new(Directory::Temp, unique_name("fs-it_publishes_new_content"));
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;
        let content = b"hello world".to_vec();

        // Publish new content (when = None means expect empty)
        let version = did
            .clone()
            .memory()
            .space("local")
            .cell("test")
            .publish(content.clone(), None)
            .perform(&provider)
            .await?;

        assert!(!version.is_empty());

        // Resolve to verify
        let resolved = did
            .memory()
            .space("local")
            .cell("test")
            .resolve()
            .perform(&provider)
            .await?;

        let edition = resolved.expect("should have content");
        assert_eq!(edition.content, content);
        assert_eq!(edition.version.as_bytes(), version.as_bytes());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_existing_content() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_updates_existing_content"),
        );
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        // Create initial content
        let v1 = did
            .clone()
            .memory()
            .space("local")
            .cell("test")
            .publish(b"initial", None)
            .perform(&provider)
            .await?;

        // Update with correct edition
        let v2 = did
            .clone()
            .memory()
            .space("local")
            .cell("test")
            .publish(b"updated", Some(v1.clone()))
            .perform(&provider)
            .await?;

        assert_ne!(v1, v2);

        // Verify update
        let edition = did
            .memory()
            .space("local")
            .cell("test")
            .resolve()
            .perform(&provider)
            .await?;

        let publication = edition.expect("should have content");
        assert_eq!(publication.content, b"updated".to_vec());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_on_edition_mismatch() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_fails_on_edition_mismatch"),
        );
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        // Create initial content
        did.clone()
            .memory()
            .space("local")
            .cell("test")
            .publish(b"initial", None)
            .perform(&provider)
            .await?;

        // Try to update with wrong edition
        let wrong_edition = Version::from(Blake3Hash::hash(b"wrong"));
        let result = did
            .memory()
            .space("local")
            .cell("test")
            .publish(b"updated", Some(wrong_edition))
            .perform(&provider)
            .await;

        assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_creating_when_exists() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_fails_creating_when_exists"),
        );
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        // Create initial content
        did.clone()
            .memory()
            .space("local")
            .cell("test")
            .publish(b"initial", None)
            .perform(&provider)
            .await?;

        // Try to create again (when = None means expect empty)
        let result = did
            .memory()
            .space("local")
            .cell("test")
            .publish(b"new", None)
            .perform(&provider)
            .await;

        assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_content() -> anyhow::Result<()> {
        let location = StorageLocation::new(Directory::Temp, unique_name("fs-it_retracts_content"));
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        // Create content
        let version = did
            .clone()
            .memory()
            .space("local")
            .cell("test")
            .publish(b"to be deleted", None)
            .perform(&provider)
            .await?;

        // Retract with correct edition
        did.clone()
            .memory()
            .space("local")
            .cell("test")
            .retract(version)
            .perform(&provider)
            .await?;

        // Verify deleted
        let edition = did
            .memory()
            .space("local")
            .cell("test")
            .resolve()
            .perform(&provider)
            .await?;

        assert!(edition.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_retract_on_edition_mismatch() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_fails_retract_on_edition_mismatch"),
        );
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        // Create content
        did.clone()
            .memory()
            .space("local")
            .cell("test")
            .publish(b"content", None)
            .perform(&provider)
            .await?;

        // Try to retract with wrong edition
        let wrong_version = Version::from(Blake3Hash::hash(b"wrong"));
        let result = did
            .memory()
            .space("local")
            .cell("test")
            .retract(wrong_version)
            .perform(&provider)
            .await;

        assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_spaces() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_handles_different_spaces"),
        );
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        // Publish to different spaces
        did.clone()
            .memory()
            .space("space1")
            .cell("cell")
            .publish(b"content1", None)
            .perform(&provider)
            .await?;

        did.clone()
            .memory()
            .space("space2")
            .cell("cell")
            .publish(b"content2", None)
            .perform(&provider)
            .await?;

        // Resolve from space1
        let result1 = did
            .clone()
            .memory()
            .space("space1")
            .cell("cell")
            .resolve()
            .perform(&provider)
            .await?;
        assert_eq!(result1.unwrap().content, b"content1".to_vec());

        // Resolve from space2
        let result2 = did
            .memory()
            .space("space2")
            .cell("cell")
            .resolve()
            .perform(&provider)
            .await?;
        assert_eq!(result2.unwrap().content, b"content2".to_vec());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_with_stale_edition_when_value_matches() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            "fs-it_succeeds_with_stale_edition_when_value_matches",
        );
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;
        let content = b"desired value".to_vec();

        // Create initial content
        did.clone()
            .memory()
            .space("local")
            .cell("test")
            .publish(content.clone(), None)
            .perform(&provider)
            .await?;

        // Try to publish same content with wrong edition - should succeed
        let wrong_version = Version::from(Blake3Hash::hash(b"wrong"));
        let result = did
            .memory()
            .space("local")
            .cell("test")
            .publish(content.clone(), Some(wrong_version))
            .perform(&provider)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Version::from(Blake3Hash::hash(&content)));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_produces_deterministic_content_hash() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_produces_deterministic_content_hash"),
        );
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;
        let content = b"same content".to_vec();

        // Create value at cell1
        let edition1 = did
            .clone()
            .memory()
            .space("local")
            .cell("cell1")
            .publish(content.clone(), None)
            .perform(&provider)
            .await?;

        // Create same value at cell2
        let edition2 = did
            .memory()
            .space("local")
            .cell("cell2")
            .publish(content, None)
            .perform(&provider)
            .await?;

        // Same content should produce same edition (content hash)
        assert_eq!(edition1, edition2);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_retracting_already_retracted() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            "fs-it_succeeds_retracting_already_retracted",
        );
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        // Try to retract non-existent cell - should succeed
        let wrong_version = Version::from(Blake3Hash::hash(b"wrong"));
        let result = did
            .memory()
            .space("local")
            .cell("nonexistent")
            .retract(wrong_version)
            .perform(&provider)
            .await;

        assert!(result.is_ok());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_nested_spaces() -> anyhow::Result<()> {
        let location =
            StorageLocation::new(Directory::Temp, unique_name("fs-it_handles_nested_spaces"));
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;
        let content = b"nested content".to_vec();

        // Publish to nested space path
        let edition = did
            .clone()
            .memory()
            .space("parent/child/grandchild")
            .cell("cell")
            .publish(content.clone(), None)
            .perform(&provider)
            .await?;

        assert!(!edition.is_empty());

        // Resolve to verify
        let resolved = did
            .memory()
            .space("parent/child/grandchild")
            .cell("cell")
            .resolve()
            .perform(&provider)
            .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_to_nested_cell() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_publishes_to_nested_cell"),
        );
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;
        let content = b"nested cell content".to_vec();

        // Publish to a cell with a path separator, without pre-creating dirs.
        // This mirrors how Branch::mount uses "local/main" as an address.
        let version = did
            .clone()
            .memory()
            .space("local")
            .cell("subdir/cell")
            .publish(content.clone(), None)
            .perform(&provider)
            .await?;

        assert!(!version.is_empty());

        let resolved = did
            .memory()
            .space("local")
            .cell("subdir/cell")
            .resolve()
            .perform(&provider)
            .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_empty_content() -> anyhow::Result<()> {
        let location =
            StorageLocation::new(Directory::Temp, unique_name("fs-it_handles_empty_content"));
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;
        let content = vec![];

        let version = did
            .clone()
            .memory()
            .space("local")
            .cell("empty")
            .publish(content.clone(), None)
            .perform(&provider)
            .await?;

        assert!(!version.is_empty());

        let edition = did
            .memory()
            .space("local")
            .cell("empty")
            .resolve()
            .perform(&provider)
            .await?;

        let publication = edition.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_content() -> anyhow::Result<()> {
        let location =
            StorageLocation::new(Directory::Temp, unique_name("fs-it_handles_large_content"));
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;
        // 1MB content
        let content: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

        let version = did
            .clone()
            .memory()
            .space("local")
            .cell("large")
            .publish(content.clone(), None)
            .perform(&provider)
            .await?;

        assert!(!version.is_empty());

        let resolved = did
            .memory()
            .space("local")
            .cell("large")
            .resolve()
            .perform(&provider)
            .await?;

        let edition = resolved.expect("should have content");
        assert_eq!(edition.content, content);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_despite_stale_lock_file() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_publishes_despite_stale_lock"),
        );
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        // First publish to create the directory structure
        did.clone()
            .memory()
            .space("local")
            .cell("test")
            .publish(b"initial", None)
            .perform(&provider)
            .await?;

        // Manually create a stale lock file (as if a process crashed)
        let cell_handle = provider.memory()?.resolve("local")?.cell("test")?;
        let cell_path: std::path::PathBuf = cell_handle.try_into()?;
        let lock_path = cell_path.with_extension("lock");
        // Write a fake PID that does not correspond to a running process
        std::fs::write(&lock_path, b"999999999")?;
        assert!(lock_path.exists(), "stale lock file should exist");

        // Publish should succeed by clearing the stale lock
        let edition = did
            .clone()
            .memory()
            .space("local")
            .cell("test")
            .resolve()
            .perform(&provider)
            .await?;
        let edition = edition.unwrap().version;

        let v2 = did
            .memory()
            .space("local")
            .cell("test")
            .publish(b"after stale lock", Some(edition))
            .perform(&provider)
            .await?;

        assert!(!v2.is_empty());
        Ok(())
    }
}
