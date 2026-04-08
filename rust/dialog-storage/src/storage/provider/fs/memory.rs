//! Memory capability provider for filesystem.
//!
//! Layout: `{space_root}/memory/{space}/{cell}`
//!
//! Implements transactional cell storage with CAS (Compare-And-Swap) semantics.
//! Uses PID-based file locking for cross-process coordination and BLAKE3
//! content hashing for edition tracking.

use super::{FileSystem, FileSystemError, FileSystemHandle};
use async_trait::async_trait;
use base58::ToBase58;

const MEMORY: &str = "memory";

impl FileSystem {
    /// Returns the handle for this space's memory directory.
    pub fn memory(&self) -> Result<FileSystemHandle, FileSystemError> {
        self.resolve(MEMORY)
    }
}
use dialog_capability::{Capability, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::memory::{
    MemoryError, Publication, Publish, PublishCapability, Resolve, ResolveCapability, Retract,
    RetractCapability,
};
use pidlock::Pidlock;
use std::path::PathBuf;

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

/// RAII guard that acquires a PID lock and releases it when dropped.
///
/// Handles stale lock detection and recovery automatically.
struct PidlockGuard(Pidlock);

impl PidlockGuard {
    /// Acquire a PID lock at the given handle.
    ///
    /// If a stale lock exists (from a dead process), it will be automatically
    /// cleaned up and the lock acquired.
    ///
    /// If the lock is held by an active process, returns an error immediately
    /// rather than waiting. This is intentional - the STM layer will retry
    /// the entire transaction, which is the correct behavior since the locked
    /// value will likely change anyway.
    fn acquire(path: PathBuf) -> Result<Self, FileSystemError> {
        let path_str = path
            .to_str()
            .ok_or_else(|| FileSystemError::Lock("Lock path is not valid UTF-8".to_string()))?;

        // If something other than a regular file exists at the lock path
        // (e.g. a directory from a previous bug), fail early. Pidlock
        // panics if it tries to remove_file on a directory.
        if path.exists() && !path.is_file() {
            return Err(FileSystemError::Lock(format!(
                "Lock path is not a regular file: {}",
                path_str
            )));
        }

        let mut lock = Pidlock::new(path_str);

        match lock.acquire() {
            Ok(()) => Ok(Self(lock)),
            Err(pidlock::PidlockError::LockExists) => {
                // get_owner() checks if the PID in the lock file is still
                // alive. If not, it removes the stale file so a retry can
                // succeed. If the process is alive, we fail immediately
                // and let the STM layer retry the whole transaction.
                match lock.get_owner() {
                    Some(pid) => Err(FileSystemError::Lock(format!(
                        "Concurrent write in progress (lock held by pid {pid})",
                    ))),
                    None => {
                        // Stale lock was removed by get_owner(). Retry once.
                        lock.acquire().map(|()| Self(lock)).map_err(|e| {
                            FileSystemError::Lock(format!("Failed to acquire lock: {e:?}"))
                        })
                    }
                }
            }
            Err(e) => Err(FileSystemError::Lock(format!(
                "Failed to acquire lock: {e:?}"
            ))),
        }
    }
}

impl Drop for PidlockGuard {
    fn drop(&mut self) {
        let _ = self.0.release();
    }
}

/// Format edition bytes for error messages.
fn format_edition(edition: Option<&[u8]>) -> Option<String> {
    edition.map(base58::ToBase58::to_base58)
}

/// Helper methods for cell-related paths.
impl FileSystemHandle {
    fn cell(&self, name: &str) -> Result<Self, FileSystemError> {
        self.resolve(name)
    }

    /// Acquire a PID lock for this handle by appending `.lock` to its path.
    fn lock(&self) -> Result<PidlockGuard, FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        let lock_path = path.with_extension("lock");
        // Ensure parent directory exists for the lock file
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| FileSystemError::Io(e.to_string()))?;
        }
        PidlockGuard::acquire(lock_path)
    }

    fn temp(&self, hash: &[u8; 32]) -> Result<Self, FileSystemError> {
        let path: PathBuf = self.clone().try_into()?;
        let tmp_name = format!(
            "{}.{}.tmp",
            path.file_name().and_then(|n| n.to_str()).unwrap_or("cell"),
            hash.to_base58()
        );
        let tmp_path = path.with_file_name(tmp_name);
        tmp_path.try_into()
    }
}

#[async_trait]
impl Provider<Resolve> for FileSystem {
    async fn execute(
        &self,
        effect: Capability<Resolve>,
    ) -> Result<Option<Publication>, MemoryError> {
        let space = effect.space();
        let cell = effect.cell();

        let handle = self.memory()?.resolve(space)?.cell(cell)?;

        match handle.read_optional().await? {
            Some(bytes) => {
                let edition = Blake3Hash::hash(&bytes);
                Ok(Some(Publication {
                    content: bytes,
                    edition: edition.as_bytes().to_vec(),
                }))
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl Provider<Publish> for FileSystem {
    async fn execute(&self, effect: Capability<Publish>) -> Result<Vec<u8>, MemoryError> {
        let space = effect.space();
        let cell = effect.cell();
        let content = effect.content().to_vec();
        let expected_edition = effect.when().map(|e| e.to_vec());

        let cell_handle = self.memory()?.resolve(space)?.cell(cell)?;

        // Acquire lock for exclusive access
        let _guard = cell_handle.lock()?;

        // Read current value to check CAS condition
        let current_edition: Option<[u8; 32]> = cell_handle
            .read_optional()
            .await?
            .map(|bytes| content_hash(&bytes));

        // Compute new edition
        let new_edition = content_hash(&content);

        // If current value already matches desired value, succeed without writing
        if current_edition.as_ref() == Some(&new_edition) {
            return Ok(new_edition.to_vec());
        }

        // Check CAS condition
        match (expected_edition.as_deref(), &current_edition) {
            // Creating new: require cell doesn't exist
            (None, Some(current)) => {
                return Err(MemoryError::EditionMismatch {
                    expected: None,
                    actual: format_edition(Some(current.as_slice())),
                });
            }
            // Updating existing: require edition matches
            (Some(expected), Some(current)) => {
                if expected != current.as_slice() {
                    return Err(MemoryError::EditionMismatch {
                        expected: format_edition(Some(expected)),
                        actual: format_edition(Some(current.as_slice())),
                    });
                }
            }
            // Updating non-existent: fail
            (Some(expected), None) => {
                return Err(MemoryError::EditionMismatch {
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

        Ok(new_edition.to_vec())
    }
}

#[async_trait]
impl Provider<Retract> for FileSystem {
    async fn execute(&self, effect: Capability<Retract>) -> Result<(), MemoryError> {
        let space = effect.space();
        let cell = effect.cell();
        let expected_edition = effect.when().to_vec();

        let cell_handle = self.memory()?.resolve(space)?.cell(cell)?;

        // If space directory doesn't exist, the cell doesn't exist either
        if !cell_handle.exists().await {
            return Ok(());
        }

        // Acquire lock for exclusive access
        let _guard = cell_handle.lock()?;

        // Read current value to check CAS condition
        let current_bytes = match cell_handle.read_optional().await? {
            Some(bytes) => bytes,
            None => return Ok(()),
        };

        let current_edition = content_hash(&current_bytes);

        // Check CAS condition
        if expected_edition != current_edition.as_slice() {
            return Err(MemoryError::EditionMismatch {
                expected: format_edition(Some(&expected_edition)),
                actual: format_edition(Some(current_edition.as_slice())),
            });
        }

        // Delete the file
        cell_handle.remove().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::Resource;
    use dialog_capability::Subject;
    use dialog_effects::memory::{Cell, Memory, Space};
    use dialog_effects::storage::{Directory, Location as StorageLocation};

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

    async fn unique_subject() -> Subject {
        let signer = dialog_credentials::Ed25519Signer::generate().await.unwrap();
        Subject::from(dialog_varsig::Principal::did(&signer))
    }

    #[dialog_common::test]
    async fn it_resolves_non_existent_cell() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_resolves_non_existent_cell"),
        );
        let provider = FileSystem::open(&location).await?;
        let subject = unique_subject().await;

        let effect = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("missing"))
            .invoke(Resolve);

        let result = effect.perform(&provider).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_new_content() -> anyhow::Result<()> {
        let location =
            StorageLocation::new(Directory::Temp, unique_name("fs-it_publishes_new_content"));
        let provider = FileSystem::open(&location).await?;
        let subject = unique_subject().await;
        let content = b"hello world".to_vec();

        // Publish new content (when = None means expect empty)
        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&provider)
            .await?;

        assert!(!edition.is_empty());

        // Resolve to verify
        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Resolve)
            .perform(&provider)
            .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);
        assert_eq!(publication.edition, edition);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_existing_content() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_updates_existing_content"),
        );
        let provider = FileSystem::open(&location).await?;
        let subject = unique_subject().await;

        // Create initial content
        let edition1 = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"initial", None))
            .perform(&provider)
            .await?;

        // Update with correct edition
        let edition2 = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"updated", Some(edition1.clone())))
            .perform(&provider)
            .await?;

        assert_ne!(edition1, edition2);

        // Verify update
        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Resolve)
            .perform(&provider)
            .await?;

        let publication = resolved.expect("should have content");
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
        let subject = unique_subject().await;

        // Create initial content
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"initial", None))
            .perform(&provider)
            .await?;

        // Try to update with wrong edition
        let wrong_edition = Blake3Hash::hash(b"wrong").as_bytes().to_vec();
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"updated", Some(wrong_edition)))
            .perform(&provider)
            .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_creating_when_exists() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_fails_creating_when_exists"),
        );
        let provider = FileSystem::open(&location).await?;
        let subject = unique_subject().await;

        // Create initial content
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"initial", None))
            .perform(&provider)
            .await?;

        // Try to create again (when = None means expect empty)
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"new", None))
            .perform(&provider)
            .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_content() -> anyhow::Result<()> {
        let location = StorageLocation::new(Directory::Temp, unique_name("fs-it_retracts_content"));
        let provider = FileSystem::open(&location).await?;
        let subject = unique_subject().await;

        // Create content
        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"to be deleted", None))
            .perform(&provider)
            .await?;

        // Retract with correct edition
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Retract::new(edition))
            .perform(&provider)
            .await?;

        // Verify deleted
        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Resolve)
            .perform(&provider)
            .await?;

        assert!(resolved.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_retract_on_edition_mismatch() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_fails_retract_on_edition_mismatch"),
        );
        let provider = FileSystem::open(&location).await?;
        let subject = unique_subject().await;

        // Create content
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"content", None))
            .perform(&provider)
            .await?;

        // Try to retract with wrong edition
        let wrong_edition = Blake3Hash::hash(b"wrong").as_bytes().to_vec();
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Retract::new(wrong_edition))
            .perform(&provider)
            .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_spaces() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_handles_different_spaces"),
        );
        let provider = FileSystem::open(&location).await?;
        let subject = unique_subject().await;

        // Publish to different spaces
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("space1"))
            .attenuate(Cell::new("cell"))
            .invoke(Publish::new(b"content1", None))
            .perform(&provider)
            .await?;

        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("space2"))
            .attenuate(Cell::new("cell"))
            .invoke(Publish::new(b"content2", None))
            .perform(&provider)
            .await?;

        // Resolve from space1
        let result1 = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("space1"))
            .attenuate(Cell::new("cell"))
            .invoke(Resolve)
            .perform(&provider)
            .await?;
        assert_eq!(result1.unwrap().content, b"content1".to_vec());

        // Resolve from space2
        let result2 = subject
            .attenuate(Memory)
            .attenuate(Space::new("space2"))
            .attenuate(Cell::new("cell"))
            .invoke(Resolve)
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
        let subject = unique_subject().await;
        let content = b"desired value".to_vec();

        // Create initial content
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&provider)
            .await?;

        // Try to publish same content with wrong edition - should succeed
        let wrong_edition = Blake3Hash::hash(b"wrong").as_bytes().to_vec();
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(content.clone(), Some(wrong_edition)))
            .perform(&provider)
            .await;

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Blake3Hash::hash(&content).as_bytes().to_vec()
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_produces_deterministic_content_hash() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_produces_deterministic_content_hash"),
        );
        let provider = FileSystem::open(&location).await?;
        let subject = unique_subject().await;
        let content = b"same content".to_vec();

        // Create value at cell1
        let edition1 = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("cell1"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&provider)
            .await?;

        // Create same value at cell2
        let edition2 = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("cell2"))
            .invoke(Publish::new(content, None))
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
        let subject = unique_subject().await;

        // Try to retract non-existent cell - should succeed
        let wrong_edition = Blake3Hash::hash(b"wrong").as_bytes().to_vec();
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("nonexistent"))
            .invoke(Retract::new(wrong_edition))
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
        let subject = unique_subject().await;
        let content = b"nested content".to_vec();

        // Publish to nested space path
        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("parent/child/grandchild"))
            .attenuate(Cell::new("cell"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&provider)
            .await?;

        assert!(!edition.is_empty());

        // Resolve to verify
        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("parent/child/grandchild"))
            .attenuate(Cell::new("cell"))
            .invoke(Resolve)
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
        let subject = unique_subject().await;
        let content = b"nested cell content".to_vec();

        // Publish to a cell with a path separator, without pre-creating dirs.
        // This mirrors how Branch::mount uses "local/main" as an address.
        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("subdir/cell"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&provider)
            .await?;

        assert!(!edition.is_empty());

        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("subdir/cell"))
            .invoke(Resolve)
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
        let subject = unique_subject().await;
        let content = vec![];

        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("empty"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&provider)
            .await?;

        assert!(!edition.is_empty());

        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("empty"))
            .invoke(Resolve)
            .perform(&provider)
            .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_content() -> anyhow::Result<()> {
        let location =
            StorageLocation::new(Directory::Temp, unique_name("fs-it_handles_large_content"));
        let provider = FileSystem::open(&location).await?;
        let subject = unique_subject().await;
        // 1MB content
        let content: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("large"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&provider)
            .await?;

        assert!(!edition.is_empty());

        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("large"))
            .invoke(Resolve)
            .perform(&provider)
            .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_despite_stale_lock_file() -> anyhow::Result<()> {
        let location = StorageLocation::new(
            Directory::Temp,
            unique_name("fs-it_publishes_despite_stale_lock"),
        );
        let provider = FileSystem::open(&location).await?;
        let subject = unique_subject().await;

        // First publish to create the directory structure
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"initial", None))
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
        let resolved = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Resolve)
            .perform(&provider)
            .await?;
        let edition = resolved.unwrap().edition;

        let edition2 = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"after stale lock", Some(edition)))
            .perform(&provider)
            .await?;

        assert!(!edition2.is_empty());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_lock_when_held_by_same_process() -> anyhow::Result<()> {
        // Verifies that when our own process holds the lock, acquire returns
        // an error immediately (not a spin). This matters because all tests
        // run in the same process and share a PID.
        let dir = std::env::temp_dir().join(unique_name("fs-same-pid-lock"));
        std::fs::create_dir_all(&dir)?;

        let lock_path = dir.join("cell.lock");
        let _guard = PidlockGuard::acquire(lock_path.clone())?;

        // Second acquire from same process should fail immediately
        let result = PidlockGuard::acquire(lock_path);
        let err = match result {
            Ok(_) => panic!("expected lock to fail when held by same process"),
            Err(e) => e,
        };
        assert!(
            matches!(err, FileSystemError::Lock(_)),
            "expected Lock error, got: {err:?}"
        );

        drop(_guard);
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_lock_with_trailing_slash_path() -> anyhow::Result<()> {
        // Reproduces the bug where FileSystemHandle's trailing-slash URLs
        // produced PathBufs like "/tmp/.../test.lock/" which pidlock can
        // never create (create_new fails on trailing-slash paths) and
        // get_owner returns None (no file exists), causing an infinite
        // busy loop in the old unbounded retry code.
        let dir = std::env::temp_dir().join(unique_name("fs-trailing-slash-lock"));
        std::fs::create_dir_all(&dir)?;

        let bad_path = dir.join("cell.lock/"); // trailing slash
        let result = PidlockGuard::acquire(bad_path);

        // Should fail with a bounded retry error, not spin forever
        let err = match result {
            Ok(_) => panic!("expected lock to fail with trailing-slash path"),
            Err(e) => e,
        };
        assert!(
            matches!(err, FileSystemError::Lock(_)),
            "expected Lock error, got: {err:?}"
        );

        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_lock_when_directory_exists_at_lock_path() -> anyhow::Result<()> {
        // If a directory exists where the lock file should be (e.g. from
        // a previous buggy run), acquire should fail, not spin or panic.
        let dir = std::env::temp_dir().join(unique_name("fs-lock-dir-exists"));
        std::fs::create_dir_all(&dir)?;

        let lock_path = dir.join("cell.lock");
        std::fs::create_dir_all(&lock_path)?;
        assert!(lock_path.is_dir());

        let err = match PidlockGuard::acquire(lock_path) {
            Ok(_) => panic!("expected lock to fail when a directory exists at the lock path"),
            Err(e) => e,
        };
        assert!(
            matches!(err, FileSystemError::Lock(_)),
            "expected Lock error, got: {err:?}"
        );

        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
