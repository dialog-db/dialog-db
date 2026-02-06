//! Memory capability provider for filesystem.
//!
//! Implements transactional cell storage with CAS (Compare-And-Swap) semantics.
//! Uses PID-based file locking for cross-process coordination and BLAKE3
//! content hashing for edition tracking.

use super::{FileSystem, FileSystemError, Location};
use async_trait::async_trait;
use base58::ToBase58;
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
    /// Acquire a PID lock at the given path.
    ///
    /// If a stale lock exists (from a dead process), it will be automatically
    /// cleaned up and the lock acquired.
    ///
    /// If the lock is held by an active process, returns an error immediately
    /// rather than waiting. This is intentional - the STM layer will retry
    /// the entire transaction, which is the correct behavior since the locked
    /// value will likely change anyway.
    fn new(path: PathBuf) -> Result<Self, FileSystemError> {
        let path_str = path
            .to_str()
            .ok_or_else(|| FileSystemError::Lock("Lock path is not valid UTF-8".to_string()))?;

        let mut lock = Pidlock::new(path_str);

        // Acquire lock, handling stale locks
        loop {
            match lock.acquire() {
                Ok(()) => return Ok(Self(lock)),
                Err(pidlock::PidlockError::LockExists) => {
                    // get_owner() checks if the PID is valid and clears stale locks
                    match lock.get_owner() {
                        Some(pid) => {
                            // Fail immediately rather than wait - the value is being
                            // modified so the edition will change anyway. Let STM
                            // retry the transaction with the new edition.
                            return Err(FileSystemError::Lock(format!(
                                "Concurrent write in progress (lock held by pid {})",
                                pid
                            )));
                        }
                        None => {
                            // Lock was stale and cleared by get_owner(), retry
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
impl Location {
    fn cell(&self, name: &str) -> Result<Self, FileSystemError> {
        self.resolve(name)
    }

    fn lock(&self, cell: &str) -> Result<Self, FileSystemError> {
        self.resolve(&format!("{}.lock", cell))
    }

    fn temp(&self, cell: &str, hash: &[u8; 32]) -> Result<Self, FileSystemError> {
        self.resolve(&format!("{}.{}.tmp", cell, hash.to_base58()))
    }
}

#[async_trait]
impl Provider<Resolve> for FileSystem {
    async fn execute(
        &mut self,
        effect: Capability<Resolve>,
    ) -> Result<Option<Publication>, MemoryError> {
        let subject = effect.subject().into();
        let space = effect.space();
        let cell = effect.cell();

        let path: PathBuf = self
            .memory(&subject)?
            .resolve(space)?
            .cell(cell)?
            .try_into()?;

        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let edition = Blake3Hash::hash(&bytes);
                Ok(Some(Publication {
                    content: bytes,
                    edition: edition.as_bytes().to_vec(),
                }))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(MemoryError::Storage(e.to_string())),
        }
    }
}

#[async_trait]
impl Provider<Publish> for FileSystem {
    async fn execute(&mut self, effect: Capability<Publish>) -> Result<Vec<u8>, MemoryError> {
        let subject = effect.subject().into();
        let space = effect.space();
        let cell = effect.cell();
        let content = effect.content().to_vec();
        let expected_edition = effect.when().map(|e| e.to_vec());

        let space_location = self.memory(&subject)?.resolve(space)?;

        // Ensure space directory exists
        space_location.ensure_dir().await?;

        let path: PathBuf = space_location.cell(cell)?.try_into()?;
        let lock_path: PathBuf = space_location.lock(cell)?.try_into()?;

        // Acquire lock for exclusive access
        let _guard = PidlockGuard::new(lock_path)?;

        // Read current value to check CAS condition
        let current_edition: Option<[u8; 32]> = match tokio::fs::read(&path).await {
            Ok(bytes) => Some(content_hash(&bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(MemoryError::Storage(e.to_string())),
        };

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

        // Write to temp file (hash in name prevents conflicts if cleanup fails)
        let tmp_path: PathBuf = space_location.temp(cell, &new_edition)?.try_into()?;
        tokio::fs::write(&tmp_path, &content)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        // Atomic rename
        tokio::fs::rename(&tmp_path, &path)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        Ok(new_edition.to_vec())
    }
}

#[async_trait]
impl Provider<Retract> for FileSystem {
    async fn execute(&mut self, effect: Capability<Retract>) -> Result<(), MemoryError> {
        let subject = effect.subject().into();
        let space = effect.space();
        let cell = effect.cell();
        let expected_edition = effect.when().to_vec();

        let space_location = self.memory(&subject)?.resolve(space)?;
        let space_path: PathBuf = space_location.clone().try_into()?;

        // If space directory doesn't exist, the cell doesn't exist either
        if !space_path.exists() {
            return Ok(());
        }

        let path: PathBuf = space_location.cell(cell)?.try_into()?;
        let lock_path: PathBuf = space_location.lock(cell)?.try_into()?;

        // Acquire lock for exclusive access
        let _guard = PidlockGuard::new(lock_path)?;

        // Read current value to check CAS condition
        let current_bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Already deleted, succeed
                return Ok(());
            }
            Err(e) => return Err(MemoryError::Storage(e.to_string())),
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
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(MemoryError::Storage(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::Subject;
    use dialog_effects::memory::{Cell, Memory, Space};

    fn unique_subject(prefix: &str) -> Subject {
        Subject::from(format!(
            "did:test:{}-{}",
            prefix,
            dialog_common::time::now()
                .duration_since(dialog_common::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[dialog_common::test]
    async fn it_resolves_non_existent_cell() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-resolve-none");

        let effect = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("missing"))
            .invoke(Resolve);

        let result = effect.perform(&mut provider).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_new_content() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-publish-new");
        let content = b"hello world".to_vec();

        // Publish new content (when = None means expect empty)
        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&mut provider)
            .await?;

        assert!(!edition.is_empty());

        // Resolve to verify
        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);
        assert_eq!(publication.edition, edition);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_existing_content() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-publish-update");

        // Create initial content
        let edition1 = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"initial", None))
            .perform(&mut provider)
            .await?;

        // Update with correct edition
        let edition2 = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"updated", Some(edition1.clone())))
            .perform(&mut provider)
            .await?;

        assert_ne!(edition1, edition2);

        // Verify update
        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, b"updated".to_vec());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_on_edition_mismatch() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-mismatch");

        // Create initial content
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"initial", None))
            .perform(&mut provider)
            .await?;

        // Try to update with wrong edition
        let wrong_edition = Blake3Hash::hash(b"wrong").as_bytes().to_vec();
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"updated", Some(wrong_edition)))
            .perform(&mut provider)
            .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_creating_when_exists() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-create-exists");

        // Create initial content
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"initial", None))
            .perform(&mut provider)
            .await?;

        // Try to create again (when = None means expect empty)
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"new", None))
            .perform(&mut provider)
            .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_content() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-retract");

        // Create content
        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"to be deleted", None))
            .perform(&mut provider)
            .await?;

        // Retract with correct edition
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Retract::new(edition))
            .perform(&mut provider)
            .await?;

        // Verify deleted
        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;

        assert!(resolved.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_retract_on_edition_mismatch() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-retract-mismatch");

        // Create content
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(b"content", None))
            .perform(&mut provider)
            .await?;

        // Try to retract with wrong edition
        let wrong_edition = Blake3Hash::hash(b"wrong").as_bytes().to_vec();
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Retract::new(wrong_edition))
            .perform(&mut provider)
            .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_spaces() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-spaces");

        // Publish to different spaces
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("space1"))
            .attenuate(Cell::new("cell"))
            .invoke(Publish::new(b"content1", None))
            .perform(&mut provider)
            .await?;

        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("space2"))
            .attenuate(Cell::new("cell"))
            .invoke(Publish::new(b"content2", None))
            .perform(&mut provider)
            .await?;

        // Resolve from space1
        let result1 = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("space1"))
            .attenuate(Cell::new("cell"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;
        assert_eq!(result1.unwrap().content, b"content1".to_vec());

        // Resolve from space2
        let result2 = subject
            .attenuate(Memory)
            .attenuate(Space::new("space2"))
            .attenuate(Cell::new("cell"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;
        assert_eq!(result2.unwrap().content, b"content2".to_vec());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_with_stale_edition_when_value_matches() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-stale-ok");
        let content = b"desired value".to_vec();

        // Create initial content
        subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&mut provider)
            .await?;

        // Try to publish same content with wrong edition - should succeed
        let wrong_edition = Blake3Hash::hash(b"wrong").as_bytes().to_vec();
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("test"))
            .invoke(Publish::new(content.clone(), Some(wrong_edition)))
            .perform(&mut provider)
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
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-deterministic-hash");
        let content = b"same content".to_vec();

        // Create value at cell1
        let edition1 = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("cell1"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&mut provider)
            .await?;

        // Create same value at cell2
        let edition2 = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("cell2"))
            .invoke(Publish::new(content, None))
            .perform(&mut provider)
            .await?;

        // Same content should produce same edition (content hash)
        assert_eq!(edition1, edition2);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_retracting_already_retracted() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-retract-already-retracted");

        // Try to retract non-existent cell - should succeed
        let wrong_edition = Blake3Hash::hash(b"wrong").as_bytes().to_vec();
        let result = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("nonexistent"))
            .invoke(Retract::new(wrong_edition))
            .perform(&mut provider)
            .await;

        assert!(result.is_ok());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_nested_spaces() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-nested-spaces");
        let content = b"nested content".to_vec();

        // Publish to nested space path
        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("parent/child/grandchild"))
            .attenuate(Cell::new("cell"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&mut provider)
            .await?;

        assert!(!edition.is_empty());

        // Resolve to verify
        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("parent/child/grandchild"))
            .attenuate(Cell::new("cell"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_empty_content() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-empty");
        let content = vec![];

        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("empty"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&mut provider)
            .await?;

        assert!(!edition.is_empty());

        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("empty"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_content() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut provider = FileSystem::mount(tempdir.path().to_path_buf())?;
        let subject = unique_subject("memory-large");
        // 1MB content
        let content: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

        let edition = subject
            .clone()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("large"))
            .invoke(Publish::new(content.clone(), None))
            .perform(&mut provider)
            .await?;

        assert!(!edition.is_empty());

        let resolved = subject
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("large"))
            .invoke(Resolve)
            .perform(&mut provider)
            .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }
}
