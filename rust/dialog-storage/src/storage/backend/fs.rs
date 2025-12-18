use crate::{DialogStorageError, StorageSink};
use async_trait::async_trait;
use base58::ToBase58;
use dialog_common::{ConditionalSend, ConditionalSync};
use futures_util::{Stream, TryStreamExt, future::try_join_all};
use pidlock::Pidlock;
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};

use super::{StorageBackend, TransactionalMemoryBackend};

/// A 32-byte content hash used as the edition for CAS operations.
pub type ContentHash = [u8; 32];

/// A basic file-system-based [StorageBackend] implementation. All values are
/// stored inside a root directory as files named after their (base58-encoded)
/// keys.
#[derive(Clone)]
pub struct FileSystemStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone,
{
    root_dir: PathBuf,
    key_type: PhantomData<Key>,
    value_type: PhantomData<Value>,
}

impl<Key, Value> FileSystemStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone,
{
    /// Creates a new [`FileSystemStorageBackend`] that stores files in
    /// `root_dir`.
    pub async fn new<Pathlike>(root_dir: Pathlike) -> Result<Self, DialogStorageError>
    where
        Pathlike: AsRef<Path>,
    {
        let root_dir = root_dir.as_ref().to_owned();
        tokio::fs::create_dir_all(&root_dir)
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        Ok(Self {
            root_dir,
            key_type: PhantomData,
            value_type: PhantomData,
        })
    }

    /// Encode a key to a filesystem-safe filename using base58.
    ///
    /// Used by `StorageBackend` to handle arbitrary binary keys.
    fn make_encoded_path(&self, key: &Key) -> Result<PathBuf, DialogStorageError>
    where
        Key: AsRef<[u8]>,
    {
        Ok(self.root_dir.join(key.as_ref().to_base58()))
    }

    /// Join the address with the root directory to form the file path.
    ///
    /// The address is treated as a path component. If it contains invalid
    /// path characters, filesystem operations will fail with an appropriate error.
    ///
    /// Used by `TransactionalMemoryBackend` which expects addresses to be valid paths.
    fn make_path(&self, key: &Key) -> Result<PathBuf, DialogStorageError>
    where
        Key: AsRef<[u8]>,
    {
        let key_str = std::str::from_utf8(key.as_ref())
            .map_err(|e| DialogStorageError::StorageBackend(format!("Invalid address: {e}")))?;
        Ok(self.root_dir.join(key_str))
    }

    fn make_lock_path(&self, key: &Key) -> Result<PathBuf, DialogStorageError>
    where
        Key: AsRef<[u8]>,
    {
        let mut path = self.make_path(key)?;
        path.set_extension("lock");
        Ok(path)
    }

    fn make_temp_path(&self, key: &Key, hash: &ContentHash) -> Result<PathBuf, DialogStorageError>
    where
        Key: AsRef<[u8]>,
    {
        let hash_suffix = hash.to_base58();
        let mut path = self.make_path(key)?;
        path.set_extension(format!("{}.tmp", hash_suffix));
        Ok(path)
    }
}

/// Compute BLAKE3 hash of content.
fn content_hash(content: &[u8]) -> ContentHash {
    blake3::hash(content).into()
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
    fn new(path: PathBuf) -> Result<Self, DialogStorageError> {
        let path = path.to_str().ok_or_else(|| {
            DialogStorageError::StorageBackend("Lock path is not valid UTF-8".to_string())
        })?;

        let mut lock = Pidlock::new(path);

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
                            return Err(DialogStorageError::StorageBackend(format!(
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
                    return Err(DialogStorageError::StorageBackend(format!(
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

#[async_trait]
impl<Key, Value> StorageBackend for FileSystemStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + Clone + From<Vec<u8>> + ConditionalSync,
{
    type Key = Key;
    type Value = Value;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        tokio::fs::write(self.make_encoded_path(&key)?, value)
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
        Ok(())
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let path = self.make_encoded_path(key)?;
        if !path.exists() {
            return Ok(None);
        }

        tokio::fs::read(path)
            .await
            .map(|value| Some(Value::from(value)))
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))
    }
}

/// Transactional memory backend using PID-based file locking and content hashing for CAS.
///
/// This implementation uses a PID lock file to serialize writes, with BLAKE3 content
/// hashes as editions. This provides reliable optimistic concurrency for file-based
/// storage with automatic stale lock recovery.
///
/// # Edition Strategy
///
/// Uses a BLAKE3 hash of the file contents as the edition ([`ContentHash`]). CAS succeeds
/// when the current file's hash matches the expected edition. This eliminates issues with
/// filesystem timestamp resolution and clock skew.
///
/// # Locking Protocol
///
/// Uses [`pidlock`] for cross-platform PID-based file locking:
/// 1. Acquire exclusive lock (`.lock` file with our PID)
/// 2. Read current content and verify hash matches expected edition
/// 3. Write new content via atomic temp file + rename
/// 4. Release lock
///
/// If a process crashes while holding the lock, `pidlock` detects the stale lock
/// (dead PID) and allows recovery.
///
/// # Limitations
///
/// - **Single-machine only**: PID-based locking doesn't work across network filesystems
/// - **Blocking lock acquisition**: Writers wait for the lock (no immediate failure option)
///
/// For distributed scenarios, use S3 with ETags which provides server-enforced CAS.
#[async_trait]
impl<Key, Value> TransactionalMemoryBackend for FileSystemStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + Clone + From<Vec<u8>> + ConditionalSync,
{
    type Address = Key;
    type Value = Value;
    type Error = DialogStorageError;
    type Edition = ContentHash;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        let path = self.make_path(address)?;

        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let hash = content_hash(&bytes);
                Ok(Some((Value::from(bytes), hash)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(DialogStorageError::StorageBackend(format!("{e}"))),
        }
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        let path = self.make_path(address)?;
        let _lock = PidlockGuard::new(self.make_lock_path(address)?)?;

        // Read current content and compute hash
        let (current_bytes, current_hash) = match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let hash = content_hash(&bytes);
                (Some(bytes), Some(hash))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (None, None),
            Err(e) => return Err(DialogStorageError::StorageBackend(format!("{e}"))),
        };

        // Perform the operation
        match content {
            Some(new_value) => {
                let new_bytes = new_value.as_ref();
                let new_hash = content_hash(new_bytes);

                // If current value already matches desired value, succeed without writing
                if current_hash.as_ref() == Some(&new_hash) {
                    return Ok(Some(new_hash));
                }

                // Check edition only if we need to write
                if current_hash.as_ref() != edition {
                    return Err(DialogStorageError::StorageBackend(
                        "CAS condition failed: edition mismatch".to_string(),
                    ));
                }

                // Write to temp file (hash in name prevents conflicts if cleanup fails)
                let temp_path = self.make_temp_path(address, &new_hash)?;
                tokio::fs::write(&temp_path, new_bytes)
                    .await
                    .map_err(|e| DialogStorageError::StorageBackend(format!("{e}")))?;

                // Atomic rename
                tokio::fs::rename(&temp_path, &path)
                    .await
                    .map_err(|e| DialogStorageError::StorageBackend(format!("{e}")))?;

                Ok(Some(new_hash))
            }
            None => {
                // Delete operation - if already deleted, succeed
                if current_bytes.is_none() {
                    return Ok(None);
                }

                // Check edition only if we need to delete
                if current_hash.as_ref() != edition {
                    return Err(DialogStorageError::StorageBackend(
                        "CAS condition failed: edition mismatch".to_string(),
                    ));
                }

                match tokio::fs::remove_file(&path).await {
                    Ok(()) => Ok(None),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(DialogStorageError::StorageBackend(format!("{e}"))),
                }
            }
        }
    }
}

#[async_trait]
impl<Key, Value> StorageSink for FileSystemStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + Clone + From<Vec<u8>> + ConditionalSync,
{
    async fn write<EntryStream>(
        &mut self,
        stream: EntryStream,
    ) -> Result<(), <Self as StorageBackend>::Error>
    where
        EntryStream: Stream<
                Item = Result<
                    (
                        <Self as StorageBackend>::Key,
                        <Self as StorageBackend>::Value,
                    ),
                    <Self as StorageBackend>::Error,
                >,
            > + ConditionalSend,
    {
        tokio::pin!(stream);

        let mut writes = Vec::new();

        while let Some((key, value)) = stream.try_next().await? {
            let path = self.make_encoded_path(&key)?;
            writes.push(async move {
                tokio::fs::write(path, value)
                    .await
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
                Ok(()) as Result<_, Self::Error>
            });
        }

        try_join_all(writes.into_iter()).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    async fn make_backend() -> Result<(FileSystemStorageBackend<String, Vec<u8>>, tempfile::TempDir)>
    {
        let tempdir = tempfile::tempdir()?;
        let backend = FileSystemStorageBackend::new(tempdir.path()).await?;
        Ok((backend, tempdir))
    }

    // StorageBackend tests

    #[dialog_common::test]
    async fn it_returns_none_for_non_existent_key() -> Result<()> {
        let (backend, _tempdir) = make_backend().await?;

        let result = backend.get(&"missing".to_string()).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_sets_and_gets_value() -> Result<()> {
        let (mut backend, _tempdir) = make_backend().await?;

        let key = "test-key".to_string();
        let value = b"test-value".to_vec();

        backend.set(key.clone(), value.clone()).await?;

        let result = backend.get(&key).await?;
        assert_eq!(result, Some(value));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_overwrites_existing_value() -> Result<()> {
        let (mut backend, _tempdir) = make_backend().await?;

        let key = "test-key".to_string();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        backend.set(key.clone(), value1).await?;
        backend.set(key.clone(), value2.clone()).await?;

        let result = backend.get(&key).await?;
        assert_eq!(result, Some(value2));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_binary_keys() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let mut backend = FileSystemStorageBackend::<Vec<u8>, Vec<u8>>::new(tempdir.path()).await?;

        // Binary key with non-UTF8 bytes
        let key = vec![0x00, 0xff, 0xfe, 0x01];
        let value = b"binary key value".to_vec();

        backend.set(key.clone(), value.clone()).await?;

        let result = backend.get(&key).await?;
        assert_eq!(result, Some(value));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_empty_value() -> Result<()> {
        let (mut backend, _tempdir) = make_backend().await?;

        let key = "empty-key".to_string();
        let value = vec![];

        backend.set(key.clone(), value.clone()).await?;

        let result = backend.get(&key).await?;
        assert_eq!(result, Some(value));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_multiple_keys() -> Result<()> {
        let (mut backend, _tempdir) = make_backend().await?;

        let key1 = "key1".to_string();
        let key2 = "key2".to_string();
        let key3 = "key3".to_string();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();
        let value3 = b"value3".to_vec();

        backend.set(key1.clone(), value1.clone()).await?;
        backend.set(key2.clone(), value2.clone()).await?;
        backend.set(key3.clone(), value3.clone()).await?;

        assert_eq!(backend.get(&key1).await?, Some(value1));
        assert_eq!(backend.get(&key2).await?, Some(value2));
        assert_eq!(backend.get(&key3).await?, Some(value3));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_value() -> Result<()> {
        let (mut backend, _tempdir) = make_backend().await?;

        let key = "large-key".to_string();
        // 1MB value
        let value: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

        backend.set(key.clone(), value.clone()).await?;

        let result = backend.get(&key).await?;
        assert_eq!(result, Some(value));
        Ok(())
    }

    // TransactionalMemoryBackend tests

    #[dialog_common::test]
    async fn it_resolves_non_existent_address() -> Result<()> {
        let (backend, _tempdir) = make_backend().await?;

        let result = backend.resolve(&"missing".to_string()).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_creates_new_value() -> Result<()> {
        let (backend, _tempdir) = make_backend().await?;

        // Create new value (edition = None means "expect not to exist")
        let content = b"hello world".to_vec();
        let edition = backend
            .replace(&"test-key".to_string(), None, Some(content.clone()))
            .await?;

        assert!(edition.is_some());

        // Verify it can be resolved
        let resolved = backend.resolve(&"test-key".to_string()).await?;
        assert!(resolved.is_some());
        let (value, resolved_edition) = resolved.unwrap();
        assert_eq!(value, content);
        assert_eq!(Some(resolved_edition), edition);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_existing_value() -> Result<()> {
        let (backend, _tempdir) = make_backend().await?;

        // Create initial value
        let initial = b"initial".to_vec();
        let edition1 = backend
            .replace(&"test-key".to_string(), None, Some(initial))
            .await?
            .unwrap();

        // Update with correct edition
        let updated = b"updated".to_vec();
        let edition2 = backend
            .replace(
                &"test-key".to_string(),
                Some(&edition1),
                Some(updated.clone()),
            )
            .await?;

        assert!(edition2.is_some());
        assert_ne!(edition1, edition2.unwrap());

        // Verify update
        let (value, _) = backend.resolve(&"test-key".to_string()).await?.unwrap();
        assert_eq!(value, updated);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_on_edition_mismatch() -> Result<()> {
        let (backend, _tempdir) = make_backend().await?;

        // Create initial value
        let initial = b"initial".to_vec();
        let _edition = backend
            .replace(&"test-key".to_string(), None, Some(initial))
            .await?;

        // Try to update with wrong edition
        let wrong_edition = content_hash(b"wrong");
        let result = backend
            .replace(
                &"test-key".to_string(),
                Some(&wrong_edition),
                Some(b"new value".to_vec()),
            )
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("edition mismatch"));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_creating_when_exists() -> Result<()> {
        let (backend, _tempdir) = make_backend().await?;

        // Create initial value
        backend
            .replace(&"test-key".to_string(), None, Some(b"exists".to_vec()))
            .await?;

        // Try to create again (edition = None means "expect not to exist")
        let result = backend
            .replace(&"test-key".to_string(), None, Some(b"new".to_vec()))
            .await;

        assert!(result.is_err());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_deletes_value() -> Result<()> {
        let (backend, _tempdir) = make_backend().await?;

        // Create value
        let edition = backend
            .replace(&"test-key".to_string(), None, Some(b"to delete".to_vec()))
            .await?
            .unwrap();

        // Delete with correct edition
        let result = backend
            .replace(&"test-key".to_string(), Some(&edition), None)
            .await?;

        assert!(result.is_none());

        // Verify deleted
        let resolved = backend.resolve(&"test-key".to_string()).await?;
        assert!(resolved.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_subdirectory_addresses() -> Result<()> {
        let (backend, tempdir) = make_backend().await?;

        // Create subdirectory structure
        let subdir = tempdir.path().join("subdir");
        tokio::fs::create_dir_all(&subdir).await?;

        // Use address with subdirectory
        let content = b"nested content".to_vec();
        let edition = backend
            .replace(
                &"subdir/nested-key".to_string(),
                None,
                Some(content.clone()),
            )
            .await?;

        assert!(edition.is_some());

        let (value, _) = backend
            .resolve(&"subdir/nested-key".to_string())
            .await?
            .unwrap();
        assert_eq!(value, content);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_invalid_utf8_address() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let backend = FileSystemStorageBackend::<Vec<u8>, Vec<u8>>::new(tempdir.path()).await?;

        // Invalid UTF-8 bytes
        let invalid_address = vec![0xff, 0xfe];
        let result = backend.resolve(&invalid_address).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid address"));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_with_stale_edition_when_value_matches() -> Result<()> {
        let (backend, _tempdir) = make_backend().await?;

        // Create initial value
        let content = b"desired value".to_vec();
        let _edition = backend
            .replace(&"test-key".to_string(), None, Some(content.clone()))
            .await?;

        // Try to replace with wrong edition but same value - should succeed
        let wrong_edition = content_hash(b"wrong");
        let result = backend
            .replace(
                &"test-key".to_string(),
                Some(&wrong_edition),
                Some(content.clone()),
            )
            .await;

        assert!(result.is_ok());
        // Should return the hash of the content
        assert_eq!(result.unwrap(), Some(content_hash(&content)));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_deleting_already_deleted() -> Result<()> {
        let (backend, _tempdir) = make_backend().await?;

        // Try to delete non-existent key with wrong edition - should succeed
        let wrong_edition = content_hash(b"wrong");
        let result = backend
            .replace(&"test-key".to_string(), Some(&wrong_edition), None)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
        Ok(())
    }
}
