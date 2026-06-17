//! Native filesystem backend for [`FileSystemHandle`], backed by [`tokio::fs`].
//!
//! The handle's `file:` URL is converted to a [`PathBuf`] for each operation.
//! Path layout and containment live in the shared [`super`] module; this file
//! only performs I/O.

use super::{FileSystem, FileSystemError, FileSystemHandle};
use crate::resource::Resource;
use dialog_effects::storage::{Directory, Location};
use std::env;
use std::io;
use std::path::PathBuf;
use tokio::fs;
use url::Url;

/// Subdirectory name used to namespace dialog storage inside the
/// platform's data and temp directories.
pub(super) const STORAGE_NAMESPACE: &str = "dialog";

#[async_trait::async_trait]
impl Resource<Location> for FileSystem {
    type Error = FileSystemError;

    async fn open(location: &Location) -> Result<Self, FileSystemError> {
        Ok(Self(FileSystemHandle::try_from(location)?))
    }
}

/// Resolve a `Location` (Directory + name) to a filesystem handle.
impl TryFrom<&Location> for FileSystemHandle {
    type Error = FileSystemError;

    fn try_from(location: &Location) -> Result<Self, FileSystemError> {
        let base = match &location.directory {
            Directory::Profile => {
                let data_dir = dirs::data_dir().ok_or_else(|| {
                    FileSystemError::Io("could not determine platform data directory".into())
                })?;
                data_dir.join(STORAGE_NAMESPACE)
            }
            Directory::Current => {
                env::current_dir().map_err(|e| FileSystemError::Io(e.to_string()))?
            }
            Directory::Temp => env::temp_dir(),
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

impl TryFrom<PathBuf> for FileSystemHandle {
    type Error = FileSystemError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        // Ensure the path is absolute for proper URL conversion
        let absolute = if path.is_absolute() {
            path
        } else {
            env::current_dir()
                .map_err(|e| FileSystemError::Io(e.to_string()))?
                .join(path)
        };

        // Convert to file: URL, ensuring trailing slash for directory
        let url = Url::from_file_path(&absolute)
            .map_err(|_| FileSystemError::Io("Invalid path for URL conversion".to_string()))?;

        url.try_into()
    }
}

impl TryFrom<FileSystemHandle> for PathBuf {
    type Error = FileSystemError;

    fn try_from(location: FileSystemHandle) -> Result<Self, Self::Error> {
        PathBuf::try_from(&location)
    }
}

impl TryFrom<&FileSystemHandle> for PathBuf {
    type Error = FileSystemError;

    fn try_from(location: &FileSystemHandle) -> Result<Self, Self::Error> {
        let path = location
            .url()
            .to_file_path()
            .map_err(|_| FileSystemError::Io("Failed to convert URL to path".to_string()))?;

        // Strip trailing slash added by FileSystemHandle for URL semantics.
        // Filesystem operations (read, write, rename) need clean file paths.
        // Use `to_str` (not `to_string_lossy`) so non-UTF-8 paths don't
        // collide via lossy substitutions; require UTF-8 for the trim.
        let s = path.to_str().ok_or_else(|| {
            FileSystemError::Io("Path is not valid UTF-8 and cannot be normalized".to_string())
        })?;
        if s.ends_with('/') && s.len() > 1 {
            Ok(PathBuf::from(s.trim_end_matches('/')))
        } else {
            Ok(path)
        }
    }
}

pub(super) async fn ensure_dir(handle: &FileSystemHandle) -> Result<(), FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    fs::create_dir_all(&path)
        .await
        .map_err(|e| FileSystemError::Io(e.to_string()))
}

pub(super) async fn read(handle: &FileSystemHandle) -> Result<Vec<u8>, FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    fs::read(&path)
        .await
        .map_err(|e| FileSystemError::Io(e.to_string()))
}

pub(super) async fn read_optional(
    handle: &FileSystemHandle,
) -> Result<Option<Vec<u8>>, FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    match fs::read(&path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(FileSystemError::Io(e.to_string())),
    }
}

pub(super) async fn write(
    handle: &FileSystemHandle,
    contents: &[u8],
) -> Result<(), FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))?;
    }
    fs::write(&path, contents)
        .await
        .map_err(|e| FileSystemError::Io(e.to_string()))
}

pub(super) async fn rename(
    from: &FileSystemHandle,
    to: &FileSystemHandle,
) -> Result<(), FileSystemError> {
    let from_path: PathBuf = from.try_into()?;
    let to_path: PathBuf = to.try_into()?;
    fs::rename(&from_path, &to_path)
        .await
        .map_err(|e| FileSystemError::Io(e.to_string()))
}

pub(super) async fn remove(handle: &FileSystemHandle) -> Result<(), FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    match fs::remove_file(&path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(FileSystemError::Io(e.to_string())),
    }
}

pub(super) async fn list(handle: &FileSystemHandle) -> Result<Vec<String>, FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    let mut entries = match fs::read_dir(&path).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
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

pub(super) async fn exists(handle: &FileSystemHandle) -> bool {
    let Ok(path) = PathBuf::try_from(handle) else {
        return false;
    };
    path.exists()
}

/// Acquire a cross-process PID lock for the given handle's CAS critical
/// section. The lock file lives at `{path}.lock`.
pub(crate) async fn lock(handle: &FileSystemHandle) -> Result<LockGuard, FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    let lock_path = path.with_extension("lock");
    // Ensure parent directory exists for the lock file
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| FileSystemError::Io(e.to_string()))?;
    }
    LockGuard::acquire(lock_path)
}

/// RAII guard that acquires a PID lock and releases it when dropped.
///
/// Handles stale lock detection and recovery automatically.
pub(crate) struct LockGuard(pidlock::Pidlock);

impl LockGuard {
    /// Acquire a PID lock at the given path.
    ///
    /// If a stale lock exists (from a dead process), it will be automatically
    /// cleaned up and the lock acquired.
    ///
    /// If the lock is held by an active process, returns an error immediately
    /// rather than waiting. This is intentional - the STM layer will retry
    /// the entire transaction, which is the correct behavior since the locked
    /// value will likely change anyway.
    fn acquire(path: PathBuf) -> Result<Self, FileSystemError> {
        // pidlock 0.2 handles stale-lock cleanup atomically inside acquire()
        // and surfaces malformed paths (directory at lock path, etc.) as
        // PidlockError::IOError instead of panicking — so no defensive
        // pre-check or retry loop is needed. new_validated also rejects
        // unusable paths up front and creates the parent directory.
        let mut lock = pidlock::Pidlock::new_validated(&path)
            .map_err(|e| FileSystemError::Lock(format!("Invalid lock path: {e:?}")))?;

        match lock.acquire() {
            Ok(()) => Ok(Self(lock)),
            Err(pidlock::PidlockError::LockExists) => {
                // Holder is alive. Look up its PID for diagnostics; if we
                // can't read it, just say so.
                let holder = lock
                    .get_owner()
                    .ok()
                    .flatten()
                    .map(|pid| pid.to_string())
                    .unwrap_or_else(|| "<unknown>".into());
                Err(FileSystemError::Lock(format!(
                    "Concurrent write in progress (lock held by pid {holder})",
                )))
            }
            Err(e) => Err(FileSystemError::Lock(format!(
                "Failed to acquire lock: {e:?}"
            ))),
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = self.0.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    async fn it_fails_lock_when_held_by_same_process() -> anyhow::Result<()> {
        // Verifies that when our own process holds the lock, acquire returns
        // an error immediately (not a spin). This matters because all tests
        // run in the same process and share a PID.
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        let _guard = LockGuard::acquire(lock_path.clone())?;

        // Second acquire from same process should fail immediately
        let result = LockGuard::acquire(lock_path);
        let err = match result {
            Ok(_) => panic!("expected lock to fail when held by same process"),
            Err(e) => e,
        };
        assert!(
            matches!(err, FileSystemError::Lock(_)),
            "expected Lock error, got: {err:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_lock_with_trailing_slash_path() -> anyhow::Result<()> {
        // Reproduces the bug where FileSystemHandle's trailing-slash URLs
        // produced PathBufs like "/tmp/.../test.lock/" which pidlock can
        // never create (create_new fails on trailing-slash paths) and
        // get_owner returns None (no file exists), causing an infinite
        // busy loop in the old unbounded retry code.
        let dir = tempfile::tempdir()?;
        let bad_path = dir.path().join("cell.lock/"); // trailing slash
        let result = LockGuard::acquire(bad_path);

        // Should fail with a bounded retry error, not spin forever
        let err = match result {
            Ok(_) => panic!("expected lock to fail with trailing-slash path"),
            Err(e) => e,
        };
        assert!(
            matches!(err, FileSystemError::Lock(_)),
            "expected Lock error, got: {err:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_lock_when_directory_exists_at_lock_path() -> anyhow::Result<()> {
        // If a directory exists where the lock file should be (e.g. from
        // a previous buggy run), acquire should fail, not spin or panic.
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        std::fs::create_dir_all(&lock_path)?;
        assert!(lock_path.is_dir());

        let err = match LockGuard::acquire(lock_path) {
            Ok(_) => panic!("expected lock to fail when a directory exists at the lock path"),
            Err(e) => e,
        };
        assert!(
            matches!(err, FileSystemError::Lock(_)),
            "expected Lock error, got: {err:?}"
        );
        Ok(())
    }
}
