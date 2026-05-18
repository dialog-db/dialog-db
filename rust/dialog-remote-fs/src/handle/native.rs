//! Native filesystem handle backed by `tokio::fs` and `PathBuf`.
//!
//! Containment is enforced at `resolve` time: segments containing `..`,
//! path separators, or absolute paths are rejected. The on-disk layout is
//! byte-identical to `dialog_storage::storage::provider::fs::FileSystem`
//! so a directory written by this handle can be read by any
//! `dialog-storage` consumer using that provider, and vice versa.

use super::FsHandle;
use crate::FsError;
use async_trait::async_trait;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs;

/// A handle to a directory or file under a registered FS-remote root.
#[derive(Clone, Debug)]
pub(crate) struct NativeHandle {
    path: PathBuf,
}

impl NativeHandle {
    /// Construct a handle for the given path. Callers should pass an
    /// absolute path — relative paths are not normalised here.
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// The underlying filesystem path this handle points at.
    #[allow(dead_code)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

fn validate_segment(segment: &str) -> Result<(), FsError> {
    if segment.is_empty() {
        return Err(FsError::Containment(
            "empty path segment is not allowed".into(),
        ));
    }
    if segment == "." || segment == ".." {
        return Err(FsError::Containment(format!(
            "navigation segment '{}' is not allowed",
            segment
        )));
    }
    if segment.contains('\0') {
        return Err(FsError::Containment(
            "NUL byte in path segment is not allowed".into(),
        ));
    }
    // Reject path separators inside a single segment; callers must
    // resolve one segment at a time.
    if segment.contains('/') || segment.contains('\\') {
        return Err(FsError::Containment(format!(
            "path separator in segment '{}' is not allowed",
            segment
        )));
    }
    if Path::new(segment).is_absolute() {
        return Err(FsError::Containment(format!(
            "absolute path segment '{}' is not allowed",
            segment
        )));
    }
    Ok(())
}

#[async_trait]
impl FsHandle for NativeHandle {
    async fn resolve(&self, segment: &str) -> Result<Self, FsError> {
        validate_segment(segment)?;
        Ok(Self::new(self.path.join(segment)))
    }

    async fn read_optional(&self) -> Result<Option<Vec<u8>>, FsError> {
        match fs::read(&self.path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(FsError::Io(e.to_string())),
        }
    }

    async fn write(&self, contents: &[u8]) -> Result<(), FsError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| FsError::Io(e.to_string()))?;
        }
        fs::write(&self.path, contents)
            .await
            .map_err(|e| FsError::Io(e.to_string()))
    }

    async fn rename(&self, to: &Self) -> Result<(), FsError> {
        if let Some(parent) = to.path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| FsError::Io(e.to_string()))?;
        }
        fs::rename(&self.path, &to.path)
            .await
            .map_err(|e| FsError::Io(e.to_string()))
    }

    async fn remove(&self) -> Result<(), FsError> {
        match fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(FsError::Io(e.to_string())),
        }
    }

    async fn list(&self) -> Result<Vec<String>, FsError> {
        let mut entries = match fs::read_dir(&self.path).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(FsError::Io(e.to_string())),
        };

        let mut names = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| FsError::Io(e.to_string()))?
        {
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }
        Ok(names)
    }

    async fn exists(&self) -> bool {
        fs::metadata(&self.path).await.is_ok()
    }

    async fn ensure_dir(&self) -> Result<(), FsError> {
        fs::create_dir_all(&self.path)
            .await
            .map_err(|e| FsError::Io(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[dialog_common::test]
    async fn it_rejects_dotdot_segments() {
        let tmp = tempdir().unwrap();
        let handle = NativeHandle::new(tmp.path().to_path_buf());
        let err = handle.resolve("..").await.unwrap_err();
        assert!(matches!(err, FsError::Containment(_)));
    }

    #[dialog_common::test]
    async fn it_rejects_path_separators_in_segments() {
        let tmp = tempdir().unwrap();
        let handle = NativeHandle::new(tmp.path().to_path_buf());
        let err = handle.resolve("foo/bar").await.unwrap_err();
        assert!(matches!(err, FsError::Containment(_)));
    }

    #[dialog_common::test]
    async fn it_reads_returns_none_for_missing() {
        let tmp = tempdir().unwrap();
        let handle = NativeHandle::new(tmp.path().join("missing"));
        let result = handle.read_optional().await.unwrap();
        assert_eq!(result, None);
    }

    #[dialog_common::test]
    async fn it_writes_and_reads_back_a_file() {
        let tmp = tempdir().unwrap();
        let handle = NativeHandle::new(tmp.path().to_path_buf())
            .resolve("sub")
            .await
            .unwrap()
            .resolve("hello")
            .await
            .unwrap();
        handle.write(b"world").await.unwrap();
        let read_back = handle.read_optional().await.unwrap();
        assert_eq!(read_back.as_deref(), Some(b"world".as_slice()));
    }
}
