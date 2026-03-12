use futures_core::Stream;
use futures_util::TryFutureExt;
use tokio::fs::File;
use tokio::io::AsyncWrite;
use tokio_util::io::ReaderStream;

use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};

use crate::DialogBlobsError;

/// A sandboxed virtual filesystem rooted at a fixed directory.
///
/// Every path provided to [`Vfs`] methods is resolved relative to its root.
/// Path components like `..` are normalized away, and any result that would
/// escape the root is rejected with a [`DialogBlobsError::Path`] error.
pub struct Vfs {
    root: PathBuf,
}

impl From<PathBuf> for Vfs {
    fn from(value: PathBuf) -> Self {
        Self::new(value)
    }
}

impl Vfs {
    /// Creates a new [`Vfs`] with the given directory as its root.
    ///
    /// The root path is used as-is; callers should ensure it is absolute and
    /// already exists on disk before performing any I/O.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Converts a given path into a normalized path rooted on the internal `root`.
    /// Prevents path traversal by resolving `..` components and verifying
    /// the result stays within the root directory.
    fn local_path(&self, given_path: &str) -> Result<PathBuf, DialogBlobsError> {
        let joined = self.root.join(given_path.trim_start_matches('/'));
        let mut normalized = PathBuf::new();
        for component in joined.components() {
            match component {
                std::path::Component::ParentDir => {
                    normalized.pop();
                }
                other => {
                    normalized.push(other.as_os_str());
                }
            }
        }
        if normalized.starts_with(&self.root) {
            Ok(normalized)
        } else {
            Err(DialogBlobsError::Path(
                "Resolved path does not fall inside of configured root".into(),
            ))
        }
    }

    /// Returns the parent directory of `path`, clamped to [`self.root`].
    ///
    /// If the path has no parent or its parent falls outside the root, the
    /// root itself is returned.
    fn parent_directory(&self, path: &Path) -> PathBuf {
        path.parent()
            .filter(|p| p.starts_with(&self.root))
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| self.root.clone())
    }

    /// Creates a [`Vfs`] rooted at the platform's data directory
    /// (`<data_dir>/dialog/blobs`).
    ///
    /// Returns [`DialogBlobsError::Initialization`] if the platform data
    /// directory cannot be determined.
    pub fn with_default_root() -> Result<Self, DialogBlobsError> {
        dirs::data_dir()
            .map(|path| path.join("dialog").join("blobs").into())
            .ok_or_else(|| {
                DialogBlobsError::Initialization("Could not determine data directory".to_string())
            })
    }

    /// Opens the file at `path` (relative to root) for reading and returns a
    /// [`FileReader`] that streams its contents as [`bytes::Bytes`] chunks.
    ///
    /// Returns [`DialogBlobsError::NotFound`] if the file does not exist, or
    /// [`DialogBlobsError::Get`] for other I/O failures.
    pub async fn read_from(&self, path: &str) -> Result<FileReader, DialogBlobsError> {
        self.local_path(path)
            .map(|path| {
                File::open(path)
                    .map_err(|error| {
                        if error.kind() == ErrorKind::NotFound {
                            DialogBlobsError::NotFound(format!("File not found: {error}"))
                        } else {
                            DialogBlobsError::Get(format!("Could not open file: {error}"))
                        }
                    })
                    .map_ok(FileReader::new)
            })?
            .await
    }

    /// Creates a file at the given path (relative to root) and returns a handle for writing.
    /// Intermediate directories are created as needed.
    pub async fn write_to(&self, path: &str) -> Result<FileWriter, DialogBlobsError> {
        self.local_path(path)
            .map(|path| {
                tokio::fs::create_dir_all(self.parent_directory(&path))
                    .map_err(|error| {
                        DialogBlobsError::Put(format!(
                            "Could not create directory structure: {error}"
                        ))
                    })
                    .and_then(|_| {
                        File::create(path)
                            .map_err(|error| {
                                DialogBlobsError::Put(format!("Could not create file: {error}"))
                            })
                            .map_ok(FileWriter::new)
                    })
            })?
            .await
    }

    /// Moves a file from one rooted path to another.
    ///
    /// Intermediate directories for the destination are created as needed.
    /// Both `from` and `to` are resolved relative to root.
    pub async fn move_file(&self, from: &str, to: &str) -> Result<(), DialogBlobsError> {
        let from_full_path = self.local_path(from)?;
        let to_full_path = self.local_path(to)?;

        let destination_directory = self.parent_directory(&to_full_path);

        tokio::fs::create_dir_all(&destination_directory)
            .map_err(|error| {
                DialogBlobsError::Put(format!("Could not create directory hierarchy: {error}"))
            })
            .and_then(|_| {
                tokio::fs::rename(&from_full_path, &to_full_path).map_err(|error| {
                    DialogBlobsError::Put(format!("Failed to move file: {}", error))
                })
            })
            .await
    }
}

/// A streaming reader for a file managed by [`Vfs`].
///
/// Implements [`Stream`]`<Item = Result<`[`bytes::Bytes`]`, `[`std::io::Error`]`>>`,
/// yielding the file contents in chunks suitable for forwarding over a
/// network or piping into another async sink.
#[derive(Debug)]
pub struct FileReader {
    file: ReaderStream<File>,
}

impl FileReader {
    fn new(file: File) -> Self {
        Self {
            file: ReaderStream::new(file),
        }
    }
}

impl Stream for FileReader {
    type Item = Result<bytes::Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().file).poll_next(cx)
    }
}

/// An async writer for a file managed by [`Vfs`].
///
/// Implements [`AsyncWrite`], delegating all operations to the underlying
/// tokio [`File`] handle.
pub struct FileWriter {
    file: File,
}

impl FileWriter {
    fn new(file: File) -> Self {
        Self { file }
    }
}

impl AsyncWrite for FileWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().file).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vfs(root: &str) -> Vfs {
        Vfs::new(PathBuf::from(root))
    }

    #[test]
    fn it_resolves_a_simple_filename_under_root() {
        let v = vfs("/data/blobs");
        assert_eq!(
            v.local_path("foo.txt").unwrap(),
            PathBuf::from("/data/blobs/foo.txt")
        );
    }

    #[test]
    fn it_strips_a_leading_slash_from_the_given_path() {
        let v = vfs("/data/blobs");
        assert_eq!(
            v.local_path("/sub/file.bin").unwrap(),
            PathBuf::from("/data/blobs/sub/file.bin")
        );
    }

    #[test]
    fn it_resolves_nested_paths() {
        let v = vfs("/data/blobs");
        assert_eq!(
            v.local_path("a/b/c.txt").unwrap(),
            PathBuf::from("/data/blobs/a/b/c.txt")
        );
    }

    #[test]
    fn it_rejects_traversal_above_root() {
        let v = vfs("/data/blobs");
        assert!(v.local_path("../../etc/passwd").is_err());
    }

    #[test]
    fn it_allows_parent_traversal_that_stays_within_root() {
        let v = vfs("/data/blobs");
        assert_eq!(
            v.local_path("a/b/../c.txt").unwrap(),
            PathBuf::from("/data/blobs/a/c.txt")
        );
    }

    #[test]
    fn it_rejects_a_path_that_escapes_to_roots_sibling() {
        let v = vfs("/data/blobs");
        assert!(v.local_path("../secret").is_err());
    }
}
