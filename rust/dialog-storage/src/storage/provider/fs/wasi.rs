//! The filesystem backend of a WASI component, which has none.
//!
//! A component on WASI, such as one built for `wasm32-wasip3`, reaches its
//! storage through the host that runs it, not through a file system. The
//! [`FileSystem`] type and its providers still compile, so that a site that
//! names it (the FS remote, say) compiles too, but no [`FileSystem`] ever
//! opens: [`Resource::open`] refuses, and so each call below is unreachable in
//! practice and refuses as well.

use super::{FileReader, FileSystem, FileSystemError, FileSystemHandle};
use crate::resource::Resource;
use dialog_effects::storage::Location;

/// Why each call of this backend refuses.
fn unsupported() -> FileSystemError {
    FileSystemError::Io(
        "this target has no file system; storage is reached through the host".into(),
    )
}

#[async_trait::async_trait(?Send)]
impl Resource<Location> for FileSystem {
    type Error = FileSystemError;

    async fn open(_location: &Location) -> Result<Self, FileSystemError> {
        Err(unsupported())
    }
}

pub(super) async fn ensure_dir(_handle: &FileSystemHandle) -> Result<(), FileSystemError> {
    Err(unsupported())
}

pub(super) async fn read(_handle: &FileSystemHandle) -> Result<Vec<u8>, FileSystemError> {
    Err(unsupported())
}

pub(super) async fn read_optional(
    _handle: &FileSystemHandle,
) -> Result<Option<Vec<u8>>, FileSystemError> {
    Err(unsupported())
}

pub(super) async fn files(_handle: &FileSystemHandle) -> Result<Vec<String>, FileSystemError> {
    Err(unsupported())
}

pub(super) async fn write(
    _handle: &FileSystemHandle,
    _contents: &[u8],
) -> Result<(), FileSystemError> {
    Err(unsupported())
}

pub(super) async fn write_atomic(
    _handle: &FileSystemHandle,
    _contents: &[u8],
) -> Result<(), FileSystemError> {
    Err(unsupported())
}

pub(super) async fn rename(
    _from: &FileSystemHandle,
    _to: &FileSystemHandle,
) -> Result<(), FileSystemError> {
    Err(unsupported())
}

pub(super) async fn remove(_handle: &FileSystemHandle) -> Result<(), FileSystemError> {
    Err(unsupported())
}

pub(super) async fn list(_handle: &FileSystemHandle) -> Result<Vec<String>, FileSystemError> {
    Err(unsupported())
}

pub(super) async fn size(_handle: &FileSystemHandle) -> Result<Option<u64>, FileSystemError> {
    Err(unsupported())
}

pub(super) async fn read_at(
    _handle: &FileSystemHandle,
    _offset: u64,
    _len: usize,
) -> Result<Vec<u8>, FileSystemError> {
    Err(unsupported())
}

pub(super) async fn write_at(
    _handle: &FileSystemHandle,
    _offset: u64,
    _bytes: &[u8],
) -> Result<(), FileSystemError> {
    Err(unsupported())
}

pub(super) async fn exists(_handle: &FileSystemHandle) -> bool {
    false
}

pub(super) async fn open_reader(
    _handle: &FileSystemHandle,
    _offset: u64,
    _len: Option<u64>,
) -> Result<FileReader, FileSystemError> {
    Err(unsupported())
}

pub(super) async fn open_writer(_handle: &FileSystemHandle) -> Result<FileWriter, FileSystemError> {
    Err(unsupported())
}

/// A streaming writer, which this backend never opens.
pub struct FileWriter(std::convert::Infallible);

impl FileWriter {
    /// Append a chunk to the staged file.
    pub async fn write_all(&mut self, _bytes: &[u8]) -> Result<(), FileSystemError> {
        match self.0 {}
    }

    /// Flush and atomically move the staged file into place.
    pub async fn finish(self) -> Result<(), FileSystemError> {
        match self.0 {}
    }

    /// Flush and atomically move the staged file to `dest`.
    pub async fn finish_to(self, _dest: &FileSystemHandle) -> Result<(), FileSystemError> {
        match self.0 {}
    }

    /// Discard the staged content without committing it.
    pub async fn discard(self) -> Result<(), FileSystemError> {
        match self.0 {}
    }
}

pub(crate) async fn lock(_handle: &FileSystemHandle) -> Result<LockGuard, FileSystemError> {
    Err(unsupported())
}

/// The guard of a lock, which this backend never takes.
pub(crate) struct LockGuard;
