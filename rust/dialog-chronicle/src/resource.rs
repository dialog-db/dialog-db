//! Opening a [`Chronicle`] as the archive of a space.

use std::fmt;
use std::path::PathBuf;

use async_trait::async_trait;
use dialog_effects::archive::ArchiveError;
use dialog_effects::storage::Location;
use dialog_storage::provider::FileSystemHandle;
use dialog_storage::resource::Resource;

use crate::Chronicle;

/// The name of the archive file in a space's directory.
pub const FILE_NAME: &str = "archive.dialog";

/// Why a [`Chronicle`] could not be opened from a [`Location`].
#[derive(Debug)]
pub enum OpenError {
    /// There is no archive file at the location, and the open was not
    /// allowed to create one.
    NotFound(PathBuf),
    /// The location does not name a directory, or the file there could not
    /// be opened.
    Storage(String),
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenError::NotFound(path) => write!(f, "no archive file at {}", path.display()),
            OpenError::Storage(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for OpenError {}

impl From<ArchiveError> for OpenError {
    fn from(error: ArchiveError) -> Self {
        OpenError::Storage(error.to_string())
    }
}

/// The archive file of the space at `location`: [`FILE_NAME`] in the
/// directory the `fs` provider would keep that space in.
pub fn file_at(location: &Location) -> Result<PathBuf, OpenError> {
    let handle = FileSystemHandle::try_from(location)
        .map_err(|error| OpenError::Storage(error.to_string()))?;
    let directory =
        PathBuf::try_from(&handle).map_err(|error| OpenError::Storage(error.to_string()))?;
    Ok(directory.join(FILE_NAME))
}

/// A space's archive, opened from its location: the space's directory holds
/// one archive file instead of a directory of blocks, so a `Space` can keep
/// its blocks in a [`Chronicle`] and everything else where it was.
#[async_trait]
impl Resource<Location> for Chronicle {
    type Error = OpenError;

    async fn open(location: &Location) -> Result<Self, OpenError> {
        let path = file_at(location)?;
        if let Some(directory) = path.parent() {
            std::fs::create_dir_all(directory)
                .map_err(|error| OpenError::Storage(format!("{}: {error}", directory.display())))?;
        }
        Ok(Chronicle::open(&path)?)
    }

    fn is_not_found(error: &OpenError) -> bool {
        matches!(error, OpenError::NotFound(_))
    }

    async fn load(location: &Location) -> Result<Self, OpenError> {
        let path = file_at(location)?;
        if !path.exists() {
            return Err(OpenError::NotFound(path));
        }
        Ok(Chronicle::open(&path)?)
    }
}
