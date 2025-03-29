use crate::XStorageError;
use async_trait::async_trait;
use base58::ToBase58;
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};
use x_common::ConditionalSync;

use super::StorageBackend;

/// A basic file-system-based [StorageBackend] implementation. All values are
/// stored inside a root directory as files named after their (base58-encoded)
/// keys.
#[derive(Clone)]
pub struct FileSystemStorageBackend<Key, Value>
where
    Key: AsRef<[u8]>,
    Value: AsRef<[u8]> + From<Vec<u8>>,
{
    root_dir: PathBuf,
    key_type: PhantomData<Key>,
    value_type: PhantomData<Value>,
}

impl<Key, Value> FileSystemStorageBackend<Key, Value>
where
    Key: AsRef<[u8]>,
    Value: AsRef<[u8]> + From<Vec<u8>>,
{
    /// Creates a new [`FileSystemStorageBackend`] that stores files in
    /// `root_dir`.
    pub async fn new<Pathlike>(root_dir: Pathlike) -> Result<Self, XStorageError>
    where
        Pathlike: AsRef<Path>,
    {
        let root_dir = root_dir.as_ref().to_owned();
        tokio::fs::create_dir_all(&root_dir)
            .await
            .map_err(|error| XStorageError::StorageBackend(format!("{error}")))?;
        Ok(Self {
            root_dir,
            key_type: PhantomData,
            value_type: PhantomData,
        })
    }

    fn make_path(&self, key: &Key) -> Result<PathBuf, XStorageError>
    where
        Key: AsRef<[u8]>,
    {
        Ok(self.root_dir.join(key.as_ref().to_base58()))
    }
}

#[async_trait]
impl<Key, Value> StorageBackend for FileSystemStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + ConditionalSync,
{
    type Key = Key;
    type Value = Value;
    type Error = XStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        tokio::fs::write(self.make_path(&key)?, value)
            .await
            .map_err(|error| XStorageError::StorageBackend(format!("{error}")))?;
        Ok(())
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let path = self.make_path(key)?;
        if !path.exists() {
            return Ok(None);
        }

        tokio::fs::read(path)
            .await
            .map(|value| Some(Value::from(value)))
            .map_err(|error| XStorageError::StorageBackend(format!("{error}")))
    }
}
