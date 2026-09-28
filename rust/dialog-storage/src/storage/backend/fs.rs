use crate::DialogStorageError;
use async_trait::async_trait;
use base58::ToBase58;
use dialog_common::ConditionalSync;
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};

use super::StorageBackend;

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
            .map_err(|error| DialogStorageError::Storage(format!("{error}")))?;
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
            .map_err(|error| DialogStorageError::Storage(format!("{error}")))?;
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
            .map_err(|error| DialogStorageError::Storage(format!("{error}")))
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
}
