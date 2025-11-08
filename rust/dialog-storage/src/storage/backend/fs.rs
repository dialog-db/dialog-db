use crate::{DialogStorageError, StorageSink};
use async_trait::async_trait;
use base58::ToBase58;
use dialog_common::{ConditionalSend, ConditionalSync};
use futures_util::{Stream, TryStreamExt, future::try_join_all};
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};

use super::{StorageBackend, TransactionalMemoryBackend};

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

    fn make_path(&self, key: &Key) -> Result<PathBuf, DialogStorageError>
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
        tokio::fs::write(self.make_path(&key)?, value)
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
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
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> TransactionalMemoryBackend for FileSystemStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + Clone + From<Vec<u8>> + ConditionalSync,
{
    type Address = Key;
    type Value = Value;
    type Error = DialogStorageError;
    type Edition = std::time::SystemTime;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        let path = self.make_path(address)?;
        if !path.exists() {
            return Ok(None);
        }

        let metadata = tokio::fs::metadata(&path)
            .await
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let mtime = metadata
            .modified()
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        let value = tokio::fs::read(path)
            .await
            .map(Value::from)
            .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

        Ok(Some((value, mtime)))
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        let path = self.make_path(address)?;

        // Check CAS condition - verify mtime matches
        let current_mtime = if path.exists() {
            let metadata = tokio::fs::metadata(&path)
                .await
                .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
            Some(
                metadata
                    .modified()
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?,
            )
        } else {
            None
        };

        // Verify edition matches
        if current_mtime.as_ref() != edition {
            return Err(DialogStorageError::StorageBackend(
                "CAS condition failed: edition mismatch".to_string(),
            ));
        }

        // Perform the operation
        match content {
            Some(new_value) => {
                tokio::fs::write(&path, &new_value)
                    .await
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

                // Get new mtime
                let metadata = tokio::fs::metadata(&path)
                    .await
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
                let new_mtime = metadata
                    .modified()
                    .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;

                Ok(Some(new_mtime))
            }
            None => {
                if path.exists() {
                    tokio::fs::remove_file(&path)
                        .await
                        .map_err(|error| DialogStorageError::StorageBackend(format!("{error}")))?;
                }
                Ok(None)
            }
        }
    }
}

#[async_trait]
impl<Key, Value> StorageSink for FileSystemStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + Clone + From<Vec<u8>> + ConditionalSync + PartialEq,
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
            let path = self.make_path(&key)?;
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
