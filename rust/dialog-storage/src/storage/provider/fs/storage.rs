//! Storage capability provider for filesystem.
//!
//! Implements key-value storage effects by storing data in files under
//! `{root}/{subject}/storage/{store}/{key}`.
//!
//! Keys are used as file paths directly — slashes create subdirectories.
//! List recursively walks the directory tree and returns full relative paths.

use std::path::PathBuf;

use super::FileStore;
use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_effects::storage::{
    Delete, DeleteCapability, Get, GetCapability, List, ListCapability, ListResult, Set,
    SetCapability, StorageError,
};

fn store_dir(
    fs: &FileStore,
    subject: &dialog_capability::Did,
    store: &str,
) -> Result<PathBuf, StorageError> {
    let location = fs.storage(subject, store).map_err(to_err)?;
    PathBuf::try_from(location).map_err(to_err)
}

fn key_path(
    fs: &FileStore,
    subject: &dialog_capability::Did,
    store: &str,
    key: &[u8],
) -> Result<PathBuf, StorageError> {
    let key_str = std::str::from_utf8(key).map_err(|e| StorageError::Storage(e.to_string()))?;
    let mut path = store_dir(fs, subject, store)?;
    path.push(key_str);
    Ok(path)
}

fn to_err(e: impl std::fmt::Display) -> StorageError {
    StorageError::Storage(e.to_string())
}

async fn list_recursive(
    dir: &std::path::Path,
    base: &std::path::Path,
    keys: &mut Vec<String>,
) -> Result<(), StorageError> {
    let mut entries = tokio::fs::read_dir(dir).await.map_err(StorageError::Io)?;
    while let Some(entry) = entries.next_entry().await.map_err(StorageError::Io)? {
        let ft = entry.file_type().await.map_err(StorageError::Io)?;
        if ft.is_file()
            && let Ok(rel) = entry.path().strip_prefix(base)
            && let Some(s) = rel.to_str()
        {
            keys.push(s.to_string());
        } else if ft.is_dir() {
            Box::pin(list_recursive(&entry.path(), base, keys)).await?;
        }
    }
    Ok(())
}

#[async_trait]
impl Provider<Get> for FileStore {
    async fn execute(&self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, StorageError> {
        let path = key_path(self, effect.subject(), effect.store(), effect.key())?;
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Io(e)),
        }
    }
}

#[async_trait]
impl Provider<Set> for FileStore {
    async fn execute(&self, effect: Capability<Set>) -> Result<(), StorageError> {
        let path = key_path(self, effect.subject(), effect.store(), effect.key())?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(StorageError::Io)?;
        }
        tokio::fs::write(&path, effect.value())
            .await
            .map_err(StorageError::Io)
    }
}

#[async_trait]
impl Provider<Delete> for FileStore {
    async fn execute(&self, effect: Capability<Delete>) -> Result<(), StorageError> {
        let path = key_path(self, effect.subject(), effect.store(), effect.key())?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::Io(e)),
        }
    }
}

#[async_trait]
impl Provider<List> for FileStore {
    async fn execute(&self, effect: Capability<List>) -> Result<ListResult, StorageError> {
        let base = store_dir(self, effect.subject(), effect.store())?;
        let prefix = std::str::from_utf8(effect.prefix())
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let search_dir = if prefix.is_empty() {
            base.clone()
        } else {
            base.join(prefix)
        };

        let mut keys = Vec::new();
        if search_dir.is_dir() {
            list_recursive(&search_dir, &base, &mut keys).await?;
        } else if search_dir.is_file()
            && let Ok(rel) = search_dir.strip_prefix(&base)
            && let Some(s) = rel.to_str()
        {
            keys.push(s.to_string());
        }

        Ok(ListResult {
            keys,
            is_truncated: false,
            next_continuation_token: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::FileStore;
    use dialog_capability::{Subject, did};
    use dialog_effects::storage::{Get, List, Set, Storage, Store};

    fn store_cap(subject: Subject, store_name: &str) -> dialog_capability::Capability<Store> {
        subject.attenuate(Storage).attenuate(Store::new(store_name))
    }

    #[dialog_common::test]
    async fn set_and_get_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let fs = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let subject = Subject::from(did!("key:z6MkTest"));

        store_cap(subject.clone(), "data")
            .invoke(Set::new(b"hello".to_vec(), b"world".to_vec()))
            .perform(&fs)
            .await
            .unwrap();

        let result = store_cap(subject, "data")
            .invoke(Get::new(b"hello"))
            .perform(&fs)
            .await
            .unwrap();

        assert_eq!(result, Some(b"world".to_vec()));
    }

    #[dialog_common::test]
    async fn get_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let fs = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let subject = Subject::from(did!("key:z6MkTest"));

        let result = store_cap(subject, "data")
            .invoke(Get::new(b"missing"))
            .perform(&fs)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[dialog_common::test]
    async fn keys_with_slashes_create_directories() {
        let dir = tempfile::tempdir().unwrap();
        let fs = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let subject = Subject::from(did!("key:z6MkTest"));

        let key = b"a/b/c";

        store_cap(subject.clone(), "data")
            .invoke(Set::new(key.to_vec(), b"nested".to_vec()))
            .perform(&fs)
            .await
            .unwrap();

        let result = store_cap(subject, "data")
            .invoke(Get::new(key.to_vec()))
            .perform(&fs)
            .await
            .unwrap();

        assert_eq!(result, Some(b"nested".to_vec()));
    }

    #[dialog_common::test]
    async fn list_returns_all_keys_recursively() {
        let dir = tempfile::tempdir().unwrap();
        let fs = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let subject = Subject::from(did!("key:z6MkTest"));

        let key1 = "aud1/sub1/iss1.cid1";
        let key2 = "aud1/_/iss2.cid2";
        let key3 = "aud2/sub2/iss3.cid3";

        for (k, v) in [(key1, "d1"), (key2, "d2"), (key3, "d3")] {
            store_cap(subject.clone(), "ucan")
                .invoke(Set::new(k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                .perform(&fs)
                .await
                .unwrap();
        }

        let result = store_cap(subject, "ucan")
            .invoke(List::new(None))
            .perform(&fs)
            .await
            .unwrap();

        assert_eq!(result.keys.len(), 3);
        assert!(result.keys.contains(&key1.to_string()));
        assert!(result.keys.contains(&key2.to_string()));
        assert!(result.keys.contains(&key3.to_string()));
    }

    #[dialog_common::test]
    async fn list_with_prefix_filters() {
        let dir = tempfile::tempdir().unwrap();
        let fs = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let subject = Subject::from(did!("key:z6MkTest"));

        let key1 = "aud1/sub1/iss1.cid1";
        let key2 = "aud1/_/iss2.cid2";
        let key3 = "aud2/sub2/iss3.cid3";

        for (k, v) in [(key1, "d1"), (key2, "d2"), (key3, "d3")] {
            store_cap(subject.clone(), "ucan")
                .invoke(Set::new(k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                .perform(&fs)
                .await
                .unwrap();
        }

        let result = store_cap(subject, "ucan")
            .invoke(List::with_prefix("aud1/"))
            .perform(&fs)
            .await
            .unwrap();

        assert_eq!(result.keys.len(), 2);
        assert!(result.keys.contains(&key1.to_string()));
        assert!(result.keys.contains(&key2.to_string()));
    }
}
