use dialog_common::{Blake3Hash, ConditionalSend};
use dialog_storage::{DialogStorageError, StorageBackend};
use rkyv::{
    Deserialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    Buffer, Cache, ContentAddressedStorage, DialogSearchTreeError, Key, PersistentNode,
    SymmetryWith, Value,
};

/// Accessor for retrieving durable nodes from cache and content-addressed
/// storage.
///
/// The accessor checks for nodes in the following order:
/// 1. Cache - recently accessed nodes
/// 2. Storage - persistent content-addressed storage backend
///
/// Unflushed nodes are never read here: in-flight edits live in a
/// [`TransientTree`](crate::TransientTree)'s spine, and a
/// [`PersistentTree`](crate::PersistentTree) reads only what has been flushed to
/// storage. The accumulating delta is purely a persist-time output and is not
/// consulted on the read path.
#[derive(Clone)]
pub struct Accessor<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    cache: Cache<Blake3Hash, Buffer>,
    storage: ContentAddressedStorage<Backend>,
}

impl<Backend> Accessor<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSend,
{
    /// Creates a new accessor over the given cache and storage backend.
    pub fn new(
        cache: Cache<Blake3Hash, Buffer>,
        storage: ContentAddressedStorage<Backend>,
    ) -> Self {
        Self { cache, storage }
    }

    /// Retrieves a node by its content hash.
    ///
    /// Checks the cache first, then the storage backend. Returns an error if the
    /// node is in neither.
    pub async fn get_node<Key, Value>(
        &self,
        hash: &Blake3Hash,
    ) -> Result<PersistentNode<Key, Value>, DialogSearchTreeError>
    where
        Key: self::Key,
        Key::Archived: for<'a> CheckBytes<
                Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
            > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>
            + PartialOrd<Key>
            + PartialEq<Key>
            + SymmetryWith<Key>
            + Ord,
        Value: self::Value,
        Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        self.cache
            .get_or_fetch(hash, async |key| {
                self.storage
                    .retrieve(key)
                    .await
                    .map(|maybe_bytes| maybe_bytes.map(Buffer::from))
            })
            .await?
            .ok_or_else(|| {
                DialogSearchTreeError::Node(format!("Blob not found in storage: {}", hash))
            })
            .map(|buffer| PersistentNode::new(buffer))
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash;
    use futures_util::future::join_all;

    use crate::{
        Accessor, Cache, ContentAddressedStorage, PersistentNode, helpers::ObservingBackend,
    };

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[dialog_common::test]
    async fn it_deduplicates_concurrent_fetches_of_one_node() -> Result<()> {
        let backend = ObservingBackend::new();
        let mut storage = ContentAddressedStorage::new(backend.clone());

        let bytes = b"the bytes of one node".to_vec();
        let hash = Blake3Hash::hash(&bytes);
        storage.store(bytes, &hash).await?;

        let accessor = Accessor::new(Cache::new(), storage);
        backend.reset();

        let reads = join_all((0..8).map(|_| accessor.get_node(&hash))).await;

        for read in reads {
            let _: PersistentNode<[u8; 4], Vec<u8>> = read?;
        }
        assert_eq!(backend.read_log(), vec![hash]);

        Ok(())
    }
}
