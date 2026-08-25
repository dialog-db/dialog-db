use dialog_common::{Blake3Hash, ConditionalSend};
use dialog_storage::{DialogStorageError, StorageBackend};
use rkyv::{
    bytecheck::CheckBytes,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    Buffer, Cache, ContentAddressedStorage, DialogSearchTreeError, Key, PersistentNode, Value,
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

    /// Loads the node at `hash` into the cache, without decoding it or
    /// reporting what happened, and resolves to the hash it loaded so that a
    /// queue of these can tell which one finished.
    ///
    /// This is a read nobody is waiting on: it makes the node local so that a
    /// later [`get_node`](Self::get_node) is served from the cache. It
    /// advances only while its owner polls it, which is why a reader that
    /// needs the node meanwhile fetches for itself rather than waiting: only
    /// the owner can join it, by polling it to completion. Nothing observes
    /// its outcome, so a node that is missing or fails to load is left to the
    /// read that actually needs it.
    pub(crate) async fn warm(&self, hash: Blake3Hash) -> Blake3Hash {
        let _ = self
            .cache
            .get_or_fetch(&hash, async |key| self.retrieve(key).await)
            .await;
        hash
    }

    /// Retrieves a node by its content hash.
    ///
    /// Checks the cache first, then the storage backend. Returns an error if the
    /// node is in neither. The read is this caller's own: it never waits on
    /// a read of the same node that someone else has in flight, since only
    /// that someone could drive it.
    pub async fn get_node<Key, Value>(
        &self,
        hash: &Blake3Hash,
    ) -> Result<PersistentNode<Key, Value>, DialogSearchTreeError>
    where
        Key: self::Key,
        Value: self::Value,
        Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        let buffer = self
            .cache
            .get_or_fetch(hash, async |key| self.retrieve(key).await)
            .await?;
        Self::decode(hash, buffer)
    }

    async fn retrieve(&self, key: &Blake3Hash) -> Result<Option<Buffer>, DialogStorageError> {
        self.storage
            .retrieve(key)
            .await
            .map(|maybe_bytes| maybe_bytes.map(Buffer::from))
    }

    fn decode<Key, Value>(
        hash: &Blake3Hash,
        buffer: Option<Buffer>,
    ) -> Result<PersistentNode<Key, Value>, DialogSearchTreeError>
    where
        Key: self::Key,
        Value: self::Value,
        Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        buffer
            .ok_or_else(|| {
                DialogSearchTreeError::Node(format!("Blob not found in storage: {}", hash))
            })
            .and_then(PersistentNode::try_from)
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use futures_util::future::join_all;

    use crate::{
        Accessor, Cache, ContentAddressedStorage, Delta, PersistentNode, PersistentTree,
        helpers::ObservingBackend,
    };

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Concurrent misses on one node each read it: nothing waits on a read
    /// it cannot drive. The node lands once, and every read after that is
    /// served from the cache.
    #[dialog_common::test]
    async fn it_reads_a_node_once_it_has_landed_from_the_cache() -> Result<()> {
        let backend = ObservingBackend::new();
        let mut storage = ContentAddressedStorage::new(backend.clone());

        // A node can only be built from bytes that survive validation, so
        // the stored bytes must be a genuinely persisted node.
        let mut delta = Delta::zero();
        let tree = PersistentTree::<[u8; 4], Vec<u8>>::empty()
            .edit()
            .insert(1u32.to_be_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        let hash = tree.root().clone();

        let accessor = Accessor::new(Cache::new(), storage);
        backend.reset();

        let reads = join_all((0..8).map(|_| accessor.get_node(&hash))).await;

        for read in reads {
            let _: PersistentNode<[u8; 4], Vec<u8>> = read?;
        }
        assert_eq!(backend.read_log(), vec![hash.clone(); 8]);

        let reads = join_all((0..8).map(|_| accessor.get_node(&hash))).await;

        for read in reads {
            let _: PersistentNode<[u8; 4], Vec<u8>> = read?;
        }
        assert_eq!(backend.read_log().len(), 8, "later reads hit the cache");

        Ok(())
    }
}
