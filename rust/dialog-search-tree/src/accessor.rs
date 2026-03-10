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
    Buffer, Cache, ContentAddressedStorage, Delta, DialogSearchTreeError, Key, Node, SymmetryWith,
    Value,
};

/// Accessor for retrieving nodes from a three-tier storage hierarchy.
///
/// The accessor checks for nodes in the following order:
/// 1. Cache - recently accessed nodes
/// 2. Delta - uncommitted in-memory changes
/// 3. Storage - persistent content-addressed storage backend
#[derive(Clone)]
pub struct Accessor<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    delta: Delta<Blake3Hash, Buffer>,
    cache: Cache<Blake3Hash, Buffer>,
    storage: ContentAddressedStorage<Backend>,
}

impl<Backend> Accessor<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSend
        + 'static,
{
    /// Creates a new accessor with the provided delta, cache, and storage backend.
    pub fn new(
        delta: Delta<Blake3Hash, Buffer>,
        cache: Cache<Blake3Hash, Buffer>,
        storage: ContentAddressedStorage<Backend>,
    ) -> Self {
        Self {
            delta,
            cache,
            storage,
        }
    }

    /// Retrieves a node by its content hash.
    ///
    /// Checks the cache first, then the delta, and finally the storage backend.
    /// Returns an error if the node is not found in any layer.
    pub async fn get_node<Key, Value>(
        &self,
        hash: &Blake3Hash,
    ) -> Result<Node<Key, Value>, DialogSearchTreeError>
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
                if let Some(buffer) = self.delta.get(hash) {
                    Ok(Some(buffer))
                } else {
                    self.storage
                        .retrieve(key)
                        .await
                        .map(|maybe_bytes| maybe_bytes.map(Buffer::from))
                }
            })
            .await?
            .ok_or_else(|| {
                DialogSearchTreeError::Node(format!("Blob not found in storage: {}", hash))
            })
            .map(|buffer| Node::new(buffer))
    }
}
