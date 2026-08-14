//! Walking a tree for everything it reaches.
//!
//! [`Traversable::traverse_available`] answers "what does this tree
//! reference, and what of it do we actually hold" in one pass. That is a
//! different question from the differential's "what changed between these
//! two trees", and diffing against an empty tree to ask it does more work:
//! the differential eagerly expands the whole target before yielding
//! anything, and is then walked again.
//!
//! Absence is not corruption. A node the storage does not hold is reported
//! as [`Visit::Absent`] and the walk carries on with the rest of its
//! queue; bytes that *are* held but do not match the hash they were stored
//! under still fail it. The first is an incomplete replica, which is a
//! legitimate thing to walk; the second is a damaged one, which is not.
//!
//! What hangs beneath an absent node is unreachable by definition, so it
//! is not reported at all -- a sparse walk yields a frontier, never a
//! complete inventory of what is missing.

use async_stream::try_stream;
use dialog_common::{Blake3Hash, Buffer, ConditionalSend, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
use futures_core::Stream;
use rkyv::{
    Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    ArchivedNodeBody, ContentAddressedStorage, DialogSearchTreeError, Distribution, Key,
    PersistentNode, PersistentTree, Value,
};

/// What a gap-tolerant traversal found at one position in the tree.
#[derive(Debug, Clone)]
pub enum Visit<K, V> {
    /// The node was read.
    Present(PersistentNode<K, V>),
    /// The tree references this node, but the storage does not hold it.
    /// Whatever hangs beneath it is unreachable and will not be reported.
    Absent(Blake3Hash),
}

impl<K, V> Visit<K, V> {
    /// The node, if it was present.
    pub fn node(&self) -> Option<&PersistentNode<K, V>> {
        match self {
            Visit::Present(node) => Some(node),
            Visit::Absent(_) => None,
        }
    }
}

/// Walks a tree for every node it reaches.
pub trait Traversable<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
{
    /// Stream every node this tree reaches, reporting the ones storage does
    /// not hold rather than failing on the first.
    ///
    /// Breadth-first from the root. Child hashes are read out of each
    /// node's already-decoded body, so descending costs no extra reads.
    fn traverse_available<'a, Backend>(
        &'a self,
        storage: &'a ContentAddressedStorage<Backend>,
    ) -> impl Stream<Item = Result<Visit<Key, Value>, DialogSearchTreeError>> + 'a
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSend;
}

impl<Key, Value, D> Traversable<Key, Value> for PersistentTree<Key, Value, D>
where
    Key: self::Key + ConditionalSync + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value: for<'b> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'b>, Share>, rkyv::rancor::Error>,
    >,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    D: Distribution,
{
    fn traverse_available<'a, Backend>(
        &'a self,
        storage: &'a ContentAddressedStorage<Backend>,
    ) -> impl Stream<Item = Result<Visit<Key, Value>, DialogSearchTreeError>> + 'a
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSend,
    {
        let root = self.root().clone();

        try_stream! {
            if &root != NULL_BLAKE3_HASH {
                let mut queue = vec![root];

                while let Some(hash) = queue.pop() {
                    // `retrieve` verifies stored bytes against the hash it
                    // was asked for, so `None` here is genuinely "not
                    // stored" -- a corrupt block raises instead, and still
                    // fails the walk.
                    let Some(bytes) = storage.retrieve(&hash).await? else {
                        yield Visit::Absent(hash);
                        continue;
                    };
                    let node: PersistentNode<Key, Value> =
                        PersistentNode::try_from(Buffer::from(bytes))?;

                    if let ArchivedNodeBody::Index(index) = node.body() {
                        for link in index.links()? {
                            queue.push(link.node);
                        }
                    }

                    yield Visit::Present(node);
                }
            }
        }
    }
}
