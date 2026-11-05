use std::marker::PhantomData;

use dialog_common::Blake3Hash;
use rkyv::{
    bytecheck::CheckBytes,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{DialogSearchTreeError, Key, Nodes, PersistentNode, Value};

/// A tree's reads through the store it reads its nodes from.
///
/// It holds nothing but the borrowed store: every node comes from
/// [`Nodes::load_node`], so a node the store keeps is answered from memory
/// and every other is read from the store's blocks, checked once and kept
/// by the store (if it keeps anything). What it adds is the tree's view of
/// absence: a node a tree links to and the store does not hold is an error
/// ([`get_node`](Self::get_node)).
///
/// Unflushed nodes are never read here: in-flight edits live in a
/// [`TransientTree`](crate::TransientTree)'s spine, and a
/// [`PersistentTree`](crate::PersistentTree) reads only what its store
/// holds. The accumulating delta is purely a persist-time output and is not
/// consulted on the read path.
pub struct Accessor<'a, Key, Value, Env> {
    env: &'a Env,
    types: PhantomData<fn() -> (Key, Value)>,
}

impl<Key, Value, Env> Clone for Accessor<'_, Key, Value, Env> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Key, Value, Env> Copy for Accessor<'_, Key, Value, Env> {}

impl<'a, Key, Value, Env> Accessor<'a, Key, Value, Env>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
        Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Env: Nodes<Key, Value>,
{
    /// Reads through `env`.
    pub fn new(env: &'a Env) -> Self {
        Self {
            env,
            types: PhantomData,
        }
    }

    /// The store this reads through.
    pub fn env(&self) -> &'a Env {
        self.env
    }

    /// Whether the store keeps the nodes read through it, so a read-ahead
    /// ([`warm`](Self::warm)) has somewhere to land.
    pub(crate) fn keeps(&self) -> bool {
        self.env.kept().is_some()
    }

    /// Loads the node at `hash` into the store's memory, without reporting
    /// what happened, and resolves to the hash it loaded so that a queue of
    /// these can tell which one finished.
    ///
    /// This is a read nobody is waiting on: it makes the node local so that a
    /// later [`get_node`](Self::get_node) is served from memory. It
    /// advances only while its owner polls it, which is why a reader that
    /// needs the node meanwhile fetches for itself rather than waiting: only
    /// the owner can join it, by polling it to completion. Nothing observes
    /// its outcome, so a node that is missing or fails to load is left to the
    /// read that actually needs it. Over a store that keeps nothing it is a
    /// read whose result is dropped.
    pub(crate) async fn warm(&self, hash: Blake3Hash) -> Blake3Hash {
        let _ = self.env.load_node(&hash).await;
        hash
    }

    /// The node at `hash` if the store keeps it in memory, with no IO: a
    /// way to reach what a held node keeps with it (its piece summary, say)
    /// without loading a node that is not already here.
    pub(crate) fn cached(&self, hash: &Blake3Hash) -> Option<PersistentNode<Key, Value>> {
        self.env.kept()?.get_cached(hash)
    }

    /// Retrieves a node by its content hash.
    ///
    /// Asks the store, which answers from memory when it keeps the node and
    /// from its blocks otherwise. Returns an error if the store has no such
    /// block. The read is this caller's own: it never waits on a read of the
    /// same node that someone else has in flight, since only that someone
    /// could drive it.
    pub async fn get_node(
        &self,
        hash: &Blake3Hash,
    ) -> Result<PersistentNode<Key, Value>, DialogSearchTreeError> {
        self.env.load_node(hash).await?.ok_or_else(|| {
            DialogSearchTreeError::Node(format!("Block not found in storage: {}", hash))
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use futures_util::future::join_all;

    use crate::{
        Accessor, Buffer, Cached, Delta, NodeCache, PersistentNode, PersistentTree,
        helpers::ObservingBlocks,
    };

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Concurrent misses on one node each read it: nothing waits on a read
    /// it cannot drive. The node lands once, and every read after that is
    /// served from the cache.
    #[dialog_common::test]
    async fn it_reads_a_node_once_it_has_landed_from_the_cache() -> Result<()> {
        let backend = ObservingBlocks::new();
        let storage = backend.clone();

        // A node can only be built from bytes that survive validation, so
        // the stored bytes must be a genuinely persisted node.
        let mut delta = Delta::zero();
        let tree = PersistentTree::<[u8; 4], Vec<u8>>::empty()
            .edit()
            .insert(1u32.to_be_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta, &storage)?;
        for (_, buffer) in delta.flush() {
            storage.store(buffer);
        }
        let hash = tree.root().clone();

        let store = Cached::new(NodeCache::new(), &storage);
        let accessor = Accessor::new(&store);
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

    /// Bytes that do not check as a node are never cached: every read of
    /// them goes back to storage and fails again, and none is served a node.
    #[dialog_common::test]
    async fn it_does_not_cache_bytes_that_fail_the_check() -> Result<()> {
        let backend = ObservingBlocks::new();
        let storage = backend.clone();
        let garbage = Buffer::from(vec![0xFF; 7]);
        let hash = garbage.blake3_hash().clone();
        storage.store(garbage);

        let store = Cached::<[u8; 4], Vec<u8>, _>::new(NodeCache::new(), &storage);
        let accessor = Accessor::new(&store);
        backend.reset();

        assert!(accessor.get_node(&hash).await.is_err());
        assert!(accessor.get_node(&hash).await.is_err());
        assert_eq!(backend.read_log(), vec![hash.clone(); 2]);

        Ok(())
    }
}
