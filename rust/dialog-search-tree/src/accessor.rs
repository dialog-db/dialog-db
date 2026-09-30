use std::marker::PhantomData;

use dialog_capability::Provider;
use dialog_common::{Blake3Hash, ConditionalSync};
use rkyv::{
    bytecheck::CheckBytes,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{DialogSearchTreeError, Key, LoadBlock, NodeCache, PersistentNode, Value};

/// Accessor for retrieving durable nodes from cache and the environment.
///
/// The accessor checks for nodes in the following order:
/// 1. Cache - recently accessed nodes, already checked
/// 2. Environment - the [`LoadBlock`] provider, whose bytes are checked once as
///    they become a node and enter the cache
///
/// The accessor borrows its environment for as long as it reads; it never
/// owns it.
///
/// Unflushed nodes are never read here: in-flight edits live in a
/// [`TransientTree`](crate::TransientTree)'s spine, and a
/// [`PersistentTree`](crate::PersistentTree) reads only what has been flushed to
/// storage. The accumulating delta is purely a persist-time output and is not
/// consulted on the read path.
pub struct Accessor<'a, Key, Value, Env> {
    cache: NodeCache<Key, Value>,
    env: &'a Env,
    types: PhantomData<fn() -> (Key, Value)>,
}

impl<Key, Value, Env> Clone for Accessor<'_, Key, Value, Env>
where
    Key: self::Key,
    Value: self::Value,
{
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            env: self.env,
            types: PhantomData,
        }
    }
}

impl<'a, Key, Value, Env> Accessor<'a, Key, Value, Env>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
        Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Env: Provider<LoadBlock> + ConditionalSync,
{
    /// Creates a new accessor over the given cache and environment.
    pub fn new(cache: NodeCache<Key, Value>, env: &'a Env) -> Self {
        Self {
            cache,
            env,
            types: PhantomData,
        }
    }

    /// Loads the node at `hash` into the cache, without reporting what
    /// happened, and resolves to the hash it loaded so that a queue of these
    /// can tell which one finished.
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
    /// Checks the cache first, then the environment. Returns an error if the
    /// node is in neither. The read is this caller's own: it never waits on
    /// a read of the same node that someone else has in flight, since only
    /// that someone could drive it.
    pub async fn get_node(
        &self,
        hash: &Blake3Hash,
    ) -> Result<PersistentNode<Key, Value>, DialogSearchTreeError> {
        self.cache
            .get_or_fetch(hash, async |key| self.retrieve(key).await)
            .await?
            .ok_or_else(|| {
                DialogSearchTreeError::Node(format!("Block not found in storage: {}", hash))
            })
    }

    /// The node stored under `key`, checked as it is read.
    async fn retrieve(
        &self,
        key: &Blake3Hash,
    ) -> Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError> {
        LoadBlock::new(key.clone())
            .perform(self.env)
            .await?
            .map(PersistentNode::try_from)
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use futures_util::future::join_all;

    use crate::{
        Accessor, Buffer, Cache, Delta, PersistentNode, PersistentTree, helpers::ObservingBlocks,
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
            .persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage.store(buffer);
        }
        let hash = tree.root().clone();

        let accessor = Accessor::new(Cache::new(), &storage);
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

        let accessor = Accessor::<[u8; 4], Vec<u8>, _>::new(Cache::new(), &storage);
        backend.reset();

        assert!(accessor.get_node(&hash).await.is_err());
        assert!(accessor.get_node(&hash).await.is_err());
        assert_eq!(backend.read_log(), vec![hash.clone(); 2]);

        Ok(())
    }
}
