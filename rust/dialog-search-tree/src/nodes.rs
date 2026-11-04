use std::future::Future;

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer, ConditionalSend, ConditionalSync};
use rkyv::{
    bytecheck::CheckBytes,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{DialogSearchTreeError, Key, LoadBlock, NodeCache, PersistentNode, Value};

/// Where a tree reads its nodes: a block store that keeps the nodes it has
/// checked, so a node is checked once and read from memory after that.
///
/// Every read a tree makes goes through this, whatever the read: a point
/// lookup, a range scan, a traversal, a difference, an edit loading the
/// nodes it rewrites, a persist keeping the nodes it seals. The store
/// decides what it keeps and for whom (see [`NodeCache`] and its scopes).
/// A store that keeps nothing answers every read from its blocks: a tree
/// over it is correct, and slower.
///
/// A store that keeps nothing implements this in one line, and
/// [`Cached`] gives any block store a cache of its own:
///
/// ```no_run
/// # use dialog_search_tree::{Cached, MemoryBlocks, NodeCache, Nodes};
/// // `MemoryBlocks` keeps nothing: every read is answered from its blocks.
/// let blocks = MemoryBlocks::new();
/// assert!(Nodes::<[u8; 4], Vec<u8>>::kept(&blocks).is_none());
///
/// // The same blocks, keeping the nodes read through them.
/// let store = Cached::<[u8; 4], Vec<u8>, _>::new(NodeCache::new(), &blocks);
/// assert!(store.kept().is_some());
/// ```
pub trait Nodes<Key, Value>: Provider<LoadBlock> + ConditionalSync {
    /// The nodes this store keeps in memory, if it keeps any.
    fn kept(&self) -> Option<&NodeCache<Key, Value>> {
        None
    }

    /// The node stored under `hash`, checked once: from memory when this
    /// store keeps it, else from its blocks and then kept. `None` when the
    /// store has no such block.
    ///
    /// This default is the one place a tree's node read reaches the store's
    /// blocks: every tree read comes here, so what the store keeps answers
    /// every read that can be answered from memory.
    fn load_node(
        &self,
        hash: &Blake3Hash,
    ) -> impl Future<Output = Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError>>
    + ConditionalSend
    where
        Key: self::Key,
        Value: self::Value,
        Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        async move {
            let fetch = async |hash: &Blake3Hash| {
                LoadBlock::new(hash.clone())
                    .perform(self)
                    .await?
                    .map(PersistentNode::try_from)
                    .transpose()
            };
            match self.kept() {
                Some(kept) => kept.get_or_fetch(hash, fetch).await,
                None => fetch(hash).await,
            }
        }
    }
}

/// `env`, keeping the nodes it reads in `cache`.
///
/// For a block store that has no cache of its own (tests, benches, tools):
/// every tree read through this is answered from `cache` when it holds the
/// node, and from `env` (then kept) when it does not. A persist through it
/// keeps the nodes it seals, so the next edit opens them without reading
/// them back. Clones share the cache.
pub struct Cached<'a, Key, Value, Env> {
    cache: NodeCache<Key, Value>,
    env: &'a Env,
}

impl<'a, Key, Value, Env> Cached<'a, Key, Value, Env> {
    /// `env`, keeping the nodes it reads in `cache`.
    pub fn new(cache: NodeCache<Key, Value>, env: &'a Env) -> Self {
        Self { cache, env }
    }

    /// The cache this store keeps its nodes in.
    pub fn cache(&self) -> &NodeCache<Key, Value> {
        &self.cache
    }

    /// The block store this reads through.
    pub fn env(&self) -> &'a Env {
        self.env
    }
}

impl<Key, Value, Env> Clone for Cached<'_, Key, Value, Env> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            env: self.env,
        }
    }
}

impl<Key, Value, Env> std::ops::Deref for Cached<'_, Key, Value, Env> {
    type Target = Env;

    fn deref(&self) -> &Env {
        self.env
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value, Env> Provider<LoadBlock> for Cached<'_, Key, Value, Env>
where
    Env: Provider<LoadBlock> + ConditionalSync,
{
    async fn execute(&self, load: LoadBlock) -> Result<Option<Buffer>, DialogSearchTreeError> {
        self.env.execute(load).await
    }
}

impl<Key, Value, Env> Nodes<Key, Value> for Cached<'_, Key, Value, Env>
where
    Env: Provider<LoadBlock> + ConditionalSync,
{
    fn kept(&self) -> Option<&NodeCache<Key, Value>> {
        Some(&self.cache)
    }
}
