use dialog_capability::Provider;
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, Holds, Priority};
use dialog_effects::archive::Load;
use dialog_effects::archive::prelude::CatalogScope;
use dialog_storage::DialogStorageError;
use rkyv::{
    bytecheck::CheckBytes,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{DialogSearchTreeError, Key, NodeCache, PersistentNode, Value};

/// The node stored under `hash` in `catalog`, read through `env`: from the
/// node cache `env` holds when it holds the node, else loaded and checked,
/// then kept. `None` when `env` can reach no such block.
///
/// This is how a tree reads a node, whatever the read: a point lookup, a
/// range scan, a traversal, a difference, an edit loading the nodes it
/// rewrites. Every one of them comes here (or to the [`Accessor`] an
/// operation opens, which is this with the cache looked up once).
///
/// ```no_run
/// # use dialog_capability::{Subject, did};
/// # use dialog_effects::archive::prelude::ArchiveScope;
/// # use dialog_search_tree::{PersistentNode, load_node};
/// # use dialog_storage::provider::Volatile;
/// # async fn example() -> anyhow::Result<()> {
/// let env = Volatile::new();
/// let catalog = ArchiveScope::new(Subject::from(did!("key:z6MkTree"))).index();
/// # let hash = dialog_common::Blake3Hash::hash(b"a node");
/// let node: Option<PersistentNode<[u8; 4], Vec<u8>>> = load_node(&env, &catalog, &hash).await?;
/// # Ok(())
/// # }
/// ```
pub async fn load_node<Key, Value, Env>(
    env: &Env,
    catalog: &CatalogScope,
    hash: &Blake3Hash,
) -> Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key + ConditionalSend + ConditionalSync + 'static,
    Value: self::Value + ConditionalSend + ConditionalSync + 'static,
    Value::Archived: for<'b> CheckBytes<
        Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Env: Provider<Load> + Holds + ConditionalSync + ?Sized,
{
    Accessor::new(env, catalog).load_node(hash).await
}

/// How a tree's reads load the nodes its environment's cache does not hold.
///
/// A read is a [`Load`] someone is waiting on unless told otherwise: a walk
/// that warms ahead of demand ranks the hydrations it causes lower
/// ([`priority`](Self::priority)), and a reader whose answer is what the
/// local archive holds makes every load local ([`local`](Self::local)).
/// Either way the read is the tree's own, performed on the environment;
/// see [`PersistentTree::ranked`](crate::PersistentTree::ranked) and
/// [`PersistentTree::locally`](crate::PersistentTree::locally).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reading {
    /// How a hydration a read causes ranks against the site's other reads.
    pub priority: Priority,
    /// Whether a read sees only what the local archive holds.
    pub local: bool,
}

impl Default for Reading {
    fn default() -> Self {
        Self {
            priority: Priority::Demand,
            local: false,
        }
    }
}

impl Reading {
    /// The load of the block `hash` names in `catalog`, read this way.
    fn load(&self, catalog: &CatalogScope, hash: &Blake3Hash) -> Load {
        let load = Load::new(catalog.clone(), hash.clone()).with_priority(self.priority);
        if self.local { load.locally() } else { load }
    }
}

/// The node stored under `hash` in `catalog`, loaded through `env` and
/// checked: the one place a tree performs [`Load`] for a node.
///
/// `Load` is the read that hydrates: an environment that knows where the
/// catalog's subject keeps its blocks besides its local archive fetches a
/// local miss from there. Bytes that do not hash to `hash` fail the read
/// rather than reading as absent, so `None` means the block is genuinely
/// not reachable.
async fn fetch_node<Key, Value, Env>(
    env: &Env,
    catalog: &CatalogScope,
    hash: &Blake3Hash,
    reading: Reading,
) -> Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
        Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Env: Provider<Load> + ConditionalSync + ?Sized,
{
    let block = reading
        .load(catalog, hash)
        .perform(env)
        .await
        .map_err(|error| DialogSearchTreeError::Storage(error.into()))?;
    checked(block, hash)?
        .map(PersistentNode::try_from)
        .transpose()
}

/// `block` if it is the block `hash` names; a block of other bytes is a
/// corrupt read, not an absent one.
pub(crate) fn checked(
    block: Option<dialog_common::Buffer>,
    hash: &Blake3Hash,
) -> Result<Option<dialog_common::Buffer>, DialogSearchTreeError> {
    match block {
        Some(block) if block.blake3_hash() != hash => Err(DialogSearchTreeError::Storage(
            DialogStorageError::Verification(
                "Retrieved bytes did not match the provided hash".to_string(),
            ),
        )),
        block => Ok(block),
    }
}

/// A tree operation's reads: the environment it reads through, the catalog
/// the tree's blocks live in, and the node cache the environment holds for
/// that catalog's subject, looked up once for the whole operation.
///
/// A node the cache holds is answered from memory; every other is loaded
/// through the environment ([`Load`]), checked once and kept in the cache
/// (if the environment holds one). What it adds is the tree's view of
/// absence: a node a tree links to and the environment cannot reach is an
/// error ([`get_node`](Self::get_node)).
///
/// Unflushed nodes are never read here: in-flight edits live in a
/// [`TransientTree`](crate::TransientTree)'s spine, and nodes a persist
/// sealed are kept in the same cache before the flush. The accumulating
/// delta is purely a persist-time output and is not consulted on the read
/// path.
pub struct Accessor<'a, Key, Value, Env: ?Sized> {
    env: &'a Env,
    catalog: CatalogScope,
    cache: Option<NodeCache<Key, Value>>,
    reading: Reading,
}

impl<Key, Value, Env: ?Sized> Clone for Accessor<'_, Key, Value, Env> {
    fn clone(&self) -> Self {
        Self {
            env: self.env,
            catalog: self.catalog.clone(),
            cache: self.cache.clone(),
            reading: self.reading,
        }
    }
}

impl<'a, Key, Value, Env> Accessor<'a, Key, Value, Env>
where
    Key: ConditionalSend + ConditionalSync + 'static,
    Value: ConditionalSend + ConditionalSync + 'static,
    Env: Holds + ?Sized,
{
    /// Reads the blocks of `catalog` through `env`, answering from the node
    /// cache `env` holds for the catalog's subject.
    pub fn new(env: &'a Env, catalog: &CatalogScope) -> Self {
        Self {
            env,
            catalog: catalog.clone(),
            cache: NodeCache::of(env, catalog.subject()),
            reading: Reading::default(),
        }
    }

    /// These reads, loading what the cache does not hold as `reading` says.
    pub fn reading(mut self, reading: Reading) -> Self {
        self.reading = reading;
        self
    }
}

impl<'a, Key, Value, Env> Accessor<'a, Key, Value, Env>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
        Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Env: Provider<Load> + ConditionalSync + ?Sized,
{
    /// The environment this reads through.
    pub fn env(&self) -> &'a Env {
        self.env
    }

    /// The catalog this reads the blocks of.
    pub fn catalog(&self) -> &CatalogScope {
        &self.catalog
    }

    /// The node cache this answers from, if the environment holds one.
    pub fn cache(&self) -> Option<&NodeCache<Key, Value>> {
        self.cache.as_ref()
    }

    /// Whether the environment keeps the nodes read through it, so a
    /// read-ahead ([`warm`](Self::warm)) has somewhere to land.
    pub(crate) fn keeps(&self) -> bool {
        self.cache.is_some()
    }

    /// The node at `hash`, checked once: from the cache when it holds the
    /// node, else loaded and then kept. `None` when the environment can
    /// reach no such block.
    pub async fn load_node(
        &self,
        hash: &Blake3Hash,
    ) -> Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError> {
        let fetch =
            async |hash: &Blake3Hash| fetch_node(self.env, &self.catalog, hash, self.reading).await;
        match &self.cache {
            Some(cache) => cache.get_or_fetch(hash, fetch).await,
            None => fetch(hash).await,
        }
    }

    /// Loads the node at `hash` into the cache, without reporting what
    /// happened, and resolves to the hash it loaded so that a queue of
    /// these can tell which one finished.
    ///
    /// This is a read nobody is waiting on: it makes the node local so that a
    /// later [`get_node`](Self::get_node) is served from memory. It
    /// advances only while its owner polls it, which is why a reader that
    /// needs the node meanwhile fetches for itself rather than waiting: only
    /// the owner can join it, by polling it to completion. Nothing observes
    /// its outcome, so a node that is missing or fails to load is left to the
    /// read that actually needs it. Over an environment that keeps nothing
    /// it is a read whose result is dropped.
    pub(crate) async fn warm(&self, hash: Blake3Hash) -> Blake3Hash {
        let _ = self.load_node(&hash).await;
        hash
    }

    /// The node at `hash` if the cache holds it, with no IO: a way to reach
    /// what a held node keeps with it (its piece summary, say) without
    /// loading a node that is not already here.
    pub(crate) fn cached(&self, hash: &Blake3Hash) -> Option<PersistentNode<Key, Value>> {
        self.cache.as_ref()?.get_cached(hash)
    }

    /// Retrieves a node by its content hash.
    ///
    /// Answers from the cache when it holds the node and from the
    /// environment otherwise. Returns an error if the environment can reach
    /// no such block. The read is this caller's own: it never waits on a
    /// read of the same node that someone else has in flight, since only
    /// that someone could drive it.
    pub async fn get_node(
        &self,
        hash: &Blake3Hash,
    ) -> Result<PersistentNode<Key, Value>, DialogSearchTreeError> {
        self.load_node(hash).await?.ok_or_else(|| {
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
        Accessor, Buffer, Delta, PersistentNode, PersistentTree, helpers::ObservingBlocks,
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
        let tree = PersistentTree::<[u8; 4], Vec<u8>>::empty_in(crate::helpers::test_catalog())
            .edit()
            .insert(1u32.to_be_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta, &storage)?;
        for (_, buffer) in delta.flush() {
            storage.store(buffer);
        }
        let hash = tree.root().clone();

        let store = crate::helpers::Holding::new(&storage);
        let accessor =
            Accessor::<[u8; 4], Vec<u8>, _>::new(&store, &crate::helpers::test_catalog());
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

        let store = crate::helpers::Holding::new(&storage);
        let accessor =
            Accessor::<[u8; 4], Vec<u8>, _>::new(&store, &crate::helpers::test_catalog());
        backend.reset();

        assert!(accessor.get_node(&hash).await.is_err());
        assert!(accessor.get_node(&hash).await.is_err());
        assert_eq!(backend.read_log(), vec![hash.clone(); 2]);

        Ok(())
    }

    /// Answers every load with the same bytes, whatever was asked for.
    struct Answering(Option<Buffer>);

    impl dialog_common::Holds for Answering {
        fn held(&self, _key: &str) -> Option<dialog_common::Held> {
            None
        }

        fn hold(&self, _key: String, _handle: dialog_common::Held) {}
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl dialog_capability::Provider<dialog_effects::archive::Load> for Answering {
        async fn execute(
            &self,
            _: dialog_effects::archive::Load,
        ) -> Result<Option<Buffer>, dialog_effects::archive::ArchiveError> {
            Ok(self.0.clone())
        }
    }

    /// An environment handing back other bytes is a corrupt block, not an
    /// absent one: the read fails rather than reading as `None`.
    #[dialog_common::test]
    async fn it_refuses_bytes_that_do_not_hash_to_what_was_asked() -> Result<()> {
        let asked = Buffer::from(b"the block asked for".to_vec());
        let other = Buffer::from(b"some other block".to_vec());

        let read = crate::load_node::<[u8; 4], Vec<u8>, _>(
            &Answering(Some(other)),
            &crate::helpers::test_catalog(),
            asked.blake3_hash(),
        )
        .await;

        assert!(
            matches!(
                read,
                Err(crate::DialogSearchTreeError::Storage(
                    dialog_storage::DialogStorageError::Verification(_)
                ))
            ),
            "mismatched bytes must fail the read: {read:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_reads_an_unreachable_block_as_absent() -> Result<()> {
        let asked = Buffer::from(b"never stored".to_vec());

        let read = crate::load_node::<[u8; 4], Vec<u8>, _>(
            &Answering(None),
            &crate::helpers::test_catalog(),
            asked.blake3_hash(),
        )
        .await?;

        assert!(read.is_none());
        Ok(())
    }

    /// An environment that holds nothing has no node cache: a tree read
    /// through it reads every node from its blocks, and a read-ahead has
    /// nowhere to land, rather than landing in a cache made for it alone.
    #[dialog_common::test]
    fn it_finds_no_cache_on_an_environment_that_holds_nothing() {
        let blocks = crate::helpers::Blocks::new();
        let catalog = crate::helpers::test_catalog();

        assert!(crate::NodeCache::<[u8; 4], Vec<u8>>::of(&blocks, catalog.subject()).is_none());
        assert!(!Accessor::<[u8; 4], Vec<u8>, _>::new(&blocks, &catalog).keeps());
    }

    /// An environment that holds handles is given one cache, found by every
    /// later lookup, and each subject reads it through a scope of its own:
    /// a node kept for one subject is not a hit for another.
    #[dialog_common::test]
    async fn it_finds_one_cache_per_environment_scoped_by_subject() -> Result<()> {
        use dialog_capability::{Subject, did};
        use dialog_effects::archive::prelude::ArchiveScope;

        let blocks = crate::helpers::Blocks::new();
        let env = crate::helpers::Holding::new(&blocks);
        let catalog = crate::helpers::test_catalog();
        let other = ArchiveScope::new(Subject::from(did!("key:zAnotherTreeSubject"))).index();

        let mut delta = Delta::zero();
        let tree = PersistentTree::<[u8; 4], Vec<u8>>::empty_in(catalog.clone())
            .edit()
            .insert(1u32.to_be_bytes(), vec![1], &env)
            .await?
            .persist(&mut delta, &env)?;
        blocks.flush(&mut delta);

        let mine = crate::NodeCache::<[u8; 4], Vec<u8>>::of(&env, catalog.subject())
            .expect("a holding environment holds a cache");
        let theirs = crate::NodeCache::<[u8; 4], Vec<u8>>::of(&env, other.subject())
            .expect("a holding environment holds a cache");

        assert!(
            mine.get_cached(tree.root()).is_some(),
            "the persist kept the root it sealed, in the cache the env holds"
        );
        assert!(
            theirs.get_cached(tree.root()).is_none(),
            "another subject does not see the node"
        );
        assert_eq!(mine.len(), theirs.len(), "the two scopes share one cache");
        Ok(())
    }

    /// Keeps how each load it answers was asked: its priority and whether
    /// it was local.
    struct Asked {
        blocks: crate::helpers::Blocks,
        loads: std::sync::Mutex<Vec<(dialog_common::Priority, bool)>>,
    }

    impl dialog_common::Holds for Asked {
        fn held(&self, _key: &str) -> Option<dialog_common::Held> {
            None
        }

        fn hold(&self, _key: String, _handle: dialog_common::Held) {}
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl dialog_capability::Provider<dialog_effects::archive::Load> for Asked {
        async fn execute(
            &self,
            load: dialog_effects::archive::Load,
        ) -> Result<Option<Buffer>, dialog_effects::archive::ArchiveError> {
            self.loads.lock().unwrap().push((load.priority, load.local));
            load.perform(&self.blocks).await
        }
    }

    /// A tree's reading reaches every load it performs: a tree reads as a
    /// demand read that may hydrate, one ranked lower carries its priority
    /// to the env, and one made local asks for the local archive alone.
    #[dialog_common::test]
    async fn it_loads_as_the_tree_reads() -> Result<()> {
        use dialog_common::Priority;

        let blocks = crate::helpers::Blocks::new();
        let mut delta = Delta::zero();
        let tree = PersistentTree::<[u8; 4], Vec<u8>>::empty_in(crate::helpers::test_catalog())
            .edit()
            .insert(1u32.to_be_bytes(), vec![1], &blocks)
            .await?
            .persist(&mut delta, &blocks)?;
        blocks.flush(&mut delta);
        let env = Asked {
            blocks,
            loads: std::sync::Mutex::new(Vec::new()),
        };
        let key = 1u32.to_be_bytes();
        // How every load of one read was asked, emptying the record.
        let asked = || -> Vec<(Priority, bool)> {
            let mut loads = env.loads.lock().unwrap();
            let mut asked = loads.clone();
            loads.clear();
            asked.dedup();
            asked
        };

        assert_eq!(tree.get(&key, &env).await?, Some(vec![1]));
        assert_eq!(
            asked(),
            [(Priority::Demand, false)],
            "a demand read by default"
        );

        let ranked = tree.clone().ranked(Priority::Maybe);
        assert_eq!(ranked.get(&key, &env).await?, Some(vec![1]));
        assert_eq!(
            asked(),
            [(Priority::Maybe, false)],
            "ranked at its priority"
        );

        let local = tree.clone().locally();
        assert_eq!(local.get(&key, &env).await?, Some(vec![1]));
        assert_eq!(asked(), [(Priority::Demand, true)], "local loads only");

        let moved = local.with_root(tree.root().clone());
        assert_eq!(moved.get(&key, &env).await?, Some(vec![1]));
        assert_eq!(
            asked(),
            [(Priority::Demand, true)],
            "a tree moved to another root reads as it did"
        );
        Ok(())
    }
}
