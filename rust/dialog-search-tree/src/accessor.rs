use dialog_common::{Blake3Hash, Priority};
use dialog_effects::archive::prelude::CatalogScope;

use crate::nodes::{Preload, Read};
use crate::{DialogSearchTreeError, PersistentNode};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;

/// The node stored under `hash` in `catalog`, read through `env`: the
/// environment's answer to a node [`Read`]. `None` when `env` can reach no
/// such node.
///
/// This is how a tree reads a node, whatever the read: a point lookup, a
/// range scan, a traversal, a difference, an edit loading the nodes it
/// rewrites. Every one of them comes here (or to the [`Accessor`] an
/// operation opens, which is this with the catalog and the reading policy
/// kept for the whole operation).
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
    Env: Provider<Read<PersistentNode<Key, Value>>>
        + Provider<Preload<PersistentNode<Key, Value>>>
        + ConditionalSync
        + ?Sized,
{
    Accessor::new(env, catalog).load_node(hash).await
}

/// How a tree's reads ask for the nodes they need.
///
/// A read is a [`Read`] someone is waiting on unless told otherwise: a walk
/// that warms ahead of demand ranks the hydrations it causes lower
/// ([`priority`](Self::priority)), and a reader whose answer is what the
/// local archive holds makes every read local ([`local`](Self::local)).
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
    /// The read of the node `hash` names in `catalog`, asked this way.
    fn read<N>(&self, catalog: &CatalogScope, hash: &Blake3Hash) -> Read<N> {
        let read = Read::new(catalog.clone(), hash.clone()).with_priority(self.priority);
        if self.local { read.locally() } else { read }
    }

    /// The preload of the node `hash` names in `catalog`, ranked as this
    /// reads. A preload is never local: a read-ahead that may not hydrate
    /// has nothing to make cheap.
    fn preload<N>(&self, catalog: &CatalogScope, hash: &Blake3Hash) -> Preload<N> {
        Preload::new(catalog.clone(), hash.clone()).with_priority(self.priority)
    }
}

/// A tree operation's reads: the environment it reads through, the catalog
/// the tree's nodes live in, and how it asks for them.
///
/// Every node is asked for by a [`Read`] performed on the environment,
/// which answers it however it keeps nodes: from a cache it holds, from
/// its store, or hydrated from a remote. What this adds is the tree's view
/// of absence: a node a tree links to and the environment cannot reach is
/// an error ([`get_node`](Self::get_node)).
///
/// Unflushed nodes are never read here: in-flight edits live in a
/// [`TransientTree`](crate::TransientTree)'s spine, and the nodes a persist
/// sealed are read back through whatever stages its delta until they are
/// written.
pub struct Accessor<'a, Key, Value, Env: ?Sized> {
    env: &'a Env,
    catalog: CatalogScope,
    reading: Reading,
    types: std::marker::PhantomData<fn() -> (Key, Value)>,
}

impl<Key, Value, Env: ?Sized> Clone for Accessor<'_, Key, Value, Env> {
    fn clone(&self) -> Self {
        Self {
            env: self.env,
            catalog: self.catalog.clone(),
            reading: self.reading,
            types: std::marker::PhantomData,
        }
    }
}

impl<'a, Key, Value, Env: ?Sized> Accessor<'a, Key, Value, Env> {
    /// Reads the nodes of `catalog` through `env`.
    pub fn new(env: &'a Env, catalog: &CatalogScope) -> Self {
        Self {
            env,
            catalog: catalog.clone(),
            reading: Reading::default(),
            types: std::marker::PhantomData,
        }
    }

    /// These reads, asking as `reading` says.
    pub fn reading(mut self, reading: Reading) -> Self {
        self.reading = reading;
        self
    }

    /// These reads, seeing only what this replica holds: every read is
    /// [local](Read::locally), whatever this was asking before.
    pub(crate) fn local(&self) -> Self {
        let mut local = self.clone();
        local.reading.local = true;
        local
    }

    /// The environment this reads through.
    pub fn env(&self) -> &'a Env {
        self.env
    }

    /// The catalog this reads the nodes of.
    pub fn catalog(&self) -> &CatalogScope {
        &self.catalog
    }
}

impl<'a, Key, Value, Env> Accessor<'a, Key, Value, Env>
where
    Env: Provider<Read<PersistentNode<Key, Value>>>
        + Provider<Preload<PersistentNode<Key, Value>>>
        + ConditionalSync
        + ?Sized,
{
    /// The node at `hash`: the environment's answer to its [`Read`]. `None`
    /// when the environment can reach no such node.
    pub async fn load_node(
        &self,
        hash: &Blake3Hash,
    ) -> Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError> {
        self.reading
            .read(&self.catalog, hash)
            .perform(self.env)
            .await
    }

    /// Asks the environment to make the node at `hash` cheap to read
    /// ([`Preload`]), without reporting what happened, and resolves to the
    /// hash it asked for so that a queue of these can tell which one
    /// finished.
    ///
    /// This is a read nobody is waiting on. It advances only while its
    /// owner polls it, which is why a reader that needs the node meanwhile
    /// reads for itself rather than waiting: only the owner can join it,
    /// by polling it to completion. Nothing observes its outcome, so a
    /// node that is missing or fails to load is left to the read that
    /// actually needs it. An environment with nowhere to keep the node
    /// does nothing.
    pub(crate) async fn warm(&self, hash: Blake3Hash) -> Blake3Hash {
        self.reading
            .preload(&self.catalog, &hash)
            .perform(self.env)
            .await;
        hash
    }

    /// Retrieves a node by its content hash.
    ///
    /// Returns an error if the environment can reach no such node. The
    /// read is this caller's own: it never waits on a read of the same
    /// node that someone else has in flight, since only that someone could
    /// drive it.
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
    /// served from the cache the environment holds.
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
            .persist(&mut delta)?;
        storage.flush(&mut delta);
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

    #[dialog_capability::provider]
    impl dialog_capability::Provider<dialog_effects::archive::Load> for Answering {
        async fn execute(
            &self,
            _: dialog_effects::archive::Load,
        ) -> Result<Option<Buffer>, dialog_effects::archive::ArchiveError> {
            Ok(self.0.clone())
        }
    }

    crate::reads_nodes_through_blocks!(Answering);

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
    /// through it reads every node from its blocks, and a preload has
    /// nowhere to land, so it reads nothing.
    #[dialog_common::test]
    async fn it_preloads_nothing_into_an_environment_that_holds_nothing() -> Result<()> {
        let backend = ObservingBlocks::new();
        let mut delta = Delta::zero();
        let tree = PersistentTree::<[u8; 4], Vec<u8>>::empty_in(crate::helpers::test_catalog())
            .edit()
            .insert(1u32.to_be_bytes(), vec![1], &backend)
            .await?
            .persist(&mut delta)?;
        backend.flush(&mut delta);
        backend.reset();

        let accessor =
            Accessor::<[u8; 4], Vec<u8>, _>::new(&backend, &crate::helpers::test_catalog());
        accessor.warm(tree.root().clone()).await;
        assert!(
            backend.read_log().is_empty(),
            "nowhere to land, nothing read"
        );

        let _ = accessor.get_node(tree.root()).await?;
        let _ = accessor.get_node(tree.root()).await?;
        assert_eq!(backend.read_log().len(), 2, "every read reaches the blocks");
        Ok(())
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
            .persist(&mut delta)?;
        env.flush(&mut delta).await?;

        let mine = crate::NodeCache::<[u8; 4], Vec<u8>>::of(&env, catalog.subject())
            .expect("a holding environment holds a cache");
        let theirs = crate::NodeCache::<[u8; 4], Vec<u8>>::of(&env, other.subject())
            .expect("a holding environment holds a cache");

        assert!(
            mine.get_cached(tree.root()).is_some(),
            "the write kept the root it sealed, in the cache the env holds"
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

    #[dialog_capability::provider]
    impl dialog_capability::Provider<dialog_effects::archive::Load> for Asked {
        async fn execute(
            &self,
            load: dialog_effects::archive::Load,
        ) -> Result<Option<Buffer>, dialog_effects::archive::ArchiveError> {
            self.loads.lock().unwrap().push((load.priority, load.local));
            load.perform(&self.blocks).await
        }
    }

    crate::reads_nodes_through_blocks!(Asked);

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
            .persist(&mut delta)?;
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
