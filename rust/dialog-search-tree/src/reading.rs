use dialog_capability::{Provider, Subject};
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, Priority};

use crate::DialogSearchTreeError;
use crate::nodes::{Preload, Read};

/// How and where a tree asks for its nodes: whose nodes they are, how a
/// hydration a read causes ranks, and whether a read sees only this
/// replica.
///
/// This is a [`Read`] without its name, kept by a tree for every read it
/// makes. It holds no environment: an operation is handed the environment
/// it reads through, and performs [`read`](Self::read)s on it. A tree
/// reads as a demand read that may hydrate unless told otherwise: a walk
/// that warms ahead of demand ranks the hydrations it causes lower
/// ([`ranked`](Self::ranked)), and a reader whose answer is what this
/// replica holds makes every read local ([`locally`](Self::locally)). An
/// edit opened from a tree reads as the tree does, and the tree it
/// persists reads that way too; see
/// [`PersistentTree::ranked`](crate::PersistentTree::ranked) and
/// [`PersistentTree::locally`](crate::PersistentTree::locally).
///
/// ```no_run
/// # use dialog_capability::{Subject, did};
/// # use dialog_search_tree::{PersistentNode, Reading};
/// # use dialog_storage::provider::Volatile;
/// # async fn example() -> anyhow::Result<()> {
/// let env = Volatile::new();
/// let reading = Reading::new(Subject::from(did!("key:z6MkTree"))).locally();
/// # let hash = dialog_common::Blake3Hash::hash(b"a node");
/// // A node that may be absent: the environment's answer as it is.
/// let node: Option<PersistentNode<[u8; 4], Vec<u8>>> = reading.read(&hash).perform(&env).await?;
/// // A node a tree links to: absent is an error.
/// let node: PersistentNode<[u8; 4], Vec<u8>> = reading.node(&env, &hash).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Reading {
    /// Whose nodes these are: the subject whose archive keeps them.
    pub subject: Subject,
    /// How a hydration a read causes ranks against the site's other reads.
    pub priority: Priority,
    /// Whether a read sees only what this replica holds.
    pub local: bool,
}

impl Reading {
    /// Reads of `subject`'s nodes, as demand reads that may hydrate.
    pub fn new(subject: Subject) -> Self {
        Self {
            subject,
            priority: Priority::Demand,
            local: false,
        }
    }

    /// These reads, ranking the hydrations they cause as `priority`.
    pub fn ranked(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// These reads, seeing only what this replica holds: every read is
    /// [local](Read::locally).
    pub fn locally(mut self) -> Self {
        self.local = true;
        self
    }

    /// The read of the node `name` names, asked this way.
    pub fn read<N>(&self, name: &Blake3Hash) -> Read<N> {
        let read = Read::new(self.subject.clone(), name.clone()).with_priority(self.priority);
        if self.local { read.locally() } else { read }
    }

    /// The preload of the node `name` names, ranked as this reads. A
    /// preload is never local: a read-ahead that may not hydrate has
    /// nothing to make cheap.
    pub fn preload<N>(&self, name: &Blake3Hash) -> Preload<N> {
        Preload::new(self.subject.clone(), name.clone()).with_priority(self.priority)
    }

    /// The node `name` names, read through `env`, where a tree links to it:
    /// a node the environment cannot reach is an error, not an absence.
    ///
    /// This is where a tree's view of absence lives. A [`Read`] answers
    /// `None` for a node nobody can reach, which is a fine answer to "is it
    /// there?"; a tree that follows a link to a node is owed the node, so
    /// every read a tree makes along its links comes here. The read is this
    /// caller's own: it never waits on a read of the same node that someone
    /// else has in flight, since only that someone could drive it.
    pub async fn node<N, Env>(
        &self,
        env: &Env,
        name: &Blake3Hash,
    ) -> Result<N, DialogSearchTreeError>
    where
        N: ConditionalSend,
        Env: Provider<Read<N>> + ConditionalSync + ?Sized,
    {
        self.read(name).perform(env).await?.ok_or_else(|| {
            DialogSearchTreeError::Node(format!("Block not found in storage: {name}"))
        })
    }

    /// Asks `env` to make the node `name` names cheap to read ([`Preload`]),
    /// without reporting what happened, and resolves to `name` so that a
    /// queue of these can tell which one finished.
    ///
    /// This is a read nobody is waiting on. It advances only while its
    /// owner polls it, which is why a reader that needs the node meanwhile
    /// reads for itself rather than waiting: only the owner can join it,
    /// by polling it to completion. Nothing observes its outcome, so a
    /// node that is missing or fails to load is left to the read that
    /// actually needs it. An environment with nowhere to keep the node
    /// does nothing.
    pub(crate) async fn warm<N, Env>(&self, env: &Env, name: Blake3Hash) -> Blake3Hash
    where
        Env: Provider<Preload<N>> + ConditionalSync + ?Sized,
    {
        self.preload::<N>(&name).perform(env).await;
        name
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_capability::{Subject, did};
    use dialog_common::Priority;
    use futures_util::future::join_all;

    use crate::helpers::{Blocks, Holding, ObservingBlocks, test_subject};
    use crate::{Buffer, Delta, DialogSearchTreeError, PersistentNode, PersistentTree, Reading};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    type Node = PersistentNode<[u8; 4], Vec<u8>>;
    type Tree = PersistentTree<[u8; 4], Vec<u8>>;

    /// A tree of one entry, its nodes written into `blocks`.
    async fn one_entry(blocks: &Blocks) -> Result<Tree> {
        let mut delta = Delta::zero();
        let tree = Tree::empty_in(test_subject())
            .edit()
            .insert(1u32.to_be_bytes(), vec![1], blocks)
            .await?
            .persist(&mut delta)?;
        blocks.flush(&mut delta);
        Ok(tree)
    }

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
        let tree = Tree::empty_in(test_subject())
            .edit()
            .insert(1u32.to_be_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta)?;
        storage.flush(&mut delta);
        let hash = tree.root().clone();

        let store = Holding::new(&storage);
        let reading = Reading::new(test_subject());
        backend.reset();

        let reads = join_all((0..8).map(|_| reading.node::<Node, _>(&store, &hash))).await;

        for read in reads {
            read?;
        }
        assert_eq!(backend.read_log(), vec![hash.clone(); 8]);

        let reads = join_all((0..8).map(|_| reading.node::<Node, _>(&store, &hash))).await;

        for read in reads {
            read?;
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

        let store = Holding::new(&storage);
        let reading = Reading::new(test_subject());
        backend.reset();

        assert!(reading.node::<Node, _>(&store, &hash).await.is_err());
        assert!(reading.node::<Node, _>(&store, &hash).await.is_err());
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

        let read: Result<Option<Node>, _> = Reading::new(test_subject())
            .read(asked.blake3_hash())
            .perform(&Answering(Some(other)))
            .await;

        assert!(
            matches!(
                read,
                Err(DialogSearchTreeError::Storage(
                    dialog_storage::DialogStorageError::Verification(_)
                ))
            ),
            "mismatched bytes must fail the read: {read:?}"
        );
        Ok(())
    }

    /// A read answers `None` for a node nobody can reach; a tree that
    /// follows a link to it is owed the node, so the same absence through
    /// [`Reading::node`] is an error.
    #[dialog_common::test]
    async fn it_reads_an_unreachable_block_as_absent_and_a_linked_one_as_missing() -> Result<()> {
        let asked = Buffer::from(b"never stored".to_vec());
        let reading = Reading::new(test_subject());

        let read: Option<Node> = reading
            .read(asked.blake3_hash())
            .perform(&Answering(None))
            .await?;
        assert!(read.is_none());

        let linked = reading
            .node::<Node, _>(&Answering(None), asked.blake3_hash())
            .await;
        assert!(
            matches!(linked, Err(DialogSearchTreeError::Node(_))),
            "a linked node nobody holds is an error: {linked:?}"
        );
        Ok(())
    }

    /// An environment that holds nothing has no node cache: a tree read
    /// through it reads every node from its blocks, and a preload has
    /// nowhere to land, so it reads nothing.
    #[dialog_common::test]
    async fn it_preloads_nothing_into_an_environment_that_holds_nothing() -> Result<()> {
        let backend = ObservingBlocks::new();
        let mut delta = Delta::zero();
        let tree = Tree::empty_in(test_subject())
            .edit()
            .insert(1u32.to_be_bytes(), vec![1], &backend)
            .await?
            .persist(&mut delta)?;
        backend.flush(&mut delta);
        backend.reset();

        let reading = Reading::new(test_subject());
        reading.warm::<Node, _>(&backend, tree.root().clone()).await;
        assert!(
            backend.read_log().is_empty(),
            "nowhere to land, nothing read"
        );

        reading.node::<Node, _>(&backend, tree.root()).await?;
        reading.node::<Node, _>(&backend, tree.root()).await?;
        assert_eq!(backend.read_log().len(), 2, "every read reaches the blocks");
        Ok(())
    }

    /// An environment that holds handles is given one cache, found by every
    /// later lookup, and each subject reads it through a scope of its own:
    /// a node kept for one subject is not a hit for another.
    #[dialog_common::test]
    async fn it_finds_one_cache_per_environment_scoped_by_subject() -> Result<()> {
        let blocks = Blocks::new();
        let env = Holding::new(&blocks);
        let other = Subject::from(did!("key:zAnotherTreeSubject"));

        let mut delta = Delta::zero();
        let tree = Tree::empty_in(test_subject())
            .edit()
            .insert(1u32.to_be_bytes(), vec![1], &env)
            .await?
            .persist(&mut delta)?;
        env.flush(&mut delta).await?;

        let mine = crate::NodeCache::<[u8; 4], Vec<u8>>::of(&env, test_subject().did())
            .expect("a holding environment holds a cache");
        let theirs = crate::NodeCache::<[u8; 4], Vec<u8>>::of(&env, other.did())
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
    /// it was local. With `remote_only` it stands for a replica that holds
    /// none of the blocks itself: a local load finds nothing, and only a
    /// load that may hydrate reaches them.
    struct Asked {
        blocks: Blocks,
        remote_only: bool,
        loads: std::sync::Mutex<Vec<(Priority, bool)>>,
    }

    impl Asked {
        fn new(blocks: Blocks) -> Self {
            Self {
                blocks,
                remote_only: false,
                loads: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn remote_only(blocks: Blocks) -> Self {
            Self {
                remote_only: true,
                ..Self::new(blocks)
            }
        }

        /// How every load since the last call was asked, emptying the
        /// record.
        fn asked(&self) -> Vec<(Priority, bool)> {
            let mut loads = self.loads.lock().unwrap();
            let mut asked = std::mem::take(&mut *loads);
            asked.dedup();
            asked
        }
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
            if self.remote_only && load.local {
                return Ok(None);
            }
            load.perform(&self.blocks).await
        }
    }

    crate::reads_nodes_through_blocks!(Asked);

    /// A tree's reading reaches every load it performs: a tree reads as a
    /// demand read that may hydrate, one ranked lower carries its priority
    /// to the env, and one made local asks for the local archive alone.
    #[dialog_common::test]
    async fn it_loads_as_the_tree_reads() -> Result<()> {
        let blocks = Blocks::new();
        let tree = one_entry(&blocks).await?;
        let env = Asked::new(blocks);
        let key = 1u32.to_be_bytes();

        assert_eq!(tree.get(&key, &env).await?, Some(vec![1]));
        assert_eq!(
            env.asked(),
            [(Priority::Demand, false)],
            "a demand read by default"
        );

        let ranked = tree.clone().ranked(Priority::Maybe);
        assert_eq!(ranked.get(&key, &env).await?, Some(vec![1]));
        assert_eq!(
            env.asked(),
            [(Priority::Maybe, false)],
            "ranked at its priority"
        );

        let local = tree.clone().locally();
        assert_eq!(local.get(&key, &env).await?, Some(vec![1]));
        assert_eq!(env.asked(), [(Priority::Demand, true)], "local loads only");

        let moved = local.with_root(tree.root().clone());
        assert_eq!(moved.get(&key, &env).await?, Some(vec![1]));
        assert_eq!(
            env.asked(),
            [(Priority::Demand, true)],
            "a tree moved to another root reads as it did"
        );
        Ok(())
    }

    /// An edit reads the nodes it rewrites as the tree it was opened from
    /// reads, and the tree it persists reads that way too: a ranked tree's
    /// edit ranks its loads, a local tree's edit loads locally.
    #[dialog_common::test]
    async fn it_edits_as_the_tree_reads() -> Result<()> {
        let blocks = Blocks::new();
        let tree = one_entry(&blocks).await?;
        let env = Asked::new(blocks.clone());
        let key = 2u32.to_be_bytes();

        tree.edit().insert(key, vec![2], &env).await?;
        assert_eq!(
            env.asked(),
            [(Priority::Demand, false)],
            "an edit of a tree read by default reads on demand"
        );

        tree.clone()
            .ranked(Priority::Maybe)
            .edit()
            .insert(key, vec![2], &env)
            .await?;
        assert_eq!(
            env.asked(),
            [(Priority::Maybe, false)],
            "an edit of a ranked tree reads at its priority"
        );

        let mut delta = Delta::zero();
        let edited = tree
            .clone()
            .locally()
            .edit()
            .insert(key, vec![2], &env)
            .await?
            .persist(&mut delta)?;
        blocks.flush(&mut delta);
        assert_eq!(
            env.asked(),
            [(Priority::Demand, true)],
            "an edit of a local tree reads locally"
        );

        assert_eq!(edited.get(&key, &env).await?, Some(vec![2]));
        assert_eq!(
            env.asked(),
            [(Priority::Demand, true)],
            "the tree a local edit persists reads locally"
        );
        assert_eq!(edited.reading(), tree.clone().locally().reading());
        Ok(())
    }

    /// An edit of a local tree cannot hydrate: on a replica that does not
    /// hold the tree's root, the edit fails for the missing node rather than
    /// fetching it, and asks for nothing but local loads. The same edit of
    /// the tree read by default hydrates the root and goes through.
    #[dialog_common::test]
    async fn it_refuses_to_hydrate_an_edit_of_a_local_tree() -> Result<()> {
        let blocks = Blocks::new();
        let tree = one_entry(&blocks).await?;
        let env = Asked::remote_only(blocks);
        let key = 2u32.to_be_bytes();

        let mut delta = Delta::zero();
        let refused = match tree
            .clone()
            .locally()
            .edit()
            .insert(key, vec![2], &env)
            .await
        {
            Ok(edit) => Ok(edit.persist(&mut delta)?),
            Err(error) => Err(error),
        };
        assert!(
            matches!(refused, Err(DialogSearchTreeError::Node(_))),
            "a local edit of a node this replica lacks fails: {refused:?}"
        );
        assert_eq!(
            env.asked(),
            [(Priority::Demand, true)],
            "the refused edit asked only for local loads"
        );
        assert!(
            delta.flush().next().is_none(),
            "the refused edit sealed nothing"
        );

        tree.edit().insert(key, vec![2], &env).await?;
        assert_eq!(
            env.asked(),
            [(Priority::Demand, false)],
            "the edit of the tree read by default hydrates"
        );
        Ok(())
    }
}
