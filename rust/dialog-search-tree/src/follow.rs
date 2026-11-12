//! Bringing what a replica holds of one tree along to another.
//!
//! A partial replica holds the leaves its reads reached and its writes
//! minted, and leaves the rest of a tree by reference. When its head moves
//! to another tree by root (a fast-forward), the nodes that changed are
//! held by reference too, wherever they are: a read the replica answered
//! from its own archive a moment before now reaches a node it does not
//! hold. [`PersistentTree::follow`] reads those nodes first, and only
//! those: the nodes of the new tree a read would reach on its way to a
//! key the replica holds a leaf for.
//!
//! That is a different walk from the differential's. A
//! [`TreeDifference`](crate::TreeDifference) reads everything by which two
//! trees differ, whether or not this replica ever read that part of
//! either. This one is bounded by what the replica holds, so a region it
//! never read stays by reference however much of it changed.

use std::collections::HashSet;

use dialog_capability::Provider;
use dialog_common::{Blake3Hash, ConditionalSync};
use futures_util::future::try_join_all;
use rkyv::{
    Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::nodes::{Preload, Read};
use crate::{
    Accessor, ArchivedIndex, DialogSearchTreeError, Distribution, Key, NodeBody, PersistentNode,
    PersistentTree, Value,
};

/// The keys a node is routed: `lower` and above, below `upper`, which is
/// absent for the last node of a level.
#[derive(Clone, Debug)]
struct Span {
    lower: Vec<u8>,
    upper: Option<Vec<u8>>,
}

impl Span {
    /// Every key: what a root is routed.
    fn all() -> Self {
        Self {
            lower: Vec::new(),
            upper: None,
        }
    }

    /// Whether this begins below where `other` ends. Two spans share a key
    /// exactly when each begins below the other's end.
    fn begins_below_end_of(&self, other: &Span) -> bool {
        other
            .upper
            .as_deref()
            .is_none_or(|upper| self.lower.as_slice() < upper)
    }
}

/// The children of `index`, each with the keys it is routed out of the
/// `span` its parent is routed.
///
/// A child is routed the keys from its own separator up to the next
/// child's. The first is also routed every key below its separator, and
/// the last every key up to its parent's end, as
/// [`ArchivedIndex::route`] sends them.
fn children<Value>(
    index: &ArchivedIndex<Value>,
    span: &Span,
) -> Result<Vec<(Blake3Hash, Span)>, DialogSearchTreeError>
where
    Value: self::Value,
{
    let links = index.links()?;
    Ok(links
        .iter()
        .enumerate()
        .map(|(at, link)| {
            let lower = if at == 0 {
                span.lower.clone()
            } else {
                link.separator.as_slice().to_vec()
            };
            let upper = links
                .get(at + 1)
                .map(|next| next.separator.as_slice().to_vec())
                .or_else(|| span.upper.clone());
            (link.node.clone(), Span { lower, upper })
        })
        .collect())
}

/// What is known of a node of the tree a replica holds.
enum Standing {
    /// The replica has not been asked for it.
    Unread,
    /// The replica does not hold it, so nothing beneath it is reachable.
    Absent,
    /// A leaf the replica holds.
    Leaf,
}

/// The tree a replica holds, read from the replica alone and only as far
/// as it has been asked about.
struct Held<'a, Key, Value, Env: ?Sized> {
    /// Reads that see only what the replica holds.
    local: Accessor<'a, Key, Value, Env>,
    /// The deepest nodes read so far, in key order: together they are
    /// routed every key.
    nodes: Vec<(Span, Blake3Hash, Standing)>,
    /// Every node the tree has been seen to link, held or not.
    linked: HashSet<Blake3Hash>,
}

impl<'a, Key, Value, Env> Held<'a, Key, Value, Env>
where
    Key: self::Key + ConditionalSync + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    Env: Provider<Read<PersistentNode<Key, Value>>>
        + Provider<Preload<PersistentNode<Key, Value>>>
        + ConditionalSync
        + ?Sized,
{
    /// Whether the replica holds a leaf routed some key of `span`.
    ///
    /// Reads down from the nodes already read, only where `span` reaches
    /// and only until a held leaf is found.
    async fn holds_within(&mut self, span: &Span) -> Result<bool, DialogSearchTreeError> {
        let mut at = self
            .nodes
            .partition_point(|(node, _, _)| !span.begins_below_end_of(node));
        while let Some((node, hash, standing)) = self.nodes.get(at) {
            if !node.begins_below_end_of(span) {
                break;
            }
            match standing {
                Standing::Leaf => return Ok(true),
                Standing::Absent => at += 1,
                Standing::Unread => {
                    let Some(read) = self.local.load_node(hash).await? else {
                        self.nodes[at].2 = Standing::Absent;
                        at += 1;
                        continue;
                    };
                    match read.body() {
                        NodeBody::Segment(_) => {
                            self.nodes[at].2 = Standing::Leaf;
                            return Ok(true);
                        }
                        NodeBody::Index(index) => {
                            let beneath = children(index, node)?;
                            self.linked
                                .extend(beneath.iter().map(|(hash, _)| hash.clone()));
                            self.nodes.splice(
                                at..=at,
                                beneath
                                    .into_iter()
                                    .map(|(hash, span)| (span, hash, Standing::Unread)),
                            );
                        }
                    }
                }
            }
        }
        Ok(false)
    }
}

impl<Key, Value, D> PersistentTree<Key, Value, D>
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
    /// Read the nodes of this tree that a read the replica answers from
    /// what it holds of `held` would reach, and answer how many of them
    /// the replica did not hold.
    ///
    /// For a replica about to move its head from `held` to this tree by
    /// root. The two trees are walked in step, and a node of this one is
    /// read through `env`, which fetches what the replica lacks, only
    /// where the keys it is routed meet those of a leaf the replica holds
    /// of `held`. So every key the replica read from its own blocks at
    /// `held` it reads from its own blocks here.
    ///
    /// A subtree the two trees share is not read: what was held of it
    /// still is. A region the replica holds no leaf of stays by reference
    /// however much of it changed, so the reads follow what changed in
    /// what the replica uses, never the size of the change. A replica that
    /// holds every leaf of `held` ends holding every leaf of this tree.
    ///
    /// Each level's reads go out together, so the wall clock is the depth
    /// of the tree in round trips. A node `env` cannot reach fails the
    /// walk; what was read before it stays held.
    pub async fn follow<Env>(&self, held: &Self, env: &Env) -> Result<usize, DialogSearchTreeError>
    where
        Env: Provider<Read<PersistentNode<Key, Value>>>
            + Provider<Preload<PersistentNode<Key, Value>>>
            + ConditionalSync
            + ?Sized,
    {
        let (Some(root), Some(held_root)) = (self.stored_root(), held.stored_root()) else {
            return Ok(0);
        };
        if root == held_root {
            return Ok(0);
        }
        let reader = self.accessor(env);
        let local = reader.local();
        let mut held = Held {
            local: held.accessor(env).local(),
            nodes: vec![(Span::all(), held_root.clone(), Standing::Unread)],
            linked: HashSet::from([held_root.clone()]),
        };
        let mut level = vec![(root.clone(), Span::all())];
        let mut fetched = 0;
        while !level.is_empty() {
            let mut wanted = Vec::new();
            for (hash, span) in level.drain(..) {
                // What is held is asked first: answering it reads the held
                // tree down to this span, which is what lists a node the
                // two trees share.
                if held.holds_within(&span).await? && !held.linked.contains(&hash) {
                    let absent = local.load_node(&hash).await?.is_none();
                    wanted.push((hash, span, absent));
                }
            }
            let nodes =
                try_join_all(wanted.iter().map(|(hash, _, _)| reader.get_node(hash))).await?;
            for (node, (_, span, absent)) in nodes.iter().zip(&wanted) {
                fetched += usize::from(*absent);
                if let NodeBody::Index(index) = node.body() {
                    level.extend(children(index, span)?);
                }
            }
        }
        Ok(fetched)
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::Result;
    use dialog_capability::Provider;
    use dialog_common::Buffer;
    use dialog_effects::archive::{ArchiveError, Get, Load};

    use crate::helpers::Blocks;
    use crate::{Delta, PersistentTree};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    type Tree = PersistentTree<[u8; 4], Vec<u8>>;

    /// A replica over an origin that holds everything: a load reads what
    /// the replica holds, else fetches from the origin and keeps what it
    /// fetched, counting each fetch.
    struct Replica {
        local: Blocks,
        origin: Blocks,
        fetched: Arc<AtomicUsize>,
    }

    impl Replica {
        fn of(origin: &Blocks) -> Self {
            Self {
                local: Blocks::new(),
                origin: origin.clone(),
                fetched: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn fetched(&self) -> usize {
            self.fetched.load(Ordering::Relaxed)
        }
    }

    impl dialog_common::Holds for Replica {
        fn held(&self, _key: &str) -> Option<dialog_common::Held> {
            None
        }

        fn hold(&self, _key: String, _handle: dialog_common::Held) {}
    }

    #[dialog_capability::provider]
    impl Provider<Load> for Replica {
        async fn execute(&self, load: Load) -> Result<Option<Buffer>, ArchiveError> {
            if let Some(block) = self.local.get(&load.digest) {
                return Ok(Some(block));
            }
            if load.local {
                return Ok(None);
            }
            let fetched = self.origin.get(&load.digest);
            if let Some(block) = &fetched {
                self.fetched.fetch_add(1, Ordering::Relaxed);
                self.local.store(block.clone());
            }
            Ok(fetched)
        }
    }

    #[dialog_capability::provider]
    impl Provider<Get> for Replica {
        async fn execute(
            &self,
            get: dialog_capability::Capability<Get>,
        ) -> Result<Option<Vec<u8>>, ArchiveError> {
            get.perform(&self.local).await
        }
    }

    crate::reads_nodes_through_blocks!(Replica);

    /// Big-endian, so that numeric neighbours are neighbours in the tree.
    fn key(index: u32) -> [u8; 4] {
        index.to_be_bytes()
    }

    /// Values large enough that a few dozen entries fill a leaf, so the
    /// trees here have many leaves under real index nodes.
    fn value(index: u32, edition: u8) -> Vec<u8> {
        let mut value = vec![edition; 2048];
        value[..4].copy_from_slice(&index.to_be_bytes());
        value
    }

    /// `tree` with every `index` of `entries` written as `edition`, its
    /// nodes stored in `origin`.
    async fn write(
        mut tree: Tree,
        entries: impl IntoIterator<Item = u32>,
        edition: u8,
        origin: &Blocks,
    ) -> Result<Tree> {
        let mut delta = Delta::zero();
        for index in entries {
            tree = tree
                .edit()
                .insert(key(index), value(index, edition), origin)
                .await?
                .persist(&mut delta)?;
            origin.flush(&mut delta);
        }
        Ok(tree)
    }

    /// A replica that read a few keys, then follows a head that rewrote
    /// some of them and grew a far region many times over, reads those
    /// keys from what it holds, and fetched only the path to them: the
    /// far region stays by reference.
    #[dialog_common::test]
    async fn it_brings_along_what_the_replica_read_and_nothing_else() -> Result<()> {
        let origin = Blocks::new();
        let base = write(
            Tree::empty_in(crate::helpers::test_catalog()),
            0..2000,
            1,
            &origin,
        )
        .await?;
        let replica = Replica::of(&origin);
        for index in 0..8 {
            assert_eq!(
                base.get(&key(index), &replica).await?,
                Some(value(index, 1))
            );
        }
        let read = replica.fetched();

        let adopted = write(base.clone(), 4..12, 2, &origin).await?;
        let adopted = write(adopted, 2000..4000, 2, &origin).await?;

        let fetched = adopted.follow(&base, &replica).await?;
        assert_eq!(
            replica.fetched() - read,
            fetched,
            "the walk reports what it fetched"
        );
        assert!(
            fetched <= read,
            "following fetches no more than the path the replica read: fetched {fetched}, read {read}"
        );

        let held = replica.fetched();
        for index in 0..8 {
            let edition = if index < 4 { 1 } else { 2 };
            assert_eq!(
                adopted.get(&key(index), &replica).await?,
                Some(value(index, edition))
            );
        }
        assert_eq!(
            replica.fetched(),
            held,
            "what the replica read before, it reads from what it holds"
        );

        assert_eq!(
            adopted.get(&key(3000), &replica).await?,
            Some(value(3000, 2))
        );
        assert!(
            replica.fetched() > held,
            "a region the replica never read was left by reference"
        );
        Ok(())
    }

    /// A replica that holds every leaf of a tree holds every leaf of the
    /// one it follows to, changed, added before, between or after.
    #[dialog_common::test]
    async fn it_keeps_a_whole_replica_whole() -> Result<()> {
        let origin = Blocks::new();
        let base = write(
            Tree::empty_in(crate::helpers::test_catalog()),
            (1000..3000).filter(|index| index % 2 == 0),
            1,
            &origin,
        )
        .await?;
        let replica = Replica::of(&origin);
        for index in (1000..3000).filter(|index| index % 2 == 0) {
            base.get(&key(index), &replica).await?;
        }

        let adopted = write(base.clone(), 0..500, 2, &origin).await?;
        let adopted = write(adopted, (1001..3000).filter(|i| i % 2 == 1), 2, &origin).await?;
        let adopted = write(adopted, 3000..3500, 2, &origin).await?;

        adopted.follow(&base, &replica).await?;
        let held = replica.fetched();
        for index in 0..3500 {
            let expected = match index {
                500..1000 => None,
                1000..3000 if index % 2 == 0 => Some(value(index, 1)),
                _ => Some(value(index, 2)),
            };
            assert_eq!(adopted.get(&key(index), &replica).await?, expected);
        }
        assert_eq!(
            replica.fetched(),
            held,
            "every key is read from what the replica holds"
        );
        Ok(())
    }

    /// A replica that holds none of a tree fetches none of the one it
    /// follows to, and two trees with one root are not read at all.
    #[dialog_common::test]
    async fn it_fetches_nothing_for_a_replica_that_holds_nothing() -> Result<()> {
        let origin = Blocks::new();
        let base = write(
            Tree::empty_in(crate::helpers::test_catalog()),
            0..500,
            1,
            &origin,
        )
        .await?;
        let adopted = write(base.clone(), 0..500, 2, &origin).await?;

        let replica = Replica::of(&origin);
        assert_eq!(adopted.follow(&base, &replica).await?, 0);
        assert_eq!(adopted.follow(&adopted, &replica).await?, 0);
        assert_eq!(replica.fetched(), 0);
        Ok(())
    }

    /// A node the environment cannot reach fails the walk rather than
    /// leaving the replica to find out at its next read.
    #[dialog_common::test]
    async fn it_fails_on_a_node_it_cannot_reach() -> Result<()> {
        let origin = Blocks::new();
        let base = write(
            Tree::empty_in(crate::helpers::test_catalog()),
            0..500,
            1,
            &origin,
        )
        .await?;
        let replica = Replica::of(&origin);
        base.get(&key(7), &replica).await?;

        // The head it follows to is stored where the replica cannot fetch
        // it from.
        let elsewhere = Blocks::new();
        let mut delta = Delta::zero();
        let unreachable = base
            .clone()
            .edit()
            .insert(key(7), value(7, 2), &origin)
            .await?
            .persist(&mut delta)?;
        elsewhere.flush(&mut delta);

        let followed = unreachable.follow(&base, &replica).await;
        assert!(
            matches!(followed, Err(crate::DialogSearchTreeError::Node(_))),
            "an unreachable node fails the walk: {followed:?}"
        );
        Ok(())
    }
}
