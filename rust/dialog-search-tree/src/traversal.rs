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
use futures_util::stream::FuturesUnordered;
use rkyv::{
    Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};
use std::collections::VecDeque;

use crate::{
    ArchivedNodeBody, ContentAddressedStorage, DialogSearchTreeError, Distribution, Key,
    PersistentNode, PersistentTree, Value,
};

/// How many block reads a traversal keeps in flight at once.
///
/// The queued reads are independent and every one of them is committed
/// work: the walk has already decided it needs these nodes, and the set
/// is bounded by the tree's fanout and by the caller's scope. So there is
/// nothing to gain by metering them out, and a cap costs a round trip per
/// wave -- 70 queued reads under a cap of 16 pay four waves where they
/// could pay one.
///
/// Measured on the soak's download phase (median of three runs per
/// profile, capped at 16 versus uncapped): intercontinental 28.6 -> 9.2
/// rounds, broadband 31.7 -> 23.4, mobile 42.6 -> 41.7 (mobile is
/// bandwidth-bound at 20 Mbps, so round trips are not its constraint).
/// Query phases were unchanged within run-to-run variance, and duplicate
/// fetches stayed at zero throughout, so partial replication is
/// unaffected -- lifting the cap changes WHEN blocks are fetched, never
/// WHICH.
///
/// The transport is the right place for any remaining limit: a browser
/// caps connections per origin on its own, and a priority queue at the
/// hydration flight is what would keep one big walk from starving a
/// concurrent demand read (bead dialog-db-88). A constant here cannot
/// express either.
const FETCH_CONCURRENCY: usize = usize::MAX;

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

    /// [`traverse_available`](Self::traverse_available) restricted to
    /// `scope`: a child subtree whose key span cannot intersect any range
    /// is never descended into, and — against a backend that reaches a
    /// remote on a miss — never fetched.
    ///
    /// This is what lets a caller materialize some regions of a
    /// tag-partitioned tree and leave others by reference (see
    /// `dialog_artifacts::merge::data_scope`).
    ///
    /// Pruning is conservative in the same direction as
    /// [`TreeDifference::compute_within`](crate::TreeDifference::compute_within):
    /// it may keep a node the scope does not need, never drop one it does.
    fn traverse_available_within<'a, Backend>(
        &'a self,
        storage: &'a ContentAddressedStorage<Backend>,
        scope: &'a [core::ops::RangeInclusive<Vec<u8>>],
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
        traverse::<Key, Value, Backend>(self.root().clone(), storage, None)
    }

    fn traverse_available_within<'a, Backend>(
        &'a self,
        storage: &'a ContentAddressedStorage<Backend>,
        scope: &'a [core::ops::RangeInclusive<Vec<u8>>],
    ) -> impl Stream<Item = Result<Visit<Key, Value>, DialogSearchTreeError>> + 'a
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSend,
    {
        traverse::<Key, Value, Backend>(self.root().clone(), storage, Some(scope))
    }
}

/// Whether the conservative key span `[lower, upper)` — `upper` absent
/// meaning open to the top of the key space — meets any range in `scope`.
///
/// A link's separator is a LOWER bound, and the separator invariant puts
/// the next link's separator strictly above this subtree's maximum key,
/// so `[own separator, next separator)` contains the subtree's true span.
/// Bounding it this way can only over-retain, never over-drop.
fn span_intersects(
    lower: &[u8],
    upper: Option<&[u8]>,
    scope: &[core::ops::RangeInclusive<Vec<u8>>],
) -> bool {
    scope.iter().any(|range| {
        lower <= range.end().as_slice()
            && match upper {
                Some(upper) => upper > range.start().as_slice(),
                None => true,
            }
    })
}

/// The walk shared by [`Traversable::traverse_available`] and
/// [`Traversable::traverse_available_within`]; `scope` of `None` keeps
/// every child.
fn traverse<'a, Key, Value, Backend>(
    root: Blake3Hash,
    storage: &'a ContentAddressedStorage<Backend>,
    scope: Option<&'a [core::ops::RangeInclusive<Vec<u8>>]>,
) -> impl Stream<Item = Result<Visit<Key, Value>, DialogSearchTreeError>> + 'a
where
    Key: self::Key + ConditionalSync + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSend,
{
    use futures_util::StreamExt as _;

    try_stream! {
        if &root != NULL_BLAKE3_HASH {
            // A continuation queue rather than levels: the root goes out,
            // and every node that lands queues its children behind
            // whatever is already waiting, up to `FETCH_CONCURRENCY` in
            // flight at once. Against a backend that reaches a remote on a
            // miss the wall clock is still depth round-trips, but no read
            // waits for its slowest sibling before its own children go
            // out -- a level walk paid that barrier once per level, a round
            // trip of idle transport slots each time. Reads are polled BY
            // this generator, so on wasm (where the poll issues the
            // request) every queued read is on the wire together.
            let mut queue: VecDeque<Blake3Hash> = VecDeque::from([root]);
            let mut reads = FuturesUnordered::new();
            loop {
                while reads.len() < FETCH_CONCURRENCY {
                    let Some(hash) = queue.pop_front() else {
                        break;
                    };
                    reads.push(async move {
                        // `retrieve` verifies stored bytes against the
                        // hash it was asked for, so `None` here is
                        // genuinely "not stored" -- a corrupt block
                        // raises instead, and still fails the walk.
                        let bytes = storage.retrieve(&hash).await;
                        (hash, bytes)
                    });
                }
                let Some((hash, bytes)) = reads.next().await else {
                    break;
                };
                let Some(bytes) = bytes? else {
                    yield Visit::Absent(hash);
                    continue;
                };
                let node: PersistentNode<Key, Value> =
                    PersistentNode::try_from(Buffer::from(bytes))?;

                if let ArchivedNodeBody::Index(index) = node.body() {
                    let links = index.links()?;
                    match scope {
                        None => {
                            for link in links {
                                queue.push_back(link.node);
                            }
                        }
                        Some(scope) => {
                            // A node's separator describes its STORED
                            // content only: it is also the node's routing
                            // key and the input to its rank, so a
                            // buffered op may sit outside the span its
                            // own node advertises (see
                            // `ArchivedIndex::upper_bound`). Span alone
                            // therefore cannot decide relevance.
                            //
                            // The buffers settle it exactly, with no
                            // derivation: an op routes to exactly one
                            // link and is stored in THAT link's buffer
                            // (`link_novelty`), so asking each link's own
                            // buffer says precisely which children carry
                            // in-scope novelty. Re-deriving the routing
                            // from separators would not agree with
                            // `route`, which sends a key below the first
                            // separator to child 0 rather than to no
                            // child, and a walk that credited such a key
                            // to nobody would drop content the scope
                            // needs.
                            //
                            // A buffer that fails to decode cannot prove
                            // itself out of scope, so its node's children
                            // are all kept: over-retaining is safe,
                            // over-dropping loses content.
                            let in_scope = |key: &[u8]| {
                                scope.iter().any(|range| {
                                    key >= range.start().as_slice()
                                        && key <= range.end().as_slice()
                                })
                            };
                            for (at, link) in links.iter().enumerate() {
                                let upper =
                                    links.get(at + 1).map(|next| next.separator.as_slice());
                                let buffered = match index.buffer_for(at) {
                                    Some(buffer) => buffer
                                        .any_key::<Key>(&in_scope)
                                        .unwrap_or(true),
                                    None => false,
                                };
                                if buffered
                                    || span_intersects(&link.separator, upper, scope)
                                {
                                    queue.push_back(link.node.clone());
                                }
                            }
                        }
                    }
                }

                yield Visit::Present(node);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;
    use futures_util::StreamExt as _;

    use super::{Traversable as _, Visit};
    use crate::{ContentAddressedStorage, Delta, PersistentTree};
    use dialog_common::Blake3Hash;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A tree deep enough to have real index nodes, keyed so the leading
    /// byte partitions it into regions the way the artifact tree's tag
    /// byte does.
    async fn tagged_tree(
        storage: &mut ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
        tags: &[u8],
        per_tag: u32,
    ) -> Result<PersistentTree<[u8; 5], Vec<u8>>> {
        let mut tree = PersistentTree::<[u8; 5], Vec<u8>>::empty();
        let mut delta = Delta::zero();
        for tag in tags {
            for i in 0..per_tag {
                let mut key = [0u8; 5];
                key[0] = *tag;
                key[1..].copy_from_slice(&i.to_be_bytes());
                tree = tree
                    .edit()
                    .insert(key, vec![*tag; 512], storage)
                    .await?
                    .persist(&mut delta)?;
                for (_, buffer) in delta.flush() {
                    storage
                        .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                        .await?;
                }
            }
        }
        Ok(tree)
    }

    /// [`tagged_tree`] over any backend, so a test can observe the reads.
    async fn tagged_tree_over<B>(
        storage: &mut ContentAddressedStorage<B>,
        tags: &[u8],
        per_tag: u32,
    ) -> Result<PersistentTree<[u8; 5], Vec<u8>>>
    where
        B: dialog_storage::StorageBackend<
                Key = Blake3Hash,
                Value = Vec<u8>,
                Error = dialog_storage::DialogStorageError,
            > + dialog_common::ConditionalSend
            + dialog_common::ConditionalSync,
    {
        let mut tree = PersistentTree::<[u8; 5], Vec<u8>>::empty();
        let mut delta = Delta::zero();
        for tag in tags {
            for i in 0..per_tag {
                let mut key = [0u8; 5];
                key[0] = *tag;
                key[1..].copy_from_slice(&i.to_be_bytes());
                tree = tree
                    .edit()
                    .insert(key, vec![*tag; 512], storage)
                    .await?
                    .persist(&mut delta)?;
                for (_, buffer) in delta.flush() {
                    storage
                        .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                        .await?;
                }
            }
        }
        Ok(tree)
    }

    /// A deep, narrow tree: a segment target small enough that a thousand
    /// tiny entries branch several levels deep, so a walk that pays a
    /// barrier per level pays several.
    async fn paced_tree_over<B>(
        storage: &mut ContentAddressedStorage<B>,
        keys: core::ops::Range<u32>,
    ) -> Result<PersistentTree<[u8; 4], Vec<u8>>>
    where
        B: dialog_storage::StorageBackend<
                Key = Blake3Hash,
                Value = Vec<u8>,
                Error = dialog_storage::DialogStorageError,
            > + dialog_common::ConditionalSend
            + dialog_common::ConditionalSync,
    {
        let manifest = crate::Manifest {
            max_segment: 512,
            frame_ceiling_factor: 0,
            ..crate::Manifest::default()
        };
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();
        for i in keys {
            tree = crate::TransientTree::with_manifest(
                tree.root().clone(),
                tree.node_cache(),
                manifest,
            )
            .insert(i.to_be_bytes(), vec![i as u8], storage)
            .await?
            .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }
        Ok(tree)
    }

    fn tag_span(tag: u8) -> core::ops::RangeInclusive<Vec<u8>> {
        vec![tag]..=vec![tag, 0xFF, 0xFF, 0xFF, 0xFF]
    }

    /// Every entry a scoped walk's nodes hold that falls inside the scope
    /// must be exactly the set an unscoped walk would have surfaced there:
    /// pruning may cost extra nodes, never in-scope entries.
    #[dialog_common::test]
    async fn it_keeps_every_in_scope_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let tree = tagged_tree(&mut storage, &[0, 1, 3], 1200).await?;
        let scope = [tag_span(0)];

        let collect = |scoped: bool| {
            let storage = &storage;
            let tree = &tree;
            let scope = &scope;
            async move {
                let visits = if scoped {
                    futures_util::future::Either::Left(
                        tree.traverse_available_within(storage, scope),
                    )
                } else {
                    futures_util::future::Either::Right(tree.traverse_available(storage))
                };
                futures_util::pin_mut!(visits);
                let mut keys: Vec<Vec<u8>> = Vec::new();
                let mut nodes = 0usize;
                while let Some(visit) = visits.next().await {
                    if let Visit::Present(node) = visit? {
                        nodes += 1;
                        if let crate::ArchivedNodeBody::Segment(segment) = node.body() {
                            segment.for_each_entry::<[u8; 5], _>(|key, _| {
                                keys.push(key.to_vec());
                                Ok(())
                            })?;
                        }
                    }
                }
                keys.sort();
                anyhow::Ok((keys, nodes))
            }
        };

        let (all_keys, all_nodes) = collect(false).await?;
        let (scoped_keys, scoped_nodes) = collect(true).await?;

        let in_scope = |key: &Vec<u8>| key.first() == Some(&0u8);
        let expected: Vec<Vec<u8>> = all_keys.iter().filter(|k| in_scope(k)).cloned().collect();
        let got: Vec<Vec<u8>> = scoped_keys
            .iter()
            .filter(|k| in_scope(k))
            .cloned()
            .collect();

        assert_eq!(
            got, expected,
            "a scoped walk must surface every in-scope entry the full walk holds"
        );
        assert!(
            scoped_nodes < all_nodes,
            "scoping must actually prune: visited {scoped_nodes} of {all_nodes} nodes"
        );
        Ok(())
    }

    /// The whole key space as one scope is the unscoped walk.
    #[dialog_common::test]
    async fn it_matches_the_unscoped_walk_at_full_scope() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let tree = tagged_tree(&mut storage, &[0, 1, 3], 200).await?;
        let full = [Vec::new()..=vec![0xFF; 5]];

        let hashes = |scoped: bool| {
            let storage = &storage;
            let tree = &tree;
            let full = &full;
            async move {
                let visits = if scoped {
                    futures_util::future::Either::Left(
                        tree.traverse_available_within(storage, full),
                    )
                } else {
                    futures_util::future::Either::Right(tree.traverse_available(storage))
                };
                futures_util::pin_mut!(visits);
                let mut seen = Vec::new();
                while let Some(visit) = visits.next().await {
                    if let Visit::Present(node) = visit? {
                        seen.push(node.hash().clone());
                    }
                }
                seen.sort();
                anyhow::Ok(seen)
            }
        };

        assert_eq!(hashes(true).await?, hashes(false).await?);
        Ok(())
    }

    /// An in-scope op that exists ONLY as buffered novelty must keep the
    /// child it routes to.
    ///
    /// This pins the direction the scoped walk must never take: dropping a
    /// child whose in-scope content the walk cannot see from separators
    /// alone. The buffers say exactly which child carries what -- an op
    /// routes to one link and lives in that link's buffer -- so the walk
    /// asks each link's own buffer rather than re-deriving the routing from
    /// spans, which does not agree with `route` at the edges.
    #[dialog_common::test]
    async fn it_keeps_a_child_whose_only_in_scope_content_is_buffered() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // Stored content is tag 2 and tag 3 only: every stored key, and
        // every separator, sits outside the tag-1 scope.
        let tree = tagged_tree(&mut storage, &[2, 3], 1200).await?;

        // Tag-1 keys exist ONLY as buffered novelty.
        let mut hitchhiker = crate::HitchhikerTree::open(&tree);
        for i in 0..4u32 {
            let mut key = [0u8; 5];
            key[0] = 1;
            key[1..].copy_from_slice(&i.to_be_bytes());
            hitchhiker = hitchhiker.insert(key, vec![0u8; 8], &storage).await?;
        }
        let mut delta = Delta::zero();
        let root = hitchhiker.persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        let tree = PersistentTree::<[u8; 5], Vec<u8>>::from_hash(root);

        // Fixture check: the ops must live as novelty, or this pins nothing.
        let buffered = |tree: &PersistentTree<[u8; 5], Vec<u8>>| {
            let storage = &storage;
            let tree = tree.clone();
            async move {
                let visits = tree.traverse_available(storage);
                futures_util::pin_mut!(visits);
                let mut count = 0usize;
                while let Some(visit) = visits.next().await {
                    if let Visit::Present(node) = visit?
                        && let crate::ArchivedNodeBody::Index(index) = node.body()
                    {
                        count += index
                            .all_novelty::<[u8; 5]>()?
                            .into_iter()
                            .filter(|entry| entry.key[0] == 1)
                            .count();
                    }
                }
                anyhow::Ok(count)
            }
        };
        assert_eq!(
            buffered(&tree).await?,
            4,
            "fixture: the in-scope ops must live as novelty, not in a leaf"
        );

        let scope = [tag_span(1)];
        let mut found = 0usize;
        let visits = tree.traverse_available_within(&storage, &scope);
        futures_util::pin_mut!(visits);
        while let Some(visit) = visits.next().await {
            if let Visit::Present(node) = visit? {
                match node.body() {
                    crate::ArchivedNodeBody::Segment(segment) => {
                        segment.for_each_entry::<[u8; 5], _>(|key, _| {
                            if key[0] == 1 {
                                found += 1;
                            }
                            Ok(())
                        })?;
                    }
                    crate::ArchivedNodeBody::Index(index) => {
                        found += index
                            .all_novelty::<[u8; 5]>()?
                            .into_iter()
                            .filter(|entry| entry.key[0] == 1)
                            .count();
                    }
                }
            }
        }

        assert_eq!(
            found, 4,
            "a scoped walk must surface in-scope ops that live only as novelty"
        );
        Ok(())
    }

    /// A scope the tree's keys cannot meet prunes every child whose span
    /// is bounded away from it.
    ///
    /// The rightmost child is the documented exception: a link carries only
    /// a lower bound, and the last one has no successor to bound it above,
    /// so its span runs open to the top of the key space and it is kept.
    /// That is the conservative direction — over-retain, never over-drop —
    /// and it is the same bound `TreeDifference::retain_scope` works with.
    #[dialog_common::test]
    async fn it_prunes_a_scope_the_tree_cannot_meet() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let tree = tagged_tree(&mut storage, &[0, 1], 400).await?;
        let scope = [tag_span(0xFE)];

        let mut all = 0usize;
        {
            let visits = tree.traverse_available(&storage);
            futures_util::pin_mut!(visits);
            while let Some(visit) = visits.next().await {
                if matches!(visit?, Visit::Present(_)) {
                    all += 1;
                }
            }
        }

        let mut pruned = 0usize;
        {
            let visits = tree.traverse_available_within(&storage, &scope);
            futures_util::pin_mut!(visits);
            while let Some(visit) = visits.next().await {
                if matches!(visit?, Visit::Present(_)) {
                    pruned += 1;
                }
            }
        }

        assert!(
            all > 2,
            "the fixture must have real branching to prune (got {all} nodes)"
        );
        assert!(
            pruned < all,
            "an unmeetable scope must prune: kept {pruned} of {all}"
        );
        Ok(())
    }

    /// The per-level fan-out must survive a consumer that takes ONE item
    /// per poll.
    ///
    /// The level's reads live inside this generator, and the generator
    /// only runs while its consumer polls it. A consumer that awaits each
    /// visit before asking for the next is the common shape (a download
    /// draining the walk), so the fan-out has to hold under exactly that
    /// discipline, and it has to be asserted: a reactor-backed runtime
    /// would carry the sockets regardless of who polls and hide a walk
    /// that had quietly gone one read at a time.
    ///
    /// `ObservingBackend` yields once inside every read, so an overlap
    /// here means the reads genuinely coexisted rather than merely
    /// completing back to back.
    /// The walk is a continuation queue, not a level walk: a node's
    /// children are queued the moment it lands, so under a connection
    /// cap a child is requested while its parent's siblings
    /// are still in flight. A level walk drains a level before opening
    /// the next -- every read of the level awaited before the next
    /// level's first goes out -- which over a capped transport is a
    /// round trip of idle slots per level; on a deep tree that is most
    /// of the walk.
    #[dialog_common::test]
    async fn it_walks_without_level_barriers() -> Result<()> {
        use crate::helpers::ObservingBackend;

        // Two slots, like a browser holding few connections to a host:
        // the cap is what makes the reader's shape visible in the order
        // reads start and complete (see `ObservingBackend::with_capacity`).
        let backend = ObservingBackend::with_capacity(2);
        let mut storage = ContentAddressedStorage::new(backend.clone());
        let tree = paced_tree_over(&mut storage, 0..1200).await?;
        let levels = backend
            .levels_of::<[u8; 4], Vec<u8>>(tree.root().clone())
            .await?;
        assert!(
            levels.len() >= 3,
            "the fixture must be deep (got {} levels)",
            levels.len()
        );

        backend.reset();
        let visits = tree.traverse_available(&storage);
        futures_util::pin_mut!(visits);
        let mut seen = 0usize;
        while let Some(visit) = visits.next().await {
            match visit? {
                Visit::Present(_) => seen += 1,
                Visit::Absent(hash) => panic!("a complete tree has no absent node: {hash}"),
            }
        }
        assert!(seen > 8, "the walk must visit a real tree (saw {seen})");

        let overlapped = levels
            .windows(2)
            .filter(|pair| pair[0].len() > 1)
            .any(|pair| backend.requested_before_level_completed(&pair[0], &pair[1]));
        assert!(
            overlapped,
            "no child read was requested while its parent's level was still in flight, \
             over {} levels: the walk waits for the slowest read of each level \
             before it opens the next",
            levels.len()
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_overlaps_a_level_under_a_one_item_consumer() -> Result<()> {
        use crate::helpers::ObservingBackend;

        let backend = ObservingBackend::new();
        let mut storage = ContentAddressedStorage::new(backend.clone());
        // Wide enough that a level holds many siblings to overlap.
        let tree = tagged_tree_over(&mut storage, &[0u8, 1, 3], 1200).await?;

        backend.reset();
        let visits = tree.traverse_available(&storage);
        futures_util::pin_mut!(visits);
        let mut seen = 0usize;
        while let Some(visit) = visits.next().await {
            match visit? {
                Visit::Present(_) => seen += 1,
                Visit::Absent(hash) => panic!("a complete tree has no absent node: {hash}"),
            }
        }

        let peak = backend.peak_reads_in_flight();
        assert!(seen > 3, "the walk must visit a real tree (saw {seen})");
        assert!(
            peak > 1,
            "a level's reads must overlap even when the consumer takes one \
             item per poll: peak was {peak} over {seen} nodes, so each read \
             cost its own round trip",
        );

        Ok(())
    }

    /// The same overlap, observed THROUGH A NESTED GENERATOR, the shape
    /// the download actually has.
    ///
    /// `snapshot.rs` wraps this walk in its own `try_stream!`: the outer
    /// generator drains `traverse` with a `while let` and yields each
    /// block onward, so the level's reads sit two generators deep under a
    /// one-item-per-poll consumer. The direct-consumer sibling pins the
    /// fan-out itself; this one pins that relaying it does not lose it.
    /// (The #492 hunt once suspected exactly this nesting; it was
    /// measured innocent, and the pin keeps it that way.)
    #[dialog_common::test]
    async fn it_overlaps_a_level_through_a_nested_generator() -> Result<()> {
        use crate::helpers::ObservingBackend;

        let backend = ObservingBackend::new();
        let mut storage = ContentAddressedStorage::new(backend.clone());
        let tree = tagged_tree_over(&mut storage, &[0u8, 1, 3], 1200).await?;

        backend.reset();
        // Wrap the walk the way the snapshot export does: an outer
        // generator that drains it and re-yields each visit.
        let relayed = async_stream::try_stream! {
            let visits = tree.traverse_available(&storage);
            futures_util::pin_mut!(visits);
            while let Some(visit) = visits.next().await {
                let visit: Visit<[u8; 5], Vec<u8>> = visit?;
                yield visit;
            }
        };
        type Relayed = std::pin::Pin<
            Box<
                dyn futures_core::Stream<
                        Item = Result<Visit<[u8; 5], Vec<u8>>, crate::DialogSearchTreeError>,
                    >,
            >,
        >;
        let mut relayed: Relayed = Box::pin(relayed);

        let mut seen = 0usize;
        while let Some(visit) = relayed.next().await {
            match visit? {
                Visit::Present(_) => seen += 1,
                Visit::Absent(hash) => panic!("a complete tree has no absent node: {hash}"),
            }
        }

        let peak = backend.peak_reads_in_flight();
        assert!(seen > 3, "the walk must visit a real tree (saw {seen})");
        assert!(
            peak > 1,
            "a level's reads must still overlap when the walk is relayed \
             through an outer generator: peak was {peak} over {seen} nodes.",
        );

        Ok(())
    }
}
