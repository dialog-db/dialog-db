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

/// How many block reads one traversal level issues concurrently.
///
/// Sized for a backend that reaches a remote on a miss: enough in flight
/// to hide round-trips, small enough not to swamp a local store or a
/// remote's connection limits.
const FETCH_CONCURRENCY: usize = 16;

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
            // Level order with the whole frontier fetched concurrently:
            // a level's reads are independent, so against a backend
            // that reaches a remote on a miss the wall clock is depth
            // round-trips, not node-count round-trips.
            let mut frontier = vec![root];

            while !frontier.is_empty() {
                let level = std::mem::take(&mut frontier);
                let mut reads = futures_util::stream::iter(level.into_iter().map(
                    |hash| async move {
                        // `retrieve` verifies stored bytes against the
                        // hash it was asked for, so `None` here is
                        // genuinely "not stored" -- a corrupt block
                        // raises instead, and still fails the walk.
                        let bytes = storage.retrieve(&hash).await;
                        (hash, bytes)
                    },
                ))
                .buffer_unordered(FETCH_CONCURRENCY);

                let mut next = Vec::new();
                while let Some((hash, bytes)) = reads.next().await {
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
                                    next.push(link.node);
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
                                // So the buffer is consulted too -- but per
                                // CHILD, not per node. A buffered op belongs
                                // to the child whose span covers it (that is
                                // where the descent would route it), so an
                                // in-scope buffered key rescues that one
                                // child. Rescuing every child instead would
                                // retain the whole level, which on a
                                // tag-partitioned tree means retaining every
                                // region -- the root's buffer always holds
                                // some in-scope op.
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
                                let mut rescued = vec![false; links.len()];
                                let mut undecodable = false;
                                for (at, link) in links.iter().enumerate() {
                                    let upper =
                                        links.get(at + 1).map(|next| next.separator.as_slice());
                                    let lower = link.separator.as_slice();
                                    match index.any_novelty_key::<Key>(|key| {
                                        // The child's own span, half-open
                                        // above exactly as `span_intersects`
                                        // treats it.
                                        key >= lower
                                            && upper.is_none_or(|upper| key < upper)
                                            && in_scope(key)
                                    }) {
                                        Ok(hit) => rescued[at] = hit,
                                        Err(_) => {
                                            undecodable = true;
                                            break;
                                        }
                                    }
                                }

                                for (at, link) in links.iter().enumerate() {
                                    let upper =
                                        links.get(at + 1).map(|next| next.separator.as_slice());
                                    if undecodable
                                        || rescued[at]
                                        || span_intersects(&link.separator, upper, scope)
                                    {
                                        next.push(link.node.clone());
                                    }
                                }
                            }
                        }
                    }

                    yield Visit::Present(node);
                }
                frontier = next;
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
}
