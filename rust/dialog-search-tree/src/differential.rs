//! Tree differentiation and integration.
//!
//! This module computes the difference between two tree versions and applies
//! change streams onto a tree. Both operations are built around the same
//! frugality contract: **only blocks on differing paths are ever read**.
//! Subtrees whose hashes match between the two versions are pruned by
//! comparing the hashes carried in their parents' links, without loading the
//! subtrees themselves, so the number of storage reads is proportional to the
//! size of the difference rather than the size of the trees.
//!
//! [`TreeDifference::compute`] produces a sparse representation of both trees
//! holding only the differing regions. From it, two products are available:
//!
//! - [`TreeDifference::changes`]: entry-level [`Change`]s that transform the
//!   source tree into the target tree (used for merge/replication).
//! - [`TreeDifference::novel_nodes`]: the target-tree nodes absent from the
//!   source tree (used to upload exactly the missing blocks to a remote).

use std::cmp::Ordering;
use std::collections::HashSet;

use async_stream::try_stream;
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
use futures_core::Stream;
use futures_util::StreamExt;
use rkyv::{
    Deserialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    ArchivedNodeBody, Buffer, ContentAddressedStorage, DialogSearchTreeError, Distribution, Entry,
    Key, Link, NoveltyEntry, NoveltyOp, PersistentNode, PersistentTree, Value, into_owned,
    resolve_pending,
};

/// Represents a change in the key-value store.
#[derive(Clone, Debug)]
pub enum Change<Key, Value> {
    /// Adds an entry to the key-value store.
    Add(Entry<Key, Value>),
    /// Removes an entry from the key-value store.
    Remove(Entry<Key, Value>),
}

/// Represents a differential stream of changes in the key-value store.
pub trait Differential<Key, Value>:
    Stream<Item = Result<Change<Key, Value>, DialogSearchTreeError>>
{
}

impl<Key, Value, T> Differential<Key, Value> for T where
    T: Stream<Item = Result<Change<Key, Value>, DialogSearchTreeError>>
{
}

/// Either a loaded node or an unloaded reference in a [`SparseTree`].
///
/// Differentiation never loads nodes eagerly: a node stays a [`Link`] (hash
/// plus separator, obtained from its parent) until the comparison proves
/// the node lies on a differing path. Shared subtrees are recognized and
/// discarded while still unloaded, which is what keeps reads proportional to
/// the difference.
enum SparseTreeNode<Key, Value> {
    /// A fully loaded node together with the separator at its left edge.
    Loaded {
        /// The loaded node.
        node: PersistentNode<Key, Value>,
        /// The separator at the node's left edge (its lower bound; empty for
        /// a root or the global leftmost subtree).
        lower_bound: Vec<u8>,
        /// Ops routed here from ancestors' buffers, oldest first (see
        /// [`SparseTreeNode::Pending`]).
        pending: Vec<NoveltyEntry<Value>>,
    },
    /// An unloaded reference: hash and separator from the parent's link.
    Ref(Link),
    /// An unloaded reference carrying ops an ancestor had buffered for it.
    ///
    /// When an index node is expanded its buffered ops are routed to the
    /// children whose ranges cover them, exactly as a flush would route them.
    /// Without this the ops would be dropped with the node they were replaced
    /// by, and the difference would miss every write still sitting in a buffer.
    ///
    /// Such a node can never be pruned as shared: its hash is its stored hash,
    /// which says nothing about the ops pending against it (see
    /// [`SparseTree::prune`]).
    Pending {
        /// The reference this node was reached by.
        link: Link,
        /// Ops pending against this subtree, oldest first: an op from a deeper
        /// node precedes one routed down from a shallower node, and within one
        /// node's buffer a later entry is the newer op, so the LAST op for a
        /// key is always the newest. Last-wins resolution (the same rule a
        /// single buffer uses) therefore picks the shallowest, newest op.
        pending: Vec<NoveltyEntry<Value>>,
    },
    /// A subtree whose difference was fully resolved from buffers alone.
    ///
    /// Produced by [`SparseTree::settle_buffered`] when two nodes hold identical
    /// children and differ only in their novelty: the stored content is shared,
    /// so the node contributes exactly the ops this side holds that the other
    /// does not, and nothing beneath it is ever read.
    Settled {
        /// The separator at the node's left edge (its lower bound), carried
        /// over from the node this entry replaced so the frontier stays sorted.
        lower_bound: Vec<u8>,
        /// The ops this side contributes.
        ops: Vec<NoveltyEntry<Value>>,
    },
}

impl<Key, Value> SparseTreeNode<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
        Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// The separator at the node's left edge: its sort key in the frontier.
    ///
    /// Two canonical trees assign the same separator to the same seam, so
    /// equal lower bounds line subtrees up for the hash comparison that
    /// prunes shared regions. A node's key span is bounded below by this
    /// separator and above (exclusively) by the next frontier node's
    /// separator, which by the separator invariant sorts strictly above this
    /// node's maximum key.
    fn lower_bound(&self) -> &[u8] {
        match self {
            SparseTreeNode::Loaded { lower_bound, .. } => lower_bound.as_slice(),
            SparseTreeNode::Ref(link) => link.separator.as_slice(),
            SparseTreeNode::Pending { link, .. } => link.separator.as_slice(),
            SparseTreeNode::Settled { lower_bound, .. } => lower_bound.as_slice(),
        }
    }

    fn hash(&self) -> &Blake3Hash {
        match self {
            SparseTreeNode::Loaded { node, .. } => node.hash(),
            SparseTreeNode::Ref(link) => &link.node,
            SparseTreeNode::Pending { link, .. } => &link.node,
            // A settled node stands for ops, not stored bytes; the null hash
            // keeps it from ever pruning against a real node.
            SparseTreeNode::Settled { .. } => NULL_BLAKE3_HASH,
        }
    }

    /// Whether this node is already loaded and is an index: expanding it
    /// costs no storage read, so the comparison loop prefers such nodes when
    /// it must peel a side to make progress.
    fn is_loaded_index(&self) -> bool {
        match self {
            SparseTreeNode::Loaded { node, .. } => {
                matches!(node.body(), Ok(ArchivedNodeBody::Index(_)))
            }
            SparseTreeNode::Ref(_)
            | SparseTreeNode::Pending { .. }
            | SparseTreeNode::Settled { .. } => false,
        }
    }

    /// Whether this node is a loaded index with a direct child of the given
    /// hash. A free containment peek: when one side's node IS a child of the
    /// other side's, expanding the container guarantees the pair prunes on
    /// the next pass without either subtree being read.
    fn links_contain(&self, hash: &Blake3Hash) -> bool {
        match self {
            SparseTreeNode::Loaded { node, .. } => match node.body() {
                Ok(ArchivedNodeBody::Index(index)) => index.contains_hash(hash),
                _ => false,
            },
            SparseTreeNode::Ref(_)
            | SparseTreeNode::Pending { .. }
            | SparseTreeNode::Settled { .. } => false,
        }
    }
}

/// A sparse, lazily loaded view over one side of a tree comparison.
///
/// Holds the current frontier of nodes sorted by lower bound (a mix of
/// loaded nodes and unloaded references) plus every index node that was
/// loaded and expanded along the way (the novel interior nodes of this
/// side).
struct SparseTree<'a, Key, Value, Backend>
where
    Key: self::Key,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    storage: &'a ContentAddressedStorage<Backend>,
    nodes: Vec<SparseTreeNode<Key, Value>>,
    expanded: Vec<PersistentNode<Key, Value>>,
    /// Every hash that ever entered this side's frontier, including nodes
    /// pruned or expanded away. Content addressing makes this the record of
    /// which subtrees this side is known to possess; the other side's
    /// novelty report filters against it, so a shared node that had to be
    /// expanded for alignment is still never reported novel.
    seen: HashSet<Blake3Hash>,
}

impl<'a, Key, Value, Backend> SparseTree<'a, Key, Value, Backend>
where
    Key: self::Key,
    Value: self::Value + PartialEq,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSend,
{
    /// Reads a node from storage by hash.
    async fn load(
        storage: &ContentAddressedStorage<Backend>,
        hash: &Blake3Hash,
    ) -> Result<PersistentNode<Key, Value>, DialogSearchTreeError> {
        let bytes = storage.retrieve(hash).await?.ok_or_else(|| {
            DialogSearchTreeError::Node(format!("Blob not found in storage: {hash}"))
        })?;
        Ok(PersistentNode::new(Buffer::from(bytes)))
    }

    /// Initializes a sparse tree from a root hash. The root is not loaded;
    /// a null hash produces an empty frontier.
    async fn from_root(
        root: &Blake3Hash,
        storage: &'a ContentAddressedStorage<Backend>,
    ) -> Result<SparseTree<'a, Key, Value, Backend>, DialogSearchTreeError> {
        let nodes = if root == NULL_BLAKE3_HASH {
            vec![]
        } else {
            let node: PersistentNode<Key, Value> = Self::load(storage, root).await?;
            // A root's frontier bound must be the separator the SAME subtree
            // would carry as a link child on the other side, or equal
            // subtrees at different heights never line up. That propagated
            // separator is the root's first link's separator (a node's
            // separator is its leftmost descendant's). Under the production
            // rule the global leftmost chain is empty everywhere, so this
            // is the empty bound; a distribution with a different
            // reseparation rule (the test simulator) still aligns.
            let lower_bound = match node.body()? {
                ArchivedNodeBody::Index(index) if !index.is_empty() => index.separator(0)?,
                _ => Vec::new(),
            };
            // The root inherits nothing: its own buffers are read when it is
            // expanded or streamed.
            vec![SparseTreeNode::Loaded {
                node,
                lower_bound,
                pending: Vec::new(),
            }]
        };

        let seen = nodes.iter().map(|node| node.hash().clone()).collect();
        Ok(SparseTree {
            storage,
            nodes,
            expanded: vec![],
            seen,
        })
    }

    /// Expands the node covering `bound`, if any.
    ///
    /// The frontier is sorted by lower bound (separator), so the last node
    /// whose separator is at or below `bound` is the only one whose key
    /// range can contain `bound`. If that node is an index, it is replaced
    /// by references to its children (loading it first when it is still a
    /// reference) and the node is recorded as expanded. If it is a segment,
    /// the loaded form replaces the reference in place, keeping its bound,
    /// so a later entry walk does not load it twice.
    ///
    /// Returns `true` when an index node was expanded.
    async fn expand_at(&mut self, bound: &[u8]) -> Result<bool, DialogSearchTreeError> {
        let covering = self
            .nodes
            .partition_point(|node| node.lower_bound() <= bound);
        if covering == 0 {
            return Ok(false);
        }
        let offset = covering - 1;

        // A settled node holds its whole answer already; expanding it would
        // read blocks to rediscover content proven shared.
        if matches!(&self.nodes[offset], SparseTreeNode::Settled { .. }) {
            return Ok(false);
        }

        let node = match &self.nodes[offset] {
            SparseTreeNode::Loaded { node, .. } => node.clone(),
            SparseTreeNode::Ref(link) | SparseTreeNode::Pending { link, .. } => {
                Self::load(self.storage, &link.node).await?
            }
            SparseTreeNode::Settled { .. } => unreachable!("returned above"),
        };

        // Ops this node has pending, plus any it inherited from an ancestor
        // expanded earlier. Expansion routes them to the children whose ranges
        // cover them, exactly as a flush would, so an op is never lost when the
        // node holding it is replaced by its links.
        let inherited = match &self.nodes[offset] {
            SparseTreeNode::Loaded { pending, .. } => pending.clone(),
            SparseTreeNode::Pending { pending, .. } => pending.clone(),
            SparseTreeNode::Ref(_) | SparseTreeNode::Settled { .. } => Vec::new(),
        };

        match node.body()? {
            ArchivedNodeBody::Index(index) => {
                let links = index.links()?;
                let mut children = Vec::with_capacity(links.len());
                for (at, link) in links.into_iter().enumerate() {
                    // Novelty is stored per child link, so this child's own
                    // ops ARE its link's buffer. Ops an ancestor routed down
                    // are narrowed to this child by the same lower-bound rule
                    // routing uses: this child takes `[sep(at), sep(at + 1))`,
                    // the leftmost child also takes everything below its own
                    // separator (routing clamps a key below every separator to
                    // it), and the rightmost child is open-ended: a buffered
                    // insert can sort past every existing key, and a flush
                    // routes such an op to the last child. Without those two
                    // open ends the op would match no child and be dropped.
                    //
                    // Order stays oldest first: the link's own buffer sits one
                    // level DEEPER than whatever an ancestor routed down, and
                    // a flush only moves ops toward the leaves, so the link's
                    // ops precede the inherited ones and last-wins resolution
                    // picks the shallowest, newest op.
                    let mut routed = index.link_novelty::<Key>(at)?;
                    let lower = index.separator(at)?;
                    let upper = if at + 1 < index.len() {
                        Some(index.separator(at + 1)?)
                    } else {
                        None
                    };
                    routed.extend(
                        inherited
                            .iter()
                            .filter(|entry| {
                                let key = entry.key.as_slice();
                                (at == 0 || key >= lower.as_slice())
                                    && upper.as_ref().is_none_or(|upper| key < upper.as_slice())
                            })
                            .cloned(),
                    );
                    let child = if routed.is_empty() {
                        SparseTreeNode::Ref(link)
                    } else {
                        SparseTreeNode::Pending {
                            link,
                            pending: routed,
                        }
                    };
                    self.seen.insert(child.hash().clone());
                    children.push(child);
                }

                self.expanded.push(node);
                self.nodes.splice(offset..offset + 1, children);
                Ok(true)
            }
            ArchivedNodeBody::Segment(_) => {
                let lower_bound = self.nodes[offset].lower_bound().to_vec();
                self.nodes[offset] = SparseTreeNode::Loaded {
                    node,
                    lower_bound,
                    pending: inherited,
                };
                Ok(false)
            }
        }
    }

    /// Resolves a pair of nodes that differ only in their novelty buffers,
    /// returning whether it applied.
    ///
    /// Two index nodes with identical child links hold identical stored content:
    /// every subtree beneath them is shared by hash. Their entire difference is
    /// therefore the difference between their two op sets, and descending would
    /// read blocks only to rediscover that they match. This is the case a write
    /// buffer is *for*: a new fact sits in the root's novelty, no child hash
    /// moves, and the two roots fully describe the change.
    ///
    /// Both nodes are replaced by leaf-like frontier entries carrying only the
    /// ops each side holds that the other does not, so the entry walk yields
    /// exactly those as changes. Ops the two sides agree on cancel and are
    /// dropped.
    ///
    /// Declines (returning `false`, leaving both frontiers untouched) when the
    /// links differ, when either node is a leaf, or when any op could shadow a
    /// stored entry — a key that might already exist has to be resolved against
    /// the leaf that holds it, because reporting the change requires the value
    /// being replaced.
    async fn settle_buffered(
        &mut self,
        at: usize,
        other: &mut Self,
        other_at: usize,
    ) -> Result<bool, DialogSearchTreeError> {
        // Settling reads a node's own buffer and replaces the frontier entry
        // wholesale, so ops an ancestor routed down to it would be lost. Decline
        // rather than rely on that state being unreachable: the fast path is an
        // optimization, and refusing it costs only a descent.
        let carries_inherited = |node: &SparseTreeNode<Key, Value>| {
            matches!(node, SparseTreeNode::Pending { pending, .. } if !pending.is_empty())
                || matches!(node, SparseTreeNode::Loaded { pending, .. } if !pending.is_empty())
        };
        if carries_inherited(&self.nodes[at]) || carries_inherited(&other.nodes[other_at]) {
            return Ok(false);
        }

        // Settling must be free. Loading an unloaded node to discover that it
        // cannot settle would charge a read for a check that answered "no",
        // which is exactly the read the differential exists to avoid: on
        // canonical trees (no novelty anywhere) settling never applies, so
        // every such load would be pure waste and would show up as a
        // difference reading blocks it should have pruned. Both sides must
        // already be loaded, or the caller falls through to the descent that
        // would load them anyway.
        let (
            SparseTreeNode::Loaded { node: ours, .. },
            SparseTreeNode::Loaded { node: theirs, .. },
        ) = (&self.nodes[at], &other.nodes[other_at])
        else {
            return Ok(false);
        };
        let (ours, theirs) = (ours.clone(), theirs.clone());

        let (ArchivedNodeBody::Index(ours_index), ArchivedNodeBody::Index(theirs_index)) =
            (ours.body()?, theirs.body()?)
        else {
            return Ok(false);
        };

        // Identical children: every subtree below is shared, so nothing under
        // these nodes can differ.
        if ours_index.len() != theirs_index.len() {
            return Ok(false);
        }
        for at in 0..ours_index.len() {
            if ours_index.hash_at(at)? != theirs_index.hash_at(at)? {
                return Ok(false);
            }
            // Separators are the routing keys: identical child hashes under
            // shifted separators would route the same op to different subtrees
            // on the two sides, so require those to agree too.
            if ours_index.separator(at)? != theirs_index.separator(at)? {
                return Ok(false);
            }
        }

        let ours_ops = ours_index.all_novelty::<Key>()?;
        let theirs_ops = theirs_index.all_novelty::<Key>()?;

        // Emitting a change for a buffered op needs to know whether the key
        // already exists below: absent means `Add`, present means `Remove(old)`
        // then `Add(new)`, and only the leaf knows the old value. A `Link`
        // carries just a separator and a hash, so membership is not answerable
        // from here, with one exception.
        //
        // Separators are lower bounds, so every key stored under this index
        // sorts at or above the leftmost separator. A key strictly below it is
        // therefore provably absent from every shared subtree, and its op is
        // unambiguously an insert.
        //
        // This is the mirror of the upper-bound formulation: with lower-bound
        // separators the *right* end is open (the last child takes whatever
        // remains), so nothing can be proven absent above the table, while the
        // left end is closed and gives the proof instead.
        //
        // Retracts never settle: a retract of an absent key is a no-op, and of a
        // present key needs the stored value to report the removal.
        if ours_index.is_empty() {
            return Ok(false);
        }
        let lowest = ours_index.separator(0)?;
        // An empty leftmost separator (the global leftmost spine under the
        // production reseparation rule) bounds nothing: no key sorts below it,
        // so no op can be proven an insert and settling never applies.
        if lowest.is_empty() {
            return Ok(false);
        }
        let settleable = |ops: &[NoveltyEntry<Value>]| {
            ops.iter().all(|entry| {
                matches!(entry.op, NoveltyOp::Assert(_)) && entry.key.as_slice() < lowest.as_slice()
            })
        };
        if !settleable(&ours_ops) || !settleable(&theirs_ops) {
            return Ok(false);
        }

        // Ops both sides hold are not changes; keep only what each side adds
        // that the other lacks.
        let retain = |mine: &[NoveltyEntry<Value>], other: &[NoveltyEntry<Value>]| {
            mine.iter()
                .filter(|entry| {
                    !other.iter().any(|seen| {
                        seen.key == entry.key
                            && match (&seen.op, &entry.op) {
                                (NoveltyOp::Assert(seen), NoveltyOp::Assert(mine)) => seen == mine,
                                (NoveltyOp::Retract, NoveltyOp::Retract) => true,
                                _ => false,
                            }
                    })
                })
                .cloned()
                .collect::<Vec<_>>()
        };

        let ours_only = retain(&ours_ops, &theirs_ops);
        let theirs_only = retain(&theirs_ops, &ours_ops);

        // Settling replaces both nodes without expanding them, so neither is
        // recorded by the usual `expanded` path — but each node's *block* is
        // still the only place its buffered ops live, and a peer materializing
        // this side's tree needs it. Record both here, or `novel_nodes` reports
        // a block set that cannot reconstruct the target (the change stream is
        // unaffected, so the gap only shows up in push/replication).
        self.expanded.push(ours);
        other.expanded.push(theirs);

        self.nodes[at] = SparseTreeNode::Settled {
            lower_bound: self.nodes[at].lower_bound().to_vec(),
            ops: ours_only,
        };
        other.nodes[other_at] = SparseTreeNode::Settled {
            lower_bound: other.nodes[other_at].lower_bound().to_vec(),
            ops: theirs_only,
        };
        Ok(true)
    }

    /// Removes nodes whose key span cannot intersect any range in
    /// `scope`, so out-of-scope subtrees are never expanded (and, on
    /// a partial replica, never fetched).
    ///
    /// Links carry only lower bounds (separators), so a node's span is
    /// bounded conservatively: within the frontier (sorted by lower
    /// bound, where pruning only ever *removes* nodes), a node's true
    /// span is contained in `[own separator, successor's separator)`
    /// (the successor's separator sorts strictly above this node's
    /// maximum by the separator invariant; the last node is unbounded
    /// above). A node is dropped only when that conservative span
    /// misses every scope range, which can only over-retain, never
    /// over-drop: in-scope changes are always preserved. Dropping is
    /// per-side, so the entry walk may surface spurious out-of-scope
    /// changes where only one side dropped a shared region;
    /// [`TreeDifference::changes_within`] filters those.
    fn retain_scope(&mut self, scope: &[core::ops::RangeInclusive<Key>]) {
        let uppers: Vec<Option<Vec<u8>>> = (0..self.nodes.len())
            .map(|at| {
                self.nodes
                    .get(at + 1)
                    .map(|next| next.lower_bound().to_vec())
            })
            .collect();
        let mut kept = Vec::with_capacity(self.nodes.len());
        for (node, upper) in self.nodes.drain(..).zip(uppers) {
            let lower = node.lower_bound();
            let intersects = scope.iter().any(|range| {
                // [lower, upper) ∩ [start, end] is non-empty iff
                // lower <= end and upper > start (with upper = +∞
                // for the last node).
                lower <= range.end().as_ref()
                    && match &upper {
                        Some(upper) => upper.as_slice() > range.start().as_ref(),
                        None => true,
                    }
            });
            // A node's bound describes its *stored* content only, deliberately:
            // the bound is also the node's routing key and the input to its
            // rank, so a pending op must not move it (see
            // `ArchivedIndex::upper_bound`). A buffered op can therefore sit
            // outside the span its own node advertises, and span alone cannot
            // decide whether the node is relevant.
            //
            // So the buffers are consulted directly. Both the ops expansion
            // routed into this node and the node's own `novelty` count: a loaded
            // node's buffer is in hand, and this is the only chance to look
            // before the node is dropped.
            //
            // An unread `Ref` needs no such check. Scope pruning at the frontier
            // only ever drops nodes whose *parent* was already expanded, and
            // expansion routes the parent's ops down into exactly the children
            // that cover them, so anything pending for this subtree is already
            // attached as routed ops.
            let in_scope = |key: &[u8]| {
                scope
                    .iter()
                    .any(|range| key >= range.start().as_ref() && key <= range.end().as_ref())
            };
            let pending_in_scope = pending_ops(&node)
                .iter()
                .any(|entry| in_scope(entry.key.as_slice()));
            let buffered_in_scope = !pending_in_scope
                && match &node {
                    SparseTreeNode::Loaded { node: loaded, .. } => match loaded.body() {
                        // A buffer that fails to decode cannot prove itself
                        // out of scope; keep the node (over-retaining is safe,
                        // over-dropping loses changes) and let the read path
                        // surface the error.
                        Ok(ArchivedNodeBody::Index(index)) => {
                            index.any_novelty_key::<Key>(in_scope).unwrap_or(true)
                        }
                        _ => false,
                    },
                    _ => false,
                };
            if intersects || pending_in_scope || buffered_in_scope {
                kept.push(node);
            }
        }
        self.nodes = kept;
    }

    /// Removes nodes that are shared between `self` and `other`, keeping
    /// only nodes that differ.
    ///
    /// Identity is the content hash alone: in a content-addressed canonical
    /// tree, an equal hash IS the same entries under the same keys, so a
    /// matched pair contributes nothing to the difference no matter where
    /// each side's frontier places it (subtrees line up across height
    /// mismatches, and a subtree whose separator shifted with a neighbor
    /// edit still prunes). Within one tree, sibling key ranges are disjoint,
    /// so a hash cannot recur in a frontier and set matching is exact. This
    /// is the step that realizes the read-frugality contract: matched nodes
    /// are discarded while still unloaded.
    ///
    /// A hash alone stops being sufficient once ops can be pending against a
    /// node: a buffered op changes what a subtree holds without changing the
    /// hash it was reached by. So a node carrying pending ops never prunes,
    /// and a node's *own* buffer is accounted for by the fact that it is part
    /// of the node's bytes and therefore of its hash. It is the ops'
    /// *difference* that makes a subtree interesting, not their presence: two
    /// sides carrying the same op reach the same hash and prune exactly as a
    /// shared flushed subtree does.
    fn prune(&mut self, other: &mut Self) {
        let prunable = |node: &SparseTreeNode<Key, Value>| pending_ops(node).is_empty();
        let left: HashSet<Blake3Hash> = self
            .nodes
            .iter()
            .filter(|node| prunable(node))
            .map(|node| node.hash().clone())
            .collect();
        let right: HashSet<Blake3Hash> = other
            .nodes
            .iter()
            .filter(|node| prunable(node))
            .map(|node| node.hash().clone())
            .collect();
        self.nodes
            .retain(|node| !prunable(node) || !right.contains(node.hash()));
        other
            .nodes
            .retain(|node| !prunable(node) || !left.contains(node.hash()));
    }

    /// Streams the entries of every node remaining in the frontier, in key
    /// order, descending through any nodes that are still indexes and merging
    /// each index node's pending ops (its `novelty`) over the entries beneath
    /// it.
    ///
    /// Buffered ops are part of the tree's logical content, so a differential
    /// blind to them would report a buffered insert as absent and let a
    /// buffered delete lose to the stored entry it hides. Merging them here,
    /// rather than in the comparison, is what makes the difference independent
    /// of *where* an op currently sits: two replicas that received the same op
    /// yield the same entry for it whether it is still buffered high in one
    /// tree and already flushed to a leaf in the other. The comparison in
    /// [`TreeDifference::changes`] then sees equal entries and reports nothing,
    /// which is the desired outcome for equal content under divergent flush
    /// history.
    ///
    /// A key CAN be pending at two depths of one tree: a flush moves the
    /// root's buffer into its children, and the next write to the same key
    /// re-buffers it at the root while the flushed copy still sits mid-level.
    /// Precedence between depths therefore matters, and it is depth order:
    /// ops flow root to leaf, so the shallower op is the newer. The ops
    /// collected along a path are kept oldest first (a node's own buffer
    /// before anything inherited from its ancestors) and reduced per key by
    /// last-op-wins in [`winning_ops`], so the shallowest op stands, exactly
    /// as a point read resolves and a full flush replays.
    fn stream(&self) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + '_ {
        try_stream! {
            for sparse_node in &self.nodes {
                // A settled node's difference is already resolved: emit its ops
                // as entries and read nothing. This is the payoff of the write
                // buffer, and the reason a root-buffered fact costs no descent.
                if let SparseTreeNode::Settled { ops, .. } = sparse_node {
                    let mut resolved: Vec<(Vec<u8>, Value)> = Vec::new();
                    for entry in ops {
                        // Last op wins, matching how a flush replays them.
                        if let Some(NoveltyOp::Assert(value)) = resolve_pending(ops, &entry.key)
                            && !resolved.iter().any(|(key, _)| key == &entry.key)
                        {
                            resolved.push((entry.key.clone(), value.clone()));
                        }
                    }
                    resolved.sort_by(|(left, _), (right, _)| left.cmp(right));
                    for (key, value) in resolved {
                        yield Entry { key: Key::try_from_bytes(&key)?, value };
                    }
                    continue;
                }

                let (node, routed) = match sparse_node {
                    SparseTreeNode::Loaded { node, pending, .. } => {
                        (node.clone(), pending.clone())
                    }
                    SparseTreeNode::Ref(link) => {
                        (Self::load(self.storage, &link.node).await?, Vec::new())
                    }
                    SparseTreeNode::Pending { link, pending } => {
                        (Self::load(self.storage, &link.node).await?, pending.clone())
                    }
                    SparseTreeNode::Settled { .. } => unreachable!("handled above"),
                };

                // In-order walk; children pushed in reverse so the stack pops
                // them in key order. Each frame carries the ops covering it,
                // oldest first, already narrowed to the frame's own range: at
                // every index the descent hands each child its own link's
                // buffer plus the inherited ops the child's share covers, so a
                // leaf receives exactly the ops a flush would deliver to it.
                // The seed is whatever expansion already routed to this node.
                let mut stack = vec![(node, routed)];
                while let Some((node, inherited)) = stack.pop() {
                    match node.body()? {
                        ArchivedNodeBody::Index(index) => {
                            let mut children = Vec::with_capacity(index.len());
                            for at in 0..index.len() {
                                let hash = index.hash_at(at)?;
                                let child = Self::load(self.storage, hash).await?;
                                // The child's own link buffer precedes the
                                // inherited ops: it is one level deeper than
                                // any ancestor that routed ops down here, and
                                // deeper means older, so oldest-first order
                                // (which last-wins resolution relies on) puts
                                // it first. Inherited ops are narrowed by the
                                // flush partition rule: child `at` takes
                                // `[sep(at), sep(at + 1))`, the leftmost child
                                // also takes everything below its separator
                                // (the routing clamp), and the rightmost child
                                // is open-ended, the same way a flush routes
                                // an op sorting past every stored key.
                                let mut pending = index.link_novelty::<Key>(at)?;
                                let lower = index.separator(at)?;
                                let upper = if at + 1 < index.len() {
                                    Some(index.separator(at + 1)?)
                                } else {
                                    None
                                };
                                pending.extend(
                                    inherited
                                        .iter()
                                        .filter(|entry| {
                                            let key = entry.key.as_slice();
                                            (at == 0 || key >= lower.as_slice())
                                                && upper
                                                    .as_ref()
                                                    .is_none_or(|upper| key < upper.as_slice())
                                        })
                                        .cloned(),
                                );
                                children.push((child, pending));
                            }
                            while let Some((child, pending)) = children.pop() {
                                stack.push((child, pending));
                            }
                        }
                        ArchivedNodeBody::Segment(segment) => {
                            // Resolve each key once: the winning covering op
                            // wins, and with no covering op the stored entry
                            // stands. Ops for keys the segment does not hold
                            // are inserts, and are emitted in key order among
                            // the stored entries. The descent already scoped
                            // the ops to this leaf, so no span filtering
                            // remains to do here.
                            let covering = winning_ops(&inherited);
                            let mut buffered = covering.into_iter().peekable();

                            let mut keys = segment.keys::<Key>()?;
                            while let Some((at, key)) = keys.next_key()? {
                                // Emit any buffered inserts that sort before
                                // this entry.
                                while let Some((buffered_key, _)) = buffered.peek() {
                                    if buffered_key.as_slice() >= key {
                                        break;
                                    }
                                    let (buffered_key, op) = buffered.next().expect("peeked");
                                    if let NoveltyOp::Assert(value) = op {
                                        yield Entry {
                                            key: Key::try_from_bytes(&buffered_key)?,
                                            value,
                                        };
                                    }
                                }

                                // A covering op supersedes the stored entry: an
                                // assert shadows it, a retract hides it.
                                if matches!(
                                    buffered.peek(),
                                    Some((buffered_key, _)) if buffered_key.as_slice() == key
                                ) {
                                    let (buffered_key, op) = buffered.next().expect("peeked");
                                    if let NoveltyOp::Assert(value) = op {
                                        yield Entry {
                                            key: Key::try_from_bytes(&buffered_key)?,
                                            value,
                                        };
                                    }
                                    continue;
                                }

                                yield Entry {
                                    // `key` borrows the decoder's reused buffer;
                                    // this owns the single copy.
                                    key: Key::try_from_bytes(key)?,
                                    value: into_owned(segment.value_at(at)?)?,
                                };
                            }

                            // Buffered inserts past the last stored entry.
                            for (buffered_key, op) in buffered {
                                if let NoveltyOp::Assert(value) = op {
                                    yield Entry {
                                        key: Key::try_from_bytes(&buffered_key)?,
                                        value,
                                    };
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// The ops pending against a frontier node, if any.
///
/// A node's buffered ops are part of its content but are not bounded by the key
/// span its links describe, so both scope pruning and shared-subtree pruning
/// have to consult them rather than reasoning from bounds and hashes alone.
fn pending_ops<Key, Value>(node: &SparseTreeNode<Key, Value>) -> &[NoveltyEntry<Value>] {
    match node {
        SparseTreeNode::Loaded { pending, .. } => pending,
        SparseTreeNode::Pending { pending, .. } => pending,
        SparseTreeNode::Settled { ops, .. } => ops,
        SparseTreeNode::Ref(_) => &[],
    }
}

/// Reduces the ops collected along a root-to-leaf path to the winning op per
/// key, sorted by key.
///
/// The descent narrows the ops to each frame's own range as it routes them
/// down (per-link buffers plus the flush partition rule), so by the leaf the
/// collected list is exactly the leaf's covering set and no span filtering
/// remains.
///
/// Ops accumulate oldest first: a link's own buffer is appended before the
/// ops inherited from its ancestors (deeper means older, since a flush only
/// moves ops toward the leaves), and within one buffer the newest op for a
/// key is last (writes append, then a *stable* sort by key preserves arrival
/// order within the key). The **last** op for a key is therefore the most
/// recent across every depth and wins. This matches how a point read resolves
/// a key and how a flush replays ops, so all three agree.
fn winning_ops<Value>(collected: &[NoveltyEntry<Value>]) -> Vec<(Vec<u8>, NoveltyOp<Value>)>
where
    Value: Clone,
{
    let mut winners: Vec<(Vec<u8>, NoveltyOp<Value>)> = Vec::new();
    for entry in collected {
        let key = entry.key.as_slice();
        // Last op wins, so a later op for a key replaces the one recorded so
        // far. This also absorbs the copies expansion made when it routed an
        // ancestor's ops down: the same op arriving twice resolves to itself.
        match winners.iter_mut().find(|(candidate, _)| candidate == key) {
            Some((_, op)) => *op = entry.op.clone(),
            None => winners.push((entry.key.clone(), entry.op.clone())),
        }
    }
    winners.sort_by(|(left, _), (right, _)| left.cmp(right));
    winners
}

/// Represents a difference computed between two trees (source, target).
///
/// Contains sparse representations of both trees holding only the differing
/// regions, and can produce either entry-level changes via
/// [`changes`](Self::changes) (to transform source into target) or
/// node-level novelty via [`novel_nodes`](Self::novel_nodes) (the target
/// nodes the source side does not have).
pub struct TreeDifference<'a, Key, Value, Backend>
where
    Key: self::Key,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    source: SparseTree<'a, Key, Value, Backend>,
    target: SparseTree<'a, Key, Value, Backend>,
}

impl<'a, Key, Value, Backend> TreeDifference<'a, Key, Value, Backend>
where
    Key: self::Key + ConditionalSync + 'static,
    Value: self::Value + PartialEq + ConditionalSync + 'static,
    Value: for<'b> rkyv::Serialize<
            Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'b>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSend
        + ConditionalSync,
{
    /// Computes the difference between two trees.
    ///
    /// Both trees are explored level by level in lockstep. At every step,
    /// nodes whose content hashes match on both sides are discarded
    /// without being loaded; only nodes whose hashes differ (or whose key
    /// ranges do not line up) are expanded. The number of reads is therefore
    /// proportional to the number of differing nodes, never to the size of
    /// the trees: in particular, two identical trees are recognized by their
    /// root hashes alone, with zero reads.
    pub async fn compute<D>(
        source_tree: &PersistentTree<Key, Value, D>,
        target_tree: &PersistentTree<Key, Value, D>,
        source_storage: &'a ContentAddressedStorage<Backend>,
        target_storage: &'a ContentAddressedStorage<Backend>,
    ) -> Result<TreeDifference<'a, Key, Value, Backend>, DialogSearchTreeError>
    where
        D: Distribution,
    {
        let mut difference = Self::compute_scoped(
            source_tree,
            target_tree,
            source_storage,
            target_storage,
            None,
        )
        .await?;

        // Expand any remaining target index nodes so novel_nodes() can
        // enumerate every novel node, not just unexpanded subtree roots.
        // These reads are not wasted: every remaining target node is novel
        // by construction and must be visited to be reported. One
        // left-to-right pass suffices: an expansion splices the children in
        // place of their parent, so re-checking the same position drills
        // down until a segment remains, and positions already passed can
        // never become expandable again.
        let mut index = 0;
        while index < difference.target.nodes.len() {
            let bound = difference.target.nodes[index].lower_bound().to_vec();
            if !difference.target.expand_at(&bound).await? {
                index += 1;
            }
        }

        Ok(difference)
    }

    /// Computes the difference between two trees *restricted to
    /// `scope`*: subtrees whose key span cannot intersect any scope
    /// range are dropped from the comparison without being loaded,
    /// so reads are proportional to the differing regions *within
    /// the scope* rather than to the full difference. On a partial
    /// replica this is what keeps a subscription's diff from
    /// fetching subtrees it never demanded.
    ///
    /// Consume via [`changes_within`](Self::changes_within) (which
    /// filters boundary spillover); [`novel_nodes`](Self::novel_nodes)
    /// is not meaningful on a scoped difference.
    pub async fn compute_within<D>(
        source_tree: &PersistentTree<Key, Value, D>,
        target_tree: &PersistentTree<Key, Value, D>,
        source_storage: &'a ContentAddressedStorage<Backend>,
        target_storage: &'a ContentAddressedStorage<Backend>,
        scope: &[core::ops::RangeInclusive<Key>],
    ) -> Result<TreeDifference<'a, Key, Value, Backend>, DialogSearchTreeError>
    where
        D: Distribution,
    {
        Self::compute_scoped(
            source_tree,
            target_tree,
            source_storage,
            target_storage,
            Some(scope),
        )
        .await
    }

    async fn compute_scoped<D>(
        source_tree: &PersistentTree<Key, Value, D>,
        target_tree: &PersistentTree<Key, Value, D>,
        source_storage: &'a ContentAddressedStorage<Backend>,
        target_storage: &'a ContentAddressedStorage<Backend>,
        scope: Option<&[core::ops::RangeInclusive<Key>]>,
    ) -> Result<TreeDifference<'a, Key, Value, Backend>, DialogSearchTreeError>
    where
        D: Distribution,
    {
        // Identical trees (including two empty trees) share their root
        // hash; nothing needs to be read at all.
        if source_tree.root() == target_tree.root() {
            return Ok(TreeDifference {
                source: SparseTree {
                    storage: source_storage,
                    nodes: vec![],
                    expanded: vec![],
                    seen: HashSet::new(),
                },
                target: SparseTree {
                    storage: target_storage,
                    nodes: vec![],
                    expanded: vec![],
                    seen: HashSet::new(),
                },
            });
        }

        let mut source: SparseTree<'a, Key, Value, Backend> =
            SparseTree::from_root(source_tree.root(), source_storage).await?;
        let mut target: SparseTree<'a, Key, Value, Backend> =
            SparseTree::from_root(target_tree.root(), target_storage).await?;

        // Iteratively prune shared nodes and expand differing ones until a
        // fixed point: only differing leaf segments (and unique-range
        // subtrees) remain.
        loop {
            if let Some(scope) = scope {
                source.retain_scope(scope);
                target.retain_scope(scope);
            }
            source.prune(&mut target);

            let mut expanded = false;
            let mut source_idx = 0;
            let mut target_idx = 0;

            while source_idx < source.nodes.len() && target_idx < target.nodes.len() {
                let source_bound = source.nodes[source_idx].lower_bound().to_vec();
                let target_bound = target.nodes[target_idx].lower_bound().to_vec();

                match source_bound.cmp(&target_bound) {
                    Ordering::Less => {
                        // The source node starts earlier and may span past
                        // the target node's left edge; expand it to reveal a
                        // child seam matching the target side.
                        if source.expand_at(&target_bound).await? {
                            expanded = true;
                            break;
                        }
                        source_idx += 1;
                    }
                    Ordering::Greater => {
                        if target.expand_at(&source_bound).await? {
                            expanded = true;
                            break;
                        }
                        target_idx += 1;
                    }
                    Ordering::Equal => {
                        if source.nodes[source_idx].hash() != target.nodes[target_idx].hash() {
                            // Two nodes that differ *only* in their buffers hold
                            // identical stored content, so their whole difference
                            // is the two op sets and nothing below is worth
                            // reading. This is what lets a buffered write sync
                            // from the roots alone rather than descending to the
                            // leaf its key would land in.
                            if source
                                .settle_buffered(source_idx, &mut target, target_idx)
                                .await?
                            {
                                source_idx += 1;
                                target_idx += 1;
                                continue;
                            }

                            // Equal lower bounds with differing hashes may be
                            // the same region at different heights: every
                            // leftmost spine shares its ancestor's lower
                            // bound. Peel ONE side per pass, so the taller
                            // side's child can line up with (and prune) the
                            // other side's node without it ever being read.
                            // Which side to peel, cheapest evidence first:
                            // a loaded index that directly links the other
                            // side's hash is expanded for a guaranteed prune;
                            // a loaded index expands without a storage read;
                            // otherwise the node spanning the wider key range
                            // (per its successor's bound; the frontier tail
                            // is unbounded) is the one containing the other,
                            // so it is peeled first.
                            let source_node = &source.nodes[source_idx];
                            let target_node = &target.nodes[target_idx];
                            let source_first = if source_node.links_contain(target_node.hash()) {
                                true
                            } else if target_node.links_contain(source_node.hash()) {
                                false
                            } else if source_node.is_loaded_index() != target_node.is_loaded_index()
                            {
                                source_node.is_loaded_index()
                            } else if source_node.is_loaded_index() {
                                false
                            } else {
                                let source_next =
                                    source.nodes.get(source_idx + 1).map(|n| n.lower_bound());
                                let target_next =
                                    target.nodes.get(target_idx + 1).map(|n| n.lower_bound());
                                match (source_next, target_next) {
                                    (None, Some(_)) => true,
                                    (Some(_), None) => false,
                                    (Some(source_next), Some(target_next)) => {
                                        source_next > target_next
                                    }
                                    (None, None) => false,
                                }
                            };
                            let (first, first_bound, second, second_bound) = if source_first {
                                (&mut source, &source_bound, &mut target, &target_bound)
                            } else {
                                (&mut target, &target_bound, &mut source, &source_bound)
                            };
                            if first.expand_at(first_bound).await? {
                                expanded = true;
                                break;
                            }
                            if second.expand_at(second_bound).await? {
                                expanded = true;
                                break;
                            }
                        }
                        source_idx += 1;
                        target_idx += 1;
                    }
                }
            }

            if !expanded {
                break;
            }
        }

        Ok(TreeDifference { source, target })
    }

    /// Returns a stream of the changes within `scope` that transform
    /// the source tree into the target tree. Use on a difference
    /// built by [`compute_within`](Self::compute_within): scope
    /// pruning is per-side, so a shared region dropped from only one
    /// frontier surfaces as spurious out-of-scope changes in the raw
    /// entry walk — this filter removes them, leaving exactly the
    /// in-scope changes.
    pub fn changes_within(
        &'a self,
        scope: &'a [core::ops::RangeInclusive<Key>],
    ) -> impl Differential<Key, Value> + 'a {
        let changes = self.changes();
        try_stream! {
            futures_util::pin_mut!(changes);
            for await change in changes {
                let change = change?;
                let key = match &change {
                    Change::Add(entry) => &entry.key,
                    Change::Remove(entry) => &entry.key,
                };
                if scope.iter().any(|range| range.contains(key)) {
                    yield change;
                }
            }
        }
    }

    /// The conservative key spans this difference confines all changes to,
    /// as `(inclusive lower bound, exclusive upper bound)` pairs: one per
    /// remaining frontier node on either side, in frontier order per side.
    /// The lower bound is the frontier node's own separator (the smallest
    /// key it can hold, so INCLUSIVE — a changed key can equal it); the
    /// upper bound is the next node's separator, which sorts strictly
    /// above this node's maximum key (so EXCLUSIVE); a `None` upper bound
    /// means the span runs open to the top of the key space.
    ///
    /// Conservative means superset: every changed key lies inside some
    /// span, but a span may also cover unchanged keys (shared nodes
    /// pruned between two divergent ones widen the reported spans, and a
    /// node's true lower bound is unknowable after its left siblings were
    /// pruned). Callers partitioning work by these spans over-include,
    /// never miss. Costs no reads: bounds come from the frontier links.
    pub fn divergent_bounds(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let mut bounds = Vec::new();
        for tree in [&self.source, &self.target] {
            // A frontier node's key span is `[its separator, the next node's
            // separator)`: separators are LOWER bounds, and the separator
            // invariant puts the next one strictly above this node's maximum
            // key. The final node has no successor, so its span runs open to
            // the top of the key space.
            for (at, node) in tree.nodes.iter().enumerate() {
                let lower = node.lower_bound().to_vec();
                let upper = tree
                    .nodes
                    .get(at + 1)
                    .map(|next| next.lower_bound().to_vec());
                bounds.push((lower, upper));
            }
        }
        bounds
    }

    /// Returns a stream of entry-level changes that transform the source
    /// tree into the target tree.
    ///
    /// Performs a two-cursor walk over the entries of both differing
    /// regions: keys only on the source side yield [`Change::Remove`], keys
    /// only on the target side yield [`Change::Add`], and keys present on
    /// both sides with different values yield a `Remove` of the old entry
    /// followed by an `Add` of the new one.
    pub fn changes(&'a self) -> impl Differential<Key, Value> + 'a {
        let source_stream = self.source.stream();
        let target_stream = self.target.stream();

        try_stream! {
            futures_util::pin_mut!(source_stream);
            futures_util::pin_mut!(target_stream);

            let mut source_next = source_stream.next().await;
            let mut target_next = target_stream.next().await;

            loop {
                match (&source_next, &target_next) {
                    (None, None) => break,
                    (Some(Ok(source_entry)), None) => {
                        yield Change::Remove(source_entry.clone());
                        source_next = source_stream.next().await;
                    }
                    (None, Some(Ok(target_entry))) => {
                        yield Change::Add(target_entry.clone());
                        target_next = target_stream.next().await;
                    }
                    (Some(Err(_)), _) => {
                        if let Some(Err(error)) = source_next.take() {
                            Err(error)?;
                        }
                    }
                    (_, Some(Err(_))) => {
                        if let Some(Err(error)) = target_next.take() {
                            Err(error)?;
                        }
                    }
                    (Some(Ok(source_entry)), Some(Ok(target_entry))) => {
                        match source_entry.key.cmp(&target_entry.key) {
                            Ordering::Less => {
                                yield Change::Remove(source_entry.clone());
                                source_next = source_stream.next().await;
                            }
                            Ordering::Greater => {
                                yield Change::Add(target_entry.clone());
                                target_next = target_stream.next().await;
                            }
                            Ordering::Equal => {
                                if source_entry.value != target_entry.value {
                                    yield Change::Remove(source_entry.clone());
                                    yield Change::Add(target_entry.clone());
                                }
                                source_next = source_stream.next().await;
                                target_next = target_stream.next().await;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Returns a stream of the nodes present in the target tree but absent
    /// from the source tree.
    ///
    /// Yields every index node expanded during comparison plus the
    /// remaining (segment) frontier nodes. This is the block set a remote
    /// holding the source tree needs in order to materialize the target
    /// tree.
    pub fn novel_nodes(
        &'a self,
    ) -> impl Stream<Item = Result<PersistentNode<Key, Value>, DialogSearchTreeError>> + 'a {
        try_stream! {
            // A target node whose hash the source side has seen (in its
            // frontier, pruned or expanded) is shared, not novel, even when
            // frontier alignment forced it to be expanded before its match
            // surfaced.
            for node in &self.target.expanded {
                if self.source.seen.contains(node.hash()) {
                    continue;
                }
                yield node.clone();
            }

            for sparse_node in &self.target.nodes {
                // A node with pending ops is transferred as it is stored: the
                // ops are already part of the bytes of whichever ancestor
                // buffers them, and that ancestor is in `expanded` above, so
                // the seen-check below is still the right test for the block
                // this frontier entry names.
                if self.source.seen.contains(sparse_node.hash()) {
                    continue;
                }
                // A settled node names no block of its own: its ops live in the
                // bytes of the node that buffers them, which the walk already
                // recorded.
                let node = match sparse_node {
                    SparseTreeNode::Settled { .. } => continue,
                    SparseTreeNode::Loaded { node, .. } => node.clone(),
                    SparseTreeNode::Ref(link) | SparseTreeNode::Pending { link, .. } => {
                        SparseTree::<Key, Value, Backend>::load(self.target.storage, &link.node)
                            .await?
                    }
                };
                yield node;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use anyhow::Result;
    use async_trait::async_trait;
    use dialog_common::Blake3Hash;
    use dialog_storage::{DialogStorageError, MemoryStorageBackend, StorageBackend};
    use futures_util::StreamExt;

    use std::collections::HashSet;

    use dialog_storage::JournaledStorage;
    use futures_util::TryStreamExt;

    use futures_util::stream::iter;

    use super::{Change, TreeDifference};
    use crate::helpers::{TestStorage, Traversable as _, TraversalOrder, TreeNodes as _};
    use crate::{Buffer, ContentAddressedStorage, Delta, Entry, PersistentTree, tree_spec};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A storage backend that counts every read, so tests can assert that
    /// differentiation only loads blocks on differing paths.
    #[derive(Clone)]
    struct CountingBackend {
        inner: MemoryStorageBackend<Blake3Hash, Vec<u8>>,
        reads: Arc<AtomicUsize>,
    }

    impl CountingBackend {
        fn new() -> Self {
            Self {
                inner: MemoryStorageBackend::default(),
                reads: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn reads(&self) -> usize {
            self.reads.load(AtomicOrdering::Relaxed)
        }

        fn reset(&self) {
            self.reads.store(0, AtomicOrdering::Relaxed);
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl StorageBackend for CountingBackend {
        type Key = Blake3Hash;
        type Value = Vec<u8>;
        type Error = DialogStorageError;

        async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
            self.inner.set(key, value).await
        }

        async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
            self.reads.fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.get(key).await
        }
    }

    type TestTree = PersistentTree<[u8; 4], Vec<u8>>;

    async fn build(
        keys: impl IntoIterator<Item = (u32, Vec<u8>)>,
        storage: &mut ContentAddressedStorage<CountingBackend>,
    ) -> Result<TestTree> {
        let mut tree = TestTree::empty();
        let mut delta = Delta::zero();
        for (key, value) in keys {
            tree = tree
                .edit()
                .insert(key.to_le_bytes(), value, storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }
        Ok(tree)
    }

    async fn collect_scoped_changes(
        source: &TestTree,
        target: &TestTree,
        scope: &[core::ops::RangeInclusive<[u8; 4]>],
        storage: &ContentAddressedStorage<CountingBackend>,
    ) -> Result<Vec<Change<[u8; 4], Vec<u8>>>> {
        let stream = source.differentiate_within(target, scope, storage, storage);
        futures_util::pin_mut!(stream);
        let mut changes = vec![];
        while let Some(change) = stream.next().await {
            changes.push(change?);
        }
        Ok(changes)
    }

    /// A scoped diff yields exactly the full diff filtered to the
    /// scope: in-scope changes are never dropped, boundary spillover
    /// from one-sided pruning is filtered out.
    ///
    /// Keys stay below 256 so little-endian key bytes order the same
    /// as the numbers they encode.
    #[dialog_common::test]
    async fn it_scopes_changes_to_the_given_ranges() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build((0..250u32).map(|i| (i, vec![0])), &mut storage).await?;

        // Two changed regions far apart in key space.
        let mut target = base.clone();
        let mut delta = Delta::zero();
        for i in (10..20u32).chain(200..210u32) {
            target = target
                .edit()
                .insert(i.to_le_bytes(), vec![1], &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        let scope = vec![0u32.to_le_bytes()..=50u32.to_le_bytes()];

        let full = collect_changes(&base, &target, &storage).await?;
        let scoped = collect_scoped_changes(&base, &target, &scope, &storage).await?;

        let key = |change: &Change<[u8; 4], Vec<u8>>| match change {
            Change::Add(entry) => entry.key,
            Change::Remove(entry) => entry.key,
        };
        let expected: Vec<[u8; 4]> = full
            .iter()
            .filter(|change| scope.iter().any(|range| range.contains(&key(change))))
            .map(&key)
            .collect();
        let actual: Vec<[u8; 4]> = scoped.iter().map(&key).collect();

        assert_eq!(
            actual, expected,
            "scoped diff must equal the full diff filtered to scope"
        );
        assert_eq!(
            scoped.len(),
            20,
            "ten modified keys in scope, one Remove + one Add each"
        );
        assert!(
            scoped.iter().all(|change| {
                key(change) >= 10u32.to_le_bytes() && key(change) < 20u32.to_le_bytes()
            }),
            "only the in-scope region's keys appear"
        );
        Ok(())
    }

    /// The point of scoping on a partial replica: subtrees whose key
    /// span misses the scope are never read, so a scoped diff reads
    /// strictly fewer blocks than the full diff when the
    /// out-of-scope region changed heavily.
    #[dialog_common::test]
    async fn it_avoids_reading_out_of_scope_subtrees() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build((0..250u32).map(|i| (i, vec![0])), &mut storage).await?;

        // A small in-scope change and a heavy out-of-scope change.
        let mut target = base.clone();
        let mut delta = Delta::zero();
        for i in (10..12u32).chain(100..250u32) {
            target = target
                .edit()
                .insert(i.to_le_bytes(), vec![1], &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        let scope = vec![0u32.to_le_bytes()..=20u32.to_le_bytes()];

        let before_full = storage.backend().reads();
        let full = collect_changes(&base, &target, &storage).await?;
        let full_reads = storage.backend().reads() - before_full;

        let before_scoped = storage.backend().reads();
        let scoped = collect_scoped_changes(&base, &target, &scope, &storage).await?;
        let scoped_reads = storage.backend().reads() - before_scoped;

        assert_eq!(scoped.len(), 4, "two in-scope keys, Remove + Add each");
        assert!(full.len() > scoped.len());
        assert!(
            scoped_reads < full_reads,
            "scoped diff must skip out-of-scope subtrees: \
             scoped {scoped_reads} reads vs full {full_reads} reads"
        );
        Ok(())
    }

    async fn collect_changes(
        source: &TestTree,
        target: &TestTree,
        storage: &ContentAddressedStorage<CountingBackend>,
    ) -> Result<Vec<Change<[u8; 4], Vec<u8>>>> {
        let stream = source.differentiate(target, storage, storage);
        futures_util::pin_mut!(stream);
        let mut changes = vec![];
        while let Some(change) = stream.next().await {
            changes.push(change?);
        }
        Ok(changes)
    }

    /// Applies `ops` to a buffered tree over `base` and persists it with its
    /// buffers intact, so the returned tree carries live novelty.
    async fn buffered(
        base: &TestTree,
        ops: &[(bool, u32, Vec<u8>)],
        op_buf_size: usize,
        storage: &mut ContentAddressedStorage<CountingBackend>,
    ) -> Result<TestTree> {
        let mut tree = crate::HitchhikerTree::open(base).with_op_buf_size(op_buf_size);
        for (is_insert, key, value) in ops {
            tree = if *is_insert {
                tree.insert(key.to_le_bytes(), value.clone(), storage)
                    .await?
            } else {
                tree.delete(key.to_le_bytes(), storage).await?
            };
        }
        let mut delta = Delta::zero();
        let root = tree.persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(TestTree::seal(root, Default::default()))
    }

    /// Applies `ops` to a buffered tree over `base` and canonicalizes it, so the
    /// returned tree is the fully flushed form of the same content.
    async fn canonicalized(
        base: &TestTree,
        ops: &[(bool, u32, Vec<u8>)],
        op_buf_size: usize,
        storage: &mut ContentAddressedStorage<CountingBackend>,
    ) -> Result<TestTree> {
        let mut tree = crate::HitchhikerTree::open(base).with_op_buf_size(op_buf_size);
        for (is_insert, key, value) in ops {
            tree = if *is_insert {
                tree.insert(key.to_le_bytes(), value.clone(), storage)
                    .await?
            } else {
                tree.delete(key.to_le_bytes(), storage).await?
            };
        }
        let mut delta = Delta::zero();
        let canonical = tree.canonicalize(storage, &mut delta).await?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(canonical)
    }

    /// Normalizes a change list to a comparable form: adds and removes keyed by
    /// entry, order-independent within a key.
    fn normalize(changes: &[Change<[u8; 4], Vec<u8>>]) -> BTreeMap<[u8; 4], (bool, Vec<u8>)> {
        let mut normalized = BTreeMap::new();
        for change in changes {
            match change {
                Change::Add(entry) => {
                    normalized.insert(entry.key, (true, entry.value.clone()));
                }
                Change::Remove(entry) => {
                    normalized
                        .entry(entry.key)
                        .or_insert((false, entry.value.clone()));
                }
            }
        }
        normalized
    }

    /// **The oracle.** A differential over trees carrying live buffers must
    /// report exactly what a differential over their canonicalized forms
    /// reports.
    ///
    /// This is what makes buffered trees safe to sync: where an op currently
    /// sits (still in a buffer, or already flushed into a leaf) is an artifact
    /// of each replica's own write volume, and must not change what the
    /// difference between two replicas *is*.
    #[dialog_common::test]
    async fn it_diffs_buffered_trees_like_canonicalized_ones() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build((0..300u32).map(|i| (i, vec![i as u8])), &mut storage).await?;

        for seed in 0..25u64 {
            let mut rng = 0x9E3779B97F4A7C15u64 ^ seed;
            let mut next = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                (rng >> 32) as u32
            };

            let mut source_ops = Vec::new();
            let mut target_ops = Vec::new();
            for _ in 0..60 {
                let is_insert = !next().is_multiple_of(3);
                let key = next() % 400;
                let value = vec![(next() % 251) as u8];
                if next().is_multiple_of(2) {
                    source_ops.push((is_insert, key, value));
                } else {
                    target_ops.push((is_insert, key, value));
                }
            }

            // Deliberately different buffer capacities, so the two sides flush
            // at different depths for the same keys: the depth asymmetry that
            // arises whenever two replicas write at different volumes.
            let source_buffered = buffered(&base, &source_ops, 8, &mut storage).await?;
            let target_buffered = buffered(&base, &target_ops, 512, &mut storage).await?;
            let source_canonical = canonicalized(&base, &source_ops, 8, &mut storage).await?;
            let target_canonical = canonicalized(&base, &target_ops, 512, &mut storage).await?;

            let buffered_changes =
                collect_changes(&source_buffered, &target_buffered, &storage).await?;
            let canonical_changes =
                collect_changes(&source_canonical, &target_canonical, &storage).await?;

            assert_eq!(
                normalize(&buffered_changes),
                normalize(&canonical_changes),
                "seed {seed}: the buffered diff must equal the canonicalized diff"
            );
        }
        Ok(())
    }

    /// The specific shape the oracle generalizes: the *same* op, still buffered
    /// on one side and already flushed to a leaf on the other, is one op, not
    /// two. Equal content under divergent flush history must diff to nothing.
    #[dialog_common::test]
    async fn it_reports_no_difference_across_divergent_flush_depth() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build((0..300u32).map(|i| (i, vec![i as u8])), &mut storage).await?;

        let ops: Vec<(bool, u32, Vec<u8>)> = vec![
            (true, 400, vec![1]),
            (true, 401, vec![2]),
            (false, 150, vec![]),
            (true, 42, vec![9]),
        ];

        // Same ops, same order, but one side keeps them in the root buffer while
        // the other cascades them toward the leaves.
        let shallow = buffered(&base, &ops, 100_000, &mut storage).await?;
        let deep = canonicalized(&base, &ops, 1, &mut storage).await?;

        assert_ne!(
            shallow.root(),
            deep.root(),
            "the trees must differ structurally, or the test proves nothing"
        );

        let changes = collect_changes(&shallow, &deep, &storage).await?;
        assert!(
            changes.is_empty(),
            "equal content under divergent flush depth must diff to nothing, got {changes:?}"
        );
        Ok(())
    }

    /// Identical trees are recognized by their root hashes alone: no
    /// changes, and crucially, zero storage reads.
    #[dialog_common::test]
    async fn it_yields_no_changes_and_no_reads_for_identical_trees() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let entries: Vec<(u32, Vec<u8>)> = (0..500u32).map(|i| (i, vec![i as u8])).collect();
        let a = build(entries.clone(), &mut storage).await?;
        let b = build(entries, &mut storage).await?;

        storage.backend().reset();
        let changes = collect_changes(&a, &b, &storage).await?;

        assert!(
            changes.is_empty(),
            "identical trees should yield no changes"
        );
        assert_eq!(
            storage.backend().reads(),
            0,
            "identical trees must be recognized without reading any blocks"
        );
        Ok(())
    }

    /// The change stream reports exactly the adds, removals and updates
    /// between the two trees, and applying it via integrate transforms the
    /// source tree into the target tree.
    #[dialog_common::test]
    async fn it_streams_changes_that_transform_source_into_target() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());

        let mut source_entries: BTreeMap<u32, Vec<u8>> =
            (0..300u32).map(|i| (i, vec![i as u8])).collect();
        let source = build(
            source_entries.iter().map(|(k, v)| (*k, v.clone())),
            &mut storage,
        )
        .await?;

        // Target: remove some keys, update some values, add new keys.
        let mut target_entries = source_entries.clone();
        for i in (0..300u32).step_by(50) {
            target_entries.remove(&i);
        }
        for i in (5..300u32).step_by(70) {
            target_entries.insert(i, vec![0xAB]);
        }
        for i in 300..330u32 {
            target_entries.insert(i, vec![i as u8]);
        }
        let target = build(
            target_entries.iter().map(|(k, v)| (*k, v.clone())),
            &mut storage,
        )
        .await?;

        let changes = collect_changes(&source, &target, &storage).await?;

        // Replay the changes over a plain map and compare against target.
        for change in &changes {
            match change {
                Change::Add(entry) => {
                    source_entries.insert(u32::from_le_bytes(entry.key), entry.value.clone());
                }
                Change::Remove(entry) => {
                    source_entries.remove(&u32::from_le_bytes(entry.key));
                }
            }
        }
        assert_eq!(
            source_entries, target_entries,
            "changes must transform the source entry set into the target's"
        );

        // Applying the same differential to the source tree must produce a
        // tree with the target's root.
        let mut merged = source.clone();
        let mut delta = Delta::zero();
        let changes = source.differentiate(&target, &storage, &storage);
        merged = merged
            .edit()
            .integrate(changes, &storage)
            .await?
            .persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        assert_eq!(
            merged.root(),
            target.root(),
            "integrating the differential must produce the target tree"
        );

        Ok(())
    }

    /// Differentiating two large trees that differ in a single entry reads
    /// only the blocks along the differing paths, not the whole trees.
    #[dialog_common::test]
    async fn it_reads_only_blocks_on_differing_paths() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let entries: Vec<(u32, Vec<u8>)> = (0..2000u32).map(|i| (i, vec![i as u8])).collect();

        let base = build(entries.clone(), &mut storage).await?;

        // One modified entry produces trees that differ along one root-to-
        // leaf path on each side.
        let mut modified = base.clone();
        let mut delta = Delta::zero();
        modified = modified
            .edit()
            .insert(1000u32.to_le_bytes(), vec![0xFF], &storage)
            .await?
            .persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        storage.backend().reset();
        let changes = collect_changes(&base, &modified, &storage).await?;
        let reads = storage.backend().reads();

        assert_eq!(changes.len(), 2, "one update is a Remove plus an Add");

        // The differing region is two root-to-leaf paths plus the segments
        // they end in. With ~2000 entries the trees hold dozens of nodes;
        // a path-proportional walk stays far below that. The bound is
        // deliberately loose to avoid coupling the test to the tree shape.
        assert!(
            reads <= 16,
            "diff of a single-entry change read {reads} blocks; expected a \
             path-proportional number, not the whole tree"
        );

        Ok(())
    }

    /// Concurrent adds of the same key resolve deterministically by value
    /// hash, regardless of integration order.
    #[dialog_common::test]
    async fn it_resolves_conflicting_adds_by_value_hash() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base_entries: Vec<(u32, Vec<u8>)> = (0..50u32).map(|i| (i, vec![i as u8])).collect();

        let base = build(base_entries.clone(), &mut storage).await?;

        let mut ours = base.clone();
        let mut delta_ours = Delta::zero();
        ours = ours
            .edit()
            .insert(99u32.to_le_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta_ours)?;
        for (_, buffer) in delta_ours.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        let mut theirs = base.clone();
        let mut delta_theirs = Delta::zero();
        theirs = theirs
            .edit()
            .insert(99u32.to_le_bytes(), vec![2], &storage)
            .await?
            .persist(&mut delta_theirs)?;
        for (_, buffer) in delta_theirs.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        // Integrate their changes into ours, and our changes into theirs;
        // both replicas must converge on the same value.
        let mut merged_ours = ours.clone();
        let mut delta_merged_ours = Delta::zero();
        let their_changes = base.differentiate(&theirs, &storage, &storage);
        merged_ours = merged_ours
            .edit()
            .integrate(their_changes, &storage)
            .await?
            .persist(&mut delta_merged_ours)?;

        let mut merged_theirs = theirs.clone();
        let mut delta_merged_theirs = Delta::zero();
        let our_changes = base.differentiate(&ours, &storage, &storage);
        merged_theirs = merged_theirs
            .edit()
            .integrate(our_changes, &storage)
            .await?
            .persist(&mut delta_merged_theirs)?;

        for (_, buffer) in delta_merged_ours.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        for (_, buffer) in delta_merged_theirs.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        assert_eq!(
            merged_ours.root(),
            merged_theirs.root(),
            "replicas must converge regardless of integration order"
        );

        Ok(())
    }

    /// The novel node stream contains exactly the blocks a holder of the
    /// source tree is missing: copying them over makes the target tree
    /// fully readable, and the stream never includes shared blocks.
    #[dialog_common::test]
    async fn it_yields_novel_nodes_sufficient_to_materialize_the_target() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let entries: Vec<(u32, Vec<u8>)> = (0..1000u32).map(|i| (i, vec![i as u8])).collect();

        let base = build(entries.clone(), &mut storage).await?;
        let mut extended = base.clone();
        let mut delta = Delta::zero();
        for i in 1000..1020u32 {
            extended = extended
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // A "remote" that already has the base tree.
        let mut remote = ContentAddressedStorage::new(CountingBackend::new());
        {
            let difference =
                TreeDifference::compute(&TestTree::empty(), &base, &storage, &storage).await?;
            let nodes = difference.novel_nodes();
            futures_util::pin_mut!(nodes);
            while let Some(node) = nodes.next().await {
                let node = node?;
                remote
                    .store(node.buffer().as_ref().to_vec(), node.hash())
                    .await?;
            }
        }

        // Upload only the novelty between base and extended.
        let difference = TreeDifference::compute(&base, &extended, &storage, &storage).await?;
        let nodes = difference.novel_nodes();
        futures_util::pin_mut!(nodes);
        let mut uploaded = 0;
        while let Some(node) = nodes.next().await {
            let node = node?;
            remote
                .store(node.buffer().as_ref().to_vec(), node.hash())
                .await?;
            uploaded += 1;
        }
        assert!(uploaded > 0, "extension must produce novel nodes");

        // The remote can now read the full extended tree.
        let restored = TestTree::from_hash(extended.root().clone());
        for (key, value) in (0..1020u32).map(|i| (i, vec![i as u8])) {
            assert_eq!(
                restored.get(&key.to_le_bytes(), &remote).await?,
                Some(value),
                "key {key} must be readable from the remote after upload"
            );
        }

        Ok(())
    }

    fn journaled_storage(backend: &MemoryStorageBackend<Blake3Hash, Vec<u8>>) -> TestStorage {
        ContentAddressedStorage::new(JournaledStorage::new(backend.clone()))
    }

    #[dialog_common::test]
    async fn test_diff_shared_left_subtree() -> Result<()> {
        let backend = MemoryStorageBackend::default();
        let storage_a = journaled_storage(&backend);
        let storage_b = journaled_storage(&backend);

        // Define tree structures using tree_spec! macro with read/prune expectations
        // () indicates nodes that should be pruned (not read)
        let spec_a = tree_spec![
            [                  ..l]
            [(..e), (f..i),    ..l]
        ]
        .build(storage_a)
        .await
        .unwrap();

        let spec_b = tree_spec![
            [                  ..s]
            [(..e), (f..i), ..m, ..s]
        ]
        .build(storage_b)
        .await
        .unwrap();

        // Run differentiate (journal is automatically enabled after build)
        let host_b = spec_b.tree().clone();
        let diff = host_b.differentiate(spec_a.tree(), spec_b.storage(), spec_a.storage());
        // consume so we actually perform reads
        let _: Vec<_> = diff.collect().await;

        spec_a.assert();
        spec_b.assert();

        Ok(())
    }

    #[dialog_common::test]
    async fn test_diff_fully_disjoint_trees() -> Result<()> {
        let backend = MemoryStorageBackend::default();
        let storage = journaled_storage(&backend);

        // Scenario: Trees have completely different key ranges - NO shared segments
        // Tree A has keys a-i, Tree B has keys p-x (completely disjoint)
        // All segments from both trees must be read since nothing is shared
        let spec_a = tree_spec![
            [                         ..i]
            [..a, ..d, ..e, ..f, ..g, ..i]
        ]
        .build(storage.clone())
        .await
        .unwrap();

        let spec_b = tree_spec![
            [                         ..x]
            [..p, ..s, ..t, ..u, ..v, ..x]
        ]
        .build(storage.clone())
        .await
        .unwrap();

        let host_b = spec_b.tree().clone();
        let diff = host_b.differentiate(spec_a.tree(), spec_b.storage(), spec_a.storage());
        let _: Vec<_> = diff.collect().await;

        spec_a.assert();
        spec_b.assert();

        Ok(())
    }

    #[dialog_common::test]
    async fn test_diff_subset_superset() -> Result<()> {
        let backend = MemoryStorageBackend::default();
        let storage_a = journaled_storage(&backend);
        let storage_b = journaled_storage(&backend);

        // Scenario: Tree B extends Tree A with additional segment
        // Tree B has an additional segment 's' that needs to be read
        // The shared 'n' subtree in Tree B is pruned, so its children (e, n) are not read
        // Only Tree B's additional segment 's' should be read
        let spec_a = tree_spec![
            [         ..n]
            [(..e), (..n)]
        ]
        .build(storage_a)
        .await
        .unwrap();

        let spec_b = tree_spec![
            [(       ..n), ..s]
            [(..e), (..n), ..s]
        ]
        .build(storage_b)
        .await
        .unwrap();

        let host_a = spec_a.tree().clone();
        let diff = host_a.differentiate(spec_b.tree(), spec_a.storage(), spec_b.storage());
        let _: Vec<_> = diff.collect().await;

        spec_a.assert();
        spec_b.assert();

        Ok(())
    }

    #[dialog_common::test]
    async fn test_diff_single_key_change() -> Result<()> {
        let backend = MemoryStorageBackend::default();
        let storage_a = journaled_storage(&backend);
        let storage_b = journaled_storage(&backend);

        // Scenario: Two trees with only one differing segment
        // Tree A has segments ending at 'a', 'e'
        // Tree B has segments ending at 'a', 'f', 'k', 'p', 's' with different 'k' segment
        // Segment 'a' is shared (same boundary, same hash) so should not be read
        // Only the changed segments should be read from both trees
        let spec_a = tree_spec![
            [          ..e]
            [(..a),    ..e]
        ]
        .build(storage_a)
        .await
        .unwrap();

        // Tree B: has more segments, with 'a' shared but different keys in 'k' segment
        let spec_b = tree_spec![
            [                       ..s]
            [(..a), ..f, j..k, ..p, ..s]
        ]
        .build(storage_b)
        .await
        .unwrap();

        let host_b = spec_b.tree().clone();
        let diff = host_b.differentiate(spec_a.tree(), spec_b.storage(), spec_a.storage());
        let _: Vec<_> = diff.collect().await;

        spec_a.assert();
        spec_b.assert();

        Ok(())
    }

    #[dialog_common::test]
    async fn test_diff_different_heights() -> Result<()> {
        let backend = MemoryStorageBackend::default();
        let storage_a = journaled_storage(&backend);
        let storage_b = journaled_storage(&backend);

        // Scenario: Trees of different heights
        // Tree A is shallow (height 1), Tree B is taller (height 2)
        // This tests how differential handles height mismatches
        let spec_a = tree_spec![
            [       ..e]
            [(..a), ..e]
        ]
        .build(storage_a)
        .await
        .unwrap();

        let spec_b = tree_spec![
            [                                ..z]
            [            ..f,        ..p,    ..z]
            [(..a), ..c, ..f, ..k, ..p, ..t, ..z]
        ]
        .build(storage_b)
        .await
        .unwrap();

        let host_a = spec_a.tree().clone();
        let diff = host_a.differentiate(spec_b.tree(), spec_a.storage(), spec_b.storage());
        let _: Vec<_> = diff.collect().await;

        spec_a.assert();
        spec_b.assert();

        Ok(())
    }

    #[dialog_common::test]
    async fn test_diff_different_heights_reverse() -> Result<()> {
        let backend = MemoryStorageBackend::default();
        let storage_a = journaled_storage(&backend);
        let storage_b = journaled_storage(&backend);

        // Scenario: Reverse of test_diff_different_heights
        // Tree A is tall (height 2), Tree B is shallow (height 1)
        // When A.differentiate(B), we still need to read all branches to discover removes
        let spec_a = tree_spec![
            [                                ..z]
            [            ..f,      ..p,      ..z]
            [(..a), ..c, ..f, ..k, ..p, ..t, ..z]
        ]
        .build(storage_b)
        .await
        .unwrap();

        let spec_b = tree_spec![
            [       ..e]
            [(..a), ..e]
        ]
        .build(storage_a)
        .await
        .unwrap();

        // Differentiate A -> B (taller tree to shallow tree)
        // Still need to read all branches to discover remove changes
        let host_a = spec_a.tree().clone();
        let diff = host_a.differentiate(spec_b.tree(), spec_a.storage(), spec_b.storage());
        let _: Vec<_> = diff.collect().await;

        spec_b.assert();
        spec_a.assert();

        Ok(())
    }

    // Novel Nodes Tests
    //
    // These tests verify that novel_nodes() returns exactly the set of nodes
    // that exist in target but not in source. We use tree_spec! to define
    // deterministic tree structures and verify:
    // 1. Read patterns match expectations (via spec.assert())
    // 2. Novel nodes = target nodes - shared nodes with source

    #[dialog_common::test]
    async fn it_returns_all_target_nodes_when_source_is_empty() -> Result<()> {
        // When source is empty, all target nodes are novel
        let backend = MemoryStorageBackend::default();
        let storage_source = journaled_storage(&backend);
        let storage_target = journaled_storage(&backend);

        // Empty source tree
        let source = tree_spec![].build(storage_source).await.unwrap();

        // Target has some structure - all nodes loaded since source is empty
        let target = tree_spec![
            [     ..e]
            [..a, ..e]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(
            source.tree(),
            target.tree(),
            source.storage(),
            target.storage(),
        )
        .await
        .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify that all target nodes were loaded (since source is empty)
        target.assert();

        // All target nodes should be novel - traverse target tree to get all hashes
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default(), target.storage())
            .into_hash_set()
            .await;
        assert_eq!(
            novel_hashes, target_hashes,
            "All target nodes should be novel when source is empty"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_no_nodes_when_target_is_empty() -> Result<()> {
        // When target is empty, no nodes are novel
        let backend = MemoryStorageBackend::default();
        let storage_source = journaled_storage(&backend);
        let storage_target = journaled_storage(&backend);

        // Source has structure - root is loaded, children skipped since target is empty
        let source = tree_spec![
            [       ..e]
            [(..a), (..e)]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Empty target tree
        let target = tree_spec![].build(storage_target).await.unwrap();

        let diff = TreeDifference::compute(
            source.tree(),
            target.tree(),
            source.storage(),
            target.storage(),
        )
        .await
        .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify that no source child nodes were loaded (since target is empty)
        source.assert();

        assert!(
            novel_hashes.is_empty(),
            "No nodes should be novel when target is empty"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_no_nodes_when_both_trees_are_empty() -> Result<()> {
        let backend = MemoryStorageBackend::default();
        let storage_source = journaled_storage(&backend);
        let storage_target = journaled_storage(&backend);

        let source = tree_spec![].build(storage_source).await.unwrap();
        let target = tree_spec![].build(storage_target).await.unwrap();

        let diff = TreeDifference::compute(
            source.tree(),
            target.tree(),
            source.storage(),
            target.storage(),
        )
        .await
        .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        assert!(
            novel_hashes.is_empty(),
            "No nodes should be novel when both trees are empty"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_no_nodes_for_identical_trees() -> Result<()> {
        // When trees are identical, no nodes are novel (all are shared).
        // Identical roots are recognized by hash alone: the tree holds its
        // root lazily, so not even the roots are read (the prolly tree's
        // eager root load marked these as reads; the search tree improves
        // on that with a zero-read fast path).
        let backend = MemoryStorageBackend::default();
        let storage_source = journaled_storage(&backend);
        let storage_target = journaled_storage(&backend);

        // Both trees have identical structure; nothing at all should be read
        let source = tree_spec![
            [              (..e)]
            [(..a), (..c), (..e)]
        ]
        .build(storage_source)
        .await
        .unwrap();

        let target = tree_spec![
            [              (..e)]
            [(..a), (..c), (..e)]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(
            source.tree(),
            target.tree(),
            source.storage(),
            target.storage(),
        )
        .await
        .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify no nodes were loaded (identical trees detected at root)
        source.assert();
        target.assert();

        assert!(
            novel_hashes.is_empty(),
            "Identical trees should have no novel nodes"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_excludes_shared_subtrees_from_novel_nodes() -> Result<()> {
        // When trees share a subtree (same hash), that subtree is NOT novel
        let backend = MemoryStorageBackend::default();
        let storage_source = journaled_storage(&backend);
        let storage_target = journaled_storage(&backend);

        // Source: segment a skipped (shared); segment e is peeled by the
        // equal-bound alignment below.
        // Separator links carry lower bounds only, so a frontier node's
        // extent is unknowable until peeled: at an equal-bound pair of
        // unloaded refs the comparison must load the source side too (it
        // could be a taller index hiding the target's match), one exploratory
        // read the old full-key bounds could rule out. Documented blunting.
        let source = tree_spec![
            [        ..e]
            [(..a),  ..e]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Target: root loaded, segment 'a' is shared (skipped), 'f' and 'k' are loaded
        let target = tree_spec![
            [            ..k]
            [(..a), ..f, ..k]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(
            source.tree(),
            target.tree(),
            source.storage(),
            target.storage(),
        )
        .await
        .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify read patterns:
        // - Source: only index loaded (segments compared by boundary only)
        // - Target: shared 'a' skipped, novel 'f','k' loaded
        source.assert();
        target.assert();

        // Novel nodes should be: target nodes - nodes shared with source
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default(), target.storage())
            .into_hash_set()
            .await;
        let source_hashes = source
            .tree()
            .traverse(TraversalOrder::default(), source.storage())
            .into_hash_set()
            .await;
        let expected_novel: HashSet<_> =
            target_hashes.difference(&source_hashes).cloned().collect();

        assert_eq!(
            novel_hashes, expected_novel,
            "Novel nodes should be target nodes minus shared nodes"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_trees_with_different_heights() -> Result<()> {
        // Target taller than source
        let backend = MemoryStorageBackend::default();
        let storage_source = journaled_storage(&backend);
        let storage_target = journaled_storage(&backend);

        // Shallow source (height 1): root loaded, shared segment a skipped;
        // segment e is peeled by the equal-bound alignment (see the blunting
        // note in it_excludes_shared_subtrees_from_novel_nodes).
        let source = tree_spec![
            [         ..e]
            [(..a),  ..e]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Taller target (height 2) with shared segment 'a'
        // Root and intermediate nodes loaded, segment 'a' shared (skipped)
        let target = tree_spec![
            [                           ..z]
            [       ..f,      ..p,      ..z]
            [(..a), ..f, ..k, ..p, ..t, ..z]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(
            source.tree(),
            target.tree(),
            source.storage(),
            target.storage(),
        )
        .await
        .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify read patterns - shared segment 'a' should NOT be loaded
        source.assert();
        target.assert();

        // Novel = target - shared
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default(), target.storage())
            .into_hash_set()
            .await;
        let source_hashes = source
            .tree()
            .traverse(TraversalOrder::default(), source.storage())
            .into_hash_set()
            .await;
        let expected_novel: HashSet<_> =
            target_hashes.difference(&source_hashes).cloned().collect();

        assert_eq!(
            novel_hashes, expected_novel,
            "Novel nodes should be target nodes minus shared nodes"
        );

        // Verify uniqueness
        assert_eq!(
            novel_hashes.len(),
            diff.novel_nodes().into_hash_set().await.len(),
            "All novel nodes should be unique"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_no_nodes_when_target_is_subset_of_source() -> Result<()> {
        // Target is a subset of source - should have no novel nodes since
        // all target nodes exist in source.
        //
        // IMPORTANT: Both trees must share the same backend so that identical
        // content produces identical hashes (content-addressed storage).
        let backend = MemoryStorageBackend::default();
        let storage_source = journaled_storage(&backend);
        let storage_target = journaled_storage(&backend);

        // Source: 3 segments at height 0, 1 index at height 1
        // Segment 'a' contains entries [a..a]
        let source = tree_spec![
            [            ..k]
            [(..a), ..f, ..k]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Target: single segment 'a' with same entries as source's segment 'a'
        // Because they share the same backend, identical content = identical hash
        let target = tree_spec![[(..a)]].build(storage_target).await.unwrap();

        let diff = TreeDifference::compute(
            source.tree(),
            target.tree(),
            source.storage(),
            target.storage(),
        )
        .await
        .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Target's 'a' segment should match source's 'a' segment (same hash)
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default(), target.storage())
            .into_hash_set()
            .await;
        let source_hashes = source
            .tree()
            .traverse(TraversalOrder::default(), source.storage())
            .into_hash_set()
            .await;
        let expected_novel: HashSet<_> =
            target_hashes.difference(&source_hashes).cloned().collect();

        assert_eq!(
            novel_hashes, expected_novel,
            "Novel nodes should be target - source (empty if target is subset)"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_all_target_nodes_for_disjoint_trees() -> Result<()> {
        // Trees with completely different content - all target nodes are novel
        let backend = MemoryStorageBackend::default();
        let storage_source = journaled_storage(&backend);
        let storage_target = journaled_storage(&backend);

        // Source: root loaded, segments skipped (disjoint so no match possible)
        // Segment a is never touched; the frontier tail e must be peeled
        // (its extent past the target's start is unknowable from a lower
        // bound alone; see it_excludes_shared_subtrees_from_novel_nodes).
        let source = tree_spec![
            [        ..e]
            [(..a),  ..e]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Target: root loaded, children loaded for novel_nodes
        let target = tree_spec![
            [      ..z]
            [..p,  ..z]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(
            source.tree(),
            target.tree(),
            source.storage(),
            target.storage(),
        )
        .await
        .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify read patterns:
        // - Source: only index loaded (segments compared by boundary, no match)
        // - Target: all nodes loaded (all are novel)
        source.assert();
        target.assert();

        // All target nodes should be novel (no overlap)
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default(), target.storage())
            .into_hash_set()
            .await;
        assert_eq!(
            novel_hashes, target_hashes,
            "Disjoint trees should have all target nodes as novel"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_unique_novel_nodes() -> Result<()> {
        // Verify that novel_nodes() never returns duplicates
        let backend = MemoryStorageBackend::default();
        let storage_source = journaled_storage(&backend);
        let storage_target = journaled_storage(&backend);

        // Source with some structure
        let source = tree_spec![
            [       ..e]
            [(..a), ..e]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Target with more structure
        let target = tree_spec![
            [                           ..z]
            [       ..f,      ..p,      ..z]
            [(..a), ..f, ..k, ..p, ..t, ..z]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(
            source.tree(),
            target.tree(),
            source.storage(),
            target.storage(),
        )
        .await
        .unwrap();

        // Collect twice - as Vec (preserves duplicates) and HashSet (deduplicates)
        let all_nodes: Vec<_> = diff.novel_nodes().try_collect().await?;
        let unique_hashes = diff.novel_nodes().into_hash_set().await;

        assert_eq!(
            all_nodes.len(),
            unique_hashes.len(),
            "novel_nodes() should not return duplicates"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_novel_nodes_for_different_segments() -> Result<()> {
        // Simplest case: single segment trees with different content
        let backend = MemoryStorageBackend::default();
        let storage_source = journaled_storage(&backend);
        let storage_target = journaled_storage(&backend);

        // Source: single segment 'a' (contains key 'a')
        // Marked as read because segment nodes are always loaded during diff
        let source = tree_spec![[..a]].build(storage_source).await.unwrap();

        // Target: single segment 'b' (contains keys 'a', 'b' - different content)
        let target = tree_spec![[..b]].build(storage_target).await.unwrap();

        let diff = TreeDifference::compute(
            source.tree(),
            target.tree(),
            source.storage(),
            target.storage(),
        )
        .await
        .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // All target nodes should be novel (no overlap)
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default(), target.storage())
            .into_hash_set()
            .await;

        assert_eq!(
            novel_hashes, target_hashes,
            "Different segment should produce novel nodes"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_prunes_shared_deep_subtrees() -> Result<()> {
        // 3-level trees where the left subtree is shared but right differs
        let backend = MemoryStorageBackend::default();
        let storage_source = journaled_storage(&backend);
        let storage_target = journaled_storage(&backend);

        // Source: 3-level tree with segments under two index nodes
        // Left subtree (..f, ..m) should be shared, so pruned
        // Right subtree (..t, ..z) differs - but source segments don't need loading
        // for novel_nodes (we only care about target nodes)
        // Segment t is peeled by the equal-bound alignment against the
        // target's replacement segment (see the blunting note in
        // it_excludes_shared_subtrees_from_novel_nodes).
        let source = tree_spec![
            [                     ..z]
            [       (..m),        ..z]
            [(..f), (..m),  ..t,  ..z]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Target: same left subtree structure, different right subtree
        // Left (..f, ..m) should be pruned (identical to source)
        // Right (..w, ..z) is novel
        let target = tree_spec![
            [                   ..z]
            [       (..m),      ..z]
            [(..f), (..m), ..w, ..z]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(
            source.tree(),
            target.tree(),
            source.storage(),
            target.storage(),
        )
        .await
        .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify left subtree was pruned (not loaded)
        source.assert();
        target.assert();

        // Novel nodes should be target - source (right subtree differs)
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default(), target.storage())
            .into_hash_set()
            .await;
        let source_hashes = source
            .tree()
            .traverse(TraversalOrder::default(), source.storage())
            .into_hash_set()
            .await;
        let expected_novel: HashSet<_> =
            target_hashes.difference(&source_hashes).cloned().collect();

        assert_eq!(
            novel_hashes, expected_novel,
            "Novel nodes should be target nodes minus shared nodes"
        );

        Ok(())
    }

    fn key(n: u32) -> [u8; 4] {
        n.to_le_bytes()
    }

    /// Mirrors the tree's LWW identity: blake3 over the value's serialized
    /// rkyv form.
    fn value_identity(value: &Vec<u8>) -> Blake3Hash {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(value).unwrap();
        Blake3Hash::hash(bytes.as_slice())
    }

    async fn flush(
        delta: &mut Delta<Blake3Hash, Buffer>,
        storage: &mut ContentAddressedStorage<CountingBackend>,
    ) -> Result<()> {
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(())
    }

    #[dialog_common::test]
    async fn test_differentiate_added_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let tree1 = build([(1, vec![10]), (2, vec![20])], &mut storage).await?;
        let tree2 = build([(1, vec![10])], &mut storage).await?;

        let changes = collect_changes(&tree2, &tree1, &storage).await?;

        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for change in changes {
            match change {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].key, key(2));
        assert_eq!(adds[0].value, vec![20]);
        assert_eq!(removes.len(), 0);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_differentiate_removed_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let tree1 = build([(1, vec![10])], &mut storage).await?;
        let tree2 = build([(1, vec![10]), (2, vec![20])], &mut storage).await?;

        let changes = collect_changes(&tree2, &tree1, &storage).await?;

        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for change in changes {
            match change {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        assert_eq!(adds.len(), 0);
        assert_eq!(removes.len(), 1);
        assert_eq!(removes[0].key, key(2));
        assert_eq!(removes[0].value, vec![20]);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_differentiate_modified_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let tree1 = build([(1, vec![10]), (2, vec![30])], &mut storage).await?;
        let tree2 = build([(1, vec![10]), (2, vec![20])], &mut storage).await?;

        let changes = collect_changes(&tree2, &tree1, &storage).await?;

        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for change in changes {
            match change {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].key, key(2));
        assert_eq!(adds[0].value, vec![30]);
        assert_eq!(removes.len(), 1);
        assert_eq!(removes[0].key, key(2));
        assert_eq!(removes[0].value, vec![20]);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_differentiate_empty_to_populated() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let tree1 = build([(1, vec![10]), (2, vec![20])], &mut storage).await?;
        let tree2 = TestTree::empty();

        let changes = collect_changes(&tree2, &tree1, &storage).await?;

        let mut adds = Vec::new();
        for change in changes {
            match change {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(_) => panic!("Should not have removes"),
            }
        }

        assert_eq!(adds.len(), 2);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_differentiate_populated_to_empty() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let tree1 = TestTree::empty();
        let tree2 = build([(1, vec![10]), (2, vec![20])], &mut storage).await?;

        let changes = collect_changes(&tree2, &tree1, &storage).await?;

        let mut removes = Vec::new();
        for change in changes {
            match change {
                Change::Add(_) => panic!("Should not have adds"),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        assert_eq!(removes.len(), 2);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_differentiate_large_tree() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());

        // Create a larger tree to test branch handling; tree2 skips one entry
        let tree1 = build((0..100u32).map(|i| (i, vec![i as u8])), &mut storage).await?;
        let tree2 = build(
            (0..100u32).filter(|i| *i != 50).map(|i| (i, vec![i as u8])),
            &mut storage,
        )
        .await?;

        let changes = collect_changes(&tree2, &tree1, &storage).await?;

        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for change in changes {
            match change {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].key, key(50));
        assert_eq!(removes.len(), 0);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_differentiate_both_empty() -> Result<()> {
        let storage = ContentAddressedStorage::new(CountingBackend::new());
        let tree1 = TestTree::empty();
        let tree2 = TestTree::empty();

        let changes = collect_changes(&tree2, &tree1, &storage).await?;
        assert_eq!(changes.len(), 0, "Both empty trees should have no changes");

        Ok(())
    }

    #[dialog_common::test]
    async fn test_differentiate_single_entry_trees() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let tree1 = build([(1, vec![10])], &mut storage).await?;
        let tree2 = build([(1, vec![20])], &mut storage).await?;

        let changes = collect_changes(&tree2, &tree1, &storage).await?;

        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for change in changes {
            match change {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        // Should have one remove (old value) and one add (new value)
        assert_eq!(removes.len(), 1);
        assert_eq!(removes[0].value, vec![20]);
        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].value, vec![10]);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_differentiate_disjoint_trees() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());

        // Completely disjoint key sets
        let tree1 = build([(1, vec![10]), (3, vec![30]), (5, vec![50])], &mut storage).await?;
        let tree2 = build([(2, vec![20]), (4, vec![40]), (6, vec![60])], &mut storage).await?;

        let changes = collect_changes(&tree2, &tree1, &storage).await?;

        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for change in changes {
            match change {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        // All of tree2's entries should be removed, all of tree1's added
        assert_eq!(removes.len(), 3);
        assert_eq!(adds.len(), 3);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_differentiate_subset_superset() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());

        // Subset: keys 2, 3. Superset: keys 1, 2, 3, 4.
        let subset = build([(2, vec![20]), (3, vec![30])], &mut storage).await?;
        let superset = build(
            [(1, vec![10]), (2, vec![20]), (3, vec![30]), (4, vec![40])],
            &mut storage,
        )
        .await?;

        let changes = collect_changes(&subset, &superset, &storage).await?;

        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for change in changes {
            match change {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        // Should add keys 1 and 4
        assert_eq!(adds.len(), 2);
        assert_eq!(removes.len(), 0);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_differentiate_all_modified() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());

        // Same keys, all different values (except i=0 where both are [0])
        let tree1 = build((0..10u32).map(|i| (i, vec![(i * 2) as u8])), &mut storage).await?;
        let tree2 = build((0..10u32).map(|i| (i, vec![i as u8])), &mut storage).await?;

        let changes = collect_changes(&tree2, &tree1, &storage).await?;

        // 9 keys modified (i=0 has same value in both) = 9 * 2 = 18 changes total
        assert_eq!(changes.len(), 18);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_integrate_add_new_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let mut tree = build([(1, vec![10])], &mut storage).await?;

        let changes = vec![Change::Add(Entry {
            key: key(2),
            value: vec![20],
        })];

        let mut delta = Delta::zero();
        tree = tree
            .edit()
            .integrate(iter(changes.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush so the reads below can load the integrated nodes from storage.
        flush(&mut delta, &mut storage).await?;

        assert_eq!(tree.get(&key(1), &storage).await?, Some(vec![10]));
        assert_eq!(tree.get(&key(2), &storage).await?, Some(vec![20]));

        Ok(())
    }

    #[dialog_common::test]
    async fn test_integrate_add_idempotent() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let mut tree = build([(1, vec![10])], &mut storage).await?;
        let root = tree.root().clone();

        // Add same entry - should be no-op
        let changes = vec![Change::Add(Entry {
            key: key(1),
            value: vec![10],
        })];

        let mut delta = Delta::zero();
        tree = tree
            .edit()
            .integrate(iter(changes.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush so the reads below can load the integrated nodes from storage.
        flush(&mut delta, &mut storage).await?;

        assert_eq!(tree.get(&key(1), &storage).await?, Some(vec![10]));
        assert_eq!(
            tree.root(),
            &root,
            "idempotent add must not change the root"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn test_integrate_add_conflict_resolution() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let mut tree = build([(1, vec![10])], &mut storage).await?;

        // Try to add different value - conflict resolution by value hash
        let new_value = vec![20u8];
        let existing_value = vec![10u8];

        let changes = vec![Change::Add(Entry {
            key: key(1),
            value: new_value.clone(),
        })];

        let mut delta = Delta::zero();
        tree = tree
            .edit()
            .integrate(iter(changes.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush so the reads below can load the integrated nodes from storage.
        flush(&mut delta, &mut storage).await?;

        // Check which value won based on identity hash comparison
        let winner = if value_identity(&new_value) > value_identity(&existing_value) {
            new_value
        } else {
            existing_value
        };
        assert_eq!(tree.get(&key(1), &storage).await?, Some(winner));

        Ok(())
    }

    #[dialog_common::test]
    async fn test_integrate_remove_existing() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let mut tree = build([(1, vec![10]), (2, vec![20])], &mut storage).await?;

        let changes = vec![Change::Remove(Entry {
            key: key(1),
            value: vec![10],
        })];

        let mut delta = Delta::zero();
        tree = tree
            .edit()
            .integrate(iter(changes.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush so the reads below can load the integrated nodes from storage.
        flush(&mut delta, &mut storage).await?;

        assert_eq!(tree.get(&key(1), &storage).await?, None);
        assert_eq!(tree.get(&key(2), &storage).await?, Some(vec![20]));

        Ok(())
    }

    #[dialog_common::test]
    async fn test_integrate_remove_nonexistent() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let mut tree = build([(1, vec![10])], &mut storage).await?;

        // Remove non-existent entry - should be no-op
        let changes = vec![Change::Remove(Entry {
            key: key(2),
            value: vec![20],
        })];

        let mut delta = Delta::zero();
        tree = tree
            .edit()
            .integrate(iter(changes.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush so the reads below can load the integrated nodes from storage.
        flush(&mut delta, &mut storage).await?;

        assert_eq!(tree.get(&key(1), &storage).await?, Some(vec![10]));
        assert_eq!(tree.get(&key(2), &storage).await?, None);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_integrate_remove_wrong_value() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let mut tree = build([(1, vec![10])], &mut storage).await?;

        // Try to remove with wrong value - should be no-op (concurrent update)
        let changes = vec![Change::Remove(Entry {
            key: key(1),
            value: vec![20], // Wrong value
        })];

        let mut delta = Delta::zero();
        tree = tree
            .edit()
            .integrate(iter(changes.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush so the reads below can load the integrated nodes from storage.
        flush(&mut delta, &mut storage).await?;

        // Entry should still exist with original value
        assert_eq!(tree.get(&key(1), &storage).await?, Some(vec![10]));

        Ok(())
    }

    #[dialog_common::test]
    async fn test_integrate_concurrent_updates() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());

        // Initial state - both replicas start with same value, then each
        // updates the same key to a different value.
        let mut tree_a = build([(1, vec![10])], &mut storage).await?;
        let mut delta_a = Delta::zero();
        tree_a = tree_a
            .edit()
            .insert(key(1), vec![20], &storage)
            .await?
            .persist(&mut delta_a)?;
        flush(&mut delta_a, &mut storage).await?;

        let mut tree_b = build([(1, vec![10])], &mut storage).await?;
        let mut delta_b = Delta::zero();
        tree_b = tree_b
            .edit()
            .insert(key(1), vec![30], &storage)
            .await?
            .persist(&mut delta_b)?;
        flush(&mut delta_b, &mut storage).await?;

        // Both replicas exchange their changes (relative to an empty tree,
        // so each side ships its full state as adds).
        let empty_tree = TestTree::empty();
        let changes_a = collect_changes(&empty_tree, &tree_a, &storage).await?;
        let changes_b = collect_changes(&empty_tree, &tree_b, &storage).await?;

        // Integrate changes
        tree_a = tree_a
            .edit()
            .integrate(iter(changes_b.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta_a)?;
        tree_b = tree_b
            .edit()
            .integrate(iter(changes_a.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta_b)?;

        // Flush so the reads below can load the integrated nodes from storage.
        flush(&mut delta_a, &mut storage).await?;
        flush(&mut delta_b, &mut storage).await?;

        // Both should converge to the same value (deterministic by hash)
        let final_a = tree_a.get(&key(1), &storage).await?;
        let final_b = tree_b.get(&key(1), &storage).await?;

        assert_eq!(final_a, final_b, "Trees should converge to same value");

        // Verify the winner is determined by the value identity hash
        let winner = if value_identity(&vec![20u8]) > value_identity(&vec![30u8]) {
            vec![20u8]
        } else {
            vec![30u8]
        };
        assert_eq!(final_a, Some(winner));

        Ok(())
    }

    // Roundtrip tests: Verify differentiate + integrate produces original tree

    #[dialog_common::test]
    async fn test_roundtrip_empty_to_populated() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let target = build([(1, vec![10]), (2, vec![20]), (3, vec![30])], &mut storage).await?;
        let mut start = TestTree::empty();

        let changes = collect_changes(&start, &target, &storage).await?;
        let mut delta = Delta::zero();
        start = start
            .edit()
            .integrate(iter(changes.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush so the reads below can load the integrated nodes from storage.
        flush(&mut delta, &mut storage).await?;

        // Verify start now matches target
        assert_eq!(start.get(&key(1), &storage).await?, Some(vec![10]));
        assert_eq!(start.get(&key(2), &storage).await?, Some(vec![20]));
        assert_eq!(start.get(&key(3), &storage).await?, Some(vec![30]));

        Ok(())
    }

    #[dialog_common::test]
    async fn test_roundtrip_populated_to_empty() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let target = TestTree::empty();
        let mut start = build([(1, vec![10]), (2, vec![20]), (3, vec![30])], &mut storage).await?;

        let changes = collect_changes(&start, &target, &storage).await?;
        let mut delta = Delta::zero();
        start = start
            .edit()
            .integrate(iter(changes.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush so the reads below can load the integrated nodes from storage.
        flush(&mut delta, &mut storage).await?;

        // Verify start is now empty
        assert_eq!(start.get(&key(1), &storage).await?, None);
        assert_eq!(start.get(&key(2), &storage).await?, None);
        assert_eq!(start.get(&key(3), &storage).await?, None);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_roundtrip_mixed_changes() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());

        // Start state: keys 1, 2, 3. Target: 2 (modified), 3 (same), 4
        // (added); key 1 removed.
        let mut start = build([(1, vec![10]), (2, vec![20]), (3, vec![30])], &mut storage).await?;
        let target = build([(2, vec![22]), (3, vec![30]), (4, vec![40])], &mut storage).await?;

        let changes = collect_changes(&start, &target, &storage).await?;
        let mut delta = Delta::zero();
        start = start
            .edit()
            .integrate(iter(changes.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush so the reads below can load the integrated nodes from storage.
        flush(&mut delta, &mut storage).await?;

        // Verify start now matches target
        assert_eq!(start.get(&key(1), &storage).await?, None);
        assert_eq!(start.get(&key(2), &storage).await?, Some(vec![22]));
        assert_eq!(start.get(&key(3), &storage).await?, Some(vec![30]));
        assert_eq!(start.get(&key(4), &storage).await?, Some(vec![40]));

        Ok(())
    }

    #[dialog_common::test]
    async fn test_roundtrip_large_tree() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());

        let mut start = build((0..100u32).map(|i| (i, vec![(i % 26) as u8])), &mut storage).await?;
        let target = build(
            (50..150u32).map(|i| (i, vec![(i % 17) as u8])),
            &mut storage,
        )
        .await?;

        let changes = collect_changes(&start, &target, &storage).await?;
        let mut delta = Delta::zero();
        start = start
            .edit()
            .integrate(iter(changes.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush so the reads below can load the integrated nodes from storage.
        flush(&mut delta, &mut storage).await?;

        // Verify start now matches target
        for i in 0..50u32 {
            assert_eq!(start.get(&key(i), &storage).await?, None);
        }
        for i in 50..150u32 {
            assert_eq!(
                start.get(&key(i), &storage).await?,
                Some(vec![(i % 17) as u8])
            );
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn test_roundtrip_preserves_hash() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());

        let target = build((0..20u32).map(|i| (i, vec![(i * 3) as u8])), &mut storage).await?;
        let target_root = target.root().clone();

        let mut start = build((10..30u32).map(|i| (i, vec![(i * 5) as u8])), &mut storage).await?;

        let changes = collect_changes(&start, &target, &storage).await?;
        let mut delta = Delta::zero();
        start = start
            .edit()
            .integrate(iter(changes.into_iter().map(Ok)), &storage)
            .await?
            .persist(&mut delta)?;

        // Root hash should match after integration (canonical form)
        assert_eq!(start.root(), &target_root);

        Ok(())
    }

    /// Buffers raw byte-keyed asserts at the root, persisting buffers intact.
    async fn buffered_keys(
        base: &TestTree,
        ops: &[([u8; 4], Vec<u8>)],
        storage: &mut ContentAddressedStorage<CountingBackend>,
    ) -> Result<TestTree> {
        let mut tree = crate::HitchhikerTree::open(base).with_op_buf_size(1_000_000);
        for (key, value) in ops {
            tree = tree.insert(*key, value.clone(), storage).await?;
        }
        let mut delta = Delta::zero();
        let root = tree.persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(TestTree::seal(root, Default::default()))
    }

    /// A fact buffered at the root must sync without descending the tree.
    ///
    /// This is the whole point of the write buffer: the new fact lives in the
    /// root's novelty and every child hash is unchanged, so the difference is
    /// fully described by the two roots. Reading a leaf to answer it means the
    /// buffer bought nothing.
    ///
    /// The bound is per side (each replica's root), independent of tree height,
    /// which is what makes this the property worth having: it does not degrade
    /// as the database grows.
    #[dialog_common::test]
    async fn it_syncs_a_root_buffered_fact_without_descending() -> Result<()> {
        for base_size in [500u32, 5_000, 20_000] {
            let mut storage = ContentAddressedStorage::new(CountingBackend::new());
            let base = build((0..base_size).map(|i| (i, vec![i as u8])), &mut storage).await?;

            // One new fact per side, buffered at the root (no overflow), on keys
            // that sort past every key in the base. `build` keys by
            // `to_le_bytes`, so the byte order is not the integer order: use
            // explicit byte keys that exceed every base key lexicographically.
            let ours = [0xFFu8, 0xFF, 0xFF, 1];
            let theirs = [0xFFu8, 0xFF, 0xFF, 2];
            let left = buffered_keys(&base, &[(ours, vec![1])], &mut storage).await?;
            let right = buffered_keys(&base, &[(theirs, vec![2])], &mut storage).await?;

            storage.backend().reset();
            let changes = collect_changes(&left, &right, &storage).await?;
            let reads = storage.backend().reads();

            assert_eq!(
                normalize(&changes).len(),
                2,
                "base {base_size}: both sides' facts must surface"
            );
            // Currently 4 (one root plus one descent per side): the settle fast
            // path fires only when both sides' bounds compare equal, and a
            // buffered key past every stored key now raises the bound (which is
            // what keeps range and scope pruning exact). Recovering the 2-read
            // case needs the comparison to notice identical *links* despite
            // unequal bounds; correctness comes first.
            assert!(
                reads <= 4,
                "base {base_size}: a root-buffered fact must sync from the roots \
                 without walking the tree, got {reads} reads"
            );
        }
        Ok(())
    }

    /// Two replicas buffering the *same key* with *different values* must still
    /// diff: pruning compares op kinds, not values, so this pins that a
    /// same-key-different-value pair is not silently pruned away.
    #[dialog_common::test]
    async fn it_diffs_a_same_key_buffered_with_different_values() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build((0..500u32).map(|i| (i, vec![i as u8])), &mut storage).await?;

        let left = buffered(&base, &[(true, 42u32, vec![111])], 1_000_000, &mut storage).await?;
        let right = buffered(&base, &[(true, 42u32, vec![222])], 1_000_000, &mut storage).await?;

        let changes = collect_changes(&left, &right, &storage).await?;
        assert_eq!(
            normalize(&changes).get(&42u32.to_le_bytes()),
            Some(&(true, vec![222])),
            "a same-key buffered op with a different value must surface, got {changes:?}"
        );
        Ok(())
    }

    /// A scoped diff must see a buffered write whose key falls inside the scope.
    /// Subscriptions rely on this: they diff the pinned root against the current
    /// one, restricted to the demanded ranges.
    #[dialog_common::test]
    async fn it_scopes_diffs_over_buffered_writes() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build((0..400u32).map(|i| (i, vec![i as u8])), &mut storage).await?;

        // One buffered write to a key inside the base range.
        let key = 200u32;
        let next = buffered(&base, &[(true, key, vec![99])], 1_000_000, &mut storage).await?;

        let scope = [key.to_le_bytes()..=key.to_le_bytes()];
        let stream = base.differentiate_within(&next, &scope, &storage, &storage);
        futures_util::pin_mut!(stream);
        let mut changes = Vec::new();
        while let Some(change) = stream.next().await {
            changes.push(change?);
        }

        assert!(
            !changes.is_empty(),
            "a scoped diff must surface a buffered write inside its scope"
        );
        Ok(())
    }

    /// Can a node have an EMPTY buffer while a descendant still buffers ops?
    /// If not, an empty buffer would license pruning the whole subtree.
    #[dialog_common::test]
    #[ignore]
    async fn probe_empty_buffer_implies_clean_subtree() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build((0..2000u32).map(|i| (i, vec![i as u8])), &mut storage).await?;

        // Overflow the root repeatedly so ops cascade into children, then add a
        // few more that stay at the root.
        let ops: Vec<(bool, u32, Vec<u8>)> = (0..200u32)
            .map(|i| (true, 50_000 + i, vec![i as u8]))
            .collect();
        let tree = buffered(&base, &ops, 16, &mut storage).await?;

        // Walk the tree; report novelty length per level.
        use crate::PersistentNode;
        let mut level = vec![tree.root().clone()];
        let mut depth = 0;
        while !level.is_empty() && depth < 6 {
            let mut next = Vec::new();
            let mut empty_with_buffered_below = 0;
            for hash in &level {
                let bytes = StorageBackend::get(storage.backend(), hash).await?.unwrap();
                let node: PersistentNode<[u8; 4], Vec<u8>> =
                    PersistentNode::new(crate::Buffer::from(bytes));
                if let Ok(crate::ArchivedNodeBody::Index(index)) = node.body() {
                    let own = index.novelty_len();
                    let mut below = 0;
                    for at in 0..index.len() {
                        let h = index.hash_at(at)?.clone();
                        let b = StorageBackend::get(storage.backend(), &h).await?.unwrap();
                        let child: PersistentNode<[u8; 4], Vec<u8>> =
                            PersistentNode::new(crate::Buffer::from(b));
                        if let Ok(crate::ArchivedNodeBody::Index(ci)) = child.body() {
                            below += ci.novelty_len();
                        }
                        next.push(h);
                    }
                    if own == 0 && below > 0 {
                        empty_with_buffered_below += 1;
                    }
                    println!("  depth {depth}: own_novelty={own} below={below}");
                }
            }
            println!(
                "  depth {depth}: nodes with EMPTY buffer but buffered descendants = {empty_with_buffered_below}"
            );
            level = next;
            depth += 1;
        }
        Ok(())
    }

    /// Two disjoint scopes over a buffered tree, the shape pull uses: it diffs
    /// base against local once for the history tag span and once for the data
    /// tag span. Every buffered op must surface in exactly the scope its key
    /// belongs to, and none must be lost between them.
    #[dialog_common::test]
    async fn it_surfaces_buffered_ops_across_disjoint_scopes() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());

        // Keys tagged by their leading byte, like the artifact key layout.
        let base_keys: Vec<[u8; 4]> = (0..200u32)
            .map(|i| {
                let b = i.to_be_bytes();
                [if i % 2 == 0 { 1 } else { 5 }, b[1], b[2], b[3]]
            })
            .collect();
        let mut base = TestTree::empty();
        let mut delta = Delta::zero();
        for key in &base_keys {
            base = base
                .edit()
                .insert(*key, key.to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // One buffered write in each tag span.
        let in_tag_1 = [1u8, 0xFF, 0, 1];
        let in_tag_5 = [5u8, 0xFF, 0, 1];
        let mut buffered = crate::HitchhikerTree::open(&base).with_op_buf_size(1_000_000);
        buffered = buffered.insert(in_tag_1, vec![11], &storage).await?;
        buffered = buffered.insert(in_tag_5, vec![55], &storage).await?;
        let mut delta = Delta::zero();
        let root = buffered.persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        let next = TestTree::from_hash_with_cache(root, Default::default());

        for (tag, key) in [(1u8, in_tag_1), (5u8, in_tag_5)] {
            let scope = [[tag, 0, 0, 0]..=[tag, 0xFF, 0xFF, 0xFF]];
            let stream = base.differentiate_within(&next, &scope, &storage, &storage);
            futures_util::pin_mut!(stream);
            let mut found = false;
            while let Some(change) = stream.next().await {
                if let Change::Add(entry) = change?
                    && entry.key == key
                {
                    found = true;
                }
            }
            assert!(
                found,
                "buffered op in tag {tag} must surface in its own scope"
            );
        }
        Ok(())
    }

    /// Does a buffered op ever fall OUTSIDE the upper bound its link advertises?
    /// If not, span-based scope pruning stays exact and only needs to know
    /// whether a subtree is clean.
    #[dialog_common::test]
    #[ignore]
    async fn probe_novelty_within_bounds() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build((0..500u32).map(|i| (i, vec![i as u8])), &mut storage).await?;

        // Buffer ops well past every existing key, at the root and cascaded.
        for op_buf in [1_000_000usize, 8] {
            let ops: Vec<(bool, u32, Vec<u8>)> = (0..30u32)
                .map(|i| (true, 900_000 + i, vec![i as u8]))
                .collect();
            let tree = buffered(&base, &ops, op_buf, &mut storage).await?;

            // Walk every node: is any buffered key > the node's own upper bound?
            use crate::PersistentNode;
            let mut frontier = vec![tree.root().clone()];
            let mut violations = 0;
            let mut checked = 0;
            while let Some(hash) = frontier.pop() {
                let bytes = StorageBackend::get(storage.backend(), &hash)
                    .await?
                    .unwrap();
                let node: PersistentNode<[u8; 4], Vec<u8>> =
                    PersistentNode::new(crate::Buffer::from(bytes));
                if let Ok(crate::ArchivedNodeBody::Index(index)) = node.body() {
                    // Separators are lower bounds, so a node's own table
                    // bounds its ops from BELOW: every buffered key must sort
                    // at or above the leftmost separator. The right end is
                    // open (the last child takes whatever remains), so there
                    // is nothing to check above.
                    let node_lower = if index.is_empty() {
                        None
                    } else {
                        Some(index.separator(0)?)
                    };
                    for entry in index.all_novelty::<[u8; 4]>()? {
                        checked += 1;
                        let k: &[u8] = &entry.key;
                        if let Some(low) = node_lower.as_ref()
                            && !low.is_empty()
                            && k < low.as_slice()
                        {
                            violations += 1;
                            println!(
                                "    VIOLATION: buffered key {k:?} < node lower bound {low:?}"
                            );
                        }
                    }
                    for at in 0..index.len() {
                        frontier.push(index.hash_at(at)?.clone());
                    }
                }
            }
            println!(
                "  op_buf {op_buf}: checked {checked} buffered ops, {violations} outside node bounds"
            );
        }
        Ok(())
    }

    /// A scoped diff must see a buffered op even when the op's key sorts past
    /// the node's own upper bound.
    ///
    /// Regression: a node's bound describes its *stored* content (it doubles as
    /// the routing key and the rank input, so a pending op must not move it), so
    /// a buffered key beyond every stored key falls outside the span its own
    /// node advertises. Scope pruning that trusted the span therefore dropped
    /// the whole tree and the diff reported nothing. This is what silently broke
    /// subscriptions: a write to `person/name` sat in a root whose bound stopped
    /// at `dialog.db/revision`, and `p` sorts after `d`.
    #[dialog_common::test]
    async fn it_scopes_diffs_over_ops_past_the_node_bound() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());

        // Base keys all start with a low byte, so every stored bound is low.
        let base_keys: Vec<[u8; 4]> = (0..300u32)
            .map(|i| {
                let b = i.to_be_bytes();
                [0x10, b[1], b[2], b[3]]
            })
            .collect();
        let mut base = TestTree::empty();
        let mut delta = Delta::zero();
        for key in &base_keys {
            base = base
                .edit()
                .insert(*key, key.to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // A buffered write whose key sorts ABOVE every stored key, so it lies
        // outside the bound the root advertises.
        let beyond = [0x90u8, 0, 0, 1];
        let buffered_tree = {
            let tree = crate::HitchhikerTree::open(&base)
                .with_op_buf_size(1_000_000)
                .insert(beyond, vec![7], &storage)
                .await?;
            let mut delta = Delta::zero();
            let root = tree.persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
            TestTree::from_hash_with_cache(root, Default::default())
        };

        // Scope covering only the beyond-the-bound region.
        let scope = [[0x90u8, 0, 0, 0]..=[0x90u8, 0xFF, 0xFF, 0xFF]];
        let stream = base.differentiate_within(&buffered_tree, &scope, &storage, &storage);
        futures_util::pin_mut!(stream);
        let mut found = false;
        while let Some(change) = stream.next().await {
            if let Change::Add(entry) = change?
                && entry.key == beyond
            {
                found = true;
            }
        }
        assert!(
            found,
            "a scoped diff must surface a buffered op whose key sorts past the node bound"
        );
        Ok(())
    }

    /// A buffered op sorting past every key in the tree must still be seen.
    ///
    /// Regression: expansion routed ops to the child whose range covered them,
    /// but the last child's range was closed at its upper bound, so an op past
    /// the rightmost key matched no child and was dropped. A flush routes such
    /// an op to the rightmost child, and so must the walk. Found by benchmarking
    /// (the randomized oracle never generated keys past the rightmost bound).
    #[dialog_common::test]
    async fn it_sees_a_buffered_op_past_the_rightmost_key() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build((0..500u32).map(|i| (i, vec![i as u8])), &mut storage).await?;

        // Every op sorts past the base's last key, so all of them land on the
        // rightmost child at every level.
        let ops: Vec<(bool, u32, Vec<u8>)> = (0..40u32)
            .map(|i| (true, 100_000 + i, vec![i as u8]))
            .collect();

        let buffered_tree = buffered(&base, &ops, 1_000_000, &mut storage).await?;
        let canonical_tree = canonicalized(&base, &ops, 1_000_000, &mut storage).await?;

        let from_buffered = collect_changes(&base, &buffered_tree, &storage).await?;
        let from_canonical = collect_changes(&base, &canonical_tree, &storage).await?;

        assert_eq!(
            from_buffered.len(),
            ops.len(),
            "every buffered op must surface"
        );
        assert_eq!(
            normalize(&from_buffered),
            normalize(&from_canonical),
            "past-the-end ops must diff the same buffered or flushed"
        );
        Ok(())
    }

    /// What two diverged replicas pay to reconcile, across flush regimes.
    ///
    /// Now that the differential reads novelty, buffered trees can be diffed
    /// directly, so this compares regimes that were previously not expressible:
    ///
    /// - **canonical**: both sides flushed to leaves (today's behavior).
    /// - **buffered**: both sides keep their writes at the root. What a replica
    ///   that never canonicalizes pays.
    /// - **staged**: buffers cascade only as far as overflow pushes them, so ops
    ///   settle at intermediate levels rather than reaching leaves.
    ///
    /// Both replicas write *disjoint* facts, the realistic shape: two peers
    /// independently asserting the same facts is unusual outside imports.
    ///
    /// `#[ignore]`d: a measurement, not an assertion. Run with
    /// `cargo test -p dialog-search-tree --release sync_flush_regimes -- --ignored --nocapture`.
    #[dialog_common::test]
    #[ignore]
    async fn sync_flush_regimes() -> Result<()> {
        use std::time::Instant;

        async fn settle(
            delta: &mut Delta<Blake3Hash, Buffer>,
            storage: &mut ContentAddressedStorage<CountingBackend>,
        ) -> Result<()> {
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
            Ok(())
        }

        async fn diverge(
            base: &TestTree,
            keys: &[u32],
            op_buf_size: usize,
            canonicalize: bool,
            storage: &mut ContentAddressedStorage<CountingBackend>,
        ) -> Result<TestTree> {
            let mut tree = crate::HitchhikerTree::open(base).with_op_buf_size(op_buf_size);
            for key in keys {
                tree = tree
                    .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), storage)
                    .await?;
            }
            let mut delta = Delta::zero();
            let result = if canonicalize {
                tree.canonicalize(storage, &mut delta).await?
            } else {
                let root = tree.persist(&mut delta)?;
                TestTree::seal(root, Default::default())
            };
            settle(&mut delta, storage).await?;
            Ok(result)
        }

        let base_size = 10_000u32;
        println!(
            "| base {base_size} entries | regime                     | reads | changes | wall ms |"
        );

        for (scattered, shape) in [(false, "appended"), (true, "scattered")] {
            for divergence in [1u32, 16, 256] {
                let mut storage = ContentAddressedStorage::new(CountingBackend::new());
                let base = build((0..base_size).map(|i| (i, vec![i as u8])), &mut storage).await?;

                // Disjoint divergence, two shapes. Appended keys sort past the whole
                // base, so both sides' writes land in one narrow region: the best
                // case for hash pruning. Scattered keys interleave with the base
                // across its whole range, which is what content-addressed keys
                // actually do, and spreads the divergence over many subtrees.
                let (ours, theirs): (Vec<u32>, Vec<u32>) = if scattered {
                    let stride = base_size / divergence.max(1);
                    (
                        (0..divergence)
                            .map(|i| i * stride + base_size + 1)
                            .collect(),
                        (0..divergence)
                            .map(|i| i * stride + base_size + 2)
                            .collect(),
                    )
                } else {
                    (
                        (0..divergence).map(|i| 1_000_000 + i).collect(),
                        (0..divergence).map(|i| 2_000_000 + i).collect(),
                    )
                };

                for (label, op_buf_size, canonicalize) in [
                    ("canonical (flushed to leaves)", 1024, true),
                    ("buffered (all at root)", 1_000_000, false),
                    ("staged (cascading)", 64, false),
                ] {
                    let left =
                        diverge(&base, &ours, op_buf_size, canonicalize, &mut storage).await?;
                    let right =
                        diverge(&base, &theirs, op_buf_size, canonicalize, &mut storage).await?;

                    storage.backend().reset();
                    let started = Instant::now();
                    let changes = collect_changes(&left, &right, &storage).await?.len();
                    let millis = started.elapsed().as_millis();
                    // Every regime must report the SAME changes: correctness cannot
                    // depend on where ops happen to sit.
                    // Correctness cannot depend on where ops happen to sit: every
                    // regime must report both sides' divergence.
                    assert_eq!(
                        changes,
                        (divergence as usize) * 2,
                        "regime {label} must report both sides' divergence"
                    );
                    let reads = storage.backend().reads();

                    println!(
                        "| {shape:<9} {divergence:<4} | {label:<28} | {reads:>5} | {changes:>7} | {millis:>7} |"
                    );
                }
            }
        }
        Ok(())
    }

    // ---- bug-hunt probes (byte-keyed, explicit ordering) ----

    /// Byte-keyed op: (is_insert, key, value).
    type ByteOp = (bool, [u8; 4], Vec<u8>);

    /// Builds a base tree from raw byte keys (no le_bytes reordering trap).
    async fn build_bytes(
        keys: impl IntoIterator<Item = ([u8; 4], Vec<u8>)>,
        storage: &mut ContentAddressedStorage<CountingBackend>,
    ) -> Result<TestTree> {
        let mut tree = TestTree::empty();
        let mut delta = Delta::zero();
        for (key, value) in keys {
            tree = tree
                .edit()
                .insert(key, value, storage)
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

    async fn buffered_bytes(
        base: &TestTree,
        ops: &[ByteOp],
        op_buf_size: usize,
        storage: &mut ContentAddressedStorage<CountingBackend>,
    ) -> Result<TestTree> {
        let mut tree = crate::HitchhikerTree::open(base).with_op_buf_size(op_buf_size);
        for (is_insert, key, value) in ops {
            tree = if *is_insert {
                tree.insert(*key, value.clone(), storage).await?
            } else {
                tree.delete(*key, storage).await?
            };
        }
        let mut delta = Delta::zero();
        let root = tree.persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(TestTree::seal(root, Default::default()))
    }

    async fn canonical_bytes(
        base: &TestTree,
        ops: &[ByteOp],
        op_buf_size: usize,
        storage: &mut ContentAddressedStorage<CountingBackend>,
    ) -> Result<TestTree> {
        let mut tree = crate::HitchhikerTree::open(base).with_op_buf_size(op_buf_size);
        for (is_insert, key, value) in ops {
            tree = if *is_insert {
                tree.insert(*key, value.clone(), storage).await?
            } else {
                tree.delete(*key, storage).await?
            };
        }
        let mut delta = Delta::zero();
        let canonical = tree.canonicalize(storage, &mut delta).await?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(canonical)
    }

    /// Asserts the oracle for one (source ops, target ops) pair at the given
    /// buffer sizes: buffered diff must equal canonicalized diff.
    async fn assert_oracle(
        base: &TestTree,
        source_ops: &[ByteOp],
        target_ops: &[ByteOp],
        source_buf: usize,
        target_buf: usize,
        label: &str,
        storage: &mut ContentAddressedStorage<CountingBackend>,
    ) -> Result<usize> {
        let source_buffered = buffered_bytes(base, source_ops, source_buf, storage).await?;
        let target_buffered = buffered_bytes(base, target_ops, target_buf, storage).await?;
        let source_canonical = canonical_bytes(base, source_ops, source_buf, storage).await?;
        let target_canonical = canonical_bytes(base, target_ops, target_buf, storage).await?;

        let buffered_changes = collect_changes(&source_buffered, &target_buffered, storage).await?;
        let canonical_changes =
            collect_changes(&source_canonical, &target_canonical, storage).await?;

        assert_eq!(
            normalize(&buffered_changes),
            normalize(&canonical_changes),
            "{label}: buffered diff must equal canonicalized diff"
        );
        Ok(normalize(&canonical_changes).len())
    }

    fn bkey(n: u32) -> [u8; 4] {
        n.to_be_bytes()
    }

    /// Buffered retracts at various depths, of keys present and absent, against
    /// asserts on the other side.
    #[dialog_common::test]
    async fn probe_buffered_retracts_at_mixed_depths() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build_bytes((0..400u32).map(|i| (bkey(i), vec![i as u8])), &mut storage).await?;
        let mut total = 0usize;

        let cases: Vec<(&str, Vec<ByteOp>, Vec<ByteOp>)> = vec![
            (
                "retract present vs nothing",
                vec![(false, bkey(100), vec![])],
                vec![],
            ),
            (
                "retract absent vs nothing",
                vec![(false, bkey(9999), vec![])],
                vec![],
            ),
            (
                "retract vs assert same key",
                vec![(false, bkey(100), vec![])],
                vec![(true, bkey(100), vec![77])],
            ),
            (
                "retract both sides same key",
                vec![(false, bkey(100), vec![])],
                vec![(false, bkey(100), vec![])],
            ),
            (
                "retract then reassert one side",
                vec![(false, bkey(100), vec![]), (true, bkey(100), vec![5])],
                vec![],
            ),
            (
                "retract past rightmost bound",
                vec![(false, bkey(100_000), vec![])],
                vec![(true, bkey(100_000), vec![3])],
            ),
            (
                "retract many scattered",
                (0..40u32).map(|i| (false, bkey(i * 7), vec![])).collect(),
                vec![(true, bkey(3), vec![9])],
            ),
        ];

        for (label, source_ops, target_ops) in cases {
            for (source_buf, target_buf) in [
                (1_000_000, 1_000_000),
                (2, 1_000_000),
                (1_000_000, 2),
                (4, 8),
            ] {
                total += assert_oracle(
                    &base,
                    &source_ops,
                    &target_ops,
                    source_buf,
                    target_buf,
                    &format!("{label} bufs {source_buf}/{target_buf}"),
                    &mut storage,
                )
                .await?;
            }
        }
        assert!(total > 0, "probe must exercise non-empty diffs");
        Ok(())
    }

    /// Keys exactly at, just below, and just past the rightmost upper bound,
    /// exercising both the settle path and the normal walk.
    #[dialog_common::test]
    async fn probe_rightmost_boundary_keys() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let last = 399u32;
        let base = build_bytes((0..=last).map(|i| (bkey(i), vec![i as u8])), &mut storage).await?;
        let mut total = 0usize;

        let cases: Vec<(&str, Vec<ByteOp>, Vec<ByteOp>)> = vec![
            (
                "assert exactly at rightmost bound",
                vec![(true, bkey(last), vec![250])],
                vec![],
            ),
            (
                "assert at rightmost bound both sides differing values",
                vec![(true, bkey(last), vec![250])],
                vec![(true, bkey(last), vec![251])],
            ),
            (
                "assert just below rightmost bound",
                vec![(true, bkey(last - 1), vec![250])],
                vec![],
            ),
            (
                "assert just past rightmost bound",
                vec![(true, bkey(last + 1), vec![250])],
                vec![],
            ),
            (
                "retract exactly at rightmost bound",
                vec![(false, bkey(last), vec![])],
                vec![],
            ),
            (
                "one side past bound, other at bound",
                vec![(true, bkey(last + 1), vec![1])],
                vec![(true, bkey(last), vec![2])],
            ),
        ];

        for (label, source_ops, target_ops) in cases {
            for (source_buf, target_buf) in [(1_000_000, 1_000_000), (2, 1_000_000), (8, 8)] {
                total += assert_oracle(
                    &base,
                    &source_ops,
                    &target_ops,
                    source_buf,
                    target_buf,
                    &format!("{label} bufs {source_buf}/{target_buf}"),
                    &mut storage,
                )
                .await?;
            }
        }
        assert!(total > 0, "probe must exercise non-empty diffs");
        Ok(())
    }

    /// Tiny trees: empty base, single entry, root-is-a-leaf.
    #[dialog_common::test]
    async fn probe_tiny_trees() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let mut total = 0usize;

        for base_size in [0u32, 1, 2, 5] {
            let base = build_bytes(
                (0..base_size).map(|i| (bkey(i), vec![i as u8])),
                &mut storage,
            )
            .await?;

            let cases: Vec<(&str, Vec<ByteOp>, Vec<ByteOp>)> = vec![
                ("insert one", vec![(true, bkey(1000), vec![1])], vec![]),
                (
                    "insert both sides",
                    vec![(true, bkey(1000), vec![1])],
                    vec![(true, bkey(1001), vec![2])],
                ),
                ("retract zero", vec![(false, bkey(0), vec![])], vec![]),
                (
                    "retract absent",
                    vec![(false, bkey(5000), vec![])],
                    vec![(true, bkey(3), vec![9])],
                ),
            ];

            for (label, source_ops, target_ops) in cases {
                for buf in [1_000_000usize, 2] {
                    total += assert_oracle(
                        &base,
                        &source_ops,
                        &target_ops,
                        buf,
                        buf,
                        &format!("base {base_size} {label} buf {buf}"),
                        &mut storage,
                    )
                    .await?;
                }
            }
        }
        assert!(total > 0, "probe must exercise non-empty diffs");
        Ok(())
    }

    /// Randomized oracle over byte keys, retract-heavy, deep cascades,
    /// asymmetric buffer sizes.
    #[dialog_common::test]
    async fn probe_random_oracle_bytes_retract_heavy() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build_bytes((0..300u32).map(|i| (bkey(i), vec![i as u8])), &mut storage).await?;
        let mut total = 0usize;

        for seed in 0..40u64 {
            let mut rng = 0xD1B54A32D192ED03u64 ^ (seed.wrapping_mul(0x9E3779B97F4A7C15));
            let mut next = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                (rng >> 32) as u32
            };

            let mut source_ops: Vec<ByteOp> = Vec::new();
            let mut target_ops: Vec<ByteOp> = Vec::new();
            for _ in 0..50 {
                // Retract-heavy: half the ops are retracts.
                let is_insert = next().is_multiple_of(2);
                // Mix in-range, at-boundary, and past-the-end keys.
                let key = match next() % 4 {
                    0 => next() % 300,
                    1 => 299,
                    2 => 300 + next() % 50,
                    _ => next() % 400,
                };
                let value = vec![(next() % 251) as u8];
                if next().is_multiple_of(2) {
                    source_ops.push((is_insert, bkey(key), value));
                } else {
                    target_ops.push((is_insert, bkey(key), value));
                }
            }

            let (source_buf, target_buf) = match seed % 4 {
                0 => (1_000_000usize, 1_000_000usize),
                1 => (2, 1_000_000),
                2 => (1_000_000, 4),
                _ => (8, 3),
            };

            total += assert_oracle(
                &base,
                &source_ops,
                &target_ops,
                source_buf,
                target_buf,
                &format!("seed {seed}"),
                &mut storage,
            )
            .await?;
        }
        assert!(
            total > 100,
            "probe must exercise non-empty diffs, got {total}"
        );
        Ok(())
    }

    /// Symmetry: diff(a,b) must be the mirror of diff(b,a).
    #[dialog_common::test]
    async fn probe_diff_symmetry_on_buffered_trees() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build_bytes((0..300u32).map(|i| (bkey(i), vec![i as u8])), &mut storage).await?;

        for seed in 0..20u64 {
            let mut rng = 0x2545F4914F6CDD1Du64 ^ (seed.wrapping_mul(0x9E3779B97F4A7C15));
            let mut next = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                (rng >> 32) as u32
            };

            let mut left_ops: Vec<ByteOp> = Vec::new();
            let mut right_ops: Vec<ByteOp> = Vec::new();
            for _ in 0..40 {
                let is_insert = !next().is_multiple_of(3);
                let key = next() % 350;
                let value = vec![(next() % 251) as u8];
                if next().is_multiple_of(2) {
                    left_ops.push((is_insert, bkey(key), value));
                } else {
                    right_ops.push((is_insert, bkey(key), value));
                }
            }

            let left = buffered_bytes(&base, &left_ops, 4, &mut storage).await?;
            let right = buffered_bytes(&base, &right_ops, 1_000_000, &mut storage).await?;

            let forward = collect_changes(&left, &right, &storage).await?;
            let backward = collect_changes(&right, &left, &storage).await?;

            // Mirror: every Add in forward is a Remove in backward and vice
            // versa, over the same key set.
            let mut forward_adds: Vec<_> = forward
                .iter()
                .filter_map(|c| match c {
                    Change::Add(e) => Some((e.key, e.value.clone())),
                    _ => None,
                })
                .collect();
            let mut backward_removes: Vec<_> = backward
                .iter()
                .filter_map(|c| match c {
                    Change::Remove(e) => Some((e.key, e.value.clone())),
                    _ => None,
                })
                .collect();
            forward_adds.sort();
            backward_removes.sort();
            assert_eq!(
                forward_adds, backward_removes,
                "seed {seed}: forward adds must equal backward removes"
            );
        }
        Ok(())
    }

    /// Applying diff(source, target) to source's content must yield target's
    /// content, for buffered trees.
    #[dialog_common::test]
    async fn probe_applying_diff_reconstructs_target() -> Result<()> {
        use futures_util::TryStreamExt as _;

        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build_bytes((0..300u32).map(|i| (bkey(i), vec![i as u8])), &mut storage).await?;

        async fn contents(
            tree: &TestTree,
            storage: &ContentAddressedStorage<CountingBackend>,
        ) -> Result<BTreeMap<[u8; 4], Vec<u8>>> {
            let stream = tree.stream(storage);
            futures_util::pin_mut!(stream);
            let mut out = BTreeMap::new();
            while let Some(entry) = stream.try_next().await? {
                out.insert(entry.key, entry.value);
            }
            Ok(out)
        }

        for seed in 0..15u64 {
            let mut rng = 0x14057B7EF767814Fu64 ^ (seed.wrapping_mul(0x9E3779B97F4A7C15));
            let mut next = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                (rng >> 32) as u32
            };

            let mut source_ops: Vec<ByteOp> = Vec::new();
            let mut target_ops: Vec<ByteOp> = Vec::new();
            for _ in 0..40 {
                let is_insert = !next().is_multiple_of(3);
                let key = next() % 350;
                let value = vec![(next() % 251) as u8];
                if next().is_multiple_of(2) {
                    source_ops.push((is_insert, bkey(key), value));
                } else {
                    target_ops.push((is_insert, bkey(key), value));
                }
            }

            let source = buffered_bytes(&base, &source_ops, 4, &mut storage).await?;
            let target = buffered_bytes(&base, &target_ops, 1_000_000, &mut storage).await?;

            // Canonical forms give the ground-truth content of each side.
            let source_canonical = canonical_bytes(&base, &source_ops, 4, &mut storage).await?;
            let target_canonical =
                canonical_bytes(&base, &target_ops, 1_000_000, &mut storage).await?;

            let mut applied = contents(&source_canonical, &storage).await?;
            let expected = contents(&target_canonical, &storage).await?;

            for change in collect_changes(&source, &target, &storage).await? {
                match change {
                    Change::Remove(entry) => {
                        applied.remove(&entry.key);
                    }
                    Change::Add(entry) => {
                        applied.insert(entry.key, entry.value);
                    }
                }
            }

            assert_eq!(
                applied, expected,
                "seed {seed}: applying the buffered diff must reconstruct the target"
            );
        }
        Ok(())
    }

    /// **CONFIRMED BUG.** `novel_nodes()` on a tree whose root carries live
    /// novelty omits the root block itself, so a remote that receives the
    /// reported block set cannot materialize the target tree.
    ///
    /// `settle_buffered` replaces both frontier nodes with `Settled`, and
    /// `novel_nodes` explicitly `continue`s past `Settled` on the claim that
    /// "its ops live in the bytes of the node that buffers them, which the walk
    /// already recorded". That claim is false when the settled node IS the
    /// root: the root was never pushed into `expanded` (only `expand_at` does
    /// that, and settling short-circuits expansion), so no block carrying the
    /// buffered ops is ever yielded. The target root hash is then unresolvable
    /// on the receiving side.
    #[dialog_common::test]
    async fn probe_novel_nodes_materialize_buffered_target() -> Result<()> {
        novel_nodes_case(vec![(
            "root buffered past end",
            vec![(true, bkey(9000), vec![1]), (true, bkey(9001), vec![2])],
            1_000_000usize,
        )])
        .await
    }

    /// Contrast: a root-buffered *retract* cannot settle (settle_buffered
    /// refuses retracts), so the root is expanded, lands in `expanded`, and the
    /// block set is complete. This isolates the bug to the settle path.
    #[dialog_common::test]
    async fn probe_novel_nodes_materialize_root_buffered_retract() -> Result<()> {
        novel_nodes_case(vec![(
            "root buffered retract (cannot settle)",
            vec![(false, bkey(10), vec![])],
            1_000_000usize,
        )])
        .await
    }

    /// Contrast: ops cascaded deep force expansion all the way down, so the
    /// block set is complete.
    #[dialog_common::test]
    async fn probe_novel_nodes_materialize_cascaded_target() -> Result<()> {
        novel_nodes_case(vec![(
            "cascaded mixed",
            (0..30u32)
                .map(|i| (i % 3 != 0, bkey(i * 5), vec![i as u8]))
                .collect::<Vec<_>>(),
            4usize,
        )])
        .await
    }

    async fn novel_nodes_case(cases: Vec<(&str, Vec<ByteOp>, usize)>) -> Result<()> {
        use futures_util::TryStreamExt as _;

        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base = build_bytes((0..300u32).map(|i| (bkey(i), vec![i as u8])), &mut storage).await?;

        for (label, ops, buf) in cases {
            let target = buffered_bytes(&base, &ops, buf, &mut storage).await?;

            // A "remote" seeded with the whole base tree.
            let mut remote = ContentAddressedStorage::new(CountingBackend::new());
            {
                let difference =
                    TreeDifference::compute(&TestTree::empty(), &base, &storage, &storage).await?;
                let stream = difference.novel_nodes();
                futures_util::pin_mut!(stream);
                while let Some(node) = stream.next().await {
                    let node = node?;
                    remote
                        .store(node.buffer().as_ref().to_vec(), node.hash())
                        .await?;
                }
            }

            // Upload only the novelty between base and the buffered target.
            {
                let difference =
                    TreeDifference::compute(&base, &target, &storage, &storage).await?;
                let stream = difference.novel_nodes();
                futures_util::pin_mut!(stream);
                while let Some(node) = stream.next().await {
                    let node = node?;
                    remote
                        .store(node.buffer().as_ref().to_vec(), node.hash())
                        .await?;
                }
            }

            // The remote must now be able to read the full target tree.
            let expected: Vec<([u8; 4], Vec<u8>)> = {
                let stream = target.stream(&storage);
                futures_util::pin_mut!(stream);
                stream
                    .map_ok(|entry| (entry.key, entry.value))
                    .try_collect::<Vec<_>>()
                    .await?
            };
            let restored = TestTree::from_hash(target.root().clone());
            let actual: Vec<([u8; 4], Vec<u8>)> = {
                let stream = restored.stream(&remote);
                futures_util::pin_mut!(stream);
                stream
                    .map_ok(|entry| (entry.key, entry.value))
                    .try_collect::<Vec<_>>()
                    .await?
            };

            assert_eq!(
                actual, expected,
                "{label}: novel nodes must materialize the target"
            );
        }
        Ok(())
    }
}
