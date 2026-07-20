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
    Key, Link, PersistentNode, PersistentTree, Value, into_owned,
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
    },
    /// An unloaded reference: hash and separator from the parent's link.
    Ref(Link),
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
        }
    }

    fn hash(&self) -> &Blake3Hash {
        match self {
            SparseTreeNode::Loaded { node, .. } => node.hash(),
            SparseTreeNode::Ref(link) => &link.node,
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
            SparseTreeNode::Ref(_) => false,
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
            SparseTreeNode::Ref(_) => false,
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
    Value: self::Value,
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
            vec![SparseTreeNode::Loaded { node, lower_bound }]
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

        let node = match &self.nodes[offset] {
            SparseTreeNode::Loaded { node, .. } => node.clone(),
            SparseTreeNode::Ref(link) => Self::load(self.storage, &link.node).await?,
        };

        match node.body()? {
            ArchivedNodeBody::Index(index) => {
                let children = index
                    .links()?
                    .into_iter()
                    .map(SparseTreeNode::Ref)
                    .collect::<Vec<_>>();
                for child in &children {
                    self.seen.insert(child.hash().clone());
                }

                self.expanded.push(node);
                self.nodes.splice(offset..offset + 1, children);
                Ok(true)
            }
            ArchivedNodeBody::Segment(_) => {
                let lower_bound = self.nodes[offset].lower_bound().to_vec();
                self.nodes[offset] = SparseTreeNode::Loaded { node, lower_bound };
                Ok(false)
            }
        }
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
            if intersects {
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
    fn prune(&mut self, other: &mut Self) {
        let left: HashSet<Blake3Hash> = self.nodes.iter().map(|node| node.hash().clone()).collect();
        let right: HashSet<Blake3Hash> =
            other.nodes.iter().map(|node| node.hash().clone()).collect();
        self.nodes.retain(|node| !right.contains(node.hash()));
        other.nodes.retain(|node| !left.contains(node.hash()));
    }

    /// Streams the entries of every node remaining in the frontier, in key
    /// order, descending through any nodes that are still indexes.
    fn stream(&self) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + '_ {
        try_stream! {
            for sparse_node in &self.nodes {
                let node = match sparse_node {
                    SparseTreeNode::Loaded { node, .. } => node.clone(),
                    SparseTreeNode::Ref(link) => Self::load(self.storage, &link.node).await?,
                };

                // In-order walk; children pushed in reverse so the stack
                // pops them in key order.
                let mut stack = vec![node];
                while let Some(node) = stack.pop() {
                    match node.body()? {
                        ArchivedNodeBody::Index(index) => {
                            let mut children = Vec::with_capacity(index.len());
                            for at in 0..index.len() {
                                let hash = index.hash_at(at)?;
                                children.push(Self::load(self.storage, hash).await?);
                            }
                            while let Some(child) = children.pop() {
                                stack.push(child);
                            }
                        }
                        ArchivedNodeBody::Segment(segment) => {
                            let mut keys = segment.keys::<Key>()?;
                            while let Some((at, key)) = keys.next_key()? {
                                let entry = Entry {
                                    // `key` borrows the decoder's reused buffer;
                                    // this owns the single copy.
                                    key: Key::try_from_bytes(key)?,
                                    value: into_owned(segment.value_at(at)?)?,
                                };
                                yield entry;
                            }
                        }
                    }
                }
            }
        }
    }
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
        // by construction and must be visited to be reported.
        loop {
            let mut expanded = false;
            for index in 0..difference.target.nodes.len() {
                let bound = difference.target.nodes[index].lower_bound().to_vec();
                if difference.target.expand_at(&bound).await? {
                    expanded = true;
                    break;
                }
            }
            if !expanded {
                break;
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

    /// Returns a stream of entry-level changes that transform the source
    /// tree into the target tree.
    ///
    /// The conservative key spans this difference confines all changes to,
    /// as `(exclusive lower bound, inclusive upper bound)` pairs: one per
    /// remaining frontier node on either side, in frontier order per side.
    /// A `None` lower bound means the span starts at the bottom of the key
    /// space.
    ///
    /// Conservative means superset: every changed key lies inside some
    /// span, but a span may also cover unchanged keys (shared nodes
    /// pruned between two divergent ones widen the reported spans, and a
    /// node's true lower bound is unknowable after its left siblings were
    /// pruned). Callers partitioning work by these spans over-include,
    /// never miss. Costs no reads: bounds come from the frontier links.
    pub fn divergent_bounds(&self) -> Vec<(Option<Key>, Key)> {
        let mut bounds = Vec::new();
        for tree in [&self.source, &self.target] {
            let mut previous: Option<Key> = None;
            for node in &tree.nodes {
                let upper = node.upper_bound().clone();
                bounds.push((previous.clone(), upper.clone()));
                previous = Some(upper);
            }
        }
        bounds
    }

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
                if self.source.seen.contains(sparse_node.hash()) {
                    continue;
                }
                let node = match sparse_node {
                    SparseTreeNode::Loaded { node, .. } => node.clone(),
                    SparseTreeNode::Ref(link) => {
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
}
