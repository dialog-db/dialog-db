//! Tree construction and modification logic.
//!
//! This module contains [`TreeShaper`], which encapsulates all the logic for
//! building and modifying tree structures. By separating mutation operations
//! from the read-only [`Tree`] interface, we achieve clearer separation of
//! concerns and make the codebase more maintainable.

use std::{collections::VecDeque, marker::PhantomData};

use dialog_common::Blake3Hash;
use hashbrown::HashMap;
use nonempty::NonEmpty;
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
    Buffer, Delta, DialogSearchTreeError, Entry, Key, Link, Node, NodeBody, Rank, RightNeighbor,
    SearchResult, Segment, SymmetryWith, TreeLayer, Value, distribution, into_owned,
};

/// A collection of nodes with their ranks.
type RankedNodes<Key, Value> = NonEmpty<(Node<Key, Value>, Rank)>;

const BOTTOM_RANK: Rank = 1;

/// The stateful side-effects of tree mutations are compartmentalized
/// to a MutationContext. Key ranking is intermediated by the MutationContext
/// so that ranks may be cached by key (avoiding redundant hashing and rank
/// computation). Ideally this rank cache would be kept at a higher layer of
/// abstraction so that it could be shared across mutations, but holding the
/// cache in the mutation context is a low-hanging fruit.
struct MutationContext<Key>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + crate::SymmetryWith<Key> + Ord,
{
    delta: Delta<Blake3Hash, Buffer>,
    rank_cache: HashMap<Key, Rank>,
}

impl<Key> MutationContext<Key>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + crate::SymmetryWith<Key> + Ord,
{
    pub fn new(delta: Delta<Blake3Hash, Buffer>) -> Self {
        Self {
            delta,
            rank_cache: HashMap::new(),
        }
    }

    pub fn rank(&mut self, key: &Key) -> Rank {
        if let Some(rank) = self.rank_cache.get(key) {
            *rank
        } else {
            let key_hash = Blake3Hash::hash(key.as_ref());
            let rank = distribution::geometric::rank(&key_hash);
            self.rank_cache.insert(key.clone(), rank);
            rank
        }
    }

    pub fn delta(&mut self) -> &mut Delta<Blake3Hash, Buffer> {
        &mut self.delta
    }

    pub fn take_delta(self) -> Delta<Blake3Hash, Buffer> {
        self.delta
    }
}

impl<Key> From<MutationContext<Key>> for Delta<Blake3Hash, Buffer>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + crate::SymmetryWith<Key> + Ord,
{
    fn from(value: MutationContext<Key>) -> Self {
        value.delta
    }
}

/// Handles tree construction and modification operations.
///
/// [`TreeShaper`] encapsulates the complex logic for building and modifying
/// prolly trees, including:
/// - Rank-based node collection and splitting
/// - Path reconstruction after modifications
/// - Delta management for structural changes
///
/// This struct holds the delta for a mutation operation and provides methods to
/// perform tree modifications. Each instance is tied to a specific mutation and
/// consumes itself when the operation completes.
pub struct TreeShaper<Key, Value>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + crate::SymmetryWith<Key> + Ord,
    Value: self::Value,
{
    root: Blake3Hash,
    delta: Delta<Blake3Hash, Buffer>,
    key: PhantomData<Key>,
    value: PhantomData<Value>,
}

impl<Key, Value> TreeShaper<Key, Value>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + crate::SymmetryWith<Key> + Ord,
    Key::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Key::Archived: Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Key: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    Value: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
{
    /// Creates a new [`TreeShaper`] with the given root and delta.
    ///
    /// The root represents the current root hash of the tree, and the delta
    /// represents the current state of pending changes from the tree. It will
    /// be branched internally as needed during mutation operations.
    pub fn new(root: Blake3Hash, delta: Delta<Blake3Hash, Buffer>) -> Self {
        Self {
            root,
            delta,
            key: PhantomData,
            value: PhantomData,
        }
    }

    /// Inserts a new entry into the tree, returning the new root hash and
    /// delta.
    ///
    /// This method takes an optional search result. If provided, it points to
    /// the leaf segment where the entry should be inserted. If `None`, the tree
    /// is empty and this will be the first entry.
    ///
    /// For existing trees, it:
    /// 1. Extracts the entries from the segment
    /// 2. Performs binary search to find insertion point or existing key
    /// 3. Inserts the new entry or updates existing value
    /// 4. Redistributes all entries by their intrinsic ranks
    /// 5. Rebuilds the tree path up to the root
    ///
    /// For empty trees, it simply creates a single-entry segment.
    ///
    /// If the key already exists, its value is updated. If it's new, it's
    /// inserted in sorted order.
    pub fn insert(
        self,
        new_entry: Entry<Key, Value>,
        search_result: Option<SearchResult<Key, Value>>,
    ) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError> {
        let new_entry_key = &new_entry.key;

        let (entries, search_result) = match search_result {
            Some(search_result) => {
                let segment = into_owned::<Segment<Key, Value>>(search_result.leaf.as_segment()?)?;
                let mut entries: Vec<Entry<Key, Value>> = segment.entries;

                match entries.binary_search_by(|probe| probe.key.cmp(new_entry_key)) {
                    // Entry was found; update the value.
                    Ok(index) => {
                        let Some(previous_entry) = entries.get_mut(index) else {
                            return Err(DialogSearchTreeError::Access(format!(
                                "Entry at index {} not found",
                                index,
                            )));
                        };
                        previous_entry.value = new_entry.value;
                    }
                    // Entry was not found; insert at the provided index.
                    Err(index) => {
                        entries.insert(index, new_entry);
                    }
                };

                let entries = NonEmpty::from_vec(entries).ok_or_else(|| {
                    DialogSearchTreeError::Operation(
                        "Segment has no entries after modification".into(),
                    )
                })?;

                (entries, Some(search_result))
            }

            None => (NonEmpty::singleton(new_entry), None),
        };

        self.distribute(entries, search_result)
    }

    /// Removes an entry from the tree, returning the new root hash and delta.
    ///
    /// This method takes a search result pointing to the leaf segment
    /// containing the key to remove. It handles four cases:
    ///
    /// 1. **Key doesn't exist in segment**: Returns the current root unchanged
    ///    with the original delta (no-op).
    ///
    /// 2. **Segment becomes empty**: Removes the empty segment by merging its
    ///    siblings at the parent level, then rebuilds the tree upward.
    ///
    /// 3. **Boundary-delete overflow**: The deleted entry was the segment's
    ///    last entry (its boundary), the segment still has orphan entries, and
    ///    a right-adjacent segment exists. The orphans are merged with the
    ///    right-adjacent segment's entries; the combined list is redistributed
    ///    and the two affected subtrees are stitched back together at their
    ///    lowest common ancestor. Requires the `right_neighbor` prefetch on
    ///    the search result.
    ///
    /// 4. **Ordinary shrink**: The segment still has entries after removal and
    ///    there is no overflow. The remaining entries are redistributed using
    ///    their intrinsic ranks and the tree path is rebuilt.
    pub fn delete(
        self,
        key: &Key,
        search_result: SearchResult<Key, Value>,
    ) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError> {
        let segment = search_result.leaf.as_segment()?;

        let removal_index = match segment
            .entries
            .binary_search_by(|entry| SymmetryWith::cmp(&entry.key, key))
        {
            Ok(index) => index,
            Err(_) => {
                // Key not found; return unchanged state
                return Ok((self.root, self.delta));
            }
        };

        let entry_count = segment.entries.len();
        let deleted_boundary = removal_index == entry_count - 1;

        let mut segment = into_owned::<Segment<Key, Value>>(segment)?;
        segment.entries.remove(removal_index);

        // Boundary-delete with non-empty orphans and a right-adjacent segment
        // triggers overflow: the orphans must fold into the next segment so
        // the resulting tree matches a from-scratch build.
        if deleted_boundary
            && !segment.entries.is_empty()
            && let Some(right_neighbor) = search_result.right_neighbor
        {
            let main_leaf_hash = search_result.leaf.hash().clone();
            return self.absorb_orphans_into_right_neighbor(
                segment.entries,
                main_leaf_hash,
                search_result.path,
                right_neighbor,
            );
        }

        match NonEmpty::from_vec(segment.entries) {
            Some(entries) => self.distribute(entries, Some(search_result)),
            None => self.remove_from_path(search_result.path),
        }
    }

    /// Resolves a boundary-delete overflow using the prefetched right-adjacent
    /// segment.
    ///
    /// See case (3) in [`Self::delete`] for the high-level contract.
    ///
    /// Key invariant: when a rank-R entry is deleted, it was simultaneously a
    /// boundary at levels 0..R-2, and each of those boundaries dissolves. At
    /// every level below the LCA, the main subtree's rebuild and the
    /// right-adjacent subtree's rebuild therefore fuse into a *single* index
    /// rather than remaining as siblings. The tree above the LCA is unaffected
    /// and handed off to the standard merge routine.
    ///
    /// By construction of the search (main descent follows the boundary
    /// rightward at every level; the right-adjacent descent takes leftmost
    /// children) we can rely on:
    /// - `main_layer.right_siblings == None` for every layer below the LCA, and
    /// - `right_layer.left_siblings == None` for every layer below the LCA.
    ///
    /// The fold at each level is therefore just
    /// `[main_layer.left_siblings | unified | right_layer.right_siblings]`.
    fn absorb_orphans_into_right_neighbor(
        self,
        orphans: Vec<Entry<Key, Value>>,
        main_leaf_hash: Blake3Hash,
        main_path: Vec<TreeLayer<Key, Value>>,
        right_neighbor: RightNeighbor<Key, Value>,
    ) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError> {
        let RightNeighbor {
            lca_depth,
            diverged_path,
            leaf: right_leaf,
        } = right_neighbor;

        let mut context = MutationContext::<Key>::new(self.delta.branch());

        // Both original leaves are replaced; clear any lingering delta entries.
        context.delta().remove(&main_leaf_hash);
        context.delta().remove(right_leaf.hash());

        // Merge orphans with the right-adjacent segment's entries and
        // redistribute by intrinsic rank.
        let right_segment = into_owned::<Segment<Key, Value>>(right_leaf.as_segment()?)?;
        let mut combined_entries = orphans;
        combined_entries.extend(right_segment.entries);
        let combined = NonEmpty::from_vec(combined_entries).ok_or_else(|| {
            DialogSearchTreeError::Operation("Overflow merge produced empty entry list".into())
        })?;
        let ranked = combined.map(|entry| {
            let rank = context.rank(&entry.key);
            (entry, rank)
        });
        let mut unified = Self::collect::<Entry<Key, Value>>(ranked, BOTTOM_RANK)?;

        // Split main_path into [above-LCA | LCA | below-LCA].
        let mut above_and_lca = main_path;
        let main_below = above_and_lca.split_off(lca_depth + 1);
        let lca_layer = above_and_lca.pop().ok_or_else(|| {
            DialogSearchTreeError::Operation("Main path was shorter than lca_depth".into())
        })?;
        let above_lca = above_and_lca;

        // Fuse the two subtrees level-by-level below the LCA. At every such
        // level the boundary that separated them has dissolved, so their
        // children flatten into one unified index.
        //
        // Nodes live at level 0 after the entry merge; building a level-1
        // parent uses minimum_rank 2 (level L uses minimum_rank L + 1).
        let mut level_minimum_rank: Rank = 2;
        for (main_layer, right_layer) in main_below
            .into_iter()
            .rev()
            .zip(diverged_path.into_iter().rev())
        {
            context.delta().remove(main_layer.host.hash());
            context.delta().remove(right_layer.host.hash());

            debug_assert!(
                main_layer.right_siblings.is_none(),
                "main descent follows the rightmost path; no right siblings below LCA"
            );
            debug_assert!(
                right_layer.left_siblings.is_none(),
                "right-adjacent descent is leftmost; no left siblings below LCA"
            );

            let unified_links = promote_to_ranked_links(unified, &mut context)?;

            let mut parts: Vec<NonEmpty<(Link<Key>, Rank)>> = Vec::new();
            if let Some(left) = main_layer.left_siblings {
                parts.push(into_ranked_links(left, &mut context));
            }
            parts.push(unified_links);
            if let Some(right) = right_layer.right_siblings {
                parts.push(into_ranked_links(right, &mut context));
            }

            let combined_links = concat_nonempty(parts)?;
            unified = Self::collect::<Link<_>>(combined_links, level_minimum_rank)?;
            level_minimum_rank += 1;
        }

        // At the LCA, the first right sibling was the right-descent target; it
        // has been subsumed by the unified subtree. The remaining right
        // siblings stay as peers of the unified subtree at this level.
        let right_siblings_at_lca = lca_layer.right_siblings.ok_or_else(|| {
            DialogSearchTreeError::Operation(
                "LCA layer had no right siblings during overflow".into(),
            )
        })?;
        let modified_right_siblings =
            NonEmpty::from_vec(right_siblings_at_lca.into_iter().skip(1).collect());

        let lca_has_own_siblings =
            lca_layer.left_siblings.is_some() || modified_right_siblings.is_some();

        if !lca_has_own_siblings {
            // The LCA's only children were the main- and right-descent targets;
            // both were subsumed by the unified subtree. The LCA node itself
            // disappears from the canonical tree.
            context.delta().remove(lca_layer.host.hash());

            if above_lca.is_empty() {
                // The LCA was the tree root; the unified subtree is the new
                // root. Forcing another collect here would inject an extra
                // level (a 1-child index) that never appears in a from-scratch
                // build.
                context.delta().add_all(
                    unified
                        .iter()
                        .map(|(n, _)| (n.hash().clone(), n.buffer().clone())),
                );
                return Ok((unified.head.0.hash().clone(), context.take_delta()));
            }

            // Promote the unified subtree to the LCA's level so it can slot
            // into the LCA's parent as a replacement child, then continue the
            // normal upward merge.
            let unified_links = promote_to_ranked_links(unified, &mut context)?;
            let promoted = Self::collect::<Link<_>>(unified_links, level_minimum_rank)?;
            return Self::merge_with_path(promoted, above_lca, context, level_minimum_rank + 1);
        }

        // LCA has genuine siblings: rebuild the LCA around the unified subtree
        // and the preserved siblings, then hand off to the standard merge.
        let modified_lca_layer = TreeLayer {
            host: lca_layer.host,
            left_siblings: lca_layer.left_siblings,
            right_siblings: modified_right_siblings,
        };
        let mut final_path = above_lca;
        final_path.push(modified_lca_layer);

        Self::merge_with_path(unified, final_path, context, level_minimum_rank)
    }

    /// Redistributes a non-empty list of entries into a new tree structure,
    /// rebuilding the path from leaves to root.
    ///
    /// This is the core method for tree construction and modification. It
    /// computes each entry's intrinsic rank (via the [`MutationContext`] cache),
    /// groups entries into leaf segments via [`Self::collect`], and then
    /// propagates the new segments up the search path, merging with unchanged
    /// sibling links at each level.
    ///
    /// When a `search_result` is provided, only the affected path is rebuilt;
    /// unchanged subtrees are preserved by reference for structural sharing.
    /// When `None`, the input entries become the first segment of a new tree.
    ///
    /// Returns the new root hash and a delta containing all newly created
    /// nodes.
    fn distribute(
        self,
        entries: NonEmpty<Entry<Key, Value>>,
        search_result: Option<SearchResult<Key, Value>>,
    ) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError> {
        let mut context = MutationContext::<Key>::new(self.delta.branch());
        let ranked_entries = entries.map(|entry| {
            let rank = context.rank(&entry.key);
            (entry, rank)
        });
        let nodes = Self::collect(ranked_entries, BOTTOM_RANK)?;

        let search_path = if let Some(search_result) = search_result {
            context.delta().remove(search_result.leaf.hash());
            search_result.path
        } else {
            vec![]
        };

        // Nodes start at level 0 (segments); the first level-up collect uses
        // minimum_rank 2 (the level-1 threshold).
        Self::merge_with_path(nodes, search_path, context, 2)
    }

    /// Merges a collection of nodes with a search path, rebuilding from leaves
    /// to root.
    ///
    /// This method takes an initial set of nodes (typically from modified leaf
    /// segments) and propagates them up through the tree hierarchy. At each
    /// level, it:
    /// 1. Converts nodes to links and adds them to the delta
    /// 2. Merges with left and right siblings from the search path
    /// 3. Collects the merged links into new nodes based on rank
    /// 4. Continues until a single root node is formed
    ///
    /// Sibling link ranks are recomputed from their `upper_bound` keys via the
    /// supplied [`MutationContext`], so the rank cache is reused across levels
    /// when the same boundary key appears more than once.
    ///
    /// `initial_level_minimum_rank` is the rank threshold to apply at the first
    /// level of the walk. Callers that start from raw leaf segments pass `2`;
    /// the overflow path passes a higher value when it picks up mid-walk.
    ///
    /// This is the shared path-reconstruction logic used by both insert (after
    /// distributing entries) and delete (after modifying a segment).
    fn merge_with_path(
        mut nodes: NonEmpty<(Node<Key, Value>, Rank)>,
        mut search_path: Vec<TreeLayer<Key, Value>>,
        mut context: MutationContext<Key>,
        initial_level_minimum_rank: Rank,
    ) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError> {
        let mut level_minimum_rank = initial_level_minimum_rank;

        loop {
            let links = {
                context.delta().add_all(
                    nodes
                        .iter()
                        .map(|(node, _)| (node.hash().clone(), node.buffer().clone())),
                );

                let ranked_links = nodes
                    .into_iter()
                    .map(|(node, rank)| node.to_link().map(|link| (link, rank)))
                    .collect::<Result<Vec<(Link<Key>, Rank)>, DialogSearchTreeError>>()
                    .and_then(|links| {
                        NonEmpty::from_vec(links)
                            .ok_or_else(|| DialogSearchTreeError::Node("Empty child list".into()))
                    })?;

                match search_path.pop() {
                    Some(layer) => {
                        context.delta().remove(layer.host.hash());
                        let ranked_left_siblings = layer
                            .left_siblings
                            .map(|links| into_ranked_links(links, &mut context));
                        let ranked_right_siblings = layer
                            .right_siblings
                            .map(|links| into_ranked_links(links, &mut context));

                        match (ranked_left_siblings, ranked_right_siblings) {
                            (None, None) => ranked_links,
                            (Some(ranked_left_siblings), None) => {
                                concat_nonempty(vec![ranked_left_siblings, ranked_links])?
                            }
                            (None, Some(ranked_right_siblings)) => {
                                concat_nonempty(vec![ranked_links, ranked_right_siblings])?
                            }
                            (Some(ranked_left_siblings), Some(ranked_right_siblings)) => {
                                concat_nonempty(vec![
                                    ranked_left_siblings,
                                    ranked_links,
                                    ranked_right_siblings,
                                ])?
                            }
                        }
                    }
                    None => ranked_links,
                }
            };

            nodes = Self::collect::<Link<_>>(links, level_minimum_rank)?;

            if search_path.is_empty() && nodes.len() == 1 {
                break;
            }

            level_minimum_rank += 1;
        }

        context.delta().add_all(
            nodes
                .iter()
                .map(|(node, _)| (node.hash().clone(), node.buffer().clone())),
        );

        Ok((nodes.head.0.hash().to_owned(), context.take_delta()))
    }

    /// Removes a segment from the tree when it becomes empty after deletion.
    ///
    /// This method handles the case where deleting an entry leaves a segment
    /// with no entries. It removes the segment by merging its left and right
    /// siblings at the parent level, then rebuilds the tree upward.
    ///
    /// If the removed segment was the only child (no siblings), the removal
    /// propagates upward. If it was the last segment in the entire tree, the
    /// tree becomes empty.
    fn remove_from_path(
        self,
        path: Vec<TreeLayer<Key, Value>>,
    ) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError> {
        let context = MutationContext::<Key>::new(self.delta.branch());

        Self::remove_from_path_with_context(path.into(), context)
    }

    /// Internal helper for `remove_from_path` that works with a
    /// [`MutationContext`] directly.
    ///
    /// Takes a [`VecDeque`] rather than [`Vec`] so recursive/cascading removal
    /// can `pop_front` each layer in O(1); a `Vec::remove(0)` here would push
    /// the shift cost to O(H²) across a chain of collapsed ancestors.
    fn remove_from_path_with_context(
        mut path: VecDeque<TreeLayer<Key, Value>>,
        mut context: MutationContext<Key>,
    ) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError> {
        use dialog_common::NULL_BLAKE3_HASH;

        // If there's no parent, the tree becomes empty
        let Some(layer) = path.pop_front() else {
            return Ok((NULL_BLAKE3_HASH.clone(), Delta::zero()));
        };

        context.delta().remove(layer.host.hash());

        // Collect left and right siblings, excluding the removed segment
        let mut links = Vec::new();

        if let Some(left_siblings) = layer.left_siblings {
            links.extend(left_siblings);
        }

        // Note: we skip the removed segment's link here

        if let Some(right_siblings) = layer.right_siblings {
            links.extend(right_siblings);
        }

        // If no siblings remain, propagate the removal upward
        if links.is_empty() {
            return if path.is_empty() {
                Ok((NULL_BLAKE3_HASH.clone(), context.take_delta()))
            } else {
                // Continue removing up the path with the same context
                Self::remove_from_path_with_context(path, context)
            };
        }

        // We have siblings; rebuild with them
        let Some(links) = NonEmpty::from_vec(links) else {
            return Err(DialogSearchTreeError::Node(
                "Unexpectedly empty link list".into(),
            ));
        };
        let ranked_links = into_ranked_links(links, &mut context);

        // Create nodes from the remaining links
        let nodes = Self::collect::<Link<_>>(ranked_links, 1)?;

        // Merge up the remaining path. `merge_with_path` pops from the back,
        // so hand it back a `Vec` of the still-pending ancestors.
        Self::merge_with_path(nodes, path.into(), context, 2)
    }

    /// Collects a sequence of ranked children into nodes based on a minimum
    /// rank threshold.
    ///
    /// This method groups children into nodes by accumulating them until a
    /// child with a rank exceeding the minimum threshold is encountered. When
    /// such a child is found, the accumulated children (including the high-rank
    /// child) are collected into a node, and accumulation begins anew.
    ///
    /// The algorithm:
    /// 1. Accumulate children in a pending list
    /// 2. When a child's rank > minimum_rank, create a node from pending
    ///    children
    /// 3. Continue until all children are processed
    /// 4. Any remaining children form a final node with rank = minimum_rank
    ///
    /// This rank-based partitioning is what gives the prolly tree its
    /// probabilistic splitting behavior, which in turn enables efficient
    /// structural sharing and diff computation.
    fn collect<Child>(
        children: NonEmpty<(Child, Rank)>,
        minimum_rank: Rank,
    ) -> Result<RankedNodes<Key, Value>, DialogSearchTreeError>
    where
        NodeBody<Key, Value>: TryFrom<Vec<Child>, Error = DialogSearchTreeError>,
    {
        let mut output: Vec<(Node<Key, Value>, u32)> = vec![];
        let mut pending = vec![];

        for (child, rank) in children {
            pending.push(child);
            if rank > minimum_rank {
                if pending.is_empty() {
                    return Err(DialogSearchTreeError::Node(
                        "Attempted to collect empty child list into index node".into(),
                    ));
                }
                let node = Node::new(Buffer::from(
                    NodeBody::try_from(std::mem::take(&mut pending))?.as_bytes()?,
                ));

                output.push((node, rank));
            }
        }

        if !pending.is_empty() {
            let node = Node::new(Buffer::from(NodeBody::try_from(pending)?.as_bytes()?));
            output.push((node, minimum_rank));
        }

        NonEmpty::from_vec(output).ok_or_else(|| {
            DialogSearchTreeError::Node("Node list was empty after collection".into())
        })
    }
}

/// Persists the given ranked nodes to the delta and converts them into ranked
/// links, preserving the rank that was assigned by `collect`.
///
/// This is the shared helper used when a rebuild hands off its output up one
/// level: the nodes become children of a new higher-level index, so we must
/// both commit them to the delta (by hash) and convert them to links.
fn promote_to_ranked_links<Key, Value>(
    nodes: NonEmpty<(Node<Key, Value>, Rank)>,
    context: &mut MutationContext<Key>,
) -> Result<NonEmpty<(Link<Key>, Rank)>, DialogSearchTreeError>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + crate::SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    context.delta().add_all(
        nodes
            .iter()
            .map(|(node, _)| (node.hash().clone(), node.buffer().clone())),
    );
    let links: Vec<(Link<Key>, Rank)> = nodes
        .into_iter()
        .map(|(node, rank)| node.to_link().map(|link| (link, rank)))
        .collect::<Result<_, DialogSearchTreeError>>()?;
    NonEmpty::from_vec(links).ok_or_else(|| DialogSearchTreeError::Node("Empty link list".into()))
}

/// Converts a collection of links into ranked links by computing each link's
/// rank from its node hash.
fn into_ranked_links<Key>(
    links: NonEmpty<Link<Key>>,
    context: &mut MutationContext<Key>,
) -> NonEmpty<(Link<Key>, Rank)>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + crate::SymmetryWith<Key> + Ord,
{
    links.map(|link| {
        let rank = context.rank(&link.upper_bound);
        (link, rank)
    })
}

/// Concatenates multiple non-empty lists into a single non-empty list.
//
/// TODO: Improve. Possibly remove NonEmpty as it introduces some overhead
/// compared to index comparison with slices.
fn concat_nonempty<T>(list: Vec<NonEmpty<T>>) -> Result<NonEmpty<T>, DialogSearchTreeError> {
    Ok(NonEmpty::flatten(NonEmpty::from_vec(list).ok_or(
        DialogSearchTreeError::Node("Empty child list".into()),
    )?))
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash;
    use dialog_storage::MemoryStorageBackend;
    use nonempty::NonEmpty;

    use super::TreeShaper;
    use crate::{ContentAddressedStorage, Entry, Rank, Tree, distribution, into_owned};

    type TestTree = Tree<[u8; 4], Vec<u8>>;
    type TestStorage = ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Compute the prolly-tree rank for a key given as a u32.
    fn rank_of(key: u32) -> Rank {
        distribution::geometric::rank(&Blake3Hash::hash(&key.to_le_bytes()))
    }

    /// Return keys in `range` that act as segment boundaries (rank > 1).
    fn boundary_keys(range: std::ops::Range<u32>) -> Vec<u32> {
        range.filter(|&i| rank_of(i) > 1).collect()
    }

    /// Return keys in `range` that are NOT segment boundaries (rank <= 1).
    fn interior_keys(range: std::ops::Range<u32>) -> Vec<u32> {
        range.filter(|&i| rank_of(i) <= 1).collect()
    }

    /// Build a tree by inserting the given keys in order, then flush.
    async fn build_and_flush(keys: &[u32], storage: &mut TestStorage) -> Result<TestTree> {
        let mut tree = TestTree::empty();
        for &k in keys {
            tree = tree
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), storage)
                .await?;
        }
        for (hash, buf) in tree.flush() {
            storage.store(buf.as_ref().to_vec(), &hash).await?;
        }
        Ok(tree)
    }

    #[dialog_common::test]
    async fn it_partitions_entries_at_rank_boundaries() -> Result<()> {
        let entries: Vec<_> = (0..1000u32)
            .map(|i| {
                let key = i.to_le_bytes();
                let rank = rank_of(i);
                (
                    Entry {
                        key,
                        value: vec![i as u8],
                    },
                    rank,
                )
            })
            .collect();

        let boundary_count = entries.iter().filter(|(_, r)| *r > 1).count();
        assert!(boundary_count > 0, "Need at least one boundary in 0..1000");

        let nodes = TreeShaper::<[u8; 4], Vec<u8>>::collect(
            NonEmpty::from_vec(entries.clone()).unwrap(),
            1,
        )?;

        // One segment per boundary, plus a trailing segment when the
        // last entry is not itself a boundary.
        let last_is_boundary = entries.last().map(|(_, r)| *r > 1).unwrap_or(false);
        let expected = if last_is_boundary {
            boundary_count
        } else {
            boundary_count + 1
        };
        assert_eq!(nodes.len(), expected, "Wrong number of segments");

        // Non-trailing segments carry the rank of their boundary entry
        // (> 1). The trailing segment receives rank = minimum_rank = 1.
        for (i, (_, rank)) in nodes.iter().enumerate() {
            if i < nodes.len() - 1 || last_is_boundary {
                assert!(
                    *rank > 1,
                    "Segment {i} should end with a boundary (rank > 1), got {rank}"
                );
            } else {
                assert_eq!(*rank, 1, "Trailing segment should have rank = minimum_rank");
            }
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_preserves_entry_order_within_and_across_segments() -> Result<()> {
        // Build entries sorted by [u8; 4] byte order (not u32 order),
        // since the tree's key comparison is lexicographic on bytes.
        let mut keys: Vec<[u8; 4]> = (0..500u32).map(|i| i.to_le_bytes()).collect();
        keys.sort();

        let entries: Vec<_> = keys
            .iter()
            .map(|key| {
                let rank = distribution::geometric::rank(&Blake3Hash::hash(key));
                (
                    Entry {
                        key: *key,
                        value: key.to_vec(),
                    },
                    rank,
                )
            })
            .collect();

        let nodes =
            TreeShaper::<[u8; 4], Vec<u8>>::collect(NonEmpty::from_vec(entries).unwrap(), 1)?;

        let mut prev_upper: Option<[u8; 4]> = None;
        for (node, _) in nodes.iter() {
            let segment = node.as_segment()?;

            // Entries within a segment must be sorted.
            for pair in segment.entries.windows(2) {
                assert!(pair[0].key < pair[1].key);
            }

            // Segments must not overlap and must be in ascending order.
            if let (Some(prev), Some(first)) = (prev_upper, segment.entries.first()) {
                let first_key: [u8; 4] = into_owned(&first.key)?;
                assert!(prev < first_key, "Segments must be in ascending key order");
            }
            if let Some(last) = segment.entries.last() {
                prev_upper = Some(into_owned(&last.key)?);
            }
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_preserves_total_entry_count_across_segments() -> Result<()> {
        let n = 1000u32;
        let entries: Vec<_> = (0..n)
            .map(|i| {
                let key = i.to_le_bytes();
                let rank = rank_of(i);
                (
                    Entry {
                        key,
                        value: i.to_le_bytes().to_vec(),
                    },
                    rank,
                )
            })
            .collect();

        let nodes =
            TreeShaper::<[u8; 4], Vec<u8>>::collect(NonEmpty::from_vec(entries).unwrap(), 1)?;

        let total: usize = nodes
            .iter()
            .map(|(node, _)| node.as_segment().unwrap().entries.len())
            .sum();
        assert_eq!(total, n as usize);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_produces_canonical_tree_after_deleting_a_boundary_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let all_keys: Vec<u32> = (0..1000).collect();
        let boundaries = boundary_keys(0..1000);
        assert!(
            boundaries.len() >= 2,
            "Need at least 2 boundary keys for a meaningful test; got {}",
            boundaries.len()
        );

        let mut full_tree = build_and_flush(&all_keys, &mut storage).await?;

        for &bk in boundaries.iter().take(5) {
            let mut tree_via_delete = full_tree.delete(&bk.to_le_bytes(), &storage).await?;
            for (h, b) in tree_via_delete.flush() {
                storage.store(b.as_ref().to_vec(), &h).await?;
            }

            let remaining: Vec<u32> = all_keys.iter().copied().filter(|&k| k != bk).collect();
            let tree_from_scratch = build_and_flush(&remaining, &mut storage).await?;

            assert_eq!(
                tree_via_delete.root(),
                tree_from_scratch.root(),
                "Deleting boundary key {bk} should produce the same root \
                 as building from scratch without it"
            );
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_produces_canonical_tree_after_deleting_a_non_boundary_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let all_keys: Vec<u32> = (0..1000).collect();
        let non_boundaries = interior_keys(0..1000);
        assert!(!non_boundaries.is_empty());

        let mut full_tree = build_and_flush(&all_keys, &mut storage).await?;

        for &key in non_boundaries.iter().take(5) {
            let mut tree_via_delete = full_tree.delete(&key.to_le_bytes(), &storage).await?;
            for (h, b) in tree_via_delete.flush() {
                storage.store(b.as_ref().to_vec(), &h).await?;
            }

            let remaining: Vec<u32> = all_keys.iter().copied().filter(|&k| k != key).collect();
            let tree_from_scratch = build_and_flush(&remaining, &mut storage).await?;

            assert_eq!(
                tree_via_delete.root(),
                tree_from_scratch.root(),
                "Deleting non-boundary key {key} should produce the same root \
                 as building from scratch without it"
            );
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_produces_canonical_tree_after_bulk_deletion() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let final_keys: Vec<u32> = (0..200).collect();
        let extra_keys: Vec<u32> = (200..400).collect();

        let mut all_keys = final_keys.clone();
        all_keys.extend(&extra_keys);

        let tree_direct = build_and_flush(&final_keys, &mut storage).await?;

        let mut tree_pruned = build_and_flush(&all_keys, &mut storage).await?;
        for &ek in &extra_keys {
            tree_pruned = tree_pruned.delete(&ek.to_le_bytes(), &storage).await?;
        }
        for (h, b) in tree_pruned.flush() {
            storage.store(b.as_ref().to_vec(), &h).await?;
        }

        assert_eq!(
            tree_direct.root(),
            tree_pruned.root(),
            "Build-then-prune must converge to the same root as a direct build"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_restores_original_root_after_delete_then_reinsert() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let all_keys: Vec<u32> = (0..500).collect();
        let mut original = build_and_flush(&all_keys, &mut storage).await?;

        // Mix of boundary and non-boundary keys
        let test_keys: Vec<u32> = {
            let mut keys = boundary_keys(0..500);
            keys.extend(interior_keys(0..500).into_iter().take(3));
            keys.truncate(6);
            keys
        };

        for &key in &test_keys {
            let mut after_delete = original.delete(&key.to_le_bytes(), &storage).await?;
            for (h, b) in after_delete.flush() {
                storage.store(b.as_ref().to_vec(), &h).await?;
            }

            let mut restored = after_delete
                .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                .await?;
            for (h, b) in restored.flush() {
                storage.store(b.as_ref().to_vec(), &h).await?;
            }

            assert_eq!(
                original.root(),
                restored.root(),
                "Delete then re-insert of key {key} should restore the original root"
            );
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_converges_to_same_root_regardless_of_operation_history() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // History A: insert 0..100 directly
        let tree_a = build_and_flush(&(0..100).collect::<Vec<_>>(), &mut storage).await?;

        // History B: insert 0..200, then delete 100..200
        let mut tree_b = build_and_flush(&(0..200).collect::<Vec<_>>(), &mut storage).await?;
        for i in 100..200u32 {
            tree_b = tree_b.delete(&i.to_le_bytes(), &storage).await?;
        }
        for (h, b) in tree_b.flush() {
            storage.store(b.as_ref().to_vec(), &h).await?;
        }

        assert_eq!(
            tree_a.root(),
            tree_b.root(),
            "Insert-only vs insert-then-delete must converge for the same entry set"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_produces_canonical_tree_after_emptying_a_segment() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // Find a boundary key whose segment contains only one entry
        // (the boundary itself). To guarantee this, look for adjacent
        // boundary keys with no interior keys between them, or just
        // find a boundary that is immediately preceded by another
        // boundary.
        let all_keys: Vec<u32> = (0..2000).collect();
        let boundaries = boundary_keys(0..2000);

        // Sort boundaries by byte order (same order as the tree)
        let mut byte_boundaries: Vec<(u32, [u8; 4])> =
            boundaries.iter().map(|&k| (k, k.to_le_bytes())).collect();
        byte_boundaries.sort_by(|a, b| a.1.cmp(&b.1));

        // Find a boundary that forms a single-entry segment:
        // its predecessor in byte order is also a boundary.
        let mut solo_boundary = None;
        for pair in byte_boundaries.windows(2) {
            let (_, prev_bytes) = pair[0];
            let (curr_u32, curr_bytes) = pair[1];

            // Count entries between prev and curr (exclusive) in byte order
            let entries_between = all_keys
                .iter()
                .filter(|&&k| {
                    let kb = k.to_le_bytes();
                    kb > prev_bytes && kb < curr_bytes
                })
                .count();

            if entries_between == 0 {
                solo_boundary = Some(curr_u32);
                break;
            }
        }

        // If no single-entry segment exists, skip (unlikely with 2000 keys)
        let Some(solo_key) = solo_boundary else {
            return Ok(());
        };

        let mut full_tree = build_and_flush(&all_keys, &mut storage).await?;

        let mut tree_via_delete = full_tree.delete(&solo_key.to_le_bytes(), &storage).await?;
        for (h, b) in tree_via_delete.flush() {
            storage.store(b.as_ref().to_vec(), &h).await?;
        }

        let remaining: Vec<u32> = all_keys
            .iter()
            .copied()
            .filter(|&k| k != solo_key)
            .collect();
        let tree_from_scratch = build_and_flush(&remaining, &mut storage).await?;

        assert_eq!(
            tree_via_delete.root(),
            tree_from_scratch.root(),
            "Deleting sole entry in segment (key {solo_key}) should produce canonical tree"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_produces_canonical_tree_after_deleting_first_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let all_keys: Vec<u32> = (0..1000).collect();

        // The first key in byte-lexicographic order
        let mut sorted: Vec<[u8; 4]> = all_keys.iter().map(|k| k.to_le_bytes()).collect();
        sorted.sort();
        let first_key = sorted[0];
        let first_u32 = u32::from_le_bytes(first_key);

        let mut full_tree = build_and_flush(&all_keys, &mut storage).await?;

        let mut tree_via_delete = full_tree.delete(&first_key, &storage).await?;
        for (h, b) in tree_via_delete.flush() {
            storage.store(b.as_ref().to_vec(), &h).await?;
        }

        let remaining: Vec<u32> = all_keys
            .iter()
            .copied()
            .filter(|&k| k != first_u32)
            .collect();
        let tree_from_scratch = build_and_flush(&remaining, &mut storage).await?;

        assert_eq!(
            tree_via_delete.root(),
            tree_from_scratch.root(),
            "Deleting first entry (key {first_u32}) should produce canonical tree"
        );

        Ok(())
    }

    /// Deleting the last entry (largest key in byte order) must
    /// produce a canonical tree. The rightmost segment is always a
    /// tail, so this verifies tails at the end are left intact.
    #[dialog_common::test]
    async fn it_produces_canonical_tree_after_deleting_last_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let all_keys: Vec<u32> = (0..1000).collect();

        let mut sorted: Vec<[u8; 4]> = all_keys.iter().map(|k| k.to_le_bytes()).collect();
        sorted.sort();
        let last_key = *sorted.last().unwrap();
        let last_u32 = u32::from_le_bytes(last_key);

        let mut full_tree = build_and_flush(&all_keys, &mut storage).await?;

        let mut tree_via_delete = full_tree.delete(&last_key, &storage).await?;
        for (h, b) in tree_via_delete.flush() {
            storage.store(b.as_ref().to_vec(), &h).await?;
        }

        let remaining: Vec<u32> = all_keys
            .iter()
            .copied()
            .filter(|&k| k != last_u32)
            .collect();
        let tree_from_scratch = build_and_flush(&remaining, &mut storage).await?;

        assert_eq!(
            tree_via_delete.root(),
            tree_from_scratch.root(),
            "Deleting last entry (key {last_u32}) should produce canonical tree"
        );

        Ok(())
    }
}
