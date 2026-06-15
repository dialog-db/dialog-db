//! Tree construction and modification logic.
//!
//! This module contains [`TreeShaper`], which encapsulates all the logic for
//! building and modifying tree structures. By separating mutation operations
//! from the read-only [`Tree`] interface, we achieve clearer separation of
//! concerns and make the codebase more maintainable.

use std::marker::PhantomData;

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
    Buffer, Delta, DialogSearchTreeError, Distribution, Entry, Geometric, Key, Link, Node,
    NodeBody, Rank, RightNeighbor, SearchResult, Segment, SymmetryWith, TreeLayer, Value,
    into_owned,
};

/// A collection of nodes with their ranks.
type RankedNodes<Key, Value> = NonEmpty<(Node<Key, Value>, Rank)>;

/// The rank threshold for grouping entries into leaf segments (level 0 of
/// the tree). Every key has a rank of at least 1; an entry whose key's rank
/// exceeds this threshold ends the segment it belongs to (it is a segment
/// *boundary*).
const BOTTOM_RANK: Rank = 1;

/// The rank threshold for grouping leaf segments (level 0) into the first
/// level of index nodes (level 1).
///
/// Each level of the tree uses a threshold one higher than the level below
/// it: level `L` is built by grouping level `L - 1` nodes, ending a group
/// whenever a node's rank exceeds `BOTTOM_RANK + L`. Walks that rebuild the
/// tree from the leaves up start at this threshold and increment it once per
/// level (see `TreeShaper::merge_with_path`).
const FIRST_INDEX_RANK: Rank = BOTTOM_RANK + 1;

/// The stateful side-effects of tree mutations are compartmentalized
/// to a MutationContext. Key ranking is intermediated by the MutationContext
/// so that ranks may be cached by key (avoiding redundant hashing and rank
/// computation). Ideally this rank cache would be kept at a higher layer of
/// abstraction so that it could be shared across mutations, but holding the
/// cache in the mutation context is a low-hanging fruit.
struct MutationContext<Key, D>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + crate::SymmetryWith<Key> + Ord,
    D: Distribution,
{
    delta: Delta<Blake3Hash, Buffer>,
    rank_cache: HashMap<Key, Rank>,
    distribution: PhantomData<D>,
}

impl<Key, D> MutationContext<Key, D>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + crate::SymmetryWith<Key> + Ord,
    D: Distribution,
{
    pub fn new(delta: Delta<Blake3Hash, Buffer>) -> Self {
        Self {
            delta,
            rank_cache: HashMap::new(),
            distribution: PhantomData,
        }
    }

    pub fn rank(&mut self, key: &Key) -> Rank {
        if let Some(rank) = self.rank_cache.get(key) {
            *rank
        } else {
            let rank = D::rank(key.as_ref());
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

impl<Key, D> From<MutationContext<Key, D>> for Delta<Blake3Hash, Buffer>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + crate::SymmetryWith<Key> + Ord,
    D: Distribution,
{
    fn from(value: MutationContext<Key, D>) -> Self {
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
pub struct TreeShaper<Key, Value, D = Geometric>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + crate::SymmetryWith<Key> + Ord,
    Value: self::Value,
    D: Distribution,
{
    root: Blake3Hash,
    delta: Delta<Blake3Hash, Buffer>,
    key: PhantomData<Key>,
    value: PhantomData<Value>,
    distribution: PhantomData<D>,
}

impl<Key, Value, D> TreeShaper<Key, Value, D>
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
    D: Distribution,
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
            distribution: PhantomData,
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
    /// 2. **Boundary-delete overflow**: The deleted entry was the segment's
    ///    last entry (its boundary) and a right-adjacent segment exists. The
    ///    right-adjacent segment adopts the orphans (possibly none, when the
    ///    deletion emptied the segment): the combined entries are
    ///    redistributed and the two affected subtrees are stitched back
    ///    together at their lowest common ancestor. Requires the
    ///    `right_neighbor` prefetch on the search result. See
    ///    `Self::let_right_neighbor_adopt_orphans`.
    ///
    /// 3. **Rightmost segment becomes empty**: There is no right-adjacent
    ///    segment to adopt anything, so the empty segment is removed from its
    ///    parent and the tree is rebuilt upward.
    ///
    /// 4. **Ordinary shrink**: The segment still has entries after removal and
    ///    there is no overflow. The remaining entries are redistributed using
    ///    their intrinsic ranks and the tree path is rebuilt.
    ///
    /// A deletion can leave a stale chain of single-child index nodes at the
    /// root (when the deleted key's rank was what demanded those levels);
    /// callers must collapse it, see `Tree::collapse_root_chain`.
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

        // Boundary-delete with a right-adjacent segment triggers overflow:
        // the dissolved boundary demands fusion with the right-adjacent
        // subtree so the resulting tree matches a from-scratch build. This
        // applies even when the deletion emptied the segment (zero orphans):
        // the boundary may have terminated index groups at several levels,
        // and all of them fuse with their right-adjacent peers.
        if deleted_boundary && let Some(right_neighbor) = search_result.right_neighbor {
            let main_leaf_hash = search_result.leaf.hash().clone();
            return self.let_right_neighbor_adopt_orphans(
                segment.entries,
                main_leaf_hash,
                search_result.path,
                right_neighbor,
            );
        }

        // The adoption path above did not apply, which leaves three cases:
        // a non-boundary delete (the segment's boundary still stands, so the
        // remaining entries redistribute in place), a boundary delete with no
        // right-adjacent segment (the segment was the rightmost in the tree,
        // so there is no neighbor to adopt the remaining entries and they
        // redistribute in place), or a delete that emptied the rightmost
        // segment entirely (the segment itself must be removed from its
        // parent).
        match NonEmpty::from_vec(segment.entries) {
            Some(entries) => self.distribute(entries, Some(search_result)),
            None => {
                let main_leaf_hash = search_result.leaf.hash().clone();
                self.remove_from_path(main_leaf_hash, search_result.path)
            }
        }
    }

    /// Resolves a boundary-delete overflow by letting the prefetched
    /// right-adjacent segment adopt the orphaned entries.
    ///
    /// See case (2) in [`Self::delete`] for the high-level contract. The
    /// `orphans` list may be empty (the deletion emptied the segment); the
    /// fusion below the lowest common ancestor is required regardless,
    /// because it is driven by the dissolved boundary, not by the orphans.
    ///
    /// Throughout this method "LCA" stands for *lowest common ancestor*: the
    /// deepest node on the search path that is an ancestor of both the
    /// modified leaf and its right-adjacent leaf. It is the node where the
    /// two descents fork into different children; below it they run through
    /// disjoint subtrees.
    ///
    /// Consider deleting `x`, a rank-3 key. Because every index inherits its
    /// upper bound from its last child, `x` is simultaneously the boundary of
    /// its segment *and* the upper bound of every index on the main descent
    /// below the LCA (`D` here). The right-adjacent leaf `[y]` is reached by
    /// descending leftmost from the LCA's next child (`E`):
    ///
    /// ```text
    ///                     ( LCA )
    ///                    /   |   \
    ///                  D     E     F        index nodes
    ///                /  \    |  \
    ///           [u v] [w x] [y] [z ...]     leaf segments
    ///                    ^   ^
    ///                    |   right-adjacent leaf
    ///                    main descent target: x is deleted, w is orphaned
    /// ```
    ///
    /// Deleting `x` dissolves that boundary at every one of those levels at
    /// once. A from-scratch build of the remaining keys would place the
    /// orphan `w` at the start of the *next* segment, and would never split
    /// `D` from `E` because no boundary separates their keys anymore. The
    /// rebuilt region must therefore fuse pairwise, level by level, into a
    /// single node at each level below the LCA:
    ///
    /// ```text
    ///                     ( LCA' )
    ///                     /     \
    ///                   DE       F
    ///                 /  |  \
    ///            [u v] [w y] [z ...]
    /// ```
    ///
    /// By construction of the two descents (the main descent follows the
    /// boundary rightward at every level, so the boundary's host is always
    /// the last child; the right-adjacent descent takes leftmost children)
    /// we can rely on:
    /// - `!main_layer.has_right_siblings()` for every layer below the LCA, and
    /// - `!right_layer.has_left_siblings()` for every layer below the LCA.
    ///
    /// The fold that produces each fused node (`[w y]`, then `DE`) is
    /// therefore just
    /// `[main_layer.left_siblings() | unified | right_layer.right_siblings()]`.
    ///
    /// The tree above the LCA is unaffected and handed off to the standard
    /// merge routine.
    fn let_right_neighbor_adopt_orphans(
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

        let mut context = MutationContext::<Key, D>::new(self.delta.branch());

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
        // level the boundary that separated them has dissolved, so the
        // children of the main-descent node and of the right-descent node
        // flatten into one unified index node (`D` and `E` becoming `DE` in
        // the diagram above).
        //
        // The unified nodes start out as leaf segments (level 0), so the
        // first fused parent is built with the level-1 threshold; each
        // further level up increments the threshold by one.
        let mut level_minimum_rank: Rank = FIRST_INDEX_RANK;
        for (main_layer, right_layer) in main_below
            .into_iter()
            .rev()
            .zip(diverged_path.into_iter().rev())
        {
            context.delta().remove(main_layer.host.hash());
            context.delta().remove(right_layer.host.hash());

            debug_assert!(
                !main_layer.has_right_siblings(),
                "main descent follows the rightmost path; no right siblings below LCA"
            );
            debug_assert!(
                !right_layer.has_left_siblings(),
                "right-adjacent descent is leftmost; no left siblings below LCA"
            );

            let unified_links = promote_to_ranked_links(unified, &mut context)?;

            // The unified subtree sits between the main descent's left siblings
            // and the right descent's right siblings (the asserts above pin that
            // those are the only siblings below the LCA).
            let combined_links = Self::splice_siblings(
                unified_links,
                main_layer.left_siblings()?,
                right_layer.right_siblings()?,
                &mut context,
            )?;
            unified = Self::collect::<Link<_>>(combined_links, level_minimum_rank)?;
            level_minimum_rank += 1;
        }

        // At the LCA, the main descent took child `lca_layer.index` and the
        // right descent took the next child (`index + 1`); both have been
        // subsumed by the unified subtree. The LCA's left siblings are the
        // children before the main target, and its surviving right siblings are
        // the children after the right target (i.e. skip the subsumed pair).
        context.delta().remove(lca_layer.host.hash());
        let lca_links = &lca_layer.host.as_index()?.links;
        let lca_left = NonEmpty::from_vec(
            lca_links[..lca_layer.index]
                .iter()
                .map(into_owned)
                .collect::<Result<Vec<Link<Key>>, _>>()?,
        );
        let lca_right = NonEmpty::from_vec(
            lca_links[(lca_layer.index + 2).min(lca_links.len())..]
                .iter()
                .map(into_owned)
                .collect::<Result<Vec<Link<Key>>, _>>()?,
        );

        let lca_has_own_siblings = lca_left.is_some() || lca_right.is_some();

        if !lca_has_own_siblings {
            // The LCA's only children were the main- and right-descent targets;
            // both were subsumed by the unified subtree. The LCA node itself
            // disappears from the canonical tree.
            if above_lca.is_empty() {
                // The LCA was the tree root; the unified subtree becomes the
                // new root via the standard merge with nothing left to merge
                // against. This can leave a stale single-child wrapper on
                // top (the merge cannot know that nothing above demands the
                // level); `Tree::collapse_root_chain` strips it.
                return Self::merge_with_path(unified, vec![], context, level_minimum_rank);
            }

            // Promote the unified subtree to the LCA's level so it can slot
            // into the LCA's parent as a replacement child, then continue the
            // normal upward merge.
            let unified_links = promote_to_ranked_links(unified, &mut context)?;
            let promoted = Self::collect::<Link<_>>(unified_links, level_minimum_rank)?;
            return Self::merge_with_path(promoted, above_lca, context, level_minimum_rank + 1);
        }

        // LCA has genuine siblings: rebuild the LCA level around the unified
        // subtree and the preserved siblings, then hand off to the standard
        // merge for everything above it.
        let unified_links = promote_to_ranked_links(unified, &mut context)?;
        let spliced = Self::splice_siblings(unified_links, lca_left, lca_right, &mut context)?;
        let collected = Self::collect::<Link<_>>(spliced, level_minimum_rank)?;

        Self::merge_with_path(collected, above_lca, context, level_minimum_rank + 1)
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
        let mut context = MutationContext::<Key, D>::new(self.delta.branch());
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
        // the level-1 threshold.
        Self::merge_with_path(nodes, search_path, context, FIRST_INDEX_RANK)
    }

    /// Splices the rebuilt children of a level between its unchanged left and
    /// right siblings, ranking the siblings as it goes.
    ///
    /// The siblings are passed already decoded (via [`TreeLayer::left_siblings`]
    /// / [`TreeLayer::right_siblings`], or assembled directly by the overflow
    /// path), so this is the single place the three cases (left only, right only,
    /// both, neither) are handled.
    fn splice_siblings(
        middle: NonEmpty<(Link<Key>, Rank)>,
        left: Option<NonEmpty<Link<Key>>>,
        right: Option<NonEmpty<Link<Key>>>,
        context: &mut MutationContext<Key, D>,
    ) -> Result<NonEmpty<(Link<Key>, Rank)>, DialogSearchTreeError> {
        let left = left.map(|links| into_ranked_links(links, context));
        let right = right.map(|links| into_ranked_links(links, context));
        match (left, right) {
            (None, None) => Ok(middle),
            (Some(left), None) => concat_nonempty(vec![left, middle]),
            (None, Some(right)) => concat_nonempty(vec![middle, right]),
            (Some(left), Some(right)) => concat_nonempty(vec![left, middle, right]),
        }
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
    /// level of the walk. Callers that start from raw leaf segments pass
    /// [`FIRST_INDEX_RANK`]; the overflow path passes a higher value when it
    /// picks up mid-walk.
    ///
    /// This is the shared path-reconstruction logic used by both insert (after
    /// distributing entries) and delete (after modifying a segment).
    fn merge_with_path(
        mut nodes: NonEmpty<(Node<Key, Value>, Rank)>,
        mut search_path: Vec<TreeLayer<Key, Value>>,
        mut context: MutationContext<Key, D>,
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
                        // Rebuilding this level needs the host's other children,
                        // so decode them from the host now.
                        let left = layer.left_siblings()?;
                        let right = layer.right_siblings()?;
                        Self::splice_siblings(ranked_links, left, right, &mut context)?
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
    /// with no entries. It removes the segment from its parent (the deepest
    /// layer of the path), regroups the parent's surviving children, and
    /// rebuilds the tree upward.
    ///
    /// If the removed segment was the only child (no siblings), the removal
    /// cascades: the now-childless parent is removed from *its* parent, and
    /// so on toward the root. Because the path is ordered root-first, the
    /// cascade pops layers from the back, and it tracks how many levels it
    /// has climbed so the survivors are regrouped with the rank threshold of
    /// the level actually being rebuilt. If the cascade consumes the whole
    /// path, the tree has become empty.
    fn remove_from_path(
        self,
        leaf_hash: Blake3Hash,
        mut path: Vec<TreeLayer<Key, Value>>,
    ) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError> {
        use dialog_common::NULL_BLAKE3_HASH;

        let mut context = MutationContext::<Key, D>::new(self.delta.branch());
        context.delta().remove(&leaf_hash);

        // The level of the removed child at the layer currently being
        // popped; the leaf's parent hosts level-0 children (segments).
        let mut level: Rank = 0;

        while let Some(layer) = path.pop() {
            context.delta().remove(layer.host.hash());

            // Collect left and right siblings, excluding the removed child.
            let mut links = Vec::new();
            if let Some(left_siblings) = layer.left_siblings()? {
                links.extend(left_siblings);
            }
            if let Some(right_siblings) = layer.right_siblings()? {
                links.extend(right_siblings);
            }

            let Some(links) = NonEmpty::from_vec(links) else {
                // The removed child was the layer's only child; the layer's
                // host dissolves and the removal cascades one level up.
                level += 1;
                continue;
            };

            // The survivors are level-`level` nodes; regroup them into
            // parents one level up and continue the standard merge from
            // there. Both thresholds follow the "level `L` is built with
            // threshold `L + 1`" rule.
            let ranked_links = into_ranked_links(links, &mut context);
            let nodes = Self::collect::<Link<_>>(ranked_links, FIRST_INDEX_RANK + level)?;
            return Self::merge_with_path(nodes, path, context, FIRST_INDEX_RANK + level + 1);
        }

        // Every layer cascaded away: the tree is empty.
        Ok((NULL_BLAKE3_HASH.clone(), context.take_delta()))
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
        let mut output: Vec<(Node<Key, Value>, Rank)> = vec![];
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
fn promote_to_ranked_links<Key, Value, D>(
    nodes: NonEmpty<(Node<Key, Value>, Rank)>,
    context: &mut MutationContext<Key, D>,
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
    D: Distribution,
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
fn into_ranked_links<Key, D>(
    links: NonEmpty<Link<Key>>,
    context: &mut MutationContext<Key, D>,
) -> NonEmpty<(Link<Key>, Rank)>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + crate::SymmetryWith<Key> + Ord,
    D: Distribution,
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
        for buffer in tree.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
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
            for buffer in tree_via_delete.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
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
            for buffer in tree_via_delete.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
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
        for buffer in tree_pruned.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
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
            for buffer in after_delete.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }

            let mut restored = after_delete
                .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                .await?;
            for buffer in restored.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
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
        for buffer in tree_b.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
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
        for buffer in tree_via_delete.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
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
        for buffer in tree_via_delete.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
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
        for buffer in tree_via_delete.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
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
