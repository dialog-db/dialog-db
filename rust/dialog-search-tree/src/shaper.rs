//! Tree construction and modification logic.
//!
//! This module contains [`TreeShaper`], which encapsulates all the logic for
//! building and modifying tree structures. By separating mutation operations
//! from the read-only [`Tree`] interface, we achieve clearer separation of
//! concerns and make the codebase more maintainable.

use std::marker::PhantomData;

use dialog_common::Blake3Hash;
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
    Buffer, Delta, DialogSearchTreeError, Entry, Key, Link, Node, NodeBody, Rank, SearchResult,
    Segment, SymmetryWith, TreeLayer, Value, distribution, into_owned,
};

/// A collection of nodes with their ranks.
type RankedNodes<Key, Value> = NonEmpty<(Node<Key, Value>, Rank)>;

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
    /// 4. Ranks all entries and distributes them into new nodes
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
        let (entries, search_result) = match search_result {
            Some(search_result) => {
                let key = new_entry.key.to_owned();
                let segment = search_result.leaf.as_segment()?;

                // Extract and modify entries
                let mut entries: Vec<Entry<Key, Value>> =
                    into_owned::<Segment<Key, Value>>(segment)?.entries;

                match entries.binary_search_by(|probe| probe.key.cmp(&key)) {
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
            None => {
                // Empty tree; just a single entry
                (NonEmpty::singleton(new_entry), None)
            }
        };

        // Rank the entries and distribute
        let next_entries = entries
            .into_iter()
            .map(|entry| {
                let rank = distribution::geometric::rank(&Blake3Hash::hash(entry.key.as_ref()));
                (entry, rank)
            })
            .collect::<Vec<_>>();

        let Some(next_entries) = NonEmpty::from_vec(next_entries) else {
            return Err(DialogSearchTreeError::Operation(
                "Insertion resulted in empty set of entries".into(),
            ));
        };

        self.distribute(next_entries, search_result)
    }

    /// Removes an entry from the tree, returning the new root hash and delta.
    ///
    /// This method takes a search result pointing to the leaf segment
    /// containing the key to remove. It handles three cases:
    ///
    /// 1. **Key doesn't exist in segment**: Returns the current root unchanged
    ///    with the original delta (no-op).
    ///
    /// 2. **Segment still has entries after removal**: Creates a new segment
    ///    with the remaining entries (no redistribution needed since ranks
    ///    haven't changed), and updates the tree path with new hashes.
    ///
    /// 3. **Segment becomes empty**: Removes the empty segment by merging its
    ///    siblings at the parent level, then rebuilds the tree upward.
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

        let mut segment = into_owned::<Segment<Key, Value>>(segment)?;
        segment.entries.remove(removal_index);

        if !segment.entries.is_empty() {
            // Still have entries; create new segment and merge up the path
            let new_segment_node = Node::<Key, Value>::new(Buffer::from(
                NodeBody::try_from(segment.entries)?.as_bytes()?,
            ));

            let mut delta = self.delta.branch();
            delta.subtract(search_result.leaf.hash());

            Self::merge_with_path(
                NonEmpty::singleton((new_segment_node, 1)),
                search_result.path,
                delta,
            )
        } else {
            // Segment is now empty; remove it from parent
            self.remove_from_path(search_result.path)
        }
    }

    /// Distributes children into a new tree structure based on their ranks,
    /// rebuilding the path from leaves to root.
    ///
    /// This is the core method for tree construction and modification. It takes
    /// a collection of ranked children (either entries or links) and organizes
    /// them into a tree hierarchy where node boundaries are determined by the
    /// rank distribution.
    ///
    /// The algorithm proceeds in iterations:
    /// 1. Collect children into nodes where ranks exceed the current minimum
    /// 2. Convert nodes to links and merge with siblings from the search path
    /// 3. Increment the minimum rank and repeat until a single root is formed
    ///
    /// When a search result is provided, the method reconstructs only the
    /// affected path through the tree, merging new nodes with unchanged sibling
    /// references. This enables efficient structural sharing where unmodified
    /// portions of the tree remain unchanged.
    ///
    /// Returns the new root hash and a delta containing all newly created
    /// nodes.
    fn distribute<Child>(
        self,
        children: NonEmpty<(Child, Rank)>,
        search_result: Option<SearchResult<Key, Value>>,
    ) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError>
    where
        NodeBody<Key, Value>: TryFrom<Vec<Child>, Error = DialogSearchTreeError>,
    {
        let nodes = Self::collect(children, 1)?;

        let mut delta = self.delta.branch();
        let search_path = if let Some(search_result) = search_result {
            delta.subtract(search_result.leaf.hash());
            search_result.path
        } else {
            vec![]
        };

        Self::merge_with_path(nodes, search_path, delta)
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
    /// This is the shared path-reconstruction logic used by both insert (after
    /// distributing entries) and delete (after modifying a segment).
    pub fn merge_with_path(
        mut nodes: NonEmpty<(Node<Key, Value>, Rank)>,
        mut search_path: Vec<TreeLayer<Key, Value>>,
        mut delta: Delta<Blake3Hash, Buffer>,
    ) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError> {
        const MINIMUM_RANK: u32 = 2;
        let mut minimum_rank = MINIMUM_RANK;

        loop {
            let links = {
                delta.add_all(
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
                        delta.subtract(layer.host.hash());
                        // TBD if we must recompute rank for siblings references
                        // when building up the tree. Attempt to try setting
                        // rank to `0` for references outside of the modified
                        // path.
                        let ranked_left_siblings = layer.left_siblings.map(into_ranked_links);
                        let ranked_right_siblings = layer.right_siblings.map(into_ranked_links);

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

            nodes = Self::collect::<Link<_>>(links, minimum_rank)?;

            if search_path.is_empty() && nodes.len() == 1 {
                break;
            }

            minimum_rank += 1;
        }

        delta.add_all(
            nodes
                .iter()
                .map(|(node, _)| (node.hash().clone(), node.buffer().clone())),
        );

        Ok((nodes.head.0.hash().to_owned(), delta))
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
        let delta = self.delta.branch();

        Self::remove_from_path_with_delta(path, delta)
    }

    /// Internal helper for `remove_from_path` that works with a delta directly.
    ///
    /// This allows recursive calls to reuse the same delta without
    /// re-branching.
    fn remove_from_path_with_delta(
        mut path: Vec<TreeLayer<Key, Value>>,
        mut delta: Delta<Blake3Hash, Buffer>,
    ) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError> {
        use dialog_common::NULL_BLAKE3_HASH;

        // If there's no parent, the tree becomes empty
        if path.is_empty() {
            return Ok((NULL_BLAKE3_HASH.clone(), Delta::zero()));
        }

        let layer = path.remove(0); // Take the parent layer

        delta.subtract(layer.host.hash());

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
                Ok((NULL_BLAKE3_HASH.clone(), delta))
            } else {
                // Continue removing up the path with the same delta
                Self::remove_from_path_with_delta(path, delta)
            };
        }

        // We have siblings; rebuild with them
        let ranked_links = links
            .into_iter()
            .map(|link| {
                let rank = distribution::geometric::rank(&link.node);
                (link, rank)
            })
            .collect::<Vec<_>>();

        let Some(ranked_links) = NonEmpty::from_vec(ranked_links) else {
            return Err(DialogSearchTreeError::Node(
                "Unexpectedly empty link list".into(),
            ));
        };

        // Create nodes from the remaining links
        let nodes = Self::collect::<Link<_>>(ranked_links, 1)?;

        // Merge up the remaining path
        Self::merge_with_path(nodes, path, delta)
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

/// Converts a collection of links into ranked links by computing each link's
/// rank from its node hash.
///
/// This helper function is used when merging sibling references during tree
/// reconstruction. The rank is computed using a geometric distribution over the
/// node hash, ensuring consistent rank assignment for the same content.
fn into_ranked_links<Key>(links: NonEmpty<Link<Key>>) -> NonEmpty<(Link<Key>, Rank)> {
    links.map(|link| {
        let rank = distribution::geometric::rank(&link.node);
        (link, rank)
    })
}

/// Concatenates multiple non-empty lists into a single non-empty list.
///
/// This utility function flattens a vector of [`NonEmpty`] collections into a
/// single [`NonEmpty`] collection. Returns an error if the input vector is
/// empty.
///
/// Used during tree reconstruction when merging left siblings, modified nodes,
/// and right siblings into a single collection for the next level of the tree.
///
/// TODO: Improve. Possibly remove NonEmpty as it introduces some overhead
/// compared to index comparison with slices.
fn concat_nonempty<T>(list: Vec<NonEmpty<T>>) -> Result<NonEmpty<T>, DialogSearchTreeError> {
    Ok(NonEmpty::flatten(NonEmpty::from_vec(list).ok_or(
        DialogSearchTreeError::Node("Empty child list".into()),
    )?))
}
