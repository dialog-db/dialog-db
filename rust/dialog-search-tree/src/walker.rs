use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use async_stream::try_stream;
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
use futures_core::Stream;
use nonempty::NonEmpty;
use rkyv::{
    Deserialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    Accessor, ArchivedNodeBody, DialogSearchTreeError, Entry, Key, Link, Node, SymmetryWith, Value,
    into_owned,
};

/// A traversal mechanism for walking through a tree structure.
pub struct TreeWalker<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + ConditionalSync,
{
    root: Blake3Hash,

    key: PhantomData<Key>,
    value: PhantomData<Value>,
}

impl<Key, Value> TreeWalker<Key, Value>
where
    Key: self::Key
        + ConditionalSync
        + 'static
        + PartialOrd<Key::Archived>
        + PartialEq<Key::Archived>
        + std::fmt::Debug,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + ConditionalSync
        + for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
{
    /// Creates a new [`TreeWalker`] with the given root hash and node fetcher.
    pub fn new(root: Blake3Hash) -> Self {
        Self {
            root,

            key: PhantomData,
            value: PhantomData,
        }
    }

    /// Returns a stream of entries within the specified key range.
    pub fn stream<R, Backend>(
        self,
        range: R,
        accessor: Accessor<Backend>,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + ConditionalSend + 'static
    where
        R: RangeBounds<Key> + ConditionalSend + 'static,
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync
            + 'static,
    {
        try_stream! {
            // Get the start key. Included/Excluded ranges are identical here,
            // the check if key is in range is below, and this will at most read
            // one unnecessary segment iff `Bound::Excluded(K)` and `K` is a
            // boundary node.
            let start_key = match range.start_bound() {
                Bound::Included(start) => start.clone(),
                Bound::Excluded(start) => start.clone(),
                Bound::Unbounded => {
                    return;
                },
            };
            let Some(search_result) = self
                .search(&start_key, accessor.clone(), SearchOptions::default())
                .await?
            else {
                return;
            };
            let mut search_path = search_result.into_indexed()?;
            let mut entered_range = false;

            while let Some((node, maybe_index)) = search_path.pop() {
                match node.body()? {
                    ArchivedNodeBody::Index(index) => {
                        let child_index = if let Some(index) = maybe_index {
                            index + 1
                        } else {
                            0
                        };

                        match index.links.get(child_index) {
                            Some(link) => {
                                let next_node = accessor.get_node(<&Blake3Hash>::from(&link.node)).await?;
                                search_path.push((node, Some(child_index)));
                                search_path.push((next_node, None));
                            }
                            None => {
                                // Parent needs to check next sibling
                                continue;
                            }
                        }

                    },
                    ArchivedNodeBody::Segment(segment) => {
                        for entry in segment.entries.iter() {
                            if range.contains(&entry.key) {
                                entered_range = true;
                                yield into_owned(entry)?;
                            } else if entered_range {
                                // We've surpassed the range; abort.
                                return;
                            }
                        }
                    },
                }
            }
        }
    }

    /// Searches for the leaf segment that would contain the given key.
    pub async fn search<Backend>(
        &self,
        key: &Key,
        accessor: Accessor<Backend>,
        options: SearchOptions,
    ) -> Result<Option<SearchResult<Key, Value>>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync
            + 'static,
    {
        if &self.root == NULL_BLAKE3_HASH {
            return Ok(None);
        }

        // Depth scales logarithmically with number of entries, so 32 is truly
        // overkill here
        const MAXIMUM_TREE_DEPTH: usize = 32;

        let mut next_node = self.root.clone();
        let mut path = vec![];

        loop {
            if path.len() > MAXIMUM_TREE_DEPTH {
                return Err(DialogSearchTreeError::Operation(format!(
                    "Tree depth exceded the soft maximum ({MAXIMUM_TREE_DEPTH})"
                )));
            }

            let node = accessor.get_node(&next_node).await?;

            match node.body()? {
                ArchivedNodeBody::Index(index) => {
                    let mut left = vec![];
                    let mut right = vec![];
                    let mut next_descendant = None;

                    for link in index.links.iter() {
                        if next_descendant.is_some() {
                            right.push(link);
                        } else if key <= &link.upper_bound {
                            next_descendant = Some(&link.node);
                        } else {
                            left.push(link);
                        }
                    }

                    if next_descendant.is_none() {
                        let last_candidate = left.pop().ok_or(DialogSearchTreeError::Operation(
                            "No upper bound found".into(),
                        ))?;

                        next_descendant = Some(&last_candidate.node);
                    }

                    path.push(TreeLayer {
                        host: node.clone(),
                        left_siblings: NonEmpty::from_vec(
                            left.into_iter()
                                .map(into_owned)
                                .collect::<Result<_, DialogSearchTreeError>>()?,
                        ),
                        right_siblings: NonEmpty::from_vec(
                            right
                                .into_iter()
                                .map(into_owned)
                                .collect::<Result<_, DialogSearchTreeError>>()?,
                        ),
                    });

                    next_node = next_descendant
                        .ok_or_else(|| {
                            DialogSearchTreeError::Operation("Next node not found".into())
                        })
                        .and_then(into_owned)?;
                }
                ArchivedNodeBody::Segment(_) => {
                    let right_neighbor = if options.prefetch_right_neighbor {
                        prefetch_right_neighbor(key, &node, &path, accessor).await?
                    } else {
                        None
                    };
                    return Ok(Some(SearchResult {
                        leaf: node,
                        path,
                        right_neighbor,
                    }));
                }
            }
        }
    }
}

/// Walks the narrow "overflow" path for [`RightNeighbor`] prefetching.
///
/// Called when [`TreeWalker::search`] lands on a leaf whose last entry matches
/// the searched key — a necessary condition for boundary-delete overflow. If
/// the search path contains any layer with a right sibling, we follow the
/// leftmost descent from the first such sibling down to the next leaf. This
/// lets [`TreeShaper::delete`] absorb orphan entries into that leaf in one
/// pass when the deleted entry turns out to be the segment boundary.
///
/// Returns `None` when either the key is not the leaf's last entry or the leaf
/// has no right-adjacent neighbor (the leaf is the rightmost segment in the
/// tree).
async fn prefetch_right_neighbor<Key, Value, Backend>(
    key: &Key,
    leaf: &Node<Key, Value>,
    path: &[TreeLayer<Key, Value>],
    accessor: Accessor<Backend>,
) -> Result<Option<RightNeighbor<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key
        + ConditionalSync
        + 'static
        + PartialOrd<Key::Archived>
        + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + ConditionalSync
        + for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + 'static,
{
    // Only prefetch when the caller's key matches the leaf's last entry;
    // boundary-delete overflow can't happen otherwise.
    let Some(leaf_upper_bound) = leaf.upper_bound()? else {
        return Ok(None);
    };
    if key != leaf_upper_bound {
        return Ok(None);
    }

    // Find the deepest ancestor with a right sibling: that's the lowest common
    // ancestor of the main descent and the right-adjacent descent.
    let Some(lca_depth) = path
        .iter()
        .rposition(|layer| layer.right_siblings.is_some())
    else {
        // The leaf is the rightmost segment in the tree; nothing to prefetch.
        return Ok(None);
    };

    // Safe to unwrap: `rposition` only matches layers whose right_siblings are
    // `Some(_)`.
    let first_right_link = &path[lca_depth]
        .right_siblings
        .as_ref()
        .expect("rposition guarantees right_siblings is Some")
        .head;
    let mut next_hash: Blake3Hash = first_right_link.node.clone();
    let mut diverged_path: Vec<TreeLayer<Key, Value>> = Vec::new();

    let right_leaf = loop {
        let node: Node<Key, Value> = accessor.get_node(&next_hash).await?;
        match node.body()? {
            ArchivedNodeBody::Index(index) => {
                let mut links = index.links.iter();
                let first = links.next().ok_or_else(|| {
                    DialogSearchTreeError::Node(
                        "Empty index node during right-neighbor descent".into(),
                    )
                })?;
                let child_hash: Blake3Hash = into_owned(&first.node)?;
                let rest: Vec<Link<Key>> = links
                    .map(into_owned)
                    .collect::<Result<_, DialogSearchTreeError>>()?;

                diverged_path.push(TreeLayer {
                    host: node.clone(),
                    left_siblings: None,
                    right_siblings: NonEmpty::from_vec(rest),
                });
                next_hash = child_hash;
            }
            ArchivedNodeBody::Segment(_) => break node,
        }
    };

    Ok(Some(RightNeighbor {
        lca_depth,
        diverged_path,
        leaf: right_leaf,
    }))
}

/// Options controlling the behavior of [`TreeWalker::search`].
///
/// `prefetch_right_neighbor` is only consumed by [`TreeShaper::delete`] to
/// resolve boundary-delete overflow. All other call sites (reads, inserts,
/// range streams) should leave it at its default of `false` to avoid the extra
/// leftmost descent that the prefetch can trigger.
#[derive(Debug, Default, Clone, Copy)]
pub struct SearchOptions {
    /// When `true`, [`TreeWalker::search`] will additionally descend to the
    /// leaf immediately right-adjacent to the found leaf when the searched key
    /// matches that leaf's last entry, populating
    /// [`SearchResult::right_neighbor`].
    pub prefetch_right_neighbor: bool,
}

/// A layer in the tree traversal path, containing a node and its sibling links.
pub struct TreeLayer<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
{
    // pub host: Blake3Hash,
    /// The node at this layer of the tree.
    pub host: Node<Key, Value>,
    /// Links to sibling nodes to the left of the current path.
    pub left_siblings: Option<NonEmpty<Link<Key>>>,
    /// Links to sibling nodes to the right of the current path.
    pub right_siblings: Option<NonEmpty<Link<Key>>>,
}

/// The path taken from the root to a leaf during a tree search.
pub type SearchPath<Key, Value> = Vec<TreeLayer<Key, Value>>;

/// An indexed path with nodes and their child indices.
pub type IndexedPath<Key, Value> = Vec<(Node<Key, Value>, Option<usize>)>;

/// The result of a tree search, containing the leaf node and the path taken to
/// reach it.
pub struct SearchResult<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// The leaf node found by the search.
    pub leaf: Node<Key, Value>,
    /// The path from root to leaf.
    pub path: SearchPath<Key, Value>,
    /// Prefetched right-adjacent segment, populated when the searched key
    /// matched the leaf's last entry and a right neighbor exists. Used by
    /// [`TreeShaper::delete`] to resolve boundary-delete overflow in one pass.
    pub right_neighbor: Option<RightNeighbor<Key, Value>>,
}

/// Prefetched information about the leaf segment immediately to the right of a
/// [`SearchResult::leaf`].
///
/// This is populated by [`TreeWalker::search`] only when the search key lands
/// on the main leaf's last entry (a boundary-delete candidate) and a
/// right-adjacent leaf exists. Its shape captures where the right-adjacent
/// descent diverges from the main descent so [`TreeShaper::delete`] can rebuild
/// both subtrees and stitch them together at the lowest common ancestor.
///
/// For the common "same-parent" overflow case (the right-adjacent leaf shares
/// a parent with the main leaf), `lca_depth == SearchResult.path.len() - 1`
/// and `diverged_path` is empty. For cross-parent overflow, `lca_depth` points
/// deeper in the shared ancestor chain and `diverged_path` records the
/// leftmost descent from there down to `leaf`'s parent.
pub struct RightNeighbor<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Depth in the main search path at which the right-adjacent descent
    /// diverges. Main and right-adjacent descents share hosts at depths
    /// `0..=lca_depth` (this depth's host is the same node in both
    /// descents, but they descend to different children).
    pub lca_depth: usize,
    /// Tree layers traversed during the leftmost descent from the first right
    /// sibling at `lca_depth` down to `leaf`'s parent. Empty when the main
    /// leaf and the right-adjacent leaf share a parent.
    pub diverged_path: Vec<TreeLayer<Key, Value>>,
    /// The right-adjacent leaf segment.
    pub leaf: Node<Key, Value>,
}

impl<Key, Value> SearchResult<Key, Value>
where
    Key: self::Key,
    Key::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>
        + PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Converts this search result into a path with child indices.
    pub fn into_indexed(mut self) -> Result<IndexedPath<Key, Value>, DialogSearchTreeError> {
        let mut path = Vec::new();
        let mut leaf = self.leaf;

        path.push((leaf.clone(), None));

        while let Some(layer) = self.path.pop() {
            let Some(leaf_upper_bound) = leaf.upper_bound()? else {
                return Err(DialogSearchTreeError::Node(
                    "Could not discover child's upper bound".to_string(),
                ));
            };
            let Some(index) = layer.host.get_child_index(leaf_upper_bound)? else {
                return Err(DialogSearchTreeError::Node(
                    "Could not find node's index relative to parent".to_string(),
                ));
            };

            leaf = layer.host;
            path.push((leaf.clone(), Some(index)));
        }

        path.reverse();

        Ok(path)
    }
}
