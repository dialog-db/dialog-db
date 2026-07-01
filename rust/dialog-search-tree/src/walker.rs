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
    Accessor, ArchivedNodeBody, DialogSearchTreeError, Entry, Key, Link, PersistentNode,
    SymmetryWith, Value, into_owned,
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
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + ConditionalSend
    where
        R: RangeBounds<Key> + ConditionalSend,
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        try_stream! {
            // Get the start key. Included/Excluded ranges are identical here,
            // the check if key is in range is below, and this will at most read
            // one unnecessary segment iff `Bound::Excluded(K)` and `K` is a
            // boundary node. An unbounded start begins at the leftmost leaf,
            // which searching for the minimum key descends to.
            let start_key = match range.start_bound() {
                Bound::Included(start) => start.clone(),
                Bound::Excluded(start) => start.clone(),
                Bound::Unbounded => <Key as self::Key>::min(),
            };
            let Some(search_result) = self
                .search(&start_key, accessor.clone(), SearchOptions::default())
                .await?
            else {
                return;
            };
            let mut search_path = search_result.into_indexed();
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
            + ConditionalSync,
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
                    // Descend into the first child whose `upper_bound >= key`,
                    // falling back to the last child when the key is above every
                    // bound. `partition_point` counts the children strictly below
                    // `key`, so it lands on that first candidate without scanning
                    // or decoding any sibling.
                    let child_index = index
                        .links
                        .partition_point(|link| &link.upper_bound < key)
                        .min(index.links.len() - 1);

                    next_node = into_owned(&index.links[child_index].node)?;

                    path.push(TreeLayer {
                        host: node.clone(),
                        index: child_index,
                    });
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
/// lets a boundary delete fold orphan entries into that leaf in one
/// pass when the deleted entry turns out to be the segment boundary.
///
/// Returns `None` when either the key is not the leaf's last entry or the leaf
/// has no right-adjacent neighbor (the leaf is the rightmost segment in the
/// tree).
async fn prefetch_right_neighbor<Key, Value, Backend>(
    key: &Key,
    leaf: &PersistentNode<Key, Value>,
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
        + ConditionalSync,
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
    let Some(lca_depth) = path.iter().rposition(|layer| layer.has_right_siblings()) else {
        // The leaf is the rightmost segment in the tree; nothing to prefetch.
        return Ok(None);
    };

    // The right-descent starts at the LCA's first right sibling (the child just
    // past the one the main descent took).
    let lca = &path[lca_depth];
    let lca_links = &lca.host.as_index()?.links;
    let mut next_hash: Blake3Hash = into_owned(&lca_links[lca.index + 1].node)?;
    let mut diverged_path: Vec<TreeLayer<Key, Value>> = Vec::new();

    let right_leaf = loop {
        let node: PersistentNode<Key, Value> = accessor.get_node(&next_hash).await?;
        match node.body()? {
            ArchivedNodeBody::Index(index) => {
                let first = index.links.first().ok_or_else(|| {
                    DialogSearchTreeError::Node(
                        "Empty index node during right-neighbor descent".into(),
                    )
                })?;
                let child_hash: Blake3Hash = into_owned(&first.node)?;

                // The right-adjacent descent is leftmost, so it always takes
                // child 0; the remaining children are its right siblings.
                diverged_path.push(TreeLayer {
                    host: node.clone(),
                    index: 0,
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
/// `prefetch_right_neighbor` is only consumed by a boundary delete to
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

/// A layer in the tree traversal path: the index node descended through and the
/// position of the child the descent took.
///
/// [`TreeWalker::search`] assembles a path of these as the copy-on-write
/// frontier for an update: each layer names a node an update rebuilds and the
/// child slot within it that changes. A layer is cheap to hold: `host` is an
/// [`Arc`]-backed [`Node`] that shares its buffer when cloned, and `index` is a
/// `usize`. The host's other children stay encoded in its buffer; a read leaves
/// them there, and a write decodes the ones it needs on demand through
/// [`left_siblings`](Self::left_siblings) /
/// [`right_siblings`](Self::right_siblings) when it rebuilds the level.
///
/// [`Arc`]: std::sync::Arc
pub struct TreeLayer<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
{
    /// The index node at this layer of the tree.
    pub host: PersistentNode<Key, Value>,
    /// Position within `host.links` of the child the descent followed.
    pub index: usize,
}

impl<Key, Value> TreeLayer<Key, Value>
where
    Key: self::Key + PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Whether the descended child has any sibling to its left. Cheap: a length
    /// comparison, no decoding.
    pub fn has_left_siblings(&self) -> bool {
        self.index > 0
    }

    /// Whether the descended child has any sibling to its right. Cheap: a length
    /// comparison, no decoding.
    pub fn has_right_siblings(&self) -> bool {
        self.host
            .as_index()
            .map(|index| self.index + 1 < index.links.len())
            .unwrap_or(false)
    }

    /// The host's children strictly to the left of the descended child, decoded
    /// to owned links. Materialized on demand: only an update that rebuilds this
    /// level calls it.
    pub fn left_siblings(&self) -> Result<Option<NonEmpty<Link<Key>>>, DialogSearchTreeError> {
        self.siblings(0, self.index)
    }

    /// The host's children strictly to the right of the descended child, decoded
    /// to owned links. Materialized on demand: only an update that rebuilds this
    /// level calls it.
    pub fn right_siblings(&self) -> Result<Option<NonEmpty<Link<Key>>>, DialogSearchTreeError> {
        let links = self.host.as_index()?.links.len();
        self.siblings(self.index + 1, links)
    }

    fn siblings(
        &self,
        start: usize,
        end: usize,
    ) -> Result<Option<NonEmpty<Link<Key>>>, DialogSearchTreeError> {
        let index = self.host.as_index()?;
        let owned = index.links[start..end]
            .iter()
            .map(into_owned)
            .collect::<Result<Vec<Link<Key>>, _>>()?;
        Ok(NonEmpty::from_vec(owned))
    }
}

/// The path taken from the root to a leaf during a tree search.
pub type SearchPath<Key, Value> = Vec<TreeLayer<Key, Value>>;

/// An indexed path with nodes and their child indices.
pub type IndexedPath<Key, Value> = Vec<(PersistentNode<Key, Value>, Option<usize>)>;

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
    pub leaf: PersistentNode<Key, Value>,
    /// The path from root to leaf.
    pub path: SearchPath<Key, Value>,
    /// Prefetched right-adjacent segment, populated when the searched key
    /// matched the leaf's last entry and a right neighbor exists. Used by
    /// a boundary delete to resolve boundary-delete overflow in one pass.
    pub right_neighbor: Option<RightNeighbor<Key, Value>>,
}

/// Prefetched information about the leaf segment immediately to the right of a
/// [`SearchResult::leaf`].
///
/// This is populated by [`TreeWalker::search`] only when the search key lands
/// on the main leaf's last entry (a boundary-delete candidate) and a
/// right-adjacent leaf exists. Its shape captures where the right-adjacent
/// descent diverges from the main descent so a boundary delete can rebuild
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
    pub leaf: PersistentNode<Key, Value>,
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
    /// Converts this search result into a root-to-leaf path of
    /// `(node, child index)` pairs, where the leaf carries `None` and each index
    /// node carries the slot of the child the search descended into.
    pub fn into_indexed(mut self) -> IndexedPath<Key, Value> {
        let mut path = Vec::new();
        path.push((self.leaf, None));

        while let Some(layer) = self.path.pop() {
            path.push((layer.host, Some(layer.index)));
        }

        path.reverse();
        path
    }
}
