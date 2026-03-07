use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use async_stream::try_stream;
use dialog_common::{Blake3Hash, NULL_BLAKE3_HASH};
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
    ArchivedNodeBody, DialogSearchTreeError, Entry, Key, Link, Node, SymmetryWith, Value,
    into_owned,
};

/// A traversal mechanism for walking through a tree structure.
pub struct TreeWalker<Key, Value, GetNode>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    GetNode: AsyncFn(&Blake3Hash) -> Result<Node<Key, Value>, DialogSearchTreeError>,
{
    root: Blake3Hash,
    get_node: GetNode,

    key: PhantomData<Key>,
    value: PhantomData<Value>,
}

impl<Key, Value, GetNode> TreeWalker<Key, Value, GetNode>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key::Archived: for<'b> CheckBytes<
        Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Key::Archived: Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
        Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Value::Archived: Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    GetNode: AsyncFn(&Blake3Hash) -> Result<Node<Key, Value>, DialogSearchTreeError>,
{
    /// Creates a new [`TreeWalker`] with the given root hash and node fetcher.
    pub fn new(root: Blake3Hash, get_node: GetNode) -> Self {
        Self {
            root,
            get_node,
            key: PhantomData,
            value: PhantomData,
        }
    }

    /// Returns a stream of entries within the specified key range.
    pub fn stream<'a, R>(
        self,
        range: R,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + 'a
    where
        Self: 'a,
        R: RangeBounds<Key> + 'a,
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
            let Some(search_result) = self.search(&start_key).await? else {
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
                                let next_node = (self.get_node)(<&Blake3Hash>::from(&link.node)).await?;
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
                                break;
                            }
                        }
                    },
                }
            }
        }
    }

    /// Searches for the leaf segment that would contain the given key.
    pub async fn search(
        &self,
        key: &Key,
    ) -> Result<Option<SearchResult<Key, Value>>, DialogSearchTreeError> {
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

            let node = (self.get_node)(&next_node).await?;

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
                    return Ok(Some(SearchResult { leaf: node, path }));
                }
            }
        }
    }
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
